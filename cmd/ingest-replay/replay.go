package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
)

type batchReader interface {
	Next() (*componentsv1.LedgerBatch, error)
	Close() error
}

type replayClock interface {
	Now() time.Time
	WaitUntil(context.Context, time.Time) error
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

func (wallClock) WaitUntil(ctx context.Context, target time.Time) error {
	delay := time.Until(target)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type ledgerResult struct {
	LedgerSequence uint32  `json:"ledger_sequence"`
	ScheduledAt    string  `json:"scheduled_at"`
	SentAt         string  `json:"sent_at"`
	AcknowledgedAt string  `json:"acknowledged_at"`
	Replayed       bool    `json:"replayed"`
	RPCLatencyMS   float64 `json:"rpc_latency_ms"`
	ScheduleLagMS  float64 `json:"schedule_lag_ms"`
	ArrivalToAckMS float64 `json:"arrival_to_ack_ms"`
	SLOExempt      bool    `json:"slo_exempt"`
	OverBudget     bool    `json:"over_budget"`
}

type replaySummary struct {
	Success                 bool           `json:"success"`
	Failure                 string         `json:"failure,omitempty"`
	Profile                 string         `json:"profile"`
	FixtureManifest         string         `json:"fixture_manifest"`
	FixtureBatchCount       int            `json:"fixture_batch_count"`
	FixtureOffset           int            `json:"fixture_offset"`
	RequestedCount          int            `json:"requested_count"`
	StartedAt               string         `json:"started_at"`
	FinishedAt              string         `json:"finished_at"`
	ElapsedSeconds          float64        `json:"elapsed_seconds"`
	Cadence                 string         `json:"cadence"`
	Jitter                  string         `json:"jitter"`
	ConfiguredDuration      string         `json:"configured_duration"`
	MaxLatency              string         `json:"max_latency"`
	Seed                    int64          `json:"seed"`
	Sent                    int            `json:"sent"`
	Acknowledged            int            `json:"acknowledged"`
	Replayed                int            `json:"replayed"`
	FirstLedger             uint32         `json:"first_ledger"`
	LastLedger              uint32         `json:"last_ledger"`
	SLOExemptBatches        int            `json:"slo_exempt_batches"`
	OverBudget              int            `json:"over_budget"`
	OverBudgetLedgers       []uint32       `json:"over_budget_ledgers,omitempty"`
	RPCLatency              latencySummary `json:"rpc_latency"`
	ScheduleLag             latencySummary `json:"schedule_lag"`
	ArrivalToAck            latencySummary `json:"arrival_to_ack"`
	RequiredIdleCheckpoints int            `json:"required_idle_checkpoints"`
	ObservedIdleCheckpoints int            `json:"observed_idle_checkpoints"`
}

func executeReplay(ctx context.Context, config replayConfig, fixtureCount int, reader batchReader, sender batchSender, results io.Writer, clock replayClock) (replaySummary, error) {
	started := clock.Now()
	var rpcLatencies, scheduleLags, arrivalLatencies []time.Duration
	summary := replaySummary{
		Profile:                 config.Profile,
		FixtureManifest:         config.Fixtures,
		FixtureBatchCount:       fixtureCount,
		FixtureOffset:           config.Offset,
		RequestedCount:          config.Count,
		StartedAt:               started.UTC().Format(time.RFC3339Nano),
		Cadence:                 config.Cadence.String(),
		Jitter:                  config.Jitter.String(),
		ConfiguredDuration:      config.Duration.String(),
		MaxLatency:              config.MaxLatency.String(),
		Seed:                    config.Seed,
		RequiredIdleCheckpoints: config.RequireCheckpoints,
	}
	finish := func(err error) (replaySummary, error) {
		finished := clock.Now()
		summary.RPCLatency = summarizeDurations(rpcLatencies)
		summary.ScheduleLag = summarizeDurations(scheduleLags)
		summary.ArrivalToAck = summarizeDurations(arrivalLatencies)
		summary.FinishedAt = finished.UTC().Format(time.RFC3339Nano)
		summary.ElapsedSeconds = finished.Sub(started).Seconds()
		if err != nil {
			summary.Failure = err.Error()
		}
		return summary, err
	}

	random := rand.New(rand.NewSource(config.Seed))
	resultsEncoder := json.NewEncoder(results)
	for skipped := 0; skipped < config.Offset; skipped++ {
		if _, err := reader.Next(); errors.Is(err, io.EOF) {
			return finish(fmt.Errorf("fixture ended after %d ledgers, cannot apply offset %d", skipped, config.Offset))
		} else if err != nil {
			return finish(err)
		}
	}
	scheduledAt := started
	for index := 0; ; index++ {
		if config.Count > 0 && index >= config.Count {
			break
		}
		if index > 0 {
			scheduledAt = scheduledAt.Add(nextInterval(config, index, random))
		}
		if config.Count == 0 && config.Duration > 0 && scheduledAt.Sub(started) >= config.Duration {
			break
		}
		if err := clock.WaitUntil(ctx, scheduledAt); err != nil {
			return finish(fmt.Errorf("wait for ledger %d schedule: %w", index, err))
		}
		batch, err := reader.Next()
		if errors.Is(err, io.EOF) {
			if config.Count > 0 && index < config.Count {
				return finish(fmt.Errorf("fixture ended after %d ledgers, requested %d", index, config.Count))
			}
			break
		}
		if err != nil {
			return finish(err)
		}

		sentAt := clock.Now()
		scheduleLag := sentAt.Sub(scheduledAt)
		if scheduleLag < 0 {
			scheduleLag = 0
		}
		ackContext, cancel := context.WithTimeout(ctx, config.AckTimeout)
		ack, rpcLatency, err := sender.Send(ackContext, batch)
		cancel()
		summary.Sent++
		if err != nil {
			return finish(err)
		}
		if ack.LedgerSequence != batch.LedgerSequence {
			return finish(fmt.Errorf("ack mismatch: sent ledger %d, acknowledged %d", batch.LedgerSequence, ack.LedgerSequence))
		}
		acknowledgedAt := sentAt.Add(rpcLatency)
		arrivalLatency := scheduleLag + rpcLatency
		sloExempt := index < config.Burst
		overBudget := !sloExempt && config.MaxLatency > 0 && arrivalLatency > config.MaxLatency
		result := ledgerResult{
			LedgerSequence: batch.LedgerSequence,
			ScheduledAt:    scheduledAt.UTC().Format(time.RFC3339Nano),
			SentAt:         sentAt.UTC().Format(time.RFC3339Nano),
			AcknowledgedAt: acknowledgedAt.UTC().Format(time.RFC3339Nano),
			Replayed:       ack.Replayed,
			RPCLatencyMS:   milliseconds(rpcLatency),
			ScheduleLagMS:  milliseconds(scheduleLag),
			ArrivalToAckMS: milliseconds(arrivalLatency),
			SLOExempt:      sloExempt,
			OverBudget:     overBudget,
		}
		if err := resultsEncoder.Encode(result); err != nil {
			return finish(fmt.Errorf("write per-ledger result: %w", err))
		}

		summary.Acknowledged++
		if summary.Acknowledged == 1 {
			summary.FirstLedger = batch.LedgerSequence
		}
		summary.LastLedger = batch.LedgerSequence
		if ack.Replayed {
			summary.Replayed++
		}
		if sloExempt {
			summary.SLOExemptBatches++
		}
		if overBudget {
			summary.OverBudget++
			summary.OverBudgetLedgers = append(summary.OverBudgetLedgers, batch.LedgerSequence)
			log.Printf("ingest replay over budget ledger %d: arrival-to-ack=%s rpc=%s schedule-lag=%s budget=%s",
				batch.LedgerSequence, arrivalLatency.Round(time.Millisecond), rpcLatency.Round(time.Millisecond), scheduleLag.Round(time.Millisecond), config.MaxLatency)
		}
		rpcLatencies = append(rpcLatencies, rpcLatency)
		scheduleLags = append(scheduleLags, scheduleLag)
		arrivalLatencies = append(arrivalLatencies, arrivalLatency)
		log.Printf("ingest replay acknowledged ledger %d in %s (schedule lag %s, replayed=%t)",
			batch.LedgerSequence, rpcLatency.Round(time.Millisecond), scheduleLag.Round(time.Millisecond), ack.Replayed)
	}
	return finish(nil)
}

func nextInterval(config replayConfig, index int, random *rand.Rand) time.Duration {
	if index < config.Burst {
		return 0
	}
	if config.Jitter == 0 {
		return config.Cadence
	}
	offset := time.Duration((random.Float64()*2 - 1) * float64(config.Jitter))
	return config.Cadence + offset
}
