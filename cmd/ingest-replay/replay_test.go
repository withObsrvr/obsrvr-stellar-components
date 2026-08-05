package main

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
)

func TestExecuteReplayAccountsForScheduleLagAndBurstExemption(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)}
	reader := &sliceBatchReader{batches: []*componentsv1.LedgerBatch{
		{LedgerSequence: 100},
		{LedgerSequence: 101},
		{LedgerSequence: 102},
	}}
	sender := &fakeBatchSender{
		clock:     clock,
		latencies: []time.Duration{500 * time.Millisecond, 500 * time.Millisecond, 100 * time.Millisecond},
	}
	config := replayConfig{
		Fixtures:   "fixture.manifest.json",
		Profile:    "catch-up",
		Cadence:    time.Second,
		MaxLatency: 400 * time.Millisecond,
		AckTimeout: time.Second,
		Count:      3,
		Burst:      2,
		Seed:       1,
	}
	var results bytes.Buffer
	summary, err := executeReplay(context.Background(), config, 3, reader, sender, &results, clock)
	if err != nil {
		t.Fatalf("execute replay: %v", err)
	}
	if summary.Acknowledged != 3 || summary.SLOExemptBatches != 2 || summary.OverBudget != 0 {
		t.Fatalf("acknowledged/exempt/over = %d/%d/%d, want 3/2/0", summary.Acknowledged, summary.SLOExemptBatches, summary.OverBudget)
	}
	if summary.RPCLatency.P95MS != 500 || summary.ArrivalToAck.MaxMS != 1000 {
		t.Fatalf("latency summaries = %+v / %+v", summary.RPCLatency, summary.ArrivalToAck)
	}
	if lines := bytes.Count(results.Bytes(), []byte("\n")); lines != 3 {
		t.Fatalf("result lines = %d, want 3", lines)
	}
}

func TestExecuteReplayGatesArrivalToAckIncludingBacklog(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	reader := &sliceBatchReader{batches: []*componentsv1.LedgerBatch{
		{LedgerSequence: 10},
		{LedgerSequence: 11},
	}}
	sender := &fakeBatchSender{
		clock:     clock,
		latencies: []time.Duration{150 * time.Millisecond, 360 * time.Millisecond},
	}
	config := replayConfig{
		Fixtures:   "fixture.manifest.json",
		Profile:    "custom",
		Cadence:    100 * time.Millisecond,
		MaxLatency: 400 * time.Millisecond,
		AckTimeout: time.Second,
		Count:      2,
		Seed:       1,
	}
	summary, err := executeReplay(context.Background(), config, 2, reader, sender, io.Discard, clock)
	if err != nil {
		t.Fatalf("execute replay: %v", err)
	}
	if summary.OverBudget != 1 || len(summary.OverBudgetLedgers) != 1 || summary.OverBudgetLedgers[0] != 11 {
		t.Fatalf("over-budget summary = %+v", summary)
	}
	if summary.ScheduleLag.MaxMS != 50 || summary.ArrivalToAck.MaxMS != 410 {
		t.Fatalf("lag/arrival = %+v / %+v, want max 50/410", summary.ScheduleLag, summary.ArrivalToAck)
	}
}

func TestExecuteReplayRejectsAckMismatch(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	reader := &sliceBatchReader{batches: []*componentsv1.LedgerBatch{{LedgerSequence: 10}}}
	sender := &fakeBatchSender{clock: clock, ackOffset: 1, latencies: []time.Duration{time.Millisecond}}
	config := replayConfig{Fixtures: "fixture", Profile: "custom", AckTimeout: time.Second, Count: 1}
	_, err := executeReplay(context.Background(), config, 1, reader, sender, io.Discard, clock)
	if err == nil || err.Error() != "ack mismatch: sent ledger 10, acknowledged 11" {
		t.Fatalf("execute replay error = %v, want ack mismatch", err)
	}
}

func TestExecuteReplayStartsAtFixtureOffset(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	reader := &sliceBatchReader{batches: []*componentsv1.LedgerBatch{
		{LedgerSequence: 10},
		{LedgerSequence: 11},
		{LedgerSequence: 12},
	}}
	sender := &fakeBatchSender{clock: clock, latencies: []time.Duration{time.Millisecond}}
	config := replayConfig{Fixtures: "fixture", Profile: "custom", AckTimeout: time.Second, Offset: 2, Count: 1}
	summary, err := executeReplay(context.Background(), config, 3, reader, sender, io.Discard, clock)
	if err != nil {
		t.Fatalf("execute replay: %v", err)
	}
	if summary.FirstLedger != 12 || summary.LastLedger != 12 || summary.FixtureOffset != 2 {
		t.Fatalf("offset summary = %+v", summary)
	}
}

func TestReplaySmokeThirtyLedgers(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	reader := &sliceBatchReader{}
	sender := &fakeBatchSender{clock: clock}
	for ledger := uint32(1000); ledger < 1030; ledger++ {
		reader.batches = append(reader.batches, &componentsv1.LedgerBatch{LedgerSequence: ledger})
		sender.latencies = append(sender.latencies, 25*time.Millisecond)
	}
	config := replayConfig{
		Fixtures:   "ci-smoke.manifest.json",
		Profile:    "custom",
		Cadence:    50 * time.Millisecond,
		Jitter:     5 * time.Millisecond,
		MaxLatency: 400 * time.Millisecond,
		AckTimeout: time.Second,
		Count:      30,
		Seed:       42,
	}
	summary, err := executeReplay(context.Background(), config, 30, reader, sender, io.Discard, clock)
	if err != nil {
		t.Fatalf("execute replay: %v", err)
	}
	if summary.Acknowledged != 30 || summary.OverBudget != 0 || summary.FirstLedger != 1000 || summary.LastLedger != 1029 {
		t.Fatalf("smoke summary = %+v", summary)
	}
}

func TestProfileDefaultsAndOverrides(t *testing.T) {
	config, err := parseConfig([]string{
		"--fixtures=fixture.json",
		"--endpoint=127.0.0.1:9000",
		"--token=test-token",
		"--profile=checkpoint",
		"--metrics-url=http://127.0.0.1:8088/metrics",
		"--jitter=0",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if config.Cadence != 5*time.Second || config.Jitter != 0 || config.Duration != time.Hour || config.RequireCheckpoints != 3 {
		t.Fatalf("checkpoint profile = %+v", config)
	}
	if _, err := parseConfig([]string{
		"--fixtures=fixture.json",
		"--endpoint=127.0.0.1:9000",
		"--token=test-token",
		"--profile=custom",
	}, io.Discard); err == nil {
		t.Fatal("custom profile without cadence succeeded")
	}
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) WaitUntil(ctx context.Context, target time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if target.After(c.now) {
		c.now = target
	}
	return nil
}

type sliceBatchReader struct {
	batches []*componentsv1.LedgerBatch
	index   int
}

func (r *sliceBatchReader) Next() (*componentsv1.LedgerBatch, error) {
	if r.index >= len(r.batches) {
		return nil, io.EOF
	}
	batch := r.batches[r.index]
	r.index++
	return batch, nil
}

func (*sliceBatchReader) Close() error { return nil }

type fakeBatchSender struct {
	clock     *fakeClock
	latencies []time.Duration
	index     int
	ackOffset uint32
}

func (s *fakeBatchSender) Send(_ context.Context, batch *componentsv1.LedgerBatch) (*componentsv1.IngestLedgerBatchAck, time.Duration, error) {
	latency := s.latencies[s.index]
	s.index++
	s.clock.now = s.clock.now.Add(latency)
	return &componentsv1.IngestLedgerBatchAck{LedgerSequence: batch.LedgerSequence + s.ackOffset}, latency, nil
}

func (*fakeBatchSender) Close() error { return nil }
