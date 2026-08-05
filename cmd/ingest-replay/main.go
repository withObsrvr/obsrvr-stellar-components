// Command ingest-replay sends recorded LedgerBatch fixtures directly to the
// BronzeIngestService on a deterministic arrival schedule.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/withObsrvr/obsrvr-stellar-components/internal/ledgerfixture"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Printf("ingest replay failed: %v", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	config, err := parseConfig(args, stdout)
	if err != nil {
		return err
	}
	manifest, err := ledgerfixture.LoadManifest(config.Fixtures)
	if err != nil {
		return err
	}
	if config.Offset >= manifest.BatchCount {
		return fmt.Errorf("--offset %d is outside the %d-batch fixture", config.Offset, manifest.BatchCount)
	}
	if config.Count > manifest.BatchCount-config.Offset {
		return fmt.Errorf("--offset %d and --count %d exceed the %d-batch fixture", config.Offset, config.Count, manifest.BatchCount)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	baselineCheckpoints := float64(0)
	if config.RequireCheckpoints > 0 {
		scrapeContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		baselineCheckpoints, err = successfulIdleCheckpoints(scrapeContext, config.MetricsURL)
		cancel()
		if err != nil {
			return fmt.Errorf("record checkpoint baseline: %w", err)
		}
	}

	sender, err := newGRPCBatchSender(ctx, config.Endpoint, config.Token)
	if err != nil {
		return err
	}
	defer sender.Close()
	reader := ledgerfixture.NewReader(config.Fixtures, manifest)
	defer reader.Close()

	resultsWriter, closeResults, err := openOutput(config.ResultsPath, io.Discard, false)
	if err != nil {
		return fmt.Errorf("open per-ledger results: %w", err)
	}

	summary, replayErr := executeReplay(ctx, config, manifest.BatchCount, reader, sender, resultsWriter, wallClock{})
	if closeErr := closeResults(); closeErr != nil {
		replayErr = errors.Join(replayErr, fmt.Errorf("close per-ledger results: %w", closeErr))
	}
	if replayErr == nil && config.Offset+summary.Acknowledged == manifest.BatchCount {
		if _, err := reader.Next(); !errors.Is(err, io.EOF) {
			if err == nil {
				err = fmt.Errorf("fixture contains more batches than its manifest declares")
			}
			replayErr = err
		}
	}
	if replayErr == nil && config.RequireCheckpoints > 0 {
		observed, checkpointErr := waitForIdleCheckpoints(ctx, config.MetricsURL, baselineCheckpoints, config.RequireCheckpoints, config.CheckpointWait)
		summary.ObservedIdleCheckpoints = observed
		if checkpointErr != nil {
			replayErr = checkpointErr
		}
	}
	if replayErr == nil && summary.Sent != summary.Acknowledged {
		replayErr = fmt.Errorf("sent %d ledgers but acknowledged %d", summary.Sent, summary.Acknowledged)
	}
	if replayErr == nil && summary.OverBudget > 0 {
		replayErr = fmt.Errorf("%d acknowledged ledgers exceeded the %s arrival-to-ack budget", summary.OverBudget, config.MaxLatency)
	}
	if replayErr != nil {
		summary.Success = false
		summary.Failure = replayErr.Error()
	} else {
		summary.Success = true
	}

	summaryWriter, closeSummary, err := openOutput(config.SummaryPath, stdout, true)
	if err != nil {
		return errors.Join(replayErr, fmt.Errorf("open replay summary: %w", err))
	}
	encoder := json.NewEncoder(summaryWriter)
	encoder.SetIndent("", "  ")
	writeErr := encoder.Encode(summary)
	closeErr := closeSummary()
	if writeErr != nil {
		writeErr = fmt.Errorf("write replay summary: %w", writeErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close replay summary: %w", closeErr)
	}
	return errors.Join(replayErr, writeErr, closeErr)
}

func openOutput(path string, fallback io.Writer, allowStdout bool) (io.Writer, func() error, error) {
	if path == "" {
		return fallback, func() error { return nil }, nil
	}
	if path == "-" {
		if !allowStdout {
			return nil, nil, fmt.Errorf("stdout is reserved for the summary")
		}
		return fallback, func() error { return nil }, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return file, file.Close, nil
}
