package backfillworker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/ingestbatch"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	bronzeColumnar "github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze/columnar"
)

// writeColumnarLedgerStream performs the same bounded source, extraction, and
// correctness checks as the Appender writer, but hands projected values to
// persistent Arrow builders and writes Parquet row groups directly. DuckDB is
// not present on this worker's write path.
func writeColumnarLedgerStream(ctx context.Context, cfg LedgerBatchConfig, nextBatch LedgerBatchSource, nextRaw RawLedgerSource, rawOpts RawLedgerOptions) (result StreamResult, resultErr error) {
	started := time.Now()
	if nextRaw != nil {
		rawOpts.DirectContractEvents = true
	}
	if err := validateStreamingConfig(cfg, nextBatch, nextRaw, rawOpts); err != nil {
		return StreamResult{}, err
	}
	absOutputDir, err := filepath.Abs(cfg.Parquet.OutputDir)
	if err != nil {
		return StreamResult{}, fmt.Errorf("resolve shard output directory: %w", err)
	}
	if err := ensureOutputDirectory(absOutputDir); err != nil {
		return StreamResult{}, err
	}
	columnar := newColumnarShardWriter(cfg.Parquet, absOutputDir)
	complete := false
	defer func() {
		if !complete {
			columnar.abort()
		}
	}()

	tableCounts := map[string]uint64{
		"main.ingest_watermarks": 0,
		"main.ledger_batches":    0,
	}
	batchAccumulator := ingestbatch.NewAccumulator()
	rawAccumulator := newRawLedgerAccumulator()
	result.SetupDuration = time.Since(started)

	for {
		if err := ctx.Err(); err != nil {
			return StreamResult{}, err
		}
		var (
			batch      *componentsv1.LedgerBatch
			rawLedger  *RawLedger
			decoded    []bronze.DecodedRow
			descriptor ingestbatch.Descriptor
			sequence   uint32
		)
		sourceStarted := time.Now()
		if nextBatch != nil {
			batch, err = nextBatch()
			result.SourceDuration += time.Since(sourceStarted)
		} else {
			var raw []byte
			raw, err = nextRaw()
			result.SourceDuration += time.Since(sourceStarted)
			if err == nil {
				extractionStarted := time.Now()
				rawLedger, err = DecodeRawLedger(raw, rawOpts)
				result.ExtractionDuration += time.Since(extractionStarted)
				if err == nil {
					result.RawViewDuration += rawLedger.ViewDuration
					result.RawDecodeDuration += rawLedger.DecodeDuration
					result.RawExtractDuration += rawLedger.ExtractDuration
					result.RawPinDuration += rawLedger.PinDuration
					result.RawEnvelopeDuration += rawLedger.EnvelopeDuration
					result.RawProjectDuration += rawLedger.ProjectDuration
				}
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return StreamResult{}, fmt.Errorf("read shard source: %w", err)
		}

		digestStarted := time.Now()
		if batch != nil {
			if err := validateNextStreamingBatch(cfg, batchAccumulator, batch); err != nil {
				return StreamResult{}, err
			}
			if err := batchAccumulator.Add(batch); err != nil {
				return StreamResult{}, err
			}
			descriptor = batchAccumulator.Totals()
			sequence = batch.LedgerSequence
		} else {
			if err := validateNextRawLedger(cfg, rawAccumulator, rawLedger); err != nil {
				return StreamResult{}, err
			}
			if err := rawAccumulator.Add(rawLedger); err != nil {
				return StreamResult{}, err
			}
			descriptor = rawAccumulator.Totals()
			sequence = rawLedger.LedgerSequence
			decoded = rawLedger.Rows
		}
		result.DigestDuration += time.Since(digestStarted)
		batchBytes := uint64(0)
		if descriptor.EncodedBytes >= result.Descriptor.EncodedBytes {
			batchBytes = descriptor.EncodedBytes - result.Descriptor.EncodedBytes
		}
		batchRows := descriptor.BronzeRows - result.Descriptor.BronzeRows
		if batchBytes > result.PeakBatchEncodedBytes {
			result.PeakBatchEncodedBytes = batchBytes
		}
		if batchRows > result.PeakBatchBronzeRows {
			result.PeakBatchBronzeRows = batchRows
		}
		result.Descriptor = descriptor
		if cfg.MaxEncodedBytes > 0 && descriptor.EncodedBytes > cfg.MaxEncodedBytes {
			return StreamResult{}, fmt.Errorf("selected range exceeds encoded byte bound: %d > %d", descriptor.EncodedBytes, cfg.MaxEncodedBytes)
		}
		if cfg.MaxBronzeRows > 0 && descriptor.BronzeRows > cfg.MaxBronzeRows {
			return StreamResult{}, fmt.Errorf("selected range exceeds Bronze row bound: %d > %d", descriptor.BronzeRows, cfg.MaxBronzeRows)
		}

		if batch != nil {
			decodeStarted := time.Now()
			decoded = bronze.DecodeTypedRowsBatches([]*componentsv1.LedgerBatch{batch}, cfg.DecodeWorkers)
			result.DecodeDuration += time.Since(decodeStarted)
			if len(decoded) != len(batch.BronzeRows) {
				return StreamResult{}, fmt.Errorf("ledger %d decoded %d Bronze rows, want %d", sequence, len(decoded), len(batch.BronzeRows))
			}
		}

		appendStarted := time.Now()
		if err := columnar.appendDecodedLedger(sequence, decoded); err != nil {
			return StreamResult{}, err
		}
		for _, row := range decoded {
			tableCounts["bronze."+row.Spec.TableName]++
		}
		if rawLedger != nil && rawLedger.ExtractedData != nil {
			contractEvents := rawLedger.ExtractedData.ContractEvents
			if err := columnar.appendContractEvents(sequence, contractEvents); err != nil {
				return StreamResult{}, err
			}
			tableCounts["bronze."+bronzeColumnar.ContractEventsTable] += uint64(len(contractEvents))
		}
		var envelope ledgerEnvelopeValues
		if batch != nil {
			envelope = ledgerEnvelopeValues{
				networkPassphrase: batch.NetworkPassphrase,
				closedAtUnix:      batch.ClosedAtUnix, schemaVersion: batch.SchemaVersion,
				extractionVersion: batch.ExtractionVersion,
				transactionCount:  len(batch.Transactions), operationCount: len(batch.Operations),
				bronzeRowCount: len(batch.BronzeRows), writtenAt: cfg.WatermarkWrittenAt,
			}
		} else {
			envelope = ledgerEnvelopeValues{
				networkPassphrase: rawLedger.NetworkPassphrase,
				closedAtUnix:      rawLedger.ClosedAtUnix, schemaVersion: rawLedger.SchemaVersion,
				extractionVersion: rawLedger.ExtractionVersion,
				transactionCount:  rawLedger.TransactionCount, operationCount: rawLedger.OperationCount,
				bronzeRowCount: rawLedger.BronzeRowCount, writtenAt: cfg.WatermarkWrittenAt,
			}
		}
		if err := columnar.appendEnvelope(sequence, envelope); err != nil {
			return StreamResult{}, err
		}
		tableCounts["main.ingest_watermarks"]++
		tableCounts["main.ledger_batches"]++
		result.AppendDuration += time.Since(appendStarted)
	}
	result.StagingDuration = time.Since(started)

	var descriptor ingestbatch.Descriptor
	if nextBatch != nil {
		descriptor, err = batchAccumulator.Descriptor()
	} else {
		descriptor, err = rawAccumulator.Descriptor()
	}
	if err != nil {
		return StreamResult{}, err
	}
	if descriptor.LedgerStart != cfg.Parquet.LedgerStart || descriptor.LedgerEnd != cfg.Parquet.LedgerEnd || descriptor.LedgerCount != cfg.Parquet.LedgerEnd-cfg.Parquet.LedgerStart+1 {
		return StreamResult{}, fmt.Errorf("stream produced range %d-%d (%d ledgers), want %d-%d", descriptor.LedgerStart, descriptor.LedgerEnd, descriptor.LedgerCount, cfg.Parquet.LedgerStart, cfg.Parquet.LedgerEnd)
	}
	result.Descriptor = descriptor

	exportStarted := time.Now()
	files, err := columnar.close()
	if err != nil {
		return StreamResult{}, err
	}
	if err := validateCompleteTableCounts(tableCounts, files); err != nil {
		removeLocalArtifacts(files)
		return StreamResult{}, err
	}
	result.ExportDuration = time.Since(exportStarted)
	result.Files = files
	complete = true
	return result, nil
}

func ensureOutputDirectory(path string) error {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return fmt.Errorf("create shard output directory: %w", err)
	}
	return nil
}
