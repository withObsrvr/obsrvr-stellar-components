package backfillworker

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/withObsrvr/obsrvr-stellar-components/internal/ingestbatch"
)

// ProbeStage selects how much of the worker pipeline a measurement run
// executes. Probes exist to attribute wall time between object acquisition and
// local CPU; they never publish artifacts, so they are evidence-only and must
// not be used to produce a registered shard.
type ProbeStage string

const (
	// ProbeStageSource reads raw ledger XDR and hashes it in source order. It
	// performs no XDR decode, no extraction, and no Arrow or Parquet work.
	ProbeStageSource ProbeStage = "source"
	// ProbeStageExtract additionally decodes, extracts, and projects each
	// ledger through the same bounded pipeline the writer uses, then discards
	// the result instead of appending it to Arrow builders.
	ProbeStageExtract ProbeStage = "extract"
	// ProbeStageFull is the complete artifact-producing run.
	ProbeStageFull ProbeStage = "full"
)

// ParseProbeStage validates a stage name.
func ParseProbeStage(value string) (ProbeStage, error) {
	switch ProbeStage(value) {
	case ProbeStageSource:
		return ProbeStageSource, nil
	case ProbeStageExtract:
		return ProbeStageExtract, nil
	case ProbeStageFull:
		return ProbeStageFull, nil
	default:
		return "", fmt.Errorf("unsupported probe stage %q (want %s, %s, or %s)", value, ProbeStageSource, ProbeStageExtract, ProbeStageFull)
	}
}

// ProbeResult reports the same source identity a full run reports, so a probe
// can be proven to have read exactly the bytes the artifact path reads.
type ProbeResult struct {
	Stage               ProbeStage
	Descriptor          ingestbatch.Descriptor
	SourceDuration      time.Duration
	DigestDuration      time.Duration
	ExtractionDuration  time.Duration
	RawViewDuration     time.Duration
	RawDecodeDuration   time.Duration
	RawExtractDuration  time.Duration
	RawPinDuration      time.Duration
	RawEnvelopeDuration time.Duration
	RawProjectDuration  time.Duration
	RawCopyDuration     time.Duration
	RawPipelineWait     time.Duration
	RawCopiedBytes      uint64
	PeakInFlightLedgers int
	PeakReorderBuffered int
	PeakLedgerBytes     uint64
}

// MeasureRawLedgerStream runs the requested probe stage over the shard's raw
// ledger range and returns phase timings without writing any artifact.
func MeasureRawLedgerStream(ctx context.Context, cfg LedgerBatchConfig, opts RawLedgerOptions, next RawLedgerSource, stage ProbeStage) (ProbeResult, error) {
	switch stage {
	case ProbeStageSource:
		return measureRawSourceStage(ctx, cfg, next)
	case ProbeStageExtract:
		return measureRawExtractStage(ctx, cfg, opts, next, DecodeRawLedger)
	default:
		return ProbeResult{}, fmt.Errorf("probe stage %q does not have a measurement-only path", stage)
	}
}

// measureRawSourceStage isolates object acquisition. It hashes each borrowed
// buffer with the same length-prefixed framing the artifact accumulator uses,
// so its payload digest is comparable to a full run's source digest.
func measureRawSourceStage(ctx context.Context, cfg LedgerBatchConfig, next RawLedgerSource) (ProbeResult, error) {
	if next == nil {
		return ProbeResult{}, fmt.Errorf("probe requires a raw ledger source")
	}
	if err := validateProbeRange(cfg); err != nil {
		return ProbeResult{}, err
	}
	result := ProbeResult{Stage: ProbeStageSource}
	hasher := sha256.New()
	expected := uint64(cfg.Parquet.LedgerEnd-cfg.Parquet.LedgerStart) + 1
	for {
		if err := ctx.Err(); err != nil {
			return ProbeResult{}, err
		}
		sourceStarted := time.Now()
		raw, err := next()
		result.SourceDuration += time.Since(sourceStarted)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ProbeResult{}, fmt.Errorf("read shard source: %w", err)
		}
		digestStarted := time.Now()
		var length [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(length[:], uint64(len(raw)))
		_, _ = hasher.Write(length[:n])
		_, _ = hasher.Write(raw)
		result.DigestDuration += time.Since(digestStarted)

		result.Descriptor.EncodedBytes += uint64(len(raw))
		result.Descriptor.LedgerCount++
		result.PeakLedgerBytes = max(result.PeakLedgerBytes, uint64(len(raw)))
		if result.Descriptor.LedgerCount > uint32(expected) {
			return ProbeResult{}, fmt.Errorf("source produced more than %d ledgers", expected)
		}
	}
	if uint64(result.Descriptor.LedgerCount) != expected {
		return ProbeResult{}, fmt.Errorf("source produced %d ledgers, want %d", result.Descriptor.LedgerCount, expected)
	}
	// The source stage never decodes, so ledger bounds are asserted from the
	// requested range rather than observed from ledger headers.
	result.Descriptor.LedgerStart = cfg.Parquet.LedgerStart
	result.Descriptor.LedgerEnd = cfg.Parquet.LedgerEnd
	result.Descriptor.PayloadSHA256 = hex.EncodeToString(hasher.Sum(nil))
	result.Descriptor.ID = result.Descriptor.PayloadSHA256
	return result, nil
}

// measureRawExtractStage adds decode, extraction, and typed projection using
// the same bounded pipeline and the same ordering checks as the writer, then
// drops each ledger instead of appending it.
// The return values are named because pipeline metrics are only final after
// the pipeline closes, which happens in a deferred call.
func measureRawExtractStage(ctx context.Context, cfg LedgerBatchConfig, opts RawLedgerOptions, next RawLedgerSource, decoder rawLedgerDecoder) (result ProbeResult, resultErr error) {
	if next == nil || decoder == nil {
		return ProbeResult{}, fmt.Errorf("probe requires a raw ledger source")
	}
	if err := validateProbeRange(cfg); err != nil {
		return ProbeResult{}, err
	}
	if cfg.WriterMode == WriterArrowParquet {
		opts.DirectColumnarTables = true
	}
	if err := validateStreamingConfig(cfg, nil, next, opts); err != nil {
		return ProbeResult{}, err
	}
	result.Stage = ProbeStageExtract

	var pipeline *rawDecodePipeline
	if effectiveRawExtractWorkers(cfg) > 1 {
		var err error
		pipeline, err = newRawDecodePipeline(ctx, rawDecodePipelineConfig{
			Workers: effectiveRawExtractWorkers(cfg), MaxInFlight: effectiveMaxInFlightLedgers(cfg),
		}, opts, next, decoder)
		if err != nil {
			return ProbeResult{}, err
		}
		defer func() {
			pipeline.Close()
			if resultErr != nil {
				result = ProbeResult{}
				return
			}
			metrics := pipeline.Metrics()
			result.SourceDuration += metrics.SourceDuration
			result.RawCopyDuration += metrics.CopyDuration
			result.RawPipelineWait += metrics.WaitDuration
			result.RawCopiedBytes += metrics.CopiedBytes
			result.PeakInFlightLedgers = max(result.PeakInFlightLedgers, metrics.PeakInFlight)
			result.PeakReorderBuffered = max(result.PeakReorderBuffered, metrics.PeakReorderBuffered)
		}()
	}

	accumulator := newRawLedgerAccumulator()
	for {
		if err := ctx.Err(); err != nil {
			return ProbeResult{}, err
		}
		var (
			ledger *RawLedger
			err    error
		)
		if pipeline != nil {
			ledger, err = pipeline.Next()
		} else {
			sourceStarted := time.Now()
			var raw []byte
			raw, err = next()
			result.SourceDuration += time.Since(sourceStarted)
			if err == nil {
				ledger, err = decoder(raw, opts)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ProbeResult{}, fmt.Errorf("read shard source: %w", err)
		}
		result.ExtractionDuration += ledger.ProcessingDuration
		result.RawViewDuration += ledger.ViewDuration
		result.RawDecodeDuration += ledger.DecodeDuration
		result.RawExtractDuration += ledger.ExtractDuration
		result.RawPinDuration += ledger.PinDuration
		result.RawEnvelopeDuration += ledger.EnvelopeDuration
		result.RawProjectDuration += ledger.ProjectDuration

		digestStarted := time.Now()
		if err := validateNextRawLedger(cfg, accumulator, ledger); err != nil {
			return ProbeResult{}, err
		}
		before := accumulator.Totals().EncodedBytes
		if err := accumulator.Add(ledger); err != nil {
			return ProbeResult{}, err
		}
		result.DigestDuration += time.Since(digestStarted)
		result.PeakLedgerBytes = max(result.PeakLedgerBytes, accumulator.Totals().EncodedBytes-before)
	}

	descriptor, err := accumulator.Descriptor()
	if err != nil {
		return ProbeResult{}, err
	}
	if descriptor.LedgerStart != cfg.Parquet.LedgerStart || descriptor.LedgerEnd != cfg.Parquet.LedgerEnd {
		return ProbeResult{}, fmt.Errorf("probe produced range %d-%d, want %d-%d", descriptor.LedgerStart, descriptor.LedgerEnd, cfg.Parquet.LedgerStart, cfg.Parquet.LedgerEnd)
	}
	result.Descriptor = descriptor
	return result, nil
}

func validateProbeRange(cfg LedgerBatchConfig) error {
	if cfg.Parquet.LedgerEnd < cfg.Parquet.LedgerStart {
		return fmt.Errorf("probe range %d-%d is inverted", cfg.Parquet.LedgerStart, cfg.Parquet.LedgerEnd)
	}
	return nil
}
