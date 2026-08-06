package backfillworker

import (
	"context"
	"io"
	"testing"
	"time"
)

func probeConfig(start, end uint32, workers int) LedgerBatchConfig {
	return LedgerBatchConfig{
		Parquet: ParquetConfig{
			OutputDir:   "probe",
			LedgerStart: start,
			LedgerEnd:   end,
			Compression: "snappy",
		},
		WriterMode:         WriterArrowParquet,
		DecodeWorkers:      1,
		RawExtractWorkers:  workers,
		MaxInFlightLedgers: workers * 2,
		WatermarkWrittenAt: time.Unix(0, 0).UTC(),
	}
}

func probeOptions() RawLedgerOptions {
	return RawLedgerOptions{
		NetworkPassphrase: "Test SDF Network ; September 2015",
		SchemaVersion:     "v1",
		ExtractionVersion: "v1",
		MaterializedAt:    time.Unix(0, 0).UTC(),
	}
}

func payloadSource(payloads [][]byte) RawLedgerSource {
	index := 0
	return func() ([]byte, error) {
		if index == len(payloads) {
			return nil, io.EOF
		}
		raw := payloads[index]
		index++
		return raw, nil
	}
}

func samplePayloads() [][]byte {
	return [][]byte{{1, 2, 3}, {4, 5}, {6, 7, 8, 9}}
}

// TestSourceStageDigestMatchesArtifactPathFraming is the property that makes
// a source probe usable as evidence: its payload digest must equal the digest
// the artifact-producing accumulator derives from the same bytes.
func TestSourceStageDigestMatchesArtifactPathFraming(t *testing.T) {
	payloads := samplePayloads()
	probe, err := MeasureRawLedgerStream(
		context.Background(),
		probeConfig(100, 102, 1),
		probeOptions(),
		payloadSource(payloads),
		ProbeStageSource,
	)
	if err != nil {
		t.Fatal(err)
	}

	accumulator := newRawLedgerAccumulator()
	for index, raw := range payloads {
		if err := accumulator.Add(&RawLedger{
			NetworkPassphrase: probeOptions().NetworkPassphrase,
			LedgerSequence:    uint32(100 + index),
			SourceXDR:         raw,
		}); err != nil {
			t.Fatal(err)
		}
	}
	want, err := accumulator.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	if probe.Descriptor.PayloadSHA256 != want.PayloadSHA256 {
		t.Fatalf("source probe digest %s, want %s", probe.Descriptor.PayloadSHA256, want.PayloadSHA256)
	}
	if probe.Descriptor.EncodedBytes != want.EncodedBytes {
		t.Fatalf("source probe read %d bytes, want %d", probe.Descriptor.EncodedBytes, want.EncodedBytes)
	}
	if probe.Descriptor.LedgerStart != 100 || probe.Descriptor.LedgerEnd != 102 || probe.Descriptor.LedgerCount != 3 {
		t.Fatalf("source probe range = %d-%d (%d)", probe.Descriptor.LedgerStart, probe.Descriptor.LedgerEnd, probe.Descriptor.LedgerCount)
	}
	if probe.PeakLedgerBytes != 4 {
		t.Fatalf("peak ledger bytes = %d, want 4", probe.PeakLedgerBytes)
	}
}

func TestSourceStageRejectsRangesItDidNotFullyRead(t *testing.T) {
	for name, end := range map[string]uint32{"short": 103, "long": 101} {
		t.Run(name, func(t *testing.T) {
			if _, err := MeasureRawLedgerStream(
				context.Background(),
				probeConfig(100, end, 1),
				probeOptions(),
				payloadSource(samplePayloads()),
				ProbeStageSource,
			); err == nil {
				t.Fatal("source probe accepted a range it did not read exactly")
			}
		})
	}
}

func TestExtractStageAttributesPhasesAndPreservesOrder(t *testing.T) {
	for _, workers := range []int{1, 3} {
		payloads := samplePayloads()
		decoder := func(raw []byte, opts RawLedgerOptions) (*RawLedger, error) {
			return &RawLedger{
				NetworkPassphrase: opts.NetworkPassphrase,
				LedgerSequence:    uint32(100 + int(raw[0])/3),
				SourceXDR:         raw,
				BronzeRowCount:    len(raw),
				ExtractDuration:   time.Millisecond,
			}, nil
		}
		probe, err := measureRawExtractStage(
			context.Background(),
			probeConfig(100, 102, workers),
			probeOptions(),
			payloadSource(payloads),
			decoder,
		)
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		if probe.Descriptor.LedgerCount != 3 || probe.Descriptor.LedgerStart != 100 || probe.Descriptor.LedgerEnd != 102 {
			t.Fatalf("workers=%d: range = %d-%d (%d)", workers, probe.Descriptor.LedgerStart, probe.Descriptor.LedgerEnd, probe.Descriptor.LedgerCount)
		}
		if probe.Descriptor.BronzeRows != 9 {
			t.Fatalf("workers=%d: bronze rows = %d, want 9", workers, probe.Descriptor.BronzeRows)
		}
		if probe.RawExtractDuration < 3*time.Millisecond {
			t.Fatalf("workers=%d: extract duration = %s, want per-ledger attribution", workers, probe.RawExtractDuration)
		}
		if workers == 1 {
			continue
		}
		// The concurrent path collects source and admission metrics only when
		// the pipeline closes, so they must survive the deferred collection.
		if probe.RawCopiedBytes != probe.Descriptor.EncodedBytes {
			t.Fatalf("workers=%d: copied %d bytes, want %d", workers, probe.RawCopiedBytes, probe.Descriptor.EncodedBytes)
		}
		if probe.PeakInFlightLedgers == 0 {
			t.Fatalf("workers=%d: pipeline metrics were dropped", workers)
		}
	}
}

func TestExtractStageRejectsOutOfOrderLedgers(t *testing.T) {
	decoder := func(raw []byte, opts RawLedgerOptions) (*RawLedger, error) {
		return &RawLedger{
			NetworkPassphrase: opts.NetworkPassphrase,
			LedgerSequence:    200,
			SourceXDR:         raw,
		}, nil
	}
	if _, err := measureRawExtractStage(
		context.Background(),
		probeConfig(100, 102, 1),
		probeOptions(),
		payloadSource(samplePayloads()),
		decoder,
	); err == nil {
		t.Fatal("extract probe accepted an out-of-order ledger")
	}
}

func TestProbeStageParsingIsClosed(t *testing.T) {
	for _, valid := range []string{"source", "extract", "full"} {
		if _, err := ParseProbeStage(valid); err != nil {
			t.Fatalf("ParseProbeStage(%q) = %v", valid, err)
		}
	}
	for _, invalid := range []string{"", "Full", "parquet", "source "} {
		if _, err := ParseProbeStage(invalid); err == nil {
			t.Fatalf("ParseProbeStage(%q) succeeded", invalid)
		}
	}
	if _, err := MeasureRawLedgerStream(
		context.Background(),
		probeConfig(100, 102, 1),
		probeOptions(),
		payloadSource(samplePayloads()),
		ProbeStageFull,
	); err == nil {
		t.Fatal("full stage returned a measurement-only result")
	}
}
