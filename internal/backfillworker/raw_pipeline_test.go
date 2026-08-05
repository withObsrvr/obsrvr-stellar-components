package backfillworker

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestRawDecodePipelineOwnsBorrowedBuffersAndRestoresOrder(t *testing.T) {
	const ledgerCount = 8
	borrowed := []byte{0}
	sourceIndex := 0
	source := func() ([]byte, error) {
		if sourceIndex == ledgerCount {
			return nil, io.EOF
		}
		borrowed[0] = byte(sourceIndex)
		sourceIndex++
		return borrowed, nil
	}
	decoder := func(raw []byte, _ RawLedgerOptions) (*RawLedger, error) {
		ordinal := int(raw[0])
		// Later ledgers finish first so ordered reassembly is exercised.
		time.Sleep(time.Duration(ledgerCount-ordinal) * time.Millisecond)
		return &RawLedger{
			LedgerSequence: uint32(100 + ordinal),
			SourceXDR:      raw,
		}, nil
	}

	pipeline, err := newRawDecodePipeline(context.Background(), rawDecodePipelineConfig{
		Workers: 4, MaxInFlight: 4,
	}, RawLedgerOptions{}, source, decoder)
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()

	var sequences []uint32
	var sourceValues []byte
	for {
		ledger, err := pipeline.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		sequences = append(sequences, ledger.LedgerSequence)
		sourceValues = append(sourceValues, ledger.SourceXDR[0])
	}
	if want := []uint32{100, 101, 102, 103, 104, 105, 106, 107}; !reflect.DeepEqual(sequences, want) {
		t.Fatalf("sequences = %v, want %v", sequences, want)
	}
	if want := []byte{0, 1, 2, 3, 4, 5, 6, 7}; !reflect.DeepEqual(sourceValues, want) {
		t.Fatalf("owned source values = %v, want %v", sourceValues, want)
	}
	metrics := pipeline.Metrics()
	if metrics.PeakInFlight > 4 {
		t.Fatalf("peak in-flight ledgers = %d, want <= 4", metrics.PeakInFlight)
	}
	if metrics.PeakInFlight < 2 {
		t.Fatalf("peak in-flight ledgers = %d, pipeline never overlapped work", metrics.PeakInFlight)
	}
	if metrics.CopiedBytes != ledgerCount {
		t.Fatalf("copied bytes = %d, want %d", metrics.CopiedBytes, ledgerCount)
	}
}

func TestRawDecodePipelineReturnsDecodeErrorsInSourceOrder(t *testing.T) {
	inputs := [][]byte{{0}, {1}, {2}, {3}}
	index := 0
	source := func() ([]byte, error) {
		if index == len(inputs) {
			return nil, io.EOF
		}
		raw := inputs[index]
		index++
		return raw, nil
	}
	wantErr := errors.New("ledger two failed")
	decoder := func(raw []byte, _ RawLedgerOptions) (*RawLedger, error) {
		if raw[0] == 2 {
			return nil, wantErr
		}
		if raw[0] == 1 {
			time.Sleep(5 * time.Millisecond)
		}
		return &RawLedger{LedgerSequence: uint32(raw[0]), SourceXDR: raw}, nil
	}
	pipeline, err := newRawDecodePipeline(context.Background(), rawDecodePipelineConfig{
		Workers: 3, MaxInFlight: 3,
	}, RawLedgerOptions{}, source, decoder)
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()

	for want := uint32(0); want < 2; want++ {
		ledger, err := pipeline.Next()
		if err != nil {
			t.Fatal(err)
		}
		if ledger.LedgerSequence != want {
			t.Fatalf("ledger = %d, want %d", ledger.LedgerSequence, want)
		}
	}
	if _, err := pipeline.Next(); !errors.Is(err, wantErr) {
		t.Fatalf("pipeline error = %v, want %v", err, wantErr)
	}
}

func TestRawDecodePipelineRejectsUnboundedConfiguration(t *testing.T) {
	for _, cfg := range []rawDecodePipelineConfig{
		{},
		{Workers: 1},
		{Workers: 2, MaxInFlight: 1},
	} {
		if _, err := newRawDecodePipeline(context.Background(), cfg, RawLedgerOptions{}, func() ([]byte, error) {
			return nil, io.EOF
		}, func([]byte, RawLedgerOptions) (*RawLedger, error) {
			return nil, nil
		}); err == nil {
			t.Fatalf("newRawDecodePipeline(%+v) succeeded", cfg)
		}
	}
}

func TestRawDecodePipelineReportedPeakNeverExceedsAdmissionBound(t *testing.T) {
	const ledgerCount = 2_000
	index := 0
	pipeline, err := newRawDecodePipeline(context.Background(), rawDecodePipelineConfig{
		Workers: 8, MaxInFlight: 8,
	}, RawLedgerOptions{}, func() ([]byte, error) {
		if index == ledgerCount {
			return nil, io.EOF
		}
		index++
		return []byte{byte(index)}, nil
	}, func(raw []byte, _ RawLedgerOptions) (*RawLedger, error) {
		return &RawLedger{SourceXDR: raw}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pipeline.Close()
	for {
		_, err := pipeline.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if peak := pipeline.Metrics().PeakInFlight; peak > 8 {
		t.Fatalf("reported peak in-flight ledgers = %d, want <= 8", peak)
	}
}
