package backfillworker

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	extract "github.com/withObsrvr/stellar-extract"
)

func TestWriteRawLedgerStreamIsDeterministic(t *testing.T) {
	ledgers := [][]byte{
		emptyRawLedgerXDR(t, 12345, 1_700_000_000),
		emptyRawLedgerXDR(t, 12346, 1_700_000_005),
		emptyRawLedgerXDR(t, 12347, 1_700_000_010),
	}
	pinned := time.Unix(1_800_000_000, 0).UTC()
	run := func(output string) StreamResult {
		index := 0
		result, err := WriteRawLedgerStream(context.Background(), LedgerBatchConfig{
			Parquet: ParquetConfig{
				OutputDir:       output,
				LedgerStart:     12345,
				LedgerEnd:       12347,
				Compression:     "zstd",
				FileTargetBytes: 1 << 20,
				FileMaxBytes:    2 << 20,
				RowGroupRows:    2048,
			},
			DecodeWorkers:      1,
			WatermarkWrittenAt: pinned,
			MaxEncodedBytes:    1 << 20,
			MaxBronzeRows:      100,
			MemoryLimit:        "128MB",
		}, RawLedgerOptions{
			NetworkPassphrase: network.PublicNetworkPassphrase,
			MaterializedAt:    pinned,
		}, func() ([]byte, error) {
			if index == len(ledgers) {
				return nil, io.EOF
			}
			raw := ledgers[index]
			index++
			return raw, nil
		})
		if err != nil {
			t.Fatalf("WriteRawLedgerStream returned error: %v", err)
		}
		return result
	}

	first := run(t.TempDir())
	second := run(t.TempDir())
	if first.Descriptor != second.Descriptor {
		t.Fatalf("source descriptors differ: first=%+v second=%+v", first.Descriptor, second.Descriptor)
	}
	if first.Descriptor.LedgerCount != 3 || first.Descriptor.BronzeRows != 3 || first.PeakBatchBronzeRows != 1 {
		t.Fatalf("descriptor = %+v, peak rows = %d", first.Descriptor, first.PeakBatchBronzeRows)
	}
	firstFiles := fileLogicalIdentities(first.Files)
	secondFiles := fileLogicalIdentities(second.Files)
	if !reflect.DeepEqual(firstFiles, secondFiles) {
		t.Fatalf("raw stream files are not byte-stable:\nfirst=%v\nsecond=%v", firstFiles, secondFiles)
	}
}

func TestArrowRawPipelineMatchesSequentialArtifacts(t *testing.T) {
	ledgers := [][]byte{
		emptyRawLedgerXDR(t, 12345, 1_700_000_000),
		emptyRawLedgerXDR(t, 12346, 1_700_000_005),
		emptyRawLedgerXDR(t, 12347, 1_700_000_010),
		emptyRawLedgerXDR(t, 12348, 1_700_000_015),
	}
	pinned := time.Unix(1_800_000_000, 0).UTC()
	run := func(output string, extractWorkers, maxInFlight int) StreamResult {
		index := 0
		borrowed := make([]byte, len(ledgers[0]))
		result, err := WriteRawLedgerStream(context.Background(), LedgerBatchConfig{
			Parquet: ParquetConfig{
				OutputDir: output, LedgerStart: 12345, LedgerEnd: 12348,
				Compression: "snappy", FileTargetBytes: 1 << 20,
				FileMaxBytes: 2 << 20, RowGroupRows: 2048,
			},
			WriterMode: WriterArrowParquet, DecodeWorkers: 1,
			RawExtractWorkers: extractWorkers, MaxInFlightLedgers: maxInFlight,
			WatermarkWrittenAt: pinned, MaxEncodedBytes: 1 << 20, MaxBronzeRows: 100,
		}, RawLedgerOptions{
			NetworkPassphrase: network.PublicNetworkPassphrase,
			MaterializedAt:    pinned,
		}, func() ([]byte, error) {
			if index == len(ledgers) {
				return nil, io.EOF
			}
			if len(ledgers[index]) != len(borrowed) {
				t.Fatalf("test ledger %d length = %d, want %d", index, len(ledgers[index]), len(borrowed))
			}
			copy(borrowed, ledgers[index])
			index++
			return borrowed, nil
		})
		if err != nil {
			t.Fatalf("WriteRawLedgerStream(%d workers): %v", extractWorkers, err)
		}
		return result
	}

	sequential := run(t.TempDir(), 1, 0)
	pipelined := run(t.TempDir(), 3, 4)
	if sequential.Descriptor != pipelined.Descriptor {
		t.Fatalf("descriptors differ:\nsequential=%+v\npipelined=%+v", sequential.Descriptor, pipelined.Descriptor)
	}
	if !reflect.DeepEqual(fileLogicalIdentities(sequential.Files), fileLogicalIdentities(pipelined.Files)) {
		t.Fatalf("pipeline artifacts differ:\nsequential=%v\npipelined=%v", fileLogicalIdentities(sequential.Files), fileLogicalIdentities(pipelined.Files))
	}
	if pipelined.PeakInFlightLedgers < 2 || pipelined.PeakInFlightLedgers > 4 {
		t.Fatalf("pipeline peak in-flight = %d, want 2-4", pipelined.PeakInFlightLedgers)
	}
	if pipelined.RawCopiedBytes != pipelined.Descriptor.EncodedBytes {
		t.Fatalf("pipeline copied bytes = %d, want source bytes %d", pipelined.RawCopiedBytes, pipelined.Descriptor.EncodedBytes)
	}
}

func fileLogicalIdentities(files []backfillmanifest.File) map[string]string {
	identities := make(map[string]string, len(files))
	for _, file := range files {
		identities[file.Table] = file.SHA256
	}
	return identities
}

func TestDecodeRawLedgerProjectsTypedRows(t *testing.T) {
	raw := emptyRawLedgerXDR(t, 12345, 1_700_000_000)
	pinned := time.Unix(1_800_000_000, 0).UTC()
	ledger, err := DecodeRawLedger(raw, RawLedgerOptions{
		NetworkPassphrase: network.PublicNetworkPassphrase,
		MaterializedAt:    pinned,
	})
	if err != nil {
		t.Fatalf("DecodeRawLedger returned error: %v", err)
	}
	if ledger.LedgerSequence != 12345 || ledger.ClosedAtUnix != 1_700_000_000 {
		t.Fatalf("ledger metadata = %d/%d", ledger.LedgerSequence, ledger.ClosedAtUnix)
	}
	if ledger.TransactionCount != 0 || ledger.OperationCount != 0 {
		t.Fatalf("envelope counts = %d transactions / %d operations", ledger.TransactionCount, ledger.OperationCount)
	}
	if len(ledger.Rows) != 1 || ledger.Rows[0].Spec.TableName != "ledgers_row_v2" {
		t.Fatalf("projected rows = %+v", ledger.Rows)
	}
	if got := ledger.Rows[0].Values[12]; got != pinned {
		t.Fatalf("pinned ingestion timestamp = %v, want %v", got, pinned)
	}
	if len(ledger.SourceXDR) != len(raw) {
		t.Fatalf("source bytes = %d, want %d", len(ledger.SourceXDR), len(raw))
	}
}

func TestDecodeRawLedgerRejectsMalformedXDR(t *testing.T) {
	_, err := DecodeRawLedger([]byte{0, 0, 0}, RawLedgerOptions{
		NetworkPassphrase: network.PublicNetworkPassphrase,
		MaterializedAt:    time.Unix(1_800_000_000, 0).UTC(),
	})
	if err == nil {
		t.Fatal("expected malformed XDR error")
	}
}

func TestPinMaterializationTimes(t *testing.T) {
	pinned := time.Unix(1_800_000_000, 0).UTC()
	closedAt := time.Unix(1_700_000_000, 0).UTC()
	data := &extract.LedgerData{
		Ledgers:      []extract.LedgerRowData{{IngestionTimestamp: time.Now()}},
		Transactions: []extract.TransactionData{{CreatedAt: closedAt}},
		Accounts:     []extract.AccountData{{CreatedAt: time.Now(), UpdatedAt: time.Now()}},
	}
	pinMaterializationTimes(data, pinned)
	if data.Ledgers[0].IngestionTimestamp != pinned || data.Accounts[0].CreatedAt != pinned || data.Accounts[0].UpdatedAt != pinned {
		t.Fatal("materialization timestamps were not pinned")
	}
	if data.Transactions[0].CreatedAt != closedAt {
		t.Fatalf("ledger-semantic transaction timestamp changed to %v", data.Transactions[0].CreatedAt)
	}
}

func emptyRawLedgerXDR(t *testing.T, sequence uint32, closeTime uint64) []byte {
	t.Helper()
	ledger := xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(sequence),
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(closeTime)},
				},
			},
		},
	}
	raw, err := ledger.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
