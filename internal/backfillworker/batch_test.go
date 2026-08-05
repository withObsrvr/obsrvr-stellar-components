package backfillworker

import (
	"context"
	"database/sql"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	"google.golang.org/protobuf/proto"
)

func TestWriteLedgerBatchShardIncludesDeterministicMetadataAndWatermarks(t *testing.T) {
	writtenAt := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	batches := testLedgerBatches(100, 101)
	firstConfig := LedgerBatchConfig{
		Parquet:            ParquetConfig{OutputDir: t.TempDir(), LedgerStart: 100, LedgerEnd: 101},
		DecodeWorkers:      2,
		WatermarkWrittenAt: writtenAt,
	}
	first, err := WriteLedgerBatchShard(context.Background(), firstConfig, batches)
	if err != nil {
		t.Fatalf("write first complete shard: %v", err)
	}
	secondConfig := firstConfig
	secondConfig.Parquet.OutputDir = t.TempDir()
	second, err := WriteLedgerBatchShard(context.Background(), secondConfig, batches)
	if err != nil {
		t.Fatalf("write second complete shard: %v", err)
	}
	if len(first) != 3 || len(second) != 3 {
		t.Fatalf("complete shard files = %d and %d, want 3", len(first), len(second))
	}
	for index := range first {
		if first[index].Table != second[index].Table || first[index].SHA256 != second[index].SHA256 {
			t.Fatalf("artifact %d differs: %+v != %+v", index, first[index], second[index])
		}
	}

	watermark := findArtifact(t, first, "main.ingest_watermarks")
	parsed, err := url.Parse(watermark.URI)
	if err != nil {
		t.Fatalf("parse watermark URI: %v", err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open verifier DuckDB: %v", err)
	}
	defer db.Close()
	var count int
	var minWrittenAt, maxWrittenAt time.Time
	if err := db.QueryRow(
		"SELECT count(*), min(written_at), max(written_at) FROM read_parquet("+bronze.SQLLiteral(parsed.Path)+")",
	).Scan(&count, &minWrittenAt, &maxWrittenAt); err != nil {
		t.Fatalf("query watermark Parquet: %v", err)
	}
	if count != 2 || !minWrittenAt.Equal(writtenAt) || !maxWrittenAt.Equal(writtenAt) {
		t.Fatalf("watermarks = count %d timestamps %s-%s", count, minWrittenAt, maxWrittenAt)
	}
}

func TestWriteLedgerBatchShardRejectsUnsupportedBronzeRowsWithoutArtifacts(t *testing.T) {
	outputDir := t.TempDir()
	batch := testLedgerBatches(100, 100)[0]
	batch.BronzeRows[0].TableName = "future_table_row_v3"
	_, err := WriteLedgerBatchShard(context.Background(), LedgerBatchConfig{
		Parquet: ParquetConfig{
			OutputDir:   outputDir,
			LedgerStart: 100,
			LedgerEnd:   100,
		},
		DecodeWorkers:      1,
		WatermarkWrittenAt: time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC),
	}, []*componentsv1.LedgerBatch{batch})
	if err == nil {
		t.Fatal("write succeeded, want unsupported table failure")
	}
	matches, globErr := filepath.Glob(filepath.Join(outputDir, "*.parquet"))
	if globErr != nil {
		t.Fatalf("glob output: %v", globErr)
	}
	if len(matches) != 0 {
		t.Fatalf("artifacts left after unsupported table rejection: %v", matches)
	}
}

func TestWriteLedgerBatchStreamReportsOneBatchDecodeWindow(t *testing.T) {
	batches := testLedgerBatches(100, 102)
	for index := 0; index < 2; index++ {
		row := proto.Clone(batches[1].BronzeRows[0]).(*componentsv1.BronzeRow)
		row.Id = "extra"
		batches[1].BronzeRows = append(batches[1].BronzeRows, row)
	}
	index := 0
	result, err := WriteLedgerBatchStream(context.Background(), LedgerBatchConfig{
		Parquet: ParquetConfig{
			OutputDir:   t.TempDir(),
			LedgerStart: 100,
			LedgerEnd:   102,
		},
		DecodeWorkers:      2,
		WatermarkWrittenAt: time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC),
		MaxEncodedBytes:    1 << 20,
		MaxBronzeRows:      10,
	}, func() (*componentsv1.LedgerBatch, error) {
		if index == len(batches) {
			return nil, io.EOF
		}
		batch := batches[index]
		batches[index] = nil
		index++
		return batch, nil
	})
	if err != nil {
		t.Fatalf("write streamed shard: %v", err)
	}
	if result.Descriptor.LedgerCount != 3 || result.Descriptor.BronzeRows != 5 {
		t.Fatalf("descriptor = %+v", result.Descriptor)
	}
	if result.PeakBatchBronzeRows != 3 || result.PeakBatchEncodedBytes == 0 {
		t.Fatalf("peak batch window = %d rows / %d bytes", result.PeakBatchBronzeRows, result.PeakBatchEncodedBytes)
	}
	if len(result.Files) != 3 {
		t.Fatalf("files = %d, want one typed table plus metadata and watermarks", len(result.Files))
	}
}

func TestWriteLedgerBatchStreamRejectsInvalidMemoryLimit(t *testing.T) {
	_, err := WriteLedgerBatchStream(context.Background(), LedgerBatchConfig{
		Parquet: ParquetConfig{
			OutputDir:   t.TempDir(),
			LedgerStart: 100,
			LedgerEnd:   100,
		},
		DecodeWorkers:      1,
		WatermarkWrittenAt: time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC),
		MemoryLimit:        "1GB'; DROP TABLE bronze.ledgers_row_v2; --",
	}, func() (*componentsv1.LedgerBatch, error) {
		return nil, io.EOF
	})
	if err == nil || !strings.Contains(err.Error(), "invalid DuckDB memory limit") {
		t.Fatalf("WriteLedgerBatchStream error = %v, want invalid memory limit", err)
	}
}

func testLedgerBatches(start, end uint32) []*componentsv1.LedgerBatch {
	batches := make([]*componentsv1.LedgerBatch, 0, end-start+1)
	for sequence := start; sequence <= end; sequence++ {
		row := &componentsv1.BronzeRow{
			Id:                "ledger",
			TableName:         "ledgers_row_v2",
			NetworkPassphrase: "testnet",
			LedgerSequence:    sequence,
			LedgerRange:       sequence,
			RowJson:           allTableFixtureJSON(sequence),
		}
		batches = append(batches, &componentsv1.LedgerBatch{
			NetworkPassphrase: "testnet",
			LedgerSequence:    sequence,
			ClosedAtUnix:      int64(sequence),
			SchemaVersion:     "v1",
			ExtractionVersion: "v1",
			BronzeRows:        []*componentsv1.BronzeRow{row},
		})
	}
	return batches
}

func findArtifact(t *testing.T, files []backfillmanifest.File, table string) backfillmanifest.File {
	t.Helper()
	for _, file := range files {
		if file.Table == table {
			return file
		}
	}
	t.Fatalf("artifact %s not found", table)
	return backfillmanifest.File{}
}
