package backfillworker

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

func TestRollingParquetProducesDeterministicBoundedParts(t *testing.T) {
	sequences := make([]uint32, 6000)
	for index := range sequences {
		sequences[index] = uint32(index + 1)
	}
	decoded := decodeLedgerRows(sequences...)
	first := writeRollingTestTable(t, t.TempDir(), decoded)
	second := writeRollingTestTable(t, t.TempDir(), decoded)
	if len(first) < 2 {
		t.Fatalf("rolling output files = %d, want multiple parts", len(first))
	}
	if len(first) != len(second) {
		t.Fatalf("rolling output file counts differ: %d != %d", len(first), len(second))
	}
	var rows uint64
	firstHashes := make([]string, len(first))
	secondHashes := make([]string, len(second))
	for index := range first {
		rows += first[index].Rows
		firstHashes[index] = first[index].SHA256
		secondHashes[index] = second[index].SHA256
		if first[index].Bytes > 1<<20 {
			t.Fatalf("part %d bytes = %d, exceeds hard maximum", index, first[index].Bytes)
		}
		parsed, err := url.Parse(first[index].URI)
		if err != nil {
			t.Fatalf("parse part %d URI: %v", index, err)
		}
		wantSuffix := fmt.Sprintf("-%05d.parquet", index)
		if !strings.HasSuffix(filepath.Base(parsed.Path), wantSuffix) {
			t.Fatalf("part %d filename %q does not end with %q", index, filepath.Base(parsed.Path), wantSuffix)
		}
	}
	if rows != uint64(len(sequences)) {
		t.Fatalf("rolling rows = %d, want %d", rows, len(sequences))
	}
	if !reflect.DeepEqual(firstHashes, secondHashes) {
		t.Fatalf("rolling hashes differ: %v != %v", firstHashes, secondHashes)
	}
}

func writeRollingTestTable(t *testing.T, outputDir string, decoded []bronze.DecodedRow) []backfillmanifest.File {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open DuckDB: %v", err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open DuckDB connection: %v", err)
	}
	defer conn.Close()
	const tableName = "ledgers_row_v2"
	if err := createLocalSchema(context.Background(), conn, []string{tableName}); err != nil {
		t.Fatalf("create local schema: %v", err)
	}
	grouped, specs, order, err := groupDecodedRows(decoded)
	if err != nil {
		t.Fatalf("group decoded rows: %v", err)
	}
	if err := appendGroupedRows(conn, grouped, specs, order); err != nil {
		t.Fatalf("append decoded rows: %v", err)
	}
	files, err := writeTypedParquetFiles(context.Background(), conn, outputDir, ParquetConfig{
		OutputDir:       outputDir,
		LedgerStart:     1,
		LedgerEnd:       6000,
		Compression:     "zstd",
		FileTargetBytes: 16 << 10,
		FileMaxBytes:    1 << 20,
		RowGroupRows:    2048,
	}, bronze.TypedTableSpecs[tableName])
	if err != nil {
		t.Fatalf("write rolling Parquet: %v", err)
	}
	return files
}
