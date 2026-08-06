package backfillworker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

type LedgerBatchConfig struct {
	Parquet             ParquetConfig
	WriterMode          string
	DecodeWorkers       int
	RawExtractWorkers   int
	MaxInFlightLedgers  int
	ParquetWriters      int
	MaxPendingRowGroups int
	WatermarkWrittenAt  time.Time
	MaxEncodedBytes     uint64
	MaxBronzeRows       uint64
	MemoryLimit         string
}

const (
	WriterDuckDBAppender = "duckdb-appender"
	WriterArrowParquet   = "arrow-parquet"
)

type envelopeTable struct {
	Name         string
	Columns      []string
	LedgerColumn string
	Rows         [][]driver.Value
}

// WriteLedgerBatchShard materializes every typed Bronze table plus the
// per-ledger metadata and watermark tables needed for complete coverage. The
// watermark timestamp is pinned by the job so retries remain byte-stable.
func WriteLedgerBatchShard(ctx context.Context, cfg LedgerBatchConfig, batches []*componentsv1.LedgerBatch) (files []backfillmanifest.File, err error) {
	index := 0
	result, err := WriteLedgerBatchStream(ctx, cfg, func() (*componentsv1.LedgerBatch, error) {
		if index >= len(batches) {
			return nil, io.EOF
		}
		batch := batches[index]
		index++
		return batch, nil
	})
	if err != nil {
		return nil, err
	}
	return result.Files, nil
}

func expectedCompleteTableCounts(batches []*componentsv1.LedgerBatch, decoded []bronze.DecodedRow) (map[string]uint64, error) {
	totalRows := 0
	for _, batch := range batches {
		totalRows += len(batch.BronzeRows)
	}
	if len(decoded) != totalRows {
		return nil, fmt.Errorf("decoded Bronze row count %d does not match source count %d", len(decoded), totalRows)
	}
	expected := map[string]uint64{
		"main.ingest_watermarks": uint64(len(batches)),
		"main.ledger_batches":    uint64(len(batches)),
	}
	rowIndex := 0
	for _, batch := range batches {
		for _, row := range batch.BronzeRows {
			decodedRow := decoded[rowIndex]
			if decodedRow.Err != nil {
				return nil, fmt.Errorf("typed Bronze row %d (%s): %w", rowIndex, row.TableName, decodedRow.Err)
			}
			if !decodedRow.OK {
				return nil, fmt.Errorf("typed Bronze row %d targets unsupported table %q", rowIndex, row.TableName)
			}
			expected["bronze."+decodedRow.Spec.TableName]++
			rowIndex++
		}
	}
	return expected, nil
}

func validateCompleteTableCounts(expected map[string]uint64, files []backfillmanifest.File) error {
	actual := make(map[string]uint64, len(files))
	for _, file := range files {
		actual[file.Table] += file.Rows
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("complete shard has %d output tables, want %d: actual=%v expected=%v", len(actual), len(expected), actual, expected)
	}
	for table, rows := range expected {
		if actual[table] != rows {
			return fmt.Errorf("complete shard table %s has %d rows, want %d", table, actual[table], rows)
		}
	}
	return nil
}

func validateLedgerBatches(cfg LedgerBatchConfig, batches []*componentsv1.LedgerBatch) error {
	if err := validateConfig(cfg.Parquet); err != nil {
		return err
	}
	if cfg.DecodeWorkers <= 0 {
		return fmt.Errorf("decode workers must be positive")
	}
	if cfg.WatermarkWrittenAt.IsZero() {
		return fmt.Errorf("pinned watermark timestamp is required")
	}
	wantCount := uint64(cfg.Parquet.LedgerEnd) - uint64(cfg.Parquet.LedgerStart) + 1
	if uint64(len(batches)) != wantCount {
		return fmt.Errorf("batch count %d does not cover shard %d-%d", len(batches), cfg.Parquet.LedgerStart, cfg.Parquet.LedgerEnd)
	}
	network := ""
	for index, batch := range batches {
		if batch == nil {
			return fmt.Errorf("batch %d is nil", index)
		}
		wantLedger := cfg.Parquet.LedgerStart + uint32(index)
		if batch.LedgerSequence != wantLedger {
			return fmt.Errorf("batch %d has ledger %d, want %d", index, batch.LedgerSequence, wantLedger)
		}
		if strings.TrimSpace(batch.NetworkPassphrase) == "" {
			return fmt.Errorf("ledger %d has empty network passphrase", batch.LedgerSequence)
		}
		if index == 0 {
			network = batch.NetworkPassphrase
		} else if batch.NetworkPassphrase != network {
			return fmt.Errorf("ledger %d changes network passphrase", batch.LedgerSequence)
		}
	}
	return nil
}

func writeEnvelopeTables(ctx context.Context, cfg LedgerBatchConfig, batches []*componentsv1.LedgerBatch) (files []backfillmanifest.File, err error) {
	absOutputDir, err := filepath.Abs(cfg.Parquet.OutputDir)
	if err != nil {
		return nil, fmt.Errorf("resolve shard output directory: %w", err)
	}
	databasePath, err := reserveTemporaryPath(absOutputDir, ".backfill-envelope-*.duckdb")
	if err != nil {
		return nil, fmt.Errorf("reserve envelope database path: %w", err)
	}
	defer func() {
		_ = os.Remove(databasePath)
		_ = os.Remove(databasePath + ".wal")
	}()

	db, err := sql.Open("duckdb", databasePath)
	if err != nil {
		return nil, fmt.Errorf("open envelope DuckDB: %w", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open envelope DuckDB connection: %w", err)
	}
	defer conn.Close()

	for _, statement := range []string{bronze.CreateLedgerBatchesSQL, bronze.CreateIngestWatermarksSQL} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return nil, fmt.Errorf("create envelope table: %w", err)
		}
	}
	tables := envelopeRows(cfg.WatermarkWrittenAt.UTC(), batches)
	for _, table := range tables {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(
			"ALTER TABLE main.%s ADD COLUMN %s UBIGINT",
			bronze.QuoteIdentifier(table.Name),
			bronze.QuoteIdentifier(ordinalColumn),
		)); err != nil {
			return nil, fmt.Errorf("add stable ordinal to %s: %w", table.Name, err)
		}
	}
	if err := appendEnvelopeRows(conn, tables); err != nil {
		return nil, err
	}

	created := make([]string, 0, len(tables))
	defer func() {
		if err == nil {
			return
		}
		for _, name := range created {
			_ = os.Remove(name)
		}
	}()
	for _, table := range tables {
		artifact, finalPath, writeErr := writeEnvelopeParquet(ctx, conn, absOutputDir, cfg.Parquet, table)
		if writeErr != nil {
			return nil, writeErr
		}
		created = append(created, finalPath)
		files = append(files, artifact)
	}
	return files, nil
}

func envelopeRows(writtenAt time.Time, batches []*componentsv1.LedgerBatch) []envelopeTable {
	ledgerRows := make([][]driver.Value, 0, len(batches))
	watermarkRows := make([][]driver.Value, 0, len(batches))
	for index, batch := range batches {
		ledgerRows = append(ledgerRows, []driver.Value{
			batch.NetworkPassphrase,
			batch.LedgerSequence,
			batch.ClosedAtUnix,
			batch.SchemaVersion,
			batch.ExtractionVersion,
			len(batch.Transactions),
			len(batch.Operations),
			len(batch.BronzeRows),
			nil,
			uint64(index),
		})
		watermarkRows = append(watermarkRows, []driver.Value{
			batch.NetworkPassphrase,
			batch.LedgerSequence,
			writtenAt,
			uint64(index),
		})
	}
	return []envelopeTable{
		{
			Name: "ingest_watermarks",
			Columns: []string{
				"network_passphrase", "ledger_sequence", "written_at", ordinalColumn,
			},
			LedgerColumn: "ledger_sequence",
			Rows:         watermarkRows,
		},
		{
			Name: "ledger_batches",
			Columns: []string{
				"network_passphrase", "ledger_sequence", "closed_at_unix", "schema_version",
				"extraction_version", "transaction_count", "operation_count", "bronze_row_count",
				"payload_json", ordinalColumn,
			},
			LedgerColumn: "ledger_sequence",
			Rows:         ledgerRows,
		},
	}
}

func appendEnvelopeRows(conn *sql.Conn, tables []envelopeTable) error {
	return conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected DuckDB driver connection type %T", driverConn)
		}
		for _, table := range tables {
			appender, err := duckdb.NewAppenderWithColumns(dc, "", "main", table.Name, table.Columns)
			if err != nil {
				return fmt.Errorf("create envelope appender for %s: %w", table.Name, err)
			}
			for index, row := range table.Rows {
				if err := appender.AppendRow(row...); err != nil {
					_ = appender.Close()
					return fmt.Errorf("append envelope row %d to %s: %w", index, table.Name, err)
				}
			}
			if err := appender.Close(); err != nil {
				return fmt.Errorf("close envelope appender for %s: %w", table.Name, err)
			}
		}
		return nil
	})
}

func writeEnvelopeParquet(ctx context.Context, conn *sql.Conn, outputDir string, cfg ParquetConfig, table envelopeTable) (backfillmanifest.File, string, error) {
	finalName := fmt.Sprintf("%010d-%010d-%s-00000.parquet", cfg.LedgerStart, cfg.LedgerEnd, table.Name)
	finalPath := filepath.Join(outputDir, finalName)
	if _, err := os.Stat(finalPath); err == nil {
		return backfillmanifest.File{}, "", fmt.Errorf("refusing to overwrite existing shard file %s", finalPath)
	} else if !os.IsNotExist(err) {
		return backfillmanifest.File{}, "", fmt.Errorf("inspect shard file %s: %w", finalPath, err)
	}
	temporaryPath, err := reserveTemporaryPath(outputDir, "."+finalName+".partial-*")
	if err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("reserve partial Parquet path: %w", err)
	}
	defer os.Remove(temporaryPath)

	outputColumns := table.Columns[:len(table.Columns)-1]
	quotedColumns := make([]string, len(outputColumns))
	for index, column := range outputColumns {
		quotedColumns[index] = bronze.QuoteIdentifier(column)
	}
	compression := strings.ToUpper(cfg.Compression)
	if compression == "" {
		compression = "ZSTD"
	}
	copySQL := fmt.Sprintf(
		"COPY (SELECT %s FROM main.%s ORDER BY %s) TO %s (FORMAT PARQUET, COMPRESSION %s)",
		strings.Join(quotedColumns, ", "),
		bronze.QuoteIdentifier(table.Name),
		bronze.QuoteIdentifier(ordinalColumn),
		bronze.SQLLiteral(temporaryPath),
		compression,
	)
	if _, err := conn.ExecContext(ctx, copySQL); err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("write Parquet for %s: %w", table.Name, err)
	}

	query := fmt.Sprintf(
		"SELECT count(*), min(%s), max(%s) FROM main.%s",
		bronze.QuoteIdentifier(table.LedgerColumn),
		bronze.QuoteIdentifier(table.LedgerColumn),
		bronze.QuoteIdentifier(table.Name),
	)
	var rowCount uint64
	var minLedger, maxLedger uint32
	if err := conn.QueryRowContext(ctx, query).Scan(&rowCount, &minLedger, &maxLedger); err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("read staged range for %s: %w", table.Name, err)
	}
	if minLedger != cfg.LedgerStart || maxLedger != cfg.LedgerEnd {
		return backfillmanifest.File{}, "", fmt.Errorf("table %s range %d-%d does not cover shard %d-%d", table.Name, minLedger, maxLedger, cfg.LedgerStart, cfg.LedgerEnd)
	}
	schemaFingerprint, err := parquetSchemaFingerprint(ctx, conn, temporaryPath)
	if err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("fingerprint Parquet schema for %s: %w", table.Name, err)
	}
	sha, bytes, err := hashFile(temporaryPath)
	if err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("hash Parquet for %s: %w", table.Name, err)
	}
	if cfg.FileTargetBytes > 0 && bytes > cfg.FileTargetBytes {
		return backfillmanifest.File{}, "", fmt.Errorf("Parquet for %s is %d bytes, exceeds the unrolled file target %d", table.Name, bytes, cfg.FileTargetBytes)
	}
	if err := publishNoReplace(temporaryPath, finalPath); err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("publish Parquet for %s: %w", table.Name, err)
	}
	return backfillmanifest.File{
		Table:                    "main." + table.Name,
		URI:                      (&url.URL{Scheme: "file", Path: finalPath}).String(),
		SHA256:                   sha,
		Bytes:                    bytes,
		Rows:                     rowCount,
		MinLedger:                minLedger,
		MaxLedger:                maxLedger,
		ParquetSchemaFingerprint: schemaFingerprint,
	}, finalPath, nil
}

func removeLocalArtifacts(files []backfillmanifest.File) {
	for _, file := range files {
		parsed, err := url.Parse(file.URI)
		if err == nil && parsed.Scheme == "file" {
			_ = os.Remove(parsed.Path)
		}
	}
}
