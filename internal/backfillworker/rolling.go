package backfillworker

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

type rollingTable struct {
	PublicName   string
	Schema       string
	Name         string
	Columns      []string
	LedgerColumn string
}

type temporaryPart struct {
	index int
	path  string
}

func writeTypedParquetFiles(ctx context.Context, conn *sql.Conn, outputDir string, cfg ParquetConfig, spec bronze.TypedTableSpec) ([]backfillmanifest.File, error) {
	if cfg.FileTargetBytes == 0 {
		artifact, _, err := writeTableParquet(ctx, conn, outputDir, cfg, spec)
		if err != nil {
			return nil, err
		}
		return []backfillmanifest.File{artifact}, nil
	}
	return writeRollingParquetFiles(ctx, conn, outputDir, cfg, rollingTable{
		PublicName:   "bronze." + spec.TableName,
		Schema:       "bronze",
		Name:         spec.TableName,
		Columns:      spec.Columns,
		LedgerColumn: spec.LedgerColumn,
	})
}

func writeEnvelopeParquetFiles(ctx context.Context, conn *sql.Conn, outputDir string, cfg ParquetConfig, table envelopeTable) ([]backfillmanifest.File, error) {
	if cfg.FileTargetBytes == 0 {
		artifact, _, err := writeEnvelopeParquet(ctx, conn, outputDir, cfg, table)
		if err != nil {
			return nil, err
		}
		return []backfillmanifest.File{artifact}, nil
	}
	return writeRollingParquetFiles(ctx, conn, outputDir, cfg, rollingTable{
		PublicName:   "main." + table.Name,
		Schema:       "main",
		Name:         table.Name,
		Columns:      table.Columns[:len(table.Columns)-1],
		LedgerColumn: table.LedgerColumn,
	})
}

// writeRollingParquetFiles uses DuckDB's FILE_SIZE_BYTES target. DuckDB rolls
// only at a row-group boundary, so FileMaxBytes remains the fail-closed bound.
// A single COPY thread and canonical public-column ordering keep part
// boundaries and hashes independent of nondeterministic extractor map walks.
// The private ordinal only breaks ties between identical public rows.
func writeRollingParquetFiles(ctx context.Context, conn *sql.Conn, outputDir string, cfg ParquetConfig, table rollingTable) (files []backfillmanifest.File, resultErr error) {
	if cfg.RowGroupRows > 0 && cfg.RowGroupRows < 2048 {
		return nil, fmt.Errorf("Parquet row group rows %d is below DuckDB's minimum 2048", cfg.RowGroupRows)
	}
	temporaryDir, err := os.MkdirTemp(outputDir, ".backfill-parts-*")
	if err != nil {
		return nil, fmt.Errorf("reserve rolling Parquet directory: %w", err)
	}
	if err := os.Remove(temporaryDir); err != nil {
		return nil, fmt.Errorf("prepare rolling Parquet directory: %w", err)
	}
	defer os.RemoveAll(temporaryDir)

	quotedColumns := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		quotedColumns[index] = bronze.QuoteIdentifier(column)
	}
	orderColumns := make([]string, 0, len(quotedColumns)+1)
	orderColumns = append(orderColumns, quotedColumns...)
	orderColumns = append(orderColumns, bronze.QuoteIdentifier(ordinalColumn))
	compression := strings.ToUpper(cfg.Compression)
	if compression == "" {
		compression = "ZSTD"
	}
	options := []string{
		"FORMAT PARQUET",
		"COMPRESSION " + compression,
		fmt.Sprintf("FILE_SIZE_BYTES %d", cfg.FileTargetBytes),
		"FILENAME_PATTERN 'part-{i}'",
	}
	if cfg.RowGroupRows > 0 {
		options = append(options, fmt.Sprintf("ROW_GROUP_SIZE %d", cfg.RowGroupRows))
	}
	if _, err := conn.ExecContext(ctx, "SET threads = 1"); err != nil {
		return nil, fmt.Errorf("set deterministic Parquet writer threads: %w", err)
	}
	copySQL := fmt.Sprintf(
		"COPY (SELECT %s FROM %s.%s ORDER BY %s) TO %s (%s)",
		strings.Join(quotedColumns, ", "),
		bronze.QuoteIdentifier(table.Schema),
		bronze.QuoteIdentifier(table.Name),
		strings.Join(orderColumns, ", "),
		bronze.SQLLiteral(temporaryDir),
		strings.Join(options, ", "),
	)
	if _, err := conn.ExecContext(ctx, copySQL); err != nil {
		return nil, fmt.Errorf("write rolling Parquet for %s: %w", table.PublicName, err)
	}
	parts, err := listTemporaryParts(temporaryDir)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("rolling Parquet for %s produced no files", table.PublicName)
	}

	created := make([]string, 0, len(parts))
	defer func() {
		if resultErr == nil {
			return
		}
		for _, name := range created {
			_ = os.Remove(name)
		}
	}()
	for outputIndex, part := range parts {
		rowCount, minLedger, maxLedger, err := parquetRange(ctx, conn, part.path, table.LedgerColumn)
		if err != nil {
			return nil, fmt.Errorf("read rolling Parquet range for %s part %d: %w", table.PublicName, part.index, err)
		}
		if minLedger < cfg.LedgerStart || maxLedger > cfg.LedgerEnd {
			return nil, fmt.Errorf("Parquet part for %s has range %d-%d outside shard %d-%d", table.PublicName, minLedger, maxLedger, cfg.LedgerStart, cfg.LedgerEnd)
		}
		schemaFingerprint, err := parquetSchemaFingerprint(ctx, conn, part.path)
		if err != nil {
			return nil, fmt.Errorf("fingerprint rolling Parquet schema for %s part %d: %w", table.PublicName, part.index, err)
		}
		sha, bytes, err := hashFile(part.path)
		if err != nil {
			return nil, fmt.Errorf("hash rolling Parquet for %s part %d: %w", table.PublicName, part.index, err)
		}
		if cfg.FileMaxBytes > 0 && bytes > cfg.FileMaxBytes {
			return nil, fmt.Errorf("Parquet for %s part %d is %d bytes, exceeds hard maximum %d", table.PublicName, part.index, bytes, cfg.FileMaxBytes)
		}
		finalName := fmt.Sprintf("%010d-%010d-%s-%05d.parquet", cfg.LedgerStart, cfg.LedgerEnd, table.Name, outputIndex)
		finalPath := filepath.Join(outputDir, finalName)
		if err := publishNoReplace(part.path, finalPath); err != nil {
			return nil, fmt.Errorf("publish Parquet for %s part %d: %w", table.PublicName, part.index, err)
		}
		created = append(created, finalPath)
		files = append(files, backfillmanifest.File{
			Table:                    table.PublicName,
			URI:                      (&url.URL{Scheme: "file", Path: finalPath}).String(),
			SHA256:                   sha,
			Bytes:                    bytes,
			Rows:                     rowCount,
			MinLedger:                minLedger,
			MaxLedger:                maxLedger,
			ParquetSchemaFingerprint: schemaFingerprint,
		})
	}
	return files, nil
}

func listTemporaryParts(dir string) ([]temporaryPart, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list rolling Parquet directory: %w", err)
	}
	parts := make([]temporaryPart, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, fmt.Errorf("rolling Parquet output contains unexpected directory %q", entry.Name())
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "part-") || !strings.HasSuffix(name, ".parquet") {
			return nil, fmt.Errorf("rolling Parquet output contains unexpected file %q", name)
		}
		indexText := strings.TrimSuffix(strings.TrimPrefix(name, "part-"), ".parquet")
		index, err := strconv.Atoi(indexText)
		if err != nil || index < 0 {
			return nil, fmt.Errorf("rolling Parquet output has invalid part name %q", name)
		}
		parts = append(parts, temporaryPart{index: index, path: filepath.Join(dir, name)})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].index < parts[j].index })
	for index, part := range parts {
		if part.index != index {
			return nil, fmt.Errorf("rolling Parquet parts are not contiguous at %d (found %d)", index, part.index)
		}
	}
	return parts, nil
}

func parquetRange(ctx context.Context, conn *sql.Conn, fileName, ledgerColumn string) (uint64, uint32, uint32, error) {
	query := fmt.Sprintf(
		"SELECT count(*), min(%s), max(%s) FROM read_parquet(%s)",
		bronze.QuoteIdentifier(ledgerColumn),
		bronze.QuoteIdentifier(ledgerColumn),
		bronze.SQLLiteral(fileName),
	)
	var count uint64
	var minLedger, maxLedger uint32
	if err := conn.QueryRowContext(ctx, query).Scan(&count, &minLedger, &maxLedger); err != nil {
		return 0, 0, 0, err
	}
	return count, minLedger, maxLedger, nil
}
