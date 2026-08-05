// Package backfillworker contains catalog-independent shard production. A
// worker owns a disposable local DuckDB database, writes typed rows through
// the Appender API, and publishes immutable Parquet files. It never attaches
// to the shared DuckLake catalog.
package backfillworker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

const ordinalColumn = "__backfill_ordinal"

type ParquetConfig struct {
	OutputDir       string
	LedgerStart     uint32
	LedgerEnd       uint32
	Compression     string
	FileTargetBytes uint64
	FileMaxBytes    uint64
	RowGroupRows    uint64
}

// WriteParquetShard writes one deterministic Parquet file per non-empty typed
// Bronze table. The local DuckDB file and partial outputs are disposable; only
// successfully linked final files are returned.
func WriteParquetShard(ctx context.Context, cfg ParquetConfig, decoded []bronze.DecodedRow) (files []backfillmanifest.File, err error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o750); err != nil {
		return nil, fmt.Errorf("create shard output directory: %w", err)
	}
	absOutputDir, err := filepath.Abs(cfg.OutputDir)
	if err != nil {
		return nil, fmt.Errorf("resolve shard output directory: %w", err)
	}

	grouped, specs, order, err := groupDecodedRows(decoded)
	if err != nil {
		return nil, err
	}
	if len(order) == 0 {
		return nil, fmt.Errorf("shard contains no typed Bronze rows")
	}

	databasePath, err := reserveTemporaryPath(absOutputDir, ".backfill-worker-*.duckdb")
	if err != nil {
		return nil, fmt.Errorf("reserve worker database path: %w", err)
	}
	defer func() {
		_ = os.Remove(databasePath)
		_ = os.Remove(databasePath + ".wal")
	}()

	db, err := sql.Open("duckdb", databasePath)
	if err != nil {
		return nil, fmt.Errorf("open worker DuckDB: %w", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open worker DuckDB connection: %w", err)
	}
	defer conn.Close()

	if err := createLocalSchema(ctx, conn, order); err != nil {
		return nil, err
	}
	if err := appendGroupedRows(conn, grouped, specs, order); err != nil {
		return nil, err
	}

	created := make([]string, 0, len(order))
	defer func() {
		if err == nil {
			return
		}
		for _, name := range created {
			_ = os.Remove(name)
		}
	}()

	for _, tableName := range order {
		artifact, finalPath, writeErr := writeTableParquet(ctx, conn, absOutputDir, cfg, specs[tableName])
		if writeErr != nil {
			return nil, writeErr
		}
		created = append(created, finalPath)
		files = append(files, artifact)
	}
	return files, nil
}

func validateConfig(cfg ParquetConfig) error {
	if strings.TrimSpace(cfg.OutputDir) == "" {
		return fmt.Errorf("output directory is required")
	}
	if cfg.LedgerStart == 0 || cfg.LedgerEnd < cfg.LedgerStart {
		return fmt.Errorf("invalid shard ledger range %d-%d", cfg.LedgerStart, cfg.LedgerEnd)
	}
	switch strings.ToLower(cfg.Compression) {
	case "", "zstd", "snappy", "uncompressed":
		return nil
	default:
		return fmt.Errorf("unsupported Parquet compression %q", cfg.Compression)
	}
}

func groupDecodedRows(decoded []bronze.DecodedRow) (map[string][]bronze.DecodedRow, map[string]bronze.TypedTableSpec, []string, error) {
	grouped := make(map[string][]bronze.DecodedRow)
	specs := make(map[string]bronze.TypedTableSpec)
	for index, row := range decoded {
		if row.Err != nil {
			return nil, nil, nil, fmt.Errorf("typed Bronze row %d: %w", index, row.Err)
		}
		if !row.OK {
			continue
		}
		known, ok := bronze.TypedTableSpecs[row.Spec.TableName]
		if !ok {
			return nil, nil, nil, fmt.Errorf("typed Bronze row %d targets unknown table %q", index, row.Spec.TableName)
		}
		if !slices.Equal(known.Columns, row.Spec.Columns) || known.LedgerColumn != row.Spec.LedgerColumn {
			return nil, nil, nil, fmt.Errorf("typed Bronze row %d has a divergent spec for %q", index, row.Spec.TableName)
		}
		if len(row.Values) != len(known.Columns) {
			return nil, nil, nil, fmt.Errorf("typed Bronze row %d for %q has %d values, want %d", index, row.Spec.TableName, len(row.Values), len(known.Columns))
		}
		grouped[known.TableName] = append(grouped[known.TableName], row)
		specs[known.TableName] = known
	}
	order := make([]string, 0, len(grouped))
	for tableName := range grouped {
		order = append(order, tableName)
	}
	sort.Strings(order)
	return grouped, specs, order, nil
}

func createLocalSchema(ctx context.Context, conn *sql.Conn, tableNames []string) error {
	if _, err := conn.ExecContext(ctx, "CREATE SCHEMA bronze"); err != nil {
		return fmt.Errorf("create worker Bronze schema: %w", err)
	}
	for _, migration := range bronze.Migrations {
		for _, stmt := range bronze.SplitSQLStatements(migration.SQL) {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("apply worker schema statement %q: %w", stmt, err)
			}
		}
	}
	for _, tableName := range tableNames {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(
			"ALTER TABLE bronze.%s ADD COLUMN %s UBIGINT",
			bronze.QuoteIdentifier(tableName),
			bronze.QuoteIdentifier(ordinalColumn),
		)); err != nil {
			return fmt.Errorf("add stable ordinal to %s: %w", tableName, err)
		}
	}
	return nil
}

func appendGroupedRows(conn *sql.Conn, grouped map[string][]bronze.DecodedRow, specs map[string]bronze.TypedTableSpec, order []string) error {
	return conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected DuckDB driver connection type %T", driverConn)
		}
		for _, tableName := range order {
			spec := specs[tableName]
			columns := append(slices.Clone(spec.Columns), ordinalColumn)
			appender, err := duckdb.NewAppenderWithColumns(dc, "", "bronze", tableName, columns)
			if err != nil {
				return fmt.Errorf("create worker appender for %s: %w", tableName, err)
			}
			values := make([]driver.Value, len(columns))
			for index, row := range grouped[tableName] {
				for column, value := range row.Values {
					values[column] = value
				}
				values[len(values)-1] = uint64(index)
				if err := appender.AppendRow(values...); err != nil {
					_ = appender.Close()
					return fmt.Errorf("append worker row %d to %s: %w", index, tableName, err)
				}
			}
			if err := appender.Close(); err != nil {
				return fmt.Errorf("close worker appender for %s: %w", tableName, err)
			}
		}
		return nil
	})
}

func writeTableParquet(ctx context.Context, conn *sql.Conn, outputDir string, cfg ParquetConfig, spec bronze.TypedTableSpec) (backfillmanifest.File, string, error) {
	finalName := fmt.Sprintf("%010d-%010d-%s-00000.parquet", cfg.LedgerStart, cfg.LedgerEnd, spec.TableName)
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

	columns := make([]string, len(spec.Columns))
	for index, column := range spec.Columns {
		columns[index] = bronze.QuoteIdentifier(column)
	}
	compression := strings.ToUpper(cfg.Compression)
	if compression == "" {
		compression = "ZSTD"
	}
	copySQL := fmt.Sprintf(
		"COPY (SELECT %s FROM bronze.%s ORDER BY %s) TO %s (FORMAT PARQUET, COMPRESSION %s)",
		strings.Join(columns, ", "),
		bronze.QuoteIdentifier(spec.TableName),
		bronze.QuoteIdentifier(ordinalColumn),
		bronze.SQLLiteral(temporaryPath),
		compression,
	)
	if _, err := conn.ExecContext(ctx, copySQL); err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("write Parquet for %s: %w", spec.TableName, err)
	}

	rowCount, minLedger, maxLedger, err := tableRange(ctx, conn, spec)
	if err != nil {
		return backfillmanifest.File{}, "", err
	}
	if minLedger < cfg.LedgerStart || maxLedger > cfg.LedgerEnd {
		return backfillmanifest.File{}, "", fmt.Errorf("table %s range %d-%d falls outside shard %d-%d", spec.TableName, minLedger, maxLedger, cfg.LedgerStart, cfg.LedgerEnd)
	}
	schemaFingerprint, err := parquetSchemaFingerprint(ctx, conn, temporaryPath)
	if err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("fingerprint Parquet schema for %s: %w", spec.TableName, err)
	}
	sha, bytes, err := hashFile(temporaryPath)
	if err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("hash Parquet for %s: %w", spec.TableName, err)
	}
	if cfg.FileTargetBytes > 0 && bytes > cfg.FileTargetBytes {
		return backfillmanifest.File{}, "", fmt.Errorf("Parquet for %s is %d bytes, exceeds the unrolled file target %d", spec.TableName, bytes, cfg.FileTargetBytes)
	}
	if err := publishNoReplace(temporaryPath, finalPath); err != nil {
		return backfillmanifest.File{}, "", fmt.Errorf("publish Parquet for %s: %w", spec.TableName, err)
	}

	return backfillmanifest.File{
		Table:                    "bronze." + spec.TableName,
		URI:                      (&url.URL{Scheme: "file", Path: finalPath}).String(),
		SHA256:                   sha,
		Bytes:                    bytes,
		Rows:                     rowCount,
		MinLedger:                minLedger,
		MaxLedger:                maxLedger,
		ParquetSchemaFingerprint: schemaFingerprint,
	}, finalPath, nil
}

func tableRange(ctx context.Context, conn *sql.Conn, spec bronze.TypedTableSpec) (uint64, uint32, uint32, error) {
	query := fmt.Sprintf(
		"SELECT count(*), min(%s), max(%s) FROM bronze.%s",
		bronze.QuoteIdentifier(spec.LedgerColumn),
		bronze.QuoteIdentifier(spec.LedgerColumn),
		bronze.QuoteIdentifier(spec.TableName),
	)
	var count uint64
	var minLedger, maxLedger uint32
	if err := conn.QueryRowContext(ctx, query).Scan(&count, &minLedger, &maxLedger); err != nil {
		return 0, 0, 0, fmt.Errorf("read staged range for %s: %w", spec.TableName, err)
	}
	return count, minLedger, maxLedger, nil
}

type schemaColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable string `json:"nullable"`
}

func parquetSchemaFingerprint(ctx context.Context, conn *sql.Conn, fileName string) (string, error) {
	rows, err := conn.QueryContext(ctx, "DESCRIBE SELECT * FROM read_parquet("+bronze.SQLLiteral(fileName)+")")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var columns []schemaColumn
	for rows.Next() {
		var name, dataType, nullable string
		var key, defaultValue, extra sql.NullString
		if err := rows.Scan(&name, &dataType, &nullable, &key, &defaultValue, &extra); err != nil {
			return "", err
		}
		columns = append(columns, schemaColumn{Name: name, Type: dataType, Nullable: nullable})
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("Parquet schema is empty")
	}
	return backfillmanifest.CanonicalDigest(columns)
}

func hashFile(name string) (string, uint64, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}
	return "sha256:" + fmt.Sprintf("%x", hash.Sum(nil)), uint64(written), nil
}

func reserveTemporaryPath(dir, pattern string) (string, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	name := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	if err := os.Remove(name); err != nil {
		return "", err
	}
	return name, nil
}

func publishNoReplace(temporaryPath, finalPath string) error {
	if err := os.Link(temporaryPath, finalPath); err != nil {
		return err
	}
	if err := os.Remove(temporaryPath); err != nil {
		_ = os.Remove(finalPath)
		return err
	}
	return nil
}
