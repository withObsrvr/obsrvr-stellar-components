package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	cfg := configFromEnv()
	if err := run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}

type config struct {
	QuackURI          string
	QuackToken        string
	QuackRemoteDB     string
	DisableSSL        bool
	SourceCatalog     string
	SourceTables      []sourceTable
	ReplicaName       string
	StartSnapshot     uint64
	TargetMode        string
	TargetCatalogPath string
	TargetDataPath    string
	TargetAttachName  string
	TargetQuackURI    string
	TargetQuackToken  string
	TargetQuackRemote string
	TargetDisableSSL  bool
	LedgerBatchSize   int
}

type sourceTable struct {
	Name         string
	LedgerColumn string
}

type checkpoint struct {
	SnapshotID uint64
	Exists     bool
}

func configFromEnv() config {
	defaultLedgerColumn := getenv("LEDGER_COLUMN", "ledger_sequence")
	overrides := parseLedgerColumnOverrides(getenv("LEDGER_COLUMN_OVERRIDES", ""))
	startSnapshot, err := parseUintEnv("START_SNAPSHOT", "0")
	if err != nil {
		log.Fatal(err)
	}
	tables, err := parseSourceTables(getenv("SOURCE_TABLES", ""), defaultLedgerColumn, overrides)
	if err != nil {
		log.Fatal(err)
	}
	return config{
		QuackURI:          getenv("QUACK_URI", "quack:127.0.0.1:9494"),
		QuackToken:        getenv("QUACK_TOKEN", ""),
		QuackRemoteDB:     sanitizeIdentifier(getenv("QUACK_REMOTE_DB", "remote_lake")),
		DisableSSL:        getenvBool("QUACK_DISABLE_SSL", true),
		SourceCatalog:     sanitizeIdentifier(getenv("SOURCE_CATALOG", "stellar_lake")),
		SourceTables:      tables,
		ReplicaName:       getenv("REPLICA_NAME", "serving_replica"),
		StartSnapshot:     startSnapshot,
		TargetMode:        strings.ToLower(getenv("TARGET_MODE", "embedded")),
		TargetCatalogPath: getenv("TARGET_DUCKLAKE_CATALOG_PATH", ""),
		TargetDataPath:    getenv("TARGET_DUCKLAKE_DATA_PATH", ""),
		TargetAttachName:  sanitizeIdentifier(getenv("TARGET_ATTACH_NAME", "serving_lake")),
		TargetQuackURI:    getenv("TARGET_QUACK_URI", ""),
		TargetQuackToken:  getenv("TARGET_QUACK_TOKEN", ""),
		TargetQuackRemote: sanitizeIdentifier(getenv("TARGET_QUACK_REMOTE_DB", "target_lake")),
		TargetDisableSSL:  getenvBool("TARGET_QUACK_DISABLE_SSL", getenvBool("QUACK_DISABLE_SSL", true)),
		LedgerBatchSize:   int(mustParseUintEnv("LEDGER_BATCH_SIZE", "1000")),
	}
}

func run(ctx context.Context, cfg config) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open DuckDB: %w", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()

	if err := initDuckDB(ctx, db, cfg); err != nil {
		return err
	}

	currentSnapshot, err := currentSnapshot(ctx, db, cfg)
	if err != nil {
		return err
	}
	log.Printf("primary snapshot=%d replica=%s tables=%d", currentSnapshot, cfg.ReplicaName, len(cfg.SourceTables))

	var tableErrors []string
	for _, table := range cfg.SourceTables {
		if err := syncTable(ctx, db, cfg, table, currentSnapshot); err != nil {
			message := redactSecrets(cfg, err.Error())
			log.Printf("table=%s sync failed: %s", table.Name, message)
			tableErrors = append(tableErrors, fmt.Sprintf("%s: %s", table.Name, message))
		}
	}
	if len(tableErrors) > 0 {
		return fmt.Errorf("replica sync completed with %d table error(s): %s", len(tableErrors), strings.Join(tableErrors, "; "))
	}
	return nil
}

func validateConfig(cfg config) error {
	if cfg.QuackToken == "" {
		return fmt.Errorf("QUACK_TOKEN is required")
	}
	if len(cfg.SourceTables) == 0 {
		return fmt.Errorf("SOURCE_TABLES is required")
	}
	switch cfg.TargetMode {
	case "embedded":
		if cfg.TargetCatalogPath == "" {
			return fmt.Errorf("TARGET_DUCKLAKE_CATALOG_PATH is required")
		}
		if cfg.TargetDataPath == "" {
			return fmt.Errorf("TARGET_DUCKLAKE_DATA_PATH is required")
		}
		if !filepath.IsAbs(cfg.TargetCatalogPath) {
			return fmt.Errorf("TARGET_DUCKLAKE_CATALOG_PATH must be absolute")
		}
		if !filepath.IsAbs(cfg.TargetDataPath) {
			return fmt.Errorf("TARGET_DUCKLAKE_DATA_PATH must be absolute")
		}
	case "quack":
		if cfg.TargetQuackURI == "" {
			return fmt.Errorf("TARGET_QUACK_URI is required when TARGET_MODE=quack")
		}
		if cfg.TargetQuackToken == "" {
			return fmt.Errorf("TARGET_QUACK_TOKEN is required when TARGET_MODE=quack")
		}
	default:
		return fmt.Errorf("unsupported TARGET_MODE %q", cfg.TargetMode)
	}
	if cfg.LedgerBatchSize <= 0 {
		return fmt.Errorf("LEDGER_BATCH_SIZE must be greater than zero")
	}
	return nil
}

func initDuckDB(ctx context.Context, db *sql.DB, cfg config) error {
	duckDBHomeBase := cfg.TargetDataPath
	if cfg.TargetMode == "quack" {
		duckDBHomeBase = filepath.Join(os.TempDir(), "ducklake-replica-sync")
	}
	if err := os.MkdirAll(duckDBHomeBase, 0o755); err != nil {
		return fmt.Errorf("create DuckDB home base directory: %w", err)
	}
	duckDBHome := filepath.Join(duckDBHomeBase, ".duckdb")
	if err := os.MkdirAll(duckDBHome, 0o755); err != nil {
		return fmt.Errorf("create DuckDB home directory: %w", err)
	}

	stmts := []string{
		fmt.Sprintf("SET home_directory='%s'", escapeSQLString(duckDBHome)),
		"INSTALL ducklake",
		"LOAD ducklake",
		"INSTALL quack",
		"LOAD quack",
		fmt.Sprintf(
			"ATTACH '%s' AS %s (TOKEN '%s', DISABLE_SSL %t)",
			escapeSQLString(cfg.QuackURI),
			cfg.QuackRemoteDB,
			escapeSQLString(cfg.QuackToken),
			cfg.DisableSSL,
		),
	}
	if cfg.TargetMode == "embedded" {
		if err := os.MkdirAll(filepath.Dir(cfg.TargetCatalogPath), 0o755); err != nil && filepath.Dir(cfg.TargetCatalogPath) != "." {
			return fmt.Errorf("create target DuckLake catalog directory: %w", err)
		}
		if err := os.MkdirAll(cfg.TargetDataPath, 0o755); err != nil {
			return fmt.Errorf("create target DuckLake data directory: %w", err)
		}
		stmts = append(stmts, fmt.Sprintf(
			"ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')",
			escapeSQLString(cfg.TargetCatalogPath),
			cfg.TargetAttachName,
			escapeSQLString(cfg.TargetDataPath),
		))
	} else {
		stmts = append(stmts, fmt.Sprintf(
			"ATTACH '%s' AS %s (TOKEN '%s', DISABLE_SSL %t)",
			escapeSQLString(cfg.TargetQuackURI),
			cfg.TargetQuackRemote,
			escapeSQLString(cfg.TargetQuackToken),
			cfg.TargetDisableSSL,
		))
	}
	for i, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init step %d (%s): %w", i+1, initStepName(stmt), err)
		}
	}
	return initTargetMetadata(ctx, db, cfg)
}

func initStepName(stmt string) string {
	stmt = strings.TrimSpace(stmt)
	switch {
	case strings.HasPrefix(stmt, "SET home_directory"):
		return "set home_directory"
	case stmt == "INSTALL ducklake":
		return "install ducklake"
	case stmt == "LOAD ducklake":
		return "load ducklake"
	case stmt == "INSTALL quack":
		return "install quack"
	case stmt == "LOAD quack":
		return "load quack"
	case strings.HasPrefix(stmt, "ATTACH 'ducklake:"):
		return "attach target DuckLake"
	case strings.HasPrefix(stmt, "ATTACH '"):
		return "attach Quack"
	default:
		fields := strings.Fields(stmt)
		if len(fields) == 0 {
			return "empty statement"
		}
		return strings.ToLower(fields[0])
	}
}

func syncTable(ctx context.Context, db *sql.DB, cfg config, table sourceTable, current uint64) error {
	if err := validateSourceTable(table); err != nil {
		return err
	}
	cp, err := loadCheckpoint(ctx, db, cfg, table)
	if err != nil {
		return err
	}
	fromSnapshot := cfg.StartSnapshot
	if cp.Exists {
		fromSnapshot = cp.SnapshotID
	}
	columns, err := ensureTargetSchema(ctx, db, cfg, table)
	if err != nil {
		return recordTableError(ctx, db, cfg, table, fromSnapshot, err)
	}
	if fromSnapshot > current {
		return recordTableError(ctx, db, cfg, table, fromSnapshot,
			fmt.Errorf("checkpoint %d for %s is ahead of current snapshot %d", fromSnapshot, table.Name, current))
	}
	if fromSnapshot >= current {
		log.Printf("table=%s already current snapshot=%d", table.Name, fromSnapshot)
		return nil
	}

	ledgers, err := changedLedgers(ctx, db, cfg, table, fromSnapshot+1, current)
	if err != nil {
		if !isMissingSnapshotError(err) {
			return recordTableError(ctx, db, cfg, table, fromSnapshot, err)
		}
		log.Printf("table=%s snapshot range [%d,%d] unavailable; starting full resync", table.Name, fromSnapshot+1, current)
		if err := fullResyncTable(ctx, db, cfg, table, columns); err != nil {
			return recordTableError(ctx, db, cfg, table, fromSnapshot, err)
		}
		if err := saveCheckpoint(ctx, db, cfg, table, current, "ok", ""); err != nil {
			return err
		}
		log.Printf("table=%s full_resync checkpoint=%d", table.Name, current)
		return nil
	}
	if len(ledgers) == 0 {
		if err := saveCheckpoint(ctx, db, cfg, table, current, "ok", ""); err != nil {
			return err
		}
		log.Printf("table=%s no changed ledgers checkpoint=%d", table.Name, current)
		return nil
	}

	if err := rebuildTargetLedgers(ctx, db, cfg, table, ledgers, columns); err != nil {
		return recordTableError(ctx, db, cfg, table, fromSnapshot, err)
	}
	if err := saveCheckpoint(ctx, db, cfg, table, current, "ok", ""); err != nil {
		return err
	}
	log.Printf("table=%s changed_ledgers=%d checkpoint=%d", table.Name, len(ledgers), current)
	return nil
}

func recordTableError(ctx context.Context, db *sql.DB, cfg config, table sourceTable, snapshot uint64, err error) error {
	message := redactSecrets(cfg, err.Error())
	if checkpointErr := saveCheckpoint(ctx, db, cfg, table, snapshot, "error", message); checkpointErr != nil {
		return fmt.Errorf("%s; additionally failed to record table error: %s", message, redactSecrets(cfg, checkpointErr.Error()))
	}
	return fmt.Errorf("%s", message)
}

func initTargetMetadata(ctx context.Context, db *sql.DB, cfg config) error {
	script := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s.replica;
CREATE TABLE IF NOT EXISTS %s.replica.sync_checkpoints (
	replica_name VARCHAR,
	source_catalog VARCHAR,
	source_table VARCHAR,
	last_snapshot_id UBIGINT,
	updated_at TIMESTAMP,
	status VARCHAR,
	error_message VARCHAR
);`, cfg.TargetAttachName, cfg.TargetAttachName)
	if err := execTargetScript(ctx, db, cfg, script); err != nil {
		return fmt.Errorf("init target metadata: %w", err)
	}
	return nil
}

func execTargetScript(ctx context.Context, db *sql.DB, cfg config, script string) error {
	if cfg.TargetMode == "quack" {
		_, err := db.ExecContext(ctx, fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.TargetQuackRemote), script)
		return err
	}
	_, err := db.ExecContext(ctx, script)
	return err
}

func queryTargetRows(ctx context.Context, db *sql.DB, cfg config, query string) (*sql.Rows, error) {
	if cfg.TargetMode == "quack" {
		return db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.TargetQuackRemote), query)
	}
	return db.QueryContext(ctx, query)
}

func ensureTargetSchema(ctx context.Context, db *sql.DB, cfg config, table sourceTable) ([]string, error) {
	if err := createTargetTableFromSource(ctx, db, cfg, table); err != nil {
		return nil, err
	}
	sourceColumns, err := loadSourceColumns(ctx, db, cfg, table)
	if err != nil {
		return nil, err
	}
	if len(sourceColumns) == 0 {
		return nil, fmt.Errorf("source table %s has no columns", table.Name)
	}
	targetColumns, err := loadTargetColumns(ctx, db, cfg, table)
	if err != nil {
		return nil, err
	}
	if diff := columnDiff(sourceColumns, targetColumns); diff != "" {
		return nil, fmt.Errorf("schema drift for %s: %s", table.Name, diff)
	}
	return sourceColumns, nil
}

func createTargetTableFromSource(ctx context.Context, db *sql.DB, cfg config, table sourceTable) error {
	sourceSelect := sourceSchemaSQL(cfg, table)
	targetTable := targetTableName(cfg, table)
	if cfg.TargetMode == "quack" {
		primaryRemote := "replica_primary"
		script := fmt.Sprintf(`ATTACH IF NOT EXISTS %s AS %s (TOKEN %s, DISABLE_SSL %t);
%s;
CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s.query(%s) WHERE 1=0;`,
			sqlLiteral(cfg.QuackURI),
			primaryRemote,
			sqlLiteral(cfg.QuackToken),
			cfg.DisableSSL,
			createTargetSchemaSQL(cfg, table),
			targetTable,
			primaryRemote,
			sqlLiteral(sourceSelect),
		)
		if err := execTargetScript(ctx, db, cfg, script); err != nil {
			return fmt.Errorf("create target table for %s: %s", table.Name, redactSecrets(cfg, err.Error()))
		}
		return nil
	}
	if _, err := db.ExecContext(ctx, createTargetSchemaSQL(cfg, table)); err != nil {
		return fmt.Errorf("create target schema for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s.query(?) WHERE 1=0", targetTable, cfg.QuackRemoteDB),
		sourceSelect,
	); err != nil {
		return fmt.Errorf("create target table for %s: %w", table.Name, err)
	}
	return nil
}

func loadSourceColumns(ctx context.Context, db *sql.DB, cfg config, table sourceTable) ([]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.QuackRemoteDB), tableColumnsSQL(cfg.SourceCatalog, table))
	if err != nil {
		return nil, fmt.Errorf("read source columns for %s: %w", table.Name, err)
	}
	defer rows.Close()
	columns, err := scanColumnNames(rows)
	if err != nil {
		return nil, fmt.Errorf("scan source columns for %s: %w", table.Name, err)
	}
	return columns, nil
}

func loadTargetColumns(ctx context.Context, db *sql.DB, cfg config, table sourceTable) ([]string, error) {
	rows, err := queryTargetRows(ctx, db, cfg, tableColumnsSQL(cfg.TargetAttachName, table))
	if err != nil {
		return nil, fmt.Errorf("read target columns for %s: %w", table.Name, err)
	}
	defer rows.Close()
	columns, err := scanColumnNames(rows)
	if err != nil {
		return nil, fmt.Errorf("scan target columns for %s: %w", table.Name, err)
	}
	return columns, nil
}

func scanColumnNames(rows *sql.Rows) ([]string, error) {
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func tableColumnsSQL(catalog string, table sourceTable) string {
	schema, tableName := splitTableName(table.Name)
	return fmt.Sprintf(`SELECT column_name
FROM information_schema.columns
WHERE table_catalog = %s
  AND table_schema = %s
  AND table_name = %s
ORDER BY ordinal_position`,
		sqlLiteral(catalog),
		sqlLiteral(schema),
		sqlLiteral(tableName),
	)
}

func sourceSchemaSQL(cfg config, table sourceTable) string {
	return fmt.Sprintf("SELECT * FROM %s.%s WHERE 1=0", cfg.SourceCatalog, table.Name)
}

func columnDiff(sourceColumns, targetColumns []string) string {
	sourceSet := map[string]struct{}{}
	targetSet := map[string]struct{}{}
	for _, column := range sourceColumns {
		sourceSet[column] = struct{}{}
	}
	for _, column := range targetColumns {
		targetSet[column] = struct{}{}
	}
	var missing, extra []string
	for column := range sourceSet {
		if _, ok := targetSet[column]; !ok {
			missing = append(missing, column)
		}
	}
	for column := range targetSet {
		if _, ok := sourceSet[column]; !ok {
			extra = append(extra, column)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	parts := []string{}
	if len(missing) > 0 {
		parts = append(parts, "missing target columns: "+strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		parts = append(parts, "extra target columns: "+strings.Join(extra, ", "))
	}
	return strings.Join(parts, "; ")
}

func currentSnapshot(ctx context.Context, db *sql.DB, cfg config) (uint64, error) {
	query := fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.QuackRemoteDB)
	rows, err := db.QueryContext(ctx, query, fmt.Sprintf("SELECT id FROM %s.current_snapshot()", cfg.SourceCatalog))
	if err != nil {
		return 0, fmt.Errorf("read current snapshot: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("iterate current snapshot: %w", err)
		}
		return 0, fmt.Errorf("current snapshot returned no rows")
	}
	var snapshot uint64
	if err := rows.Scan(&snapshot); err != nil {
		return 0, fmt.Errorf("scan current snapshot: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate current snapshot: %w", err)
	}
	return snapshot, nil
}

func loadCheckpoint(ctx context.Context, db *sql.DB, cfg config, table sourceTable) (checkpoint, error) {
	query := fmt.Sprintf(`SELECT last_snapshot_id
FROM %s.replica.sync_checkpoints
WHERE replica_name = %s
  AND source_catalog = %s
  AND source_table = %s
ORDER BY updated_at DESC
LIMIT 1`,
		cfg.TargetAttachName,
		sqlLiteral(cfg.ReplicaName),
		sqlLiteral(cfg.SourceCatalog),
		sqlLiteral(table.Name),
	)

	var snapshot uint64
	rows, err := queryTargetRows(ctx, db, cfg, query)
	if err != nil {
		return checkpoint{}, fmt.Errorf("load checkpoint for %s: %w", table.Name, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return checkpoint{}, fmt.Errorf("iterate checkpoint for %s: %w", table.Name, err)
		}
		return checkpoint{SnapshotID: cfg.StartSnapshot, Exists: false}, nil
	}
	if err := rows.Scan(&snapshot); err != nil {
		return checkpoint{}, fmt.Errorf("scan checkpoint for %s: %w", table.Name, err)
	}
	if err := rows.Err(); err != nil {
		return checkpoint{}, fmt.Errorf("iterate checkpoint for %s: %w", table.Name, err)
	}
	return checkpoint{SnapshotID: snapshot, Exists: true}, nil
}

func changedLedgers(ctx context.Context, db *sql.DB, cfg config, table sourceTable, fromSnapshot, toSnapshot uint64) ([]uint64, error) {
	schema, tableName := splitTableName(table.Name)
	nullCount, err := changedLedgerNullCount(ctx, db, cfg, table, schema, tableName, fromSnapshot, toSnapshot)
	if err != nil {
		return nil, err
	}
	if nullCount > 0 {
		return nil, fmt.Errorf("table %s has %d change rows with NULL %s between snapshots [%d,%d]", table.Name, nullCount, table.LedgerColumn, fromSnapshot, toSnapshot)
	}
	sourceSQL := changedLedgersSQL(cfg, table, schema, tableName, fromSnapshot, toSnapshot)
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.QuackRemoteDB), sourceSQL)
	if err != nil {
		return nil, fmt.Errorf("read changed ledgers for %s snapshots [%d,%d]: %w", table.Name, fromSnapshot, toSnapshot, err)
	}
	defer rows.Close()

	var ledgers []uint64
	for rows.Next() {
		var ledger uint64
		if err := rows.Scan(&ledger); err != nil {
			return nil, fmt.Errorf("scan changed ledger for %s: %w", table.Name, err)
		}
		ledgers = append(ledgers, ledger)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate changed ledgers for %s: %w", table.Name, err)
	}
	return ledgers, nil
}

func changedLedgerNullCount(ctx context.Context, db *sql.DB, cfg config, table sourceTable, schema, tableName string, fromSnapshot, toSnapshot uint64) (uint64, error) {
	sourceSQL := changedLedgerNullCountSQL(cfg, table, schema, tableName, fromSnapshot, toSnapshot)
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.QuackRemoteDB), sourceSQL)
	if err != nil {
		return 0, fmt.Errorf("count NULL changed ledgers for %s snapshots [%d,%d]: %w", table.Name, fromSnapshot, toSnapshot, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("iterate NULL changed ledger count for %s: %w", table.Name, err)
		}
		return 0, fmt.Errorf("NULL changed ledger count for %s returned no rows", table.Name)
	}
	var count uint64
	if err := rows.Scan(&count); err != nil {
		return 0, fmt.Errorf("scan NULL changed ledger count for %s: %w", table.Name, err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate NULL changed ledger count for %s: %w", table.Name, err)
	}
	return count, nil
}

func changedLedgersSQL(cfg config, table sourceTable, schema, tableName string, fromSnapshot, toSnapshot uint64) string {
	return fmt.Sprintf(
		"USE %s; USE %s; SELECT DISTINCT %s FROM table_changes('%s', %d, %d) WHERE %s IS NOT NULL ORDER BY 1",
		cfg.SourceCatalog,
		schema,
		quoteIdentifier(table.LedgerColumn),
		escapeSQLString(tableName),
		fromSnapshot,
		toSnapshot,
		quoteIdentifier(table.LedgerColumn),
	)
}

func changedLedgerNullCountSQL(cfg config, table sourceTable, schema, tableName string, fromSnapshot, toSnapshot uint64) string {
	return fmt.Sprintf(
		"USE %s; USE %s; SELECT count(*) FROM table_changes('%s', %d, %d) WHERE %s IS NULL",
		cfg.SourceCatalog,
		schema,
		escapeSQLString(tableName),
		fromSnapshot,
		toSnapshot,
		quoteIdentifier(table.LedgerColumn),
	)
}

func rebuildTargetLedgers(ctx context.Context, db *sql.DB, cfg config, table sourceTable, ledgers []uint64, columns []string) error {
	for _, batch := range chunkUint64s(ledgers, cfg.LedgerBatchSize) {
		if cfg.TargetMode == "quack" {
			if err := rebuildTargetLedgerBatchQuack(ctx, db, cfg, table, batch, columns); err != nil {
				return err
			}
			continue
		}
		if err := rebuildTargetLedgerBatchEmbedded(ctx, db, cfg, table, batch, columns); err != nil {
			return err
		}
	}
	return nil
}

type sourceLedgerBounds struct {
	HasRows   bool
	MinLedger uint64
	MaxLedger uint64
	NullRows  uint64
}

type ledgerRangeChunk struct {
	start uint64
	end   uint64
}

func fullResyncTable(ctx context.Context, db *sql.DB, cfg config, table sourceTable, columns []string) error {
	bounds, err := loadSourceLedgerBounds(ctx, db, cfg, table)
	if err != nil {
		return err
	}
	if bounds.NullRows > 0 {
		return fmt.Errorf("source table %s has %d rows with NULL %s; full resync cannot safely advance", table.Name, bounds.NullRows, table.LedgerColumn)
	}
	if !bounds.HasRows {
		return clearTargetTable(ctx, db, cfg, table)
	}
	for _, chunk := range chunkLedgerRange(bounds.MinLedger, bounds.MaxLedger, uint64(cfg.LedgerBatchSize)) {
		if cfg.TargetMode == "quack" {
			if err := rebuildTargetLedgerRangeQuack(ctx, db, cfg, table, chunk, columns); err != nil {
				return err
			}
			continue
		}
		if err := rebuildTargetLedgerRangeEmbedded(ctx, db, cfg, table, chunk, columns); err != nil {
			return err
		}
	}
	return nil
}

func loadSourceLedgerBounds(ctx context.Context, db *sql.DB, cfg config, table sourceTable) (sourceLedgerBounds, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.QuackRemoteDB), sourceLedgerBoundsSQL(cfg, table))
	if err != nil {
		return sourceLedgerBounds{}, fmt.Errorf("read source ledger bounds for %s: %w", table.Name, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return sourceLedgerBounds{}, fmt.Errorf("iterate source ledger bounds for %s: %w", table.Name, err)
		}
		return sourceLedgerBounds{}, fmt.Errorf("source ledger bounds for %s returned no rows", table.Name)
	}
	var totalRows, nullRows uint64
	var minLedger, maxLedger sql.NullInt64
	if err := rows.Scan(&totalRows, &nullRows, &minLedger, &maxLedger); err != nil {
		return sourceLedgerBounds{}, fmt.Errorf("scan source ledger bounds for %s: %w", table.Name, err)
	}
	if err := rows.Err(); err != nil {
		return sourceLedgerBounds{}, fmt.Errorf("iterate source ledger bounds for %s: %w", table.Name, err)
	}
	if totalRows == 0 || !minLedger.Valid || !maxLedger.Valid {
		return sourceLedgerBounds{HasRows: false, NullRows: nullRows}, nil
	}
	if minLedger.Int64 < 0 || maxLedger.Int64 < 0 {
		return sourceLedgerBounds{}, fmt.Errorf("source table %s has negative %s bounds", table.Name, table.LedgerColumn)
	}
	return sourceLedgerBounds{
		HasRows:   true,
		MinLedger: uint64(minLedger.Int64),
		MaxLedger: uint64(maxLedger.Int64),
		NullRows:  nullRows,
	}, nil
}

func sourceLedgerBoundsSQL(cfg config, table sourceTable) string {
	return fmt.Sprintf(`SELECT
	count(*) AS total_rows,
	coalesce(sum(CASE WHEN %s IS NULL THEN 1 ELSE 0 END), 0) AS null_rows,
	min(%s) AS min_ledger,
	max(%s) AS max_ledger
FROM %s.%s`,
		quoteIdentifier(table.LedgerColumn),
		quoteIdentifier(table.LedgerColumn),
		quoteIdentifier(table.LedgerColumn),
		cfg.SourceCatalog,
		table.Name,
	)
}

func chunkLedgerRange(minLedger, maxLedger, size uint64) []ledgerRangeChunk {
	if minLedger > maxLedger {
		return nil
	}
	if size == 0 {
		size = maxLedger - minLedger + 1
	}
	var chunks []ledgerRangeChunk
	for start := minLedger; start <= maxLedger; {
		end := start + size - 1
		if end < start || end > maxLedger {
			end = maxLedger
		}
		chunks = append(chunks, ledgerRangeChunk{start: start, end: end})
		if end == maxLedger {
			break
		}
		start = end + 1
	}
	return chunks
}

func clearTargetTable(ctx context.Context, db *sql.DB, cfg config, table sourceTable) error {
	targetTable := targetTableName(cfg, table)
	script := fmt.Sprintf(`BEGIN TRANSACTION;
DELETE FROM %s;
COMMIT;`, targetTable)
	if cfg.TargetMode == "quack" {
		if err := execTargetScript(ctx, db, cfg, script); err != nil {
			rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = execTargetScript(rbCtx, db, cfg, "ROLLBACK;")
			return fmt.Errorf("clear target table for %s: %s", table.Name, redactSecrets(cfg, err.Error()))
		}
		return nil
	}
	if _, err := db.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		return fmt.Errorf("begin clear target table for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", targetTable)); err != nil {
		_ = rollback(db)
		return fmt.Errorf("clear target table for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, "COMMIT"); err != nil {
		_ = rollback(db)
		return fmt.Errorf("commit clear target table for %s: %w", table.Name, err)
	}
	return nil
}

func rebuildTargetLedgerBatchEmbedded(ctx context.Context, db *sql.DB, cfg config, table sourceTable, ledgers []uint64, columns []string) error {
	targetTable := targetTableName(cfg, table)
	ledgerList := uintListSQL(ledgers)
	sourceSelect := sourceRowsSQL(cfg, table, ledgerList, columns)
	columnList := quoteIdentifierList(columns)

	stmts := []string{
		"BEGIN TRANSACTION",
		createTargetSchemaSQL(cfg, table),
		fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s.query(?) WHERE 1=0",
			targetTable,
			cfg.QuackRemoteDB,
		),
		fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", targetTable, quoteIdentifier(table.LedgerColumn), ledgerList),
		fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s.query(?)", targetTable, columnList, columnList, cfg.QuackRemoteDB),
		"COMMIT",
	}

	if _, err := db.ExecContext(ctx, stmts[0]); err != nil {
		return fmt.Errorf("begin target rebuild for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, stmts[1]); err != nil {
		_ = rollback(db)
		return fmt.Errorf("create target schema for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, stmts[2], sourceSelect); err != nil {
		_ = rollback(db)
		return fmt.Errorf("create target table for %s: %w", table.Name, err)
	}
	for _, stmt := range stmts[3:5] {
		if strings.Contains(stmt, ".query(?)") {
			if _, err := db.ExecContext(ctx, stmt, sourceSelect); err != nil {
				_ = rollback(db)
				return fmt.Errorf("copy target rows for %s: %w", table.Name, err)
			}
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			_ = rollback(db)
			return fmt.Errorf("rebuild target rows for %s: %w", table.Name, err)
		}
	}
	if _, err := db.ExecContext(ctx, stmts[5]); err != nil {
		_ = rollback(db)
		return fmt.Errorf("commit target rebuild for %s: %w", table.Name, err)
	}
	return nil
}

func rebuildTargetLedgerRangeEmbedded(ctx context.Context, db *sql.DB, cfg config, table sourceTable, chunk ledgerRangeChunk, columns []string) error {
	targetTable := targetTableName(cfg, table)
	sourceSelect := sourceRowsForRangeSQL(cfg, table, chunk, columns)
	columnList := quoteIdentifierList(columns)

	stmts := []string{
		"BEGIN TRANSACTION",
		createTargetSchemaSQL(cfg, table),
		fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s.query(?) WHERE 1=0",
			targetTable,
			cfg.QuackRemoteDB,
		),
		fmt.Sprintf(
			"DELETE FROM %s WHERE %s >= %d AND %s <= %d",
			targetTable,
			quoteIdentifier(table.LedgerColumn),
			chunk.start,
			quoteIdentifier(table.LedgerColumn),
			chunk.end,
		),
		fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s.query(?)", targetTable, columnList, columnList, cfg.QuackRemoteDB),
		"COMMIT",
	}

	if _, err := db.ExecContext(ctx, stmts[0]); err != nil {
		return fmt.Errorf("begin target range rebuild for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, stmts[1]); err != nil {
		_ = rollback(db)
		return fmt.Errorf("create target schema for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, stmts[2], sourceSelect); err != nil {
		_ = rollback(db)
		return fmt.Errorf("create target table for %s: %w", table.Name, err)
	}
	for _, stmt := range stmts[3:5] {
		if strings.Contains(stmt, ".query(?)") {
			if _, err := db.ExecContext(ctx, stmt, sourceSelect); err != nil {
				_ = rollback(db)
				return fmt.Errorf("copy target range rows for %s: %w", table.Name, err)
			}
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			_ = rollback(db)
			return fmt.Errorf("rebuild target range rows for %s: %w", table.Name, err)
		}
	}
	if _, err := db.ExecContext(ctx, stmts[5]); err != nil {
		_ = rollback(db)
		return fmt.Errorf("commit target range rebuild for %s: %w", table.Name, err)
	}
	return nil
}

func rebuildTargetLedgerBatchQuack(ctx context.Context, db *sql.DB, cfg config, table sourceTable, ledgers []uint64, columns []string) error {
	script := rebuildTargetLedgerBatchQuackSQL(cfg, table, ledgers, columns)
	if err := execTargetScript(ctx, db, cfg, script); err != nil {
		rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = execTargetScript(rbCtx, db, cfg, "ROLLBACK;")
		return fmt.Errorf("target quack rebuild for %s: %s", table.Name, redactSecrets(cfg, err.Error()))
	}
	return nil
}

func rebuildTargetLedgerRangeQuack(ctx context.Context, db *sql.DB, cfg config, table sourceTable, chunk ledgerRangeChunk, columns []string) error {
	script := rebuildTargetLedgerRangeQuackSQL(cfg, table, chunk, columns)
	if err := execTargetScript(ctx, db, cfg, script); err != nil {
		rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = execTargetScript(rbCtx, db, cfg, "ROLLBACK;")
		return fmt.Errorf("target quack range rebuild for %s: %s", table.Name, redactSecrets(cfg, err.Error()))
	}
	return nil
}

func rebuildTargetLedgerBatchQuackSQL(cfg config, table sourceTable, ledgers []uint64, columns []string) string {
	targetTable := targetTableName(cfg, table)
	ledgerList := uintListSQL(ledgers)
	sourceSelect := sourceRowsSQL(cfg, table, ledgerList, columns)
	columnList := quoteIdentifierList(columns)
	primaryRemote := "replica_primary"
	return fmt.Sprintf(`ATTACH IF NOT EXISTS %s AS %s (TOKEN %s, DISABLE_SSL %t);
%s;
CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s.query(%s) WHERE 1=0;
BEGIN TRANSACTION;
DELETE FROM %s WHERE %s IN (%s);
INSERT INTO %s (%s) SELECT %s FROM %s.query(%s);
COMMIT;`,
		sqlLiteral(cfg.QuackURI),
		primaryRemote,
		sqlLiteral(cfg.QuackToken),
		cfg.DisableSSL,
		createTargetSchemaSQL(cfg, table),
		targetTable,
		primaryRemote,
		sqlLiteral(sourceSelect),
		targetTable,
		quoteIdentifier(table.LedgerColumn),
		ledgerList,
		targetTable,
		columnList,
		columnList,
		primaryRemote,
		sqlLiteral(sourceSelect),
	)
}

func rebuildTargetLedgerRangeQuackSQL(cfg config, table sourceTable, chunk ledgerRangeChunk, columns []string) string {
	targetTable := targetTableName(cfg, table)
	sourceSelect := sourceRowsForRangeSQL(cfg, table, chunk, columns)
	columnList := quoteIdentifierList(columns)
	primaryRemote := "replica_primary"
	return fmt.Sprintf(`ATTACH IF NOT EXISTS %s AS %s (TOKEN %s, DISABLE_SSL %t);
%s;
CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s.query(%s) WHERE 1=0;
BEGIN TRANSACTION;
DELETE FROM %s WHERE %s >= %d AND %s <= %d;
INSERT INTO %s (%s) SELECT %s FROM %s.query(%s);
COMMIT;`,
		sqlLiteral(cfg.QuackURI),
		primaryRemote,
		sqlLiteral(cfg.QuackToken),
		cfg.DisableSSL,
		createTargetSchemaSQL(cfg, table),
		targetTable,
		primaryRemote,
		sqlLiteral(sourceSelect),
		targetTable,
		quoteIdentifier(table.LedgerColumn),
		chunk.start,
		quoteIdentifier(table.LedgerColumn),
		chunk.end,
		targetTable,
		columnList,
		columnList,
		primaryRemote,
		sqlLiteral(sourceSelect),
	)
}

func sourceRowsSQL(cfg config, table sourceTable, ledgerList string, columns []string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s.%s WHERE %s IN (%s)",
		quoteIdentifierList(columns),
		cfg.SourceCatalog,
		table.Name,
		quoteIdentifier(table.LedgerColumn),
		ledgerList,
	)
}

func sourceRowsForRangeSQL(cfg config, table sourceTable, chunk ledgerRangeChunk, columns []string) string {
	return fmt.Sprintf(
		"SELECT %s FROM %s.%s WHERE %s >= %d AND %s <= %d",
		quoteIdentifierList(columns),
		cfg.SourceCatalog,
		table.Name,
		quoteIdentifier(table.LedgerColumn),
		chunk.start,
		quoteIdentifier(table.LedgerColumn),
		chunk.end,
	)
}

func saveCheckpoint(ctx context.Context, db *sql.DB, cfg config, table sourceTable, snapshot uint64, status, message string) error {
	if cfg.TargetMode == "quack" {
		script := fmt.Sprintf(`BEGIN TRANSACTION;
%s;
%s;
COMMIT;`,
			deleteCheckpointSQL(cfg, table),
			insertCheckpointSQL(cfg, table, snapshot, status, message),
		)
		if err := execTargetScript(ctx, db, cfg, script); err != nil {
			rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = execTargetScript(rbCtx, db, cfg, "ROLLBACK;")
			return fmt.Errorf("save target quack checkpoint for %s: %s", table.Name, redactSecrets(cfg, err.Error()))
		}
		return nil
	}
	if _, err := db.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		return fmt.Errorf("begin checkpoint for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, deleteCheckpointSQL(cfg, table)); err != nil {
		_ = rollback(db)
		return fmt.Errorf("delete checkpoint for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, insertCheckpointSQL(cfg, table, snapshot, status, message)); err != nil {
		_ = rollback(db)
		return fmt.Errorf("insert checkpoint for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, "COMMIT"); err != nil {
		_ = rollback(db)
		return fmt.Errorf("commit checkpoint for %s: %w", table.Name, err)
	}
	return nil
}

func rollback(db *sql.DB) error {
	rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := db.ExecContext(rbCtx, "ROLLBACK")
	return err
}

func createTargetSchemaSQL(cfg config, table sourceTable) string {
	schema, _ := splitTableName(table.Name)
	return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", cfg.TargetAttachName, schema)
}

func targetTableName(cfg config, table sourceTable) string {
	schema, name := splitTableName(table.Name)
	return fmt.Sprintf("%s.%s.%s", cfg.TargetAttachName, schema, name)
}

func deleteCheckpointSQL(cfg config, table sourceTable) string {
	return fmt.Sprintf(`DELETE FROM %s.replica.sync_checkpoints
WHERE replica_name = %s
  AND source_catalog = %s
  AND source_table = %s`,
		cfg.TargetAttachName,
		sqlLiteral(cfg.ReplicaName),
		sqlLiteral(cfg.SourceCatalog),
		sqlLiteral(table.Name),
	)
}

func insertCheckpointSQL(cfg config, table sourceTable, snapshot uint64, status, message string) string {
	return fmt.Sprintf(`INSERT INTO %s.replica.sync_checkpoints (
	replica_name,
	source_catalog,
	source_table,
	last_snapshot_id,
	updated_at,
	status,
	error_message
) VALUES (%s, %s, %s, %d, now(), %s, %s)`,
		cfg.TargetAttachName,
		sqlLiteral(cfg.ReplicaName),
		sqlLiteral(cfg.SourceCatalog),
		sqlLiteral(table.Name),
		snapshot,
		sqlLiteral(status),
		sqlLiteral(redactSecrets(cfg, message)),
	)
}

func isMissingSnapshotError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	if !strings.Contains(message, "snapshot") {
		return false
	}
	for _, marker := range []string{"missing", "not found", "expired", "expire", "no files", "does not exist"} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func redactSecrets(cfg config, value string) string {
	for _, secret := range []string{cfg.QuackToken, cfg.TargetQuackToken} {
		secret = strings.TrimSpace(secret)
		if secret == "" {
			continue
		}
		replacements := []string{
			sqlLiteral(secret),
			escapeSQLString(secret),
			secret,
		}
		for _, replacement := range replacements {
			if replacement != "" {
				value = strings.ReplaceAll(value, replacement, "[REDACTED]")
			}
		}
	}
	return value
}

func parseSourceTables(value, defaultLedgerColumn string, overrides map[string]string) ([]sourceTable, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	var tables []sourceTable
	for _, part := range strings.Split(value, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		table := sourceTable{Name: sanitizeTableName(name), LedgerColumn: sanitizeIdentifier(defaultLedgerColumn)}
		if override, ok := overrides[table.Name]; ok {
			table.LedgerColumn = sanitizeIdentifier(override)
		}
		if err := validateSourceTable(table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func parseLedgerColumnOverrides(value string) map[string]string {
	overrides := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, val, ok := strings.Cut(part, "=")
		if !ok {
			log.Fatalf("LEDGER_COLUMN_OVERRIDES entries must be table=column, got %q", part)
		}
		overrides[sanitizeTableName(key)] = sanitizeIdentifier(val)
	}
	return overrides
}

func validateSourceTable(table sourceTable) error {
	schema, name := splitTableName(table.Name)
	if schema == "" || name == "" {
		return fmt.Errorf("source table %q must be schema.table", table.Name)
	}
	if table.LedgerColumn == "" {
		return fmt.Errorf("source table %q has empty ledger column", table.Name)
	}
	return nil
}

func splitTableName(value string) (string, string) {
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func sanitizeTableName(value string) string {
	parts := strings.Split(strings.TrimSpace(value), ".")
	for i := range parts {
		parts[i] = sanitizeIdentifier(parts[i])
	}
	return strings.Join(parts, ".")
}

func uintListSQL(values []uint64) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.FormatUint(value, 10)
	}
	return strings.Join(parts, ", ")
}

func chunkUint64s(values []uint64, size int) [][]uint64 {
	if size <= 0 {
		size = len(values)
	}
	var chunks [][]uint64
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

func parseUintEnv(key, fallback string) (uint64, error) {
	value := getenv(key, fallback)
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an unsigned integer: %w", key, err)
	}
	return parsed, nil
}

func mustParseUintEnv(key, fallback string) uint64 {
	value, err := parseUintEnv(key, fallback)
	if err != nil {
		log.Fatal(err)
	}
	return value
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	case "0", "f", "false", "n", "no", "off":
		return false
	default:
		log.Fatalf("%s must be a boolean (true/false/1/0/yes/no), got %q", key, raw)
		return fallback
	}
}

func sanitizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var b strings.Builder
	for i, r := range value {
		valid := r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9'
		if valid {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(strings.Trim(value, `"`), `"`, `""`) + `"`
}

func quoteIdentifierList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = quoteIdentifier(value)
	}
	return strings.Join(quoted, ", ")
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func sqlLiteral(value string) string {
	return "'" + escapeSQLString(value) + "'"
}
