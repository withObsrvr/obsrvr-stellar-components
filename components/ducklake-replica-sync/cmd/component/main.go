package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
		TargetCatalogPath: getenv("TARGET_DUCKLAKE_CATALOG_PATH", "ducklake/serving.ducklake"),
		TargetDataPath:    getenv("TARGET_DUCKLAKE_DATA_PATH", "ducklake/serving-data"),
		TargetAttachName:  sanitizeIdentifier(getenv("TARGET_ATTACH_NAME", "serving_lake")),
		TargetQuackURI:    getenv("TARGET_QUACK_URI", ""),
		TargetQuackToken:  getenv("TARGET_QUACK_TOKEN", ""),
		TargetQuackRemote: sanitizeIdentifier(getenv("TARGET_QUACK_REMOTE_DB", "target_lake")),
		TargetDisableSSL:  getenvBool("TARGET_QUACK_DISABLE_SSL", getenvBool("QUACK_DISABLE_SSL", true)),
		LedgerBatchSize:   int(mustParseUintEnv("LEDGER_BATCH_SIZE", "1000")),
	}
}

func run(ctx context.Context, cfg config) error {
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

	for _, table := range cfg.SourceTables {
		if err := syncTable(ctx, db, cfg, table, currentSnapshot); err != nil {
			return err
		}
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
	if fromSnapshot >= current {
		log.Printf("table=%s already current snapshot=%d", table.Name, fromSnapshot)
		return nil
	}

	ledgers, err := changedLedgers(ctx, db, cfg, table, fromSnapshot+1, current)
	if err != nil {
		return err
	}
	if len(ledgers) == 0 {
		if err := saveCheckpoint(ctx, db, cfg, table, current, "ok", ""); err != nil {
			return err
		}
		log.Printf("table=%s no changed ledgers checkpoint=%d", table.Name, current)
		return nil
	}

	if err := rebuildTargetLedgers(ctx, db, cfg, table, ledgers); err != nil {
		_ = saveCheckpoint(ctx, db, cfg, table, fromSnapshot, "error", err.Error())
		return err
	}
	if err := saveCheckpoint(ctx, db, cfg, table, current, "ok", ""); err != nil {
		return err
	}
	log.Printf("table=%s changed_ledgers=%d checkpoint=%d", table.Name, len(ledgers), current)
	return nil
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

func currentSnapshot(ctx context.Context, db *sql.DB, cfg config) (uint64, error) {
	query := fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.QuackRemoteDB)
	rows, err := db.QueryContext(ctx, query, fmt.Sprintf("SELECT id FROM %s.current_snapshot()", cfg.SourceCatalog))
	if err != nil {
		return 0, fmt.Errorf("read current snapshot: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
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

func rebuildTargetLedgers(ctx context.Context, db *sql.DB, cfg config, table sourceTable, ledgers []uint64) error {
	for _, batch := range chunkUint64s(ledgers, cfg.LedgerBatchSize) {
		if cfg.TargetMode == "quack" {
			if err := rebuildTargetLedgerBatchQuack(ctx, db, cfg, table, batch); err != nil {
				return err
			}
			continue
		}
		if err := rebuildTargetLedgerBatchEmbedded(ctx, db, cfg, table, batch); err != nil {
			return err
		}
	}
	return nil
}

func rebuildTargetLedgerBatchEmbedded(ctx context.Context, db *sql.DB, cfg config, table sourceTable, ledgers []uint64) error {
	targetTable := targetTableName(cfg, table)
	ledgerList := uintListSQL(ledgers)
	sourceSelect := sourceRowsSQL(cfg, table, ledgerList)

	stmts := []string{
		"BEGIN TRANSACTION",
		createTargetSchemaSQL(cfg, table),
		fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s.query(?) WHERE 1=0",
			targetTable,
			cfg.QuackRemoteDB,
		),
		fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", targetTable, quoteIdentifier(table.LedgerColumn), ledgerList),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s.query(?)", targetTable, cfg.QuackRemoteDB),
		"COMMIT",
	}

	if _, err := db.ExecContext(ctx, stmts[0]); err != nil {
		return fmt.Errorf("begin target rebuild for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, stmts[1]); err != nil {
		_ = rollback(ctx, db)
		return fmt.Errorf("create target schema for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, stmts[2], sourceSelect); err != nil {
		_ = rollback(ctx, db)
		return fmt.Errorf("create target table for %s: %w", table.Name, err)
	}
	for _, stmt := range stmts[3:5] {
		if strings.Contains(stmt, ".query(?)") {
			if _, err := db.ExecContext(ctx, stmt, sourceSelect); err != nil {
				_ = rollback(ctx, db)
				return fmt.Errorf("copy target rows for %s: %w", table.Name, err)
			}
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			_ = rollback(ctx, db)
			return fmt.Errorf("rebuild target rows for %s: %w", table.Name, err)
		}
	}
	if _, err := db.ExecContext(ctx, stmts[5]); err != nil {
		_ = rollback(context.Background(), db)
		return fmt.Errorf("commit target rebuild for %s: %w", table.Name, err)
	}
	return nil
}

func rebuildTargetLedgerBatchQuack(ctx context.Context, db *sql.DB, cfg config, table sourceTable, ledgers []uint64) error {
	script := rebuildTargetLedgerBatchQuackSQL(cfg, table, ledgers)
	if err := execTargetScript(ctx, db, cfg, script); err != nil {
		_ = execTargetScript(context.Background(), db, cfg, "ROLLBACK;")
		return fmt.Errorf("target quack rebuild for %s: %w", table.Name, err)
	}
	return nil
}

func rebuildTargetLedgerBatchQuackSQL(cfg config, table sourceTable, ledgers []uint64) string {
	targetTable := targetTableName(cfg, table)
	ledgerList := uintListSQL(ledgers)
	sourceSelect := sourceRowsSQL(cfg, table, ledgerList)
	primaryRemote := "replica_primary"
	return fmt.Sprintf(`ATTACH IF NOT EXISTS %s AS %s (TOKEN %s, DISABLE_SSL %t);
%s;
CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s.query(%s) WHERE 1=0;
BEGIN TRANSACTION;
DELETE FROM %s WHERE %s IN (%s);
INSERT INTO %s SELECT * FROM %s.query(%s);
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
		primaryRemote,
		sqlLiteral(sourceSelect),
	)
}

func sourceRowsSQL(cfg config, table sourceTable, ledgerList string) string {
	return fmt.Sprintf(
		"SELECT * FROM %s.%s WHERE %s IN (%s)",
		cfg.SourceCatalog,
		table.Name,
		quoteIdentifier(table.LedgerColumn),
		ledgerList,
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
			_ = execTargetScript(context.Background(), db, cfg, "ROLLBACK;")
			return fmt.Errorf("save target quack checkpoint for %s: %w", table.Name, err)
		}
		return nil
	}
	if _, err := db.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		return fmt.Errorf("begin checkpoint for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, deleteCheckpointSQL(cfg, table)); err != nil {
		_ = rollback(ctx, db)
		return fmt.Errorf("delete checkpoint for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, insertCheckpointSQL(cfg, table, snapshot, status, message)); err != nil {
		_ = rollback(ctx, db)
		return fmt.Errorf("insert checkpoint for %s: %w", table.Name, err)
	}
	if _, err := db.ExecContext(ctx, "COMMIT"); err != nil {
		_ = rollback(context.Background(), db)
		return fmt.Errorf("commit checkpoint for %s: %w", table.Name, err)
	}
	return nil
}

func rollback(ctx context.Context, db *sql.DB) error {
	rbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
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
		sqlLiteral(message),
	)
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

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func sqlLiteral(value string) string {
	return "'" + escapeSQLString(value) + "'"
}
