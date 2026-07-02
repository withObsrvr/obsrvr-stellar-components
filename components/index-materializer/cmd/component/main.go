package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
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
	QuackURI        string
	QuackToken      string
	QuackRemoteDB   string
	Catalog         string
	IndexName       string
	StartLedger     string
	EndLedger       string
	LedgerChunkSize uint64
	DisableSSL      bool
}

const defaultLedgerChunkSize = 100000

func configFromEnv() config {
	return config{
		QuackURI:        getenv("QUACK_URI", "quack:127.0.0.1:9494"),
		QuackToken:      getenv("QUACK_TOKEN", ""),
		QuackRemoteDB:   sanitizeIdentifier(getenv("QUACK_REMOTE_DB", "remote_lake")),
		Catalog:         sanitizeIdentifier(getenv("DUCKLAKE_ATTACH_NAME", "stellar_lake")),
		IndexName:       getenv("INDEX_NAME", "tx_hash_index"),
		StartLedger:     strings.TrimSpace(os.Getenv("START_LEDGER")),
		EndLedger:       strings.TrimSpace(os.Getenv("END_LEDGER")),
		LedgerChunkSize: mustParsePositiveUintEnv("LEDGER_CHUNK_SIZE", strconv.FormatUint(defaultLedgerChunkSize, 10)),
		DisableSSL:      getenvBool("QUACK_DISABLE_SSL", true),
	}
}

func run(ctx context.Context, cfg config) error {
	if cfg.QuackToken == "" {
		return fmt.Errorf("QUACK_TOKEN is required")
	}
	chunks, err := materializeChunks(cfg)
	if err != nil {
		return err
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open DuckDB Quack client: %w", err)
	}
	// Pin the pool to a single connection so the ATTACH, the materialize query,
	// and the failure ROLLBACK all run against the same DuckDB session (and the
	// same Quack server-side session). Otherwise database/sql is free to route
	// each Exec to a different pooled connection, which would leave the loaded
	// quack extension / attachment on one connection and send the rollback to
	// another that has no transaction to roll back.
	db.SetMaxOpenConns(1)
	defer db.Close()

	stmts := []string{
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
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init %q: %w", stmt, err)
		}
	}

	remoteQuery := fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.QuackRemoteDB)
	for i, chunk := range chunks {
		if _, err := db.ExecContext(ctx, remoteQuery, chunk.sql); err != nil {
			// The BEGIN/DELETE/INSERT/COMMIT script runs as one server-side query,
			// so a mid-script failure aborts the remote transaction. Attempt a
			// best-effort ROLLBACK on the same (pinned) connection to clear any
			// lingering transaction state, bounded so a degraded Quack link cannot
			// block indefinitely, and surface a failed rollback instead of hiding it.
			rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if _, rbErr := db.ExecContext(rbCtx, remoteQuery, "ROLLBACK;"); rbErr != nil {
				log.Printf("materialize %s chunk %d/%d: rollback after failure did not confirm clean state: %v", cfg.IndexName, i+1, len(chunks), rbErr)
			}
			cancel()
			return fmt.Errorf("materialize %s chunk %d/%d ledgers (%d, %d]: %w", cfg.IndexName, i+1, len(chunks), chunk.start, chunk.end, err)
		}
	}
	log.Printf("materialized %s for ledgers (%s, %s] in %d chunk(s)", cfg.IndexName, cfg.StartLedger, cfg.EndLedger, len(chunks))
	return nil
}

// ledgerRange is a validated half-open ledger interval (start, end]: start is
// exclusive, end is inclusive, and start <= end. The fields are unexported so
// parseLedgerRange is the only way to build one and those invariants always hold.
type ledgerRange struct {
	start uint64
	end   uint64
}

type materializeChunk struct {
	ledgerRange
	sql string
}

func parseLedgerRange(cfg config) (ledgerRange, error) {
	if strings.TrimSpace(cfg.StartLedger) == "" {
		return ledgerRange{}, fmt.Errorf("START_LEDGER is required")
	}
	if strings.TrimSpace(cfg.EndLedger) == "" {
		return ledgerRange{}, fmt.Errorf("END_LEDGER is required")
	}
	start, err := strconv.ParseUint(cfg.StartLedger, 10, 64)
	if err != nil {
		return ledgerRange{}, fmt.Errorf("START_LEDGER must be an unsigned integer: %w", err)
	}
	end, err := strconv.ParseUint(cfg.EndLedger, 10, 64)
	if err != nil {
		return ledgerRange{}, fmt.Errorf("END_LEDGER must be an unsigned integer: %w", err)
	}
	if start > end {
		return ledgerRange{}, fmt.Errorf("START_LEDGER %d must be <= END_LEDGER %d", start, end)
	}
	return ledgerRange{start: start, end: end}, nil
}

func materializeSQL(cfg config) (string, error) {
	chunks, err := materializeChunks(cfg)
	if err != nil {
		return "", err
	}
	sqlParts := make([]string, len(chunks))
	for i, chunk := range chunks {
		sqlParts[i] = chunk.sql
	}
	return strings.Join(sqlParts, "\n"), nil
}

func materializeChunks(cfg config) ([]materializeChunk, error) {
	lr, err := parseLedgerRange(cfg)
	if err != nil {
		return nil, err
	}
	chunkSize, err := ledgerChunkSize(cfg)
	if err != nil {
		return nil, err
	}
	ranges := chunkLedgerRange(lr, chunkSize)
	chunks := make([]materializeChunk, 0, len(ranges))
	for _, chunkRange := range ranges {
		var sqlText string
		switch cfg.IndexName {
		case "tx_hash_index":
			sqlText = txHashIndexSQL(cfg, chunkRange)
		case "contract_events_index":
			sqlText = contractEventsIndexSQL(cfg, chunkRange)
		default:
			return nil, fmt.Errorf("unsupported INDEX_NAME %q", cfg.IndexName)
		}
		chunks = append(chunks, materializeChunk{ledgerRange: chunkRange, sql: sqlText})
	}
	return chunks, nil
}

func ledgerChunkSize(cfg config) (uint64, error) {
	if cfg.LedgerChunkSize == 0 {
		return defaultLedgerChunkSize, nil
	}
	return cfg.LedgerChunkSize, nil
}

func chunkLedgerRange(lr ledgerRange, size uint64) []ledgerRange {
	if size == 0 {
		size = defaultLedgerChunkSize
	}
	if lr.start == lr.end {
		return []ledgerRange{lr}
	}
	var chunks []ledgerRange
	for start := lr.start; start < lr.end; {
		end := start + size
		if end < start || end > lr.end {
			end = lr.end
		}
		chunks = append(chunks, ledgerRange{start: start, end: end})
		start = end
	}
	return chunks
}

func txHashIndexSQL(cfg config, lr ledgerRange) string {
	return fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS %[1]s.index;
CREATE TABLE IF NOT EXISTS %[1]s.index.tx_hash_index (
	tx_hash VARCHAR,
	ledger_sequence BIGINT,
	operation_count INTEGER,
	successful BOOLEAN,
	closed_at TIMESTAMP,
	ledger_range BIGINT,
	created_at TIMESTAMP
);
BEGIN TRANSACTION;
DELETE FROM %[1]s.index.tx_hash_index
WHERE ledger_sequence > %[2]d
  AND ledger_sequence <= %[3]d;
INSERT INTO %[1]s.index.tx_hash_index
SELECT
	t.transaction_hash AS tx_hash,
	t.ledger_sequence,
	t.operation_count,
	t.successful,
	t.created_at AS closed_at,
	t.ledger_sequence / 100000 AS ledger_range,
	now() AS created_at
FROM %[1]s.bronze.transactions_row_v2 t
WHERE t.ledger_sequence > %[2]d
  AND t.ledger_sequence <= %[3]d;
COMMIT;
`, cfg.Catalog, lr.start, lr.end)
}

func contractEventsIndexSQL(cfg config, lr ledgerRange) string {
	return fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS %[1]s.index;
CREATE TABLE IF NOT EXISTS %[1]s.index.contract_events_index (
	contract_id VARCHAR,
	ledger_sequence BIGINT,
	event_count INTEGER,
	first_seen_at TIMESTAMP,
	ledger_range BIGINT,
	created_at TIMESTAMP
);
BEGIN TRANSACTION;
DELETE FROM %[1]s.index.contract_events_index
WHERE ledger_sequence > %[2]d
  AND ledger_sequence <= %[3]d;
INSERT INTO %[1]s.index.contract_events_index
SELECT
	e.contract_id,
	e.ledger_sequence,
	count(*) AS event_count,
	min(e.closed_at) AS first_seen_at,
	e.ledger_sequence / 100000 AS ledger_range,
	now() AS created_at
FROM %[1]s.bronze.contract_events_stream_v1 e
WHERE e.ledger_sequence > %[2]d
  AND e.ledger_sequence <= %[3]d
  AND e.contract_id IS NOT NULL
GROUP BY e.contract_id, e.ledger_sequence;
COMMIT;
`, cfg.Catalog, lr.start, lr.end)
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
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

func mustParsePositiveUintEnv(key, fallback string) uint64 {
	value := mustParseUintEnv(key, fallback)
	if value == 0 {
		log.Fatalf("%s must be greater than zero", key)
	}
	return value
}

func getenvBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	// These flags gate security-relevant transport behavior (e.g. DISABLE_SSL),
	// so an unrecognized value is a misconfiguration we must not silently coerce
	// to false. Accept the common boolean spellings and fail fast on anything else.
	switch strings.ToLower(raw) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	case "0", "f", "false", "n", "no", "off":
		return false
	default:
		log.Fatalf("%s must be a boolean (true/false/1/0/yes/no), got %q", key, raw)
		return fallback // unreachable; log.Fatalf exits
	}
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func sanitizeIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "stellar_lake"
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
