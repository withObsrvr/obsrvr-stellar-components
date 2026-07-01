package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	cfg := configFromEnv()
	if err := run(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}

type config struct {
	QuackURI      string
	QuackToken    string
	QuackRemoteDB string
	Catalog       string
	IndexName     string
	StartLedger   string
	EndLedger     string
	DisableSSL    bool
}

func configFromEnv() config {
	return config{
		QuackURI:      getenv("QUACK_URI", "quack:127.0.0.1:9494"),
		QuackToken:    getenv("QUACK_TOKEN", ""),
		QuackRemoteDB: sanitizeIdentifier(getenv("QUACK_REMOTE_DB", "remote_lake")),
		Catalog:       sanitizeIdentifier(getenv("DUCKLAKE_ATTACH_NAME", "stellar_lake")),
		IndexName:     getenv("INDEX_NAME", "tx_hash_index"),
		StartLedger:   getenv("START_LEDGER", "0"),
		EndLedger:     getenv("END_LEDGER", "9223372036854775807"),
		DisableSSL:    getenvBool("QUACK_DISABLE_SSL", true),
	}
}

func run(ctx context.Context, cfg config) error {
	if cfg.QuackToken == "" {
		return fmt.Errorf("QUACK_TOKEN is required")
	}
	script, err := materializeSQL(cfg)
	if err != nil {
		return err
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open DuckDB Quack client: %w", err)
	}
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
	if _, err := db.ExecContext(ctx, remoteQuery, script); err != nil {
		_, _ = db.ExecContext(context.Background(), remoteQuery, "ROLLBACK;")
		return fmt.Errorf("materialize %s: %w", cfg.IndexName, err)
	}
	log.Printf("materialized %s for ledgers (%s, %s]", cfg.IndexName, cfg.StartLedger, cfg.EndLedger)
	return nil
}

type ledgerRange struct {
	Start uint64
	End   uint64
}

func parseLedgerRange(cfg config) (ledgerRange, error) {
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
	return ledgerRange{Start: start, End: end}, nil
}

func materializeSQL(cfg config) (string, error) {
	ledgerRange, err := parseLedgerRange(cfg)
	if err != nil {
		return "", err
	}
	switch cfg.IndexName {
	case "tx_hash_index":
		return txHashIndexSQL(cfg, ledgerRange), nil
	case "contract_events_index":
		return contractEventsIndexSQL(cfg, ledgerRange), nil
	default:
		return "", fmt.Errorf("unsupported INDEX_NAME %q", cfg.IndexName)
	}
}

func txHashIndexSQL(cfg config, ledgerRange ledgerRange) string {
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
`, cfg.Catalog, ledgerRange.Start, ledgerRange.End)
}

func contractEventsIndexSQL(cfg config, ledgerRange ledgerRange) string {
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
	now() AS first_seen_at,
	e.ledger_sequence / 100000 AS ledger_range,
	now() AS created_at
FROM %[1]s.bronze.contract_events_stream_v1 e
WHERE e.ledger_sequence > %[2]d
  AND e.ledger_sequence <= %[3]d
  AND e.contract_id IS NOT NULL
GROUP BY e.contract_id, e.ledger_sequence;
COMMIT;
`, cfg.Catalog, ledgerRange.Start, ledgerRange.End)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes"
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
