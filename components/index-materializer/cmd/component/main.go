package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
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
			"ATTACH '%s' AS %s (TOKEN '%s', DISABLE_SSL true)",
			escapeSQLString(cfg.QuackURI),
			cfg.QuackRemoteDB,
			escapeSQLString(cfg.QuackToken),
		),
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("init %q: %w", stmt, err)
		}
	}

	if _, err := db.ExecContext(ctx, fmt.Sprintf("SELECT * FROM %s.query(?)", cfg.QuackRemoteDB), script); err != nil {
		return fmt.Errorf("materialize %s: %w", cfg.IndexName, err)
	}
	log.Printf("materialized %s for ledgers (%s, %s]", cfg.IndexName, cfg.StartLedger, cfg.EndLedger)
	return nil
}

func materializeSQL(cfg config) (string, error) {
	switch cfg.IndexName {
	case "tx_hash_index":
		return txHashIndexSQL(cfg), nil
	case "contract_events_index":
		return contractEventsIndexSQL(cfg), nil
	default:
		return "", fmt.Errorf("unsupported INDEX_NAME %q", cfg.IndexName)
	}
}

func txHashIndexSQL(cfg config) string {
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
LEFT JOIN %[1]s.index.tx_hash_index existing
  ON existing.tx_hash = t.transaction_hash
WHERE t.ledger_sequence > %[2]s
  AND t.ledger_sequence <= %[3]s
  AND existing.tx_hash IS NULL;
`, cfg.Catalog, cfg.StartLedger, cfg.EndLedger)
}

func contractEventsIndexSQL(cfg config) string {
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
INSERT INTO %[1]s.index.contract_events_index
SELECT
	e.contract_id,
	e.ledger_sequence,
	count(*) AS event_count,
	now() AS first_seen_at,
	e.ledger_sequence / 100000 AS ledger_range,
	now() AS created_at
FROM %[1]s.bronze.contract_events_stream_v1 e
LEFT JOIN %[1]s.index.contract_events_index existing
  ON existing.contract_id = e.contract_id
 AND existing.ledger_sequence = e.ledger_sequence
WHERE e.ledger_sequence > %[2]s
  AND e.ledger_sequence <= %[3]s
  AND e.contract_id IS NOT NULL
  AND existing.contract_id IS NULL
GROUP BY e.contract_id, e.ledger_sequence;
`, cfg.Catalog, cfg.StartLedger, cfg.EndLedger)
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
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
