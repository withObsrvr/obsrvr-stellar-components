package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestParseSourceTables(t *testing.T) {
	tables, err := parseSourceTables(
		" bronze.transactions_row_v2,bronze.contract_events_stream_v1 ",
		"ledger_sequence",
		map[string]string{"bronze.contract_events_stream_v1": "sequence"},
	)
	if err != nil {
		t.Fatalf("parse source tables: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("table count = %d, want 2", len(tables))
	}
	if tables[0].Name != "bronze.transactions_row_v2" || tables[0].LedgerColumn != "ledger_sequence" {
		t.Fatalf("first table = %+v", tables[0])
	}
	if tables[1].Name != "bronze.contract_events_stream_v1" || tables[1].LedgerColumn != "sequence" {
		t.Fatalf("second table = %+v", tables[1])
	}
}

func TestParseSourceTablesRequiresSchemaTable(t *testing.T) {
	_, err := parseSourceTables("transactions_row_v2", "ledger_sequence", nil)
	if err == nil {
		t.Fatal("expected schema.table validation error")
	}
}

func TestTargetSQLHelpers(t *testing.T) {
	cfg := config{
		TargetAttachName: "serving_lake",
		ReplicaName:      "serving_replica",
		SourceCatalog:    "stellar_lake",
	}
	table := sourceTable{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}

	if got := targetTableName(cfg, table); got != "serving_lake.bronze.transactions_row_v2" {
		t.Fatalf("target table = %q", got)
	}
	if got := createTargetSchemaSQL(cfg, table); got != "CREATE SCHEMA IF NOT EXISTS serving_lake.bronze" {
		t.Fatalf("create schema SQL = %q", got)
	}

	deleteSQL := deleteCheckpointSQL(cfg, table)
	for _, want := range []string{
		"DELETE FROM serving_lake.replica.sync_checkpoints",
		"replica_name = 'serving_replica'",
		"source_catalog = 'stellar_lake'",
		"source_table = 'bronze.transactions_row_v2'",
	} {
		if !strings.Contains(deleteSQL, want) {
			t.Fatalf("delete checkpoint SQL missing %q:\n%s", want, deleteSQL)
		}
	}

	insertSQL := insertCheckpointSQL(cfg, table, 42, "ok", "")
	for _, want := range []string{
		"INSERT INTO serving_lake.replica.sync_checkpoints",
		"'serving_replica'",
		"'stellar_lake'",
		"'bronze.transactions_row_v2'",
		"42",
		"'ok'",
	} {
		if !strings.Contains(insertSQL, want) {
			t.Fatalf("insert checkpoint SQL missing %q:\n%s", want, insertSQL)
		}
	}
}

func TestValidateConfigRequiresAbsoluteEmbeddedTargetPaths(t *testing.T) {
	cfg := validTestConfig()
	cfg.TargetCatalogPath = "ducklake/serving.ducklake"

	err := validateConfig(cfg)
	if err == nil {
		t.Fatal("expected relative target catalog path to fail")
	}
	if !strings.Contains(err.Error(), "TARGET_DUCKLAKE_CATALOG_PATH must be absolute") {
		t.Fatalf("validateConfig error = %q", err)
	}

	cfg = validTestConfig()
	cfg.TargetDataPath = "ducklake/serving-data"
	err = validateConfig(cfg)
	if err == nil {
		t.Fatal("expected relative target data path to fail")
	}
	if !strings.Contains(err.Error(), "TARGET_DUCKLAKE_DATA_PATH must be absolute") {
		t.Fatalf("validateConfig error = %q", err)
	}
}

func TestUIntListSQL(t *testing.T) {
	if got := uintListSQL([]uint64{3, 1, 2}); got != "3, 1, 2" {
		t.Fatalf("uint list = %q", got)
	}
}

func TestChangedLedgersSQL(t *testing.T) {
	cfg := config{SourceCatalog: "stellar_lake"}
	table := sourceTable{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}

	sqlText := changedLedgersSQL(cfg, table, "bronze", "transactions_row_v2", 11, 22)
	for _, want := range []string{
		"USE stellar_lake; USE bronze;",
		`SELECT DISTINCT "ledger_sequence"`,
		"FROM table_changes('transactions_row_v2', 11, 22)",
		`WHERE "ledger_sequence" IS NOT NULL`,
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("changed ledgers SQL missing %q:\n%s", want, sqlText)
		}
	}
}

func TestChangedLedgerNullCountSQL(t *testing.T) {
	cfg := config{SourceCatalog: "stellar_lake"}
	table := sourceTable{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}

	sqlText := changedLedgerNullCountSQL(cfg, table, "bronze", "transactions_row_v2", 11, 22)
	for _, want := range []string{
		"USE stellar_lake; USE bronze;",
		"SELECT count(*)",
		"FROM table_changes('transactions_row_v2', 11, 22)",
		`WHERE "ledger_sequence" IS NULL`,
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("NULL changed ledgers SQL missing %q:\n%s", want, sqlText)
		}
	}
}

func TestRebuildTargetLedgerBatchQuackSQL(t *testing.T) {
	cfg := config{
		QuackURI:         "quack:primary:9494",
		QuackToken:       "primary's secret",
		DisableSSL:       true,
		SourceCatalog:    "stellar_lake",
		TargetAttachName: "serving_lake",
	}
	table := sourceTable{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}
	columns := []string{"ledger_sequence", "transaction_hash"}

	sqlText := rebuildTargetLedgerBatchQuackSQL(cfg, table, []uint64{100, 101}, columns)
	for _, want := range []string{
		"ATTACH IF NOT EXISTS 'quack:primary:9494' AS replica_primary",
		"CREATE SCHEMA IF NOT EXISTS serving_lake.bronze",
		"CREATE TABLE IF NOT EXISTS serving_lake.bronze.transactions_row_v2",
		"BEGIN TRANSACTION;",
		`DELETE FROM serving_lake.bronze.transactions_row_v2 WHERE "ledger_sequence" IN (100, 101);`,
		`INSERT INTO serving_lake.bronze.transactions_row_v2 ("ledger_sequence", "transaction_hash") SELECT "ledger_sequence", "transaction_hash" FROM replica_primary.query`,
		`SELECT "ledger_sequence", "transaction_hash" FROM stellar_lake.bronze.transactions_row_v2 WHERE "ledger_sequence" IN (100, 101)`,
		"COMMIT;",
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("quack rebuild SQL missing %q:\n%s", want, sqlText)
		}
	}
	if strings.Contains(sqlText, "sync_checkpoints") {
		t.Fatalf("quack rebuild batch should not checkpoint before all chunks complete:\n%s", sqlText)
	}
	if !strings.Contains(sqlText, "primary''s secret") {
		t.Fatalf("quack rebuild SQL did not escape token literal:\n%s", sqlText)
	}
}

func TestRebuildTargetLedgerRangeQuackSQL(t *testing.T) {
	cfg := config{
		QuackURI:         "quack:primary:9494",
		QuackToken:       "secret",
		DisableSSL:       true,
		SourceCatalog:    "stellar_lake",
		TargetAttachName: "serving_lake",
	}
	table := sourceTable{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}

	sqlText := rebuildTargetLedgerRangeQuackSQL(cfg, table, ledgerRangeChunk{start: 100, end: 199}, []string{"ledger_sequence", "transaction_hash"})
	for _, want := range []string{
		`DELETE FROM serving_lake.bronze.transactions_row_v2 WHERE "ledger_sequence" >= 100 AND "ledger_sequence" <= 199;`,
		`INSERT INTO serving_lake.bronze.transactions_row_v2 ("ledger_sequence", "transaction_hash") SELECT "ledger_sequence", "transaction_hash" FROM replica_primary.query`,
		`SELECT "ledger_sequence", "transaction_hash" FROM stellar_lake.bronze.transactions_row_v2 WHERE "ledger_sequence" >= 100 AND "ledger_sequence" <= 199`,
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("quack range rebuild SQL missing %q:\n%s", want, sqlText)
		}
	}
}

func TestSourceLedgerBoundsSQL(t *testing.T) {
	cfg := config{SourceCatalog: "stellar_lake"}
	table := sourceTable{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}

	sqlText := sourceLedgerBoundsSQL(cfg, table)
	for _, want := range []string{
		"count(*) AS total_rows",
		`coalesce(sum(CASE WHEN "ledger_sequence" IS NULL THEN 1 ELSE 0 END), 0) AS null_rows`,
		`min("ledger_sequence") AS min_ledger`,
		"FROM stellar_lake.bronze.transactions_row_v2",
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("source ledger bounds SQL missing %q:\n%s", want, sqlText)
		}
	}
}

func TestColumnDiffNamesMissingAndExtraColumns(t *testing.T) {
	diff := columnDiff(
		[]string{"ledger_sequence", "transaction_hash", "successful"},
		[]string{"ledger_sequence", "tx_hash"},
	)
	for _, want := range []string{
		"missing target columns: successful, transaction_hash",
		"extra target columns: tx_hash",
	} {
		if !strings.Contains(diff, want) {
			t.Fatalf("column diff missing %q in %q", want, diff)
		}
	}
}

func TestInsertCheckpointSQLRedactsSecrets(t *testing.T) {
	cfg := config{
		TargetAttachName: "serving_lake",
		ReplicaName:      "serving_replica",
		SourceCatalog:    "stellar_lake",
		QuackToken:       "primary's secret",
		TargetQuackToken: "target secret",
	}
	table := sourceTable{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}

	sqlText := insertCheckpointSQL(cfg, table, 42, "error", "remote query failed with primary''s secret and target secret")
	if strings.Contains(sqlText, "primary") || strings.Contains(sqlText, "target secret") {
		t.Fatalf("checkpoint SQL leaked secret material:\n%s", sqlText)
	}
	if !strings.Contains(sqlText, "[REDACTED]") {
		t.Fatalf("checkpoint SQL did not include redaction marker:\n%s", sqlText)
	}
}

func TestRecordTableErrorPersistsRedactedCheckpoint(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	cfg := config{
		TargetAttachName: "memory",
		ReplicaName:      "serving_replica",
		SourceCatalog:    "stellar_lake",
		TargetMode:       "embedded",
		QuackToken:       "primary secret",
	}
	table := sourceTable{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}
	if err := initTargetMetadata(ctx, db, cfg); err != nil {
		t.Fatalf("init target metadata: %v", err)
	}

	err = recordTableError(ctx, db, cfg, table, 42, errors.New("schema drift exposed primary secret"))
	if err == nil {
		t.Fatalf("recordTableError returned nil")
	}
	if strings.Contains(err.Error(), "primary secret") {
		t.Fatalf("recordTableError leaked secret in returned error: %v", err)
	}

	var snapshot uint64
	var status, message string
	if err := db.QueryRow(`SELECT last_snapshot_id, status, error_message
FROM memory.replica.sync_checkpoints
WHERE replica_name = 'serving_replica'
  AND source_catalog = 'stellar_lake'
  AND source_table = 'bronze.transactions_row_v2'`).Scan(&snapshot, &status, &message); err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if snapshot != 42 || status != "error" {
		t.Fatalf("checkpoint = snapshot %d status %q, want 42/error", snapshot, status)
	}
	if strings.Contains(message, "primary secret") {
		t.Fatalf("checkpoint error leaked secret: %q", message)
	}
	if !strings.Contains(message, "[REDACTED]") {
		t.Fatalf("checkpoint error missing redaction marker: %q", message)
	}
}

func TestIsMissingSnapshotError(t *testing.T) {
	for _, message := range []string{
		"ducklake snapshot 12 not found",
		"Invalid Input Error: No snapshot found at version 8",
	} {
		if !isMissingSnapshotError(assertErr(message)) {
			t.Fatalf("expected missing snapshot error %q to be classified", message)
		}
	}
	if isMissingSnapshotError(assertErr("network timeout reading table_changes")) {
		t.Fatalf("non-snapshot error was classified as missing snapshot")
	}
}

func TestChunkUint64s(t *testing.T) {
	chunks := chunkUint64s([]uint64{1, 2, 3, 4, 5}, 2)
	if len(chunks) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(chunks))
	}
	if got := uintListSQL(chunks[0]); got != "1, 2" {
		t.Fatalf("chunk 0 = %q", got)
	}
	if got := uintListSQL(chunks[2]); got != "5" {
		t.Fatalf("chunk 2 = %q", got)
	}
}

func TestGetenvTrimsWhitespace(t *testing.T) {
	t.Setenv("DUCKLAKE_REPLICA_SYNC_TEST_VALUE", "   ")
	if got := getenv("DUCKLAKE_REPLICA_SYNC_TEST_VALUE", "fallback"); got != "fallback" {
		t.Fatalf("whitespace env = %q, want fallback", got)
	}

	t.Setenv("DUCKLAKE_REPLICA_SYNC_TEST_VALUE", "  configured  ")
	if got := getenv("DUCKLAKE_REPLICA_SYNC_TEST_VALUE", "fallback"); got != "configured" {
		t.Fatalf("trimmed env = %q, want configured", got)
	}
}

func TestInitStepNameRedactsAttachSQL(t *testing.T) {
	stmt := "ATTACH 'quack:127.0.0.1:9494' AS remote_lake (TOKEN 'secret', DISABLE_SSL true)"
	if got := initStepName(stmt); got != "attach Quack" {
		t.Fatalf("init step name = %q", got)
	}
}

func validTestConfig() config {
	return config{
		QuackURI:          "quack:127.0.0.1:9494",
		QuackToken:        "secret",
		QuackRemoteDB:     "remote_lake",
		SourceCatalog:     "stellar_lake",
		SourceTables:      []sourceTable{{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}},
		ReplicaName:       "serving_replica",
		TargetMode:        "embedded",
		TargetCatalogPath: "/tmp/serving.ducklake",
		TargetDataPath:    "/tmp/serving-data",
		TargetAttachName:  "serving_lake",
		LedgerBatchSize:   1000,
	}
}

type assertErr string

func (e assertErr) Error() string {
	return string(e)
}
