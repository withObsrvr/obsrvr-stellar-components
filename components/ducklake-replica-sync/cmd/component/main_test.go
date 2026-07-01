package main

import (
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

func TestRebuildTargetLedgerBatchQuackSQL(t *testing.T) {
	cfg := config{
		QuackURI:         "quack:primary:9494",
		QuackToken:       "primary's secret",
		DisableSSL:       true,
		SourceCatalog:    "stellar_lake",
		TargetAttachName: "serving_lake",
	}
	table := sourceTable{Name: "bronze.transactions_row_v2", LedgerColumn: "ledger_sequence"}

	sqlText := rebuildTargetLedgerBatchQuackSQL(cfg, table, []uint64{100, 101})
	for _, want := range []string{
		"ATTACH IF NOT EXISTS 'quack:primary:9494' AS replica_primary",
		"CREATE SCHEMA IF NOT EXISTS serving_lake.bronze",
		"CREATE TABLE IF NOT EXISTS serving_lake.bronze.transactions_row_v2",
		"BEGIN TRANSACTION;",
		`DELETE FROM serving_lake.bronze.transactions_row_v2 WHERE "ledger_sequence" IN (100, 101);`,
		"INSERT INTO serving_lake.bronze.transactions_row_v2 SELECT * FROM replica_primary.query",
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
