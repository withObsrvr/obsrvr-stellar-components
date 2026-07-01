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
