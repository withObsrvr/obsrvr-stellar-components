package main

import (
	"strings"
	"testing"
)

func TestMaterializeSQLRebuildsLedgerRange(t *testing.T) {
	sqlText, err := materializeSQL(config{
		Catalog:     "stellar_lake",
		IndexName:   "tx_hash_index",
		StartLedger: "100",
		EndLedger:   "200",
	})
	if err != nil {
		t.Fatalf("materialize SQL: %v", err)
	}

	for _, want := range []string{
		"BEGIN TRANSACTION;",
		"DELETE FROM stellar_lake.index.tx_hash_index",
		"WHERE ledger_sequence > 100",
		"AND ledger_sequence <= 200",
		"INSERT INTO stellar_lake.index.tx_hash_index",
		"COMMIT;",
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("SQL missing %q:\n%s", want, sqlText)
		}
	}
	if strings.Contains(sqlText, "existing.tx_hash IS NULL") {
		t.Fatalf("SQL still uses insert-only idempotency:\n%s", sqlText)
	}
}

func TestMaterializeSQLRejectsInvalidLedgerRanges(t *testing.T) {
	tests := []struct {
		name        string
		startLedger string
		endLedger   string
	}{
		{name: "non numeric start", startLedger: "1; DROP TABLE x", endLedger: "2"},
		{name: "non numeric end", startLedger: "1", endLedger: "2 OR 1=1"},
		{name: "start after end", startLedger: "3", endLedger: "2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := materializeSQL(config{
				Catalog:     "stellar_lake",
				IndexName:   "contract_events_index",
				StartLedger: tt.startLedger,
				EndLedger:   tt.endLedger,
			})
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
