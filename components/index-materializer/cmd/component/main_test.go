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
		"INSERT INTO stellar_lake.index.tx_hash_index",
		"COMMIT;",
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("SQL missing %q:\n%s", want, sqlText)
		}
	}
	// The DELETE and the INSERT must cover the identical (100, 200] window, or a
	// rebuild would drop or duplicate rows. Assert both bounds appear twice.
	assertBoundsAppearTwice(t, sqlText, "> 100", "<= 200")
	if strings.Contains(sqlText, "existing.tx_hash IS NULL") {
		t.Fatalf("SQL still uses insert-only idempotency:\n%s", sqlText)
	}
}

func TestMaterializeSQLContractEventsIndex(t *testing.T) {
	sqlText, err := materializeSQL(config{
		Catalog:     "stellar_lake",
		IndexName:   "contract_events_index",
		StartLedger: "100",
		EndLedger:   "200",
	})
	if err != nil {
		t.Fatalf("materialize SQL: %v", err)
	}

	for _, want := range []string{
		"BEGIN TRANSACTION;",
		"DELETE FROM stellar_lake.index.contract_events_index",
		"INSERT INTO stellar_lake.index.contract_events_index",
		"FROM stellar_lake.bronze.contract_events_stream_v1 e",
		"AND e.contract_id IS NOT NULL",
		"GROUP BY e.contract_id, e.ledger_sequence",
		// first_seen_at must come from the source ledger close time so it is
		// stable across rebuilds, not the wall clock at materialization.
		"min(e.closed_at) AS first_seen_at",
		"COMMIT;",
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("SQL missing %q:\n%s", want, sqlText)
		}
	}
	if strings.Contains(sqlText, "now() AS first_seen_at") {
		t.Fatalf("first_seen_at must derive from source closed_at, not now():\n%s", sqlText)
	}
	assertBoundsAppearTwice(t, sqlText, "> 100", "<= 200")
}

func TestMaterializeSQLRejectsUnsupportedIndex(t *testing.T) {
	if _, err := materializeSQL(config{
		Catalog:     "stellar_lake",
		IndexName:   "does_not_exist",
		StartLedger: "0",
		EndLedger:   "1",
	}); err == nil {
		t.Fatal("expected error for unsupported INDEX_NAME")
	}
}

// assertBoundsAppearTwice checks the range predicate is present in both the
// DELETE and the INSERT clauses (i.e. each bound occurs at least twice).
func assertBoundsAppearTwice(t *testing.T, sqlText, lower, upper string) {
	t.Helper()
	if got := strings.Count(sqlText, lower); got < 2 {
		t.Fatalf("lower bound %q appears %d times, want >= 2:\n%s", lower, got, sqlText)
	}
	if got := strings.Count(sqlText, upper); got < 2 {
		t.Fatalf("upper bound %q appears %d times, want >= 2:\n%s", upper, got, sqlText)
	}
}

func TestSanitizeIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "plain", input: "stellar_lake", want: "stellar_lake"},
		{name: "empty falls back", input: "", want: "stellar_lake"},
		{name: "whitespace falls back", input: "   ", want: "stellar_lake"},
		{name: "injection collapses to underscores", input: "lake; DROP SCHEMA index", want: "lake__DROP_SCHEMA_index"},
		{name: "leading digit replaced", input: "1lake", want: "_lake"},
		{name: "interior digits kept", input: "lake9", want: "lake9"},
		{name: "quotes and dashes replaced", input: "a'-b", want: "a__b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeIdentifier(tt.input); got != tt.want {
				t.Fatalf("sanitizeIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
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
