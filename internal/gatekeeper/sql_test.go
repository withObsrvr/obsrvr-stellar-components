package gatekeeper

import (
	"strings"
	"testing"
	"time"
)

func mustTestProposal(t *testing.T) (Proposal, string) {
	t.Helper()
	p, hash, err := ParseProposal([]byte(validProposalYAML))
	if err != nil {
		t.Fatalf("parse test proposal: %v", err)
	}
	return p, hash
}

func TestRenderProposalPinsEverySourceRead(t *testing.T) {
	p, hash := mustTestProposal(t)
	r, err := renderProposal(p, hash, LedgerRange{StartExclusive: 62080049, EndInclusive: 62080149})
	if err != nil {
		t.Fatalf("render proposal: %v", err)
	}
	for name, query := range map[string]string{"candidate": r.Transformation, "replay_a": r.ReplayTransformA, "replay_b": r.ReplayTransformB} {
		if !strings.Contains(query, `(SELECT * FROM "stellar_lake"."bronze"."token_transfers_stream_v1" AT (VERSION => 42))`) {
			t.Fatalf("%s query is not snapshot-pinned:\n%s", name, query)
		}
		if strings.Contains(query, "{{") {
			t.Fatalf("%s query has unresolved placeholder:\n%s", name, query)
		}
	}
	if !strings.Contains(r.ReplayTransformA, "ledger_sequence > 62080049") || !strings.Contains(r.ReplayTransformA, "ledger_sequence <= 62080149") {
		t.Fatalf("replay bounds not rendered:\n%s", r.ReplayTransformA)
	}
}

func TestPromotionIsOneTransactionWithReceipt(t *testing.T) {
	p, hash := mustTestProposal(t)
	r, err := renderProposal(p, hash, p.LedgerRange)
	if err != nil {
		t.Fatalf("render proposal: %v", err)
	}
	receipt := PromotionReceipt{
		PromotionID:  "promotion-1",
		ProposalHash: hash,
		PromotedRows: 12,
		PromotedAt:   time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
		Gates:        []GateResult{{Name: "reproducibility", Passed: true}},
	}
	sqlText, err := promotionSQL(p, r, receipt)
	if err != nil {
		t.Fatalf("promotion SQL: %v", err)
	}
	for _, want := range []string{
		"BEGIN TRANSACTION;",
		`DELETE FROM "stellar_lake"."silver"."asset_daily_volume"`,
		`published."asset" IS NOT DISTINCT FROM candidate."asset"`,
		`published."day" IS NOT DISTINCT FROM candidate."day"`,
		`INSERT INTO "stellar_lake"."silver"."asset_daily_volume"`,
		`INSERT INTO "stellar_lake".governance.promotions`,
		"COMMIT;",
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("promotion SQL missing %q:\n%s", want, sqlText)
		}
	}
	if strings.Index(sqlText, "BEGIN TRANSACTION;") > strings.Index(sqlText, "governance.promotions") {
		t.Fatalf("receipt is outside transaction:\n%s", sqlText)
	}
}

func TestEqualityUsesExceptAllBothWays(t *testing.T) {
	sqlText := equalitySQL("candidate_a", "candidate_b")
	if got := strings.Count(sqlText, "EXCEPT ALL"); got != 2 {
		t.Fatalf("EXCEPT ALL count = %d, want 2:\n%s", got, sqlText)
	}
}
