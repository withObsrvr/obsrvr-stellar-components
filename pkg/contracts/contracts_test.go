package contracts

import "testing"

func TestEventTypeConstants(t *testing.T) {
	if RawLedgerEventType != "stellar.ledger.v1" {
		t.Fatalf("unexpected raw ledger event type: %s", RawLedgerEventType)
	}
	if LedgerBatchEventType != "stellar.ledger.batch.v1" {
		t.Fatalf("unexpected ledger batch event type: %s", LedgerBatchEventType)
	}
}
