package ids

import "testing"

func TestStableIDsAreDeterministic(t *testing.T) {
	a := Transaction("testnet", 123, 4, "abc")
	b := Transaction("testnet", 123, 4, "abc")
	if a != b {
		t.Fatalf("stable IDs differ: %s != %s", a, b)
	}
}

func TestStableIDsUseRowKindPrefix(t *testing.T) {
	if got := Ledger("testnet", 123); got[:7] != "ledger_" {
		t.Fatalf("ledger ID should have ledger_ prefix: %s", got)
	}
}
