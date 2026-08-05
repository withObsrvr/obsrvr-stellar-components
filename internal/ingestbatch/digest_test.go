package ingestbatch

import (
	"testing"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
)

func TestDescribeIsDeterministicAndAccountsForRange(t *testing.T) {
	batches := []*componentsv1.LedgerBatch{
		{NetworkPassphrase: "pubnet", LedgerSequence: 100, BronzeRows: []*componentsv1.BronzeRow{{Id: "a"}}},
		{NetworkPassphrase: "pubnet", LedgerSequence: 101, BronzeRows: []*componentsv1.BronzeRow{{Id: "b"}, {Id: "c"}}},
	}
	first, err := Describe(batches)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	second, err := Describe(batches)
	if err != nil {
		t.Fatalf("describe again: %v", err)
	}
	if first != second {
		t.Fatalf("descriptor changed: first=%+v second=%+v", first, second)
	}
	if first.ID == "" || first.ID != first.PayloadSHA256 {
		t.Fatalf("invalid id/hash: %+v", first)
	}
	if first.LedgerStart != 100 || first.LedgerEnd != 101 || first.LedgerCount != 2 || first.BronzeRows != 3 || first.EncodedBytes == 0 {
		t.Fatalf("descriptor = %+v", first)
	}
}

func TestAccumulatorMatchesSliceDescriptor(t *testing.T) {
	batches := []*componentsv1.LedgerBatch{
		{NetworkPassphrase: "pubnet", LedgerSequence: 100, BronzeRows: []*componentsv1.BronzeRow{{Id: "a"}}},
		{NetworkPassphrase: "pubnet", LedgerSequence: 101, BronzeRows: []*componentsv1.BronzeRow{{Id: "b"}, {Id: "c"}}},
	}
	want, err := Describe(batches)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	accumulator := NewAccumulator()
	for _, batch := range batches {
		if err := accumulator.Add(batch); err != nil {
			t.Fatalf("add ledger: %v", err)
		}
	}
	got, err := accumulator.Descriptor()
	if err != nil {
		t.Fatalf("finish accumulator: %v", err)
	}
	if got != want {
		t.Fatalf("stream descriptor = %+v, want %+v", got, want)
	}
}

func TestDescribeRejectsGapAndNetworkChange(t *testing.T) {
	for _, test := range []struct {
		name    string
		batches []*componentsv1.LedgerBatch
	}{
		{name: "gap", batches: []*componentsv1.LedgerBatch{{NetworkPassphrase: "pubnet", LedgerSequence: 1}, {NetworkPassphrase: "pubnet", LedgerSequence: 3}}},
		{name: "network", batches: []*componentsv1.LedgerBatch{{NetworkPassphrase: "pubnet", LedgerSequence: 1}, {NetworkPassphrase: "testnet", LedgerSequence: 2}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Describe(test.batches); err == nil {
				t.Fatal("expected descriptor error")
			}
		})
	}
}
