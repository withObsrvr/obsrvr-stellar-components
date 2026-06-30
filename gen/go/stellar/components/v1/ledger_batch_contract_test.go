package componentsv1

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestLedgerBatchRoundTrip(t *testing.T) {
	input := &LedgerBatch{
		NetworkPassphrase: "Test SDF Network ; September 2015",
		LedgerSequence:    123,
		ClosedAtUnix:      456,
		SchemaVersion:     "v1",
		ExtractionVersion: "test",
		Ledgers: []*LedgerRow{{
			Id:                "ledger_1",
			NetworkPassphrase: "Test SDF Network ; September 2015",
			LedgerSequence:    123,
		}},
	}
	data, err := proto.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var output LedgerBatch
	if err := proto.Unmarshal(data, &output); err != nil {
		t.Fatal(err)
	}
	if output.LedgerSequence != input.LedgerSequence {
		t.Fatalf("ledger sequence mismatch: got %d want %d", output.LedgerSequence, input.LedgerSequence)
	}
}
