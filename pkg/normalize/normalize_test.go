package normalize

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestLedgerBatchIncludesBronzeRows(t *testing.T) {
	lcm := minimalLedgerCloseMeta(t, 12345, time.Unix(1700000000, 0).UTC())

	batch, err := LedgerBatch(lcm, Options{NetworkPassphrase: network.PublicNetworkPassphrase})
	if err != nil {
		t.Fatalf("LedgerBatch returned error: %v", err)
	}

	if len(batch.BronzeRows) == 0 {
		t.Fatal("expected bronze rows")
	}

	var ledgerRow *struct {
		Sequence    uint32
		LedgerRange uint32
	}
	for _, row := range batch.BronzeRows {
		if row.TableName != "ledgers_row_v2" {
			continue
		}
		ledgerRow = &struct {
			Sequence    uint32
			LedgerRange uint32
		}{}
		if err := json.Unmarshal([]byte(row.RowJson), ledgerRow); err != nil {
			t.Fatalf("unmarshal bronze ledger row json: %v", err)
		}
		if row.LedgerSequence != 12345 || row.LedgerRange != 10000 {
			t.Fatalf("unexpected bronze row partition: ledger=%d range=%d", row.LedgerSequence, row.LedgerRange)
		}
		break
	}
	if ledgerRow == nil {
		t.Fatal("expected ledgers_row_v2 bronze row")
	}
	if ledgerRow.Sequence != 12345 || ledgerRow.LedgerRange != 10000 {
		t.Fatalf("unexpected ledger row json: %+v", *ledgerRow)
	}
}

func minimalLedgerCloseMeta(t *testing.T, sequence uint32, closedAt time.Time) xdr.LedgerCloseMeta {
	t.Helper()

	meta, err := xdr.NewLedgerCloseMeta(0, xdr.LedgerCloseMetaV0{
		LedgerHeader: xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				LedgerVersion:  23,
				LedgerSeq:      xdr.Uint32(sequence),
				TotalCoins:     100000000000,
				FeePool:        100,
				BaseFee:        100,
				BaseReserve:    5000000,
				MaxTxSetSize:   1000,
				ScpValue:       xdr.StellarValue{CloseTime: xdr.TimePoint(closedAt.Unix())},
				BucketListHash: xdr.Hash{1, 2, 3},
			},
			Hash: xdr.Hash{4, 5, 6},
		},
	})
	if err != nil {
		t.Fatalf("create ledger close meta: %v", err)
	}
	return meta
}
