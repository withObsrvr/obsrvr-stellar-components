package main

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
)

func TestDuckLakeSinkMaterializesTypedBronzeTables(t *testing.T) {
	tmp := t.TempDir()
	sink, err := NewDuckLakeSink(DuckLakeConfig{
		CatalogPath: filepath.Join(tmp, "stellar.ducklake"),
		DataPath:    filepath.Join(tmp, "data"),
		AttachName:  "test_lake",
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	t.Cleanup(func() {
		if err := sink.Close(); err != nil {
			t.Fatalf("close sink: %v", err)
		}
	})

	const ledgerSequence = 12345
	rows := make([]*componentsv1.BronzeRow, 0, len(typedTableSpecs))
	tableNames := make([]string, 0, len(typedTableSpecs))
	for tableName := range typedTableSpecs {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)
	for i, tableName := range tableNames {
		rows = append(rows, &componentsv1.BronzeRow{
			Id:                fmt.Sprintf("row-%d", i),
			TableName:         tableName,
			NetworkPassphrase: "testnet",
			LedgerSequence:    ledgerSequence,
			LedgerRange:       10000,
			RowJson:           typedRowJSON(ledgerSequence),
		})
	}

	if err := sink.WriteBatch(&componentsv1.LedgerBatch{
		NetworkPassphrase: "testnet",
		LedgerSequence:    ledgerSequence,
		ClosedAtUnix:      1782900000,
		SchemaVersion:     contracts.SchemaVersion,
		ExtractionVersion: contracts.ExtractionVersion,
		BronzeRows:        rows,
	}); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	for _, tableName := range tableNames {
		spec := typedTableSpecs[tableName]
		var envelopeCount int
		if err := sink.db.QueryRow(
			"SELECT count(*) FROM bronze_rows WHERE ledger_sequence = ? AND table_name = ?",
			ledgerSequence,
			tableName,
		).Scan(&envelopeCount); err != nil {
			t.Fatalf("count envelope rows for %s: %v", tableName, err)
		}
		var typedCount int
		query := fmt.Sprintf(
			"SELECT count(*) FROM bronze.%s WHERE %s = ?",
			tableName,
			quoteIdentifier(spec.LedgerColumn),
		)
		if err := sink.db.QueryRow(query, ledgerSequence).Scan(&typedCount); err != nil {
			t.Fatalf("count typed rows for %s: %v", tableName, err)
		}
		if typedCount != envelopeCount {
			t.Fatalf("%s count mismatch: typed=%d envelope=%d", tableName, typedCount, envelopeCount)
		}
	}
}

func typedRowJSON(ledgerSequence uint32) string {
	return fmt.Sprintf(`{
		"Sequence": %[1]d,
		"LedgerSequence": %[1]d,
		"CreatedLedger": %[1]d,
		"LedgerRange": 10000,
		"LedgerHash": "ledger-hash",
		"PreviousLedgerHash": "previous-ledger-hash",
		"TransactionHash": "tx-hash",
		"TransactionID": 1,
		"OperationID": 2,
		"OperationIndex": 1,
		"EffectIndex": 0,
		"TradeIndex": 0,
		"EventIndex": 0,
		"AccountID": "account",
		"SourceAccount": "source",
		"SellerAccount": "seller",
		"BuyerAccount": "buyer",
		"Signer": "signer",
		"BalanceID": "balance",
		"OfferID": 1,
		"LiquidityPoolID": "pool",
		"ContractID": "contract",
		"ContractId": "contract",
		"ContractCodeHash": "code-hash",
		"LedgerKeyHash": "key-hash",
		"KeyHash": "key-hash",
		"EventID": "event",
		"CreatorAddress": "creator",
		"Asset": "native",
		"AssetType": "native",
		"EventType": "transfer",
		"AmountRaw": "1",
		"TrustLimit": "1",
		"PoolType": "constant_product",
		"TradeType": "orderbook",
		"Price": "1",
		"CreatedAt": "2026-01-01T00:00:00Z",
		"UpdatedAt": "2026-01-01T00:00:00Z",
		"ClosedAt": "2026-01-01T00:00:00Z",
		"IngestionTimestamp": "2026-01-01T00:00:00Z",
		"TradeTimestamp": "2026-01-01T00:00:00Z"
	}`, ledgerSequence)
}
