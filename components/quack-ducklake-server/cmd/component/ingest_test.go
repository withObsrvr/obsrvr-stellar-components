package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

// TestStageWithAppenderCoversAllTypedTables pushes one row of every typed
// bronze table through the Appender into real staging tables. The Appender is
// type-strict, so this is where any value-mapping mismatch between
// typedValues output and the staging DDL surfaces.
func TestStageWithAppenderCoversAllTypedTables(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS bronze"); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	for _, migration := range bronze.Migrations {
		for _, stmt := range bronze.SplitSQLStatements(migration.SQL) {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("staging DDL %q: %v", stmt, err)
			}
		}
	}

	const ledgerSequence = 777001
	var tableNames []string
	for name := range bronze.TypedTableSpecs {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	rows := make([]*componentsv1.BronzeRow, 0, len(tableNames))
	for i, name := range tableNames {
		rows = append(rows, &componentsv1.BronzeRow{
			Id:                fmt.Sprintf("row-%d", i),
			TableName:         name,
			NetworkPassphrase: "testnet",
			LedgerSequence:    ledgerSequence,
			LedgerRange:       770000,
			RowJson:           appenderFixtureJSON(ledgerSequence),
		})
	}
	batch := &componentsv1.LedgerBatch{
		NetworkPassphrase: "testnet",
		LedgerSequence:    ledgerSequence,
		Transactions: []*componentsv1.TransactionRow{{
			LedgerSequence:  ledgerSequence,
			TransactionHash: "tx-hash",
			EnvelopeXdr:     "envelope-xdr",
			ResultXdr:       "result-xdr",
			MetaXdr:         "meta-xdr",
		}},
		BronzeRows: rows,
	}

	decoded := bronze.DecodeTypedRows(batch)
	srv := &ingestServer{conn: conn}
	if err := srv.stageWithAppender(decoded); err != nil {
		t.Fatalf("stage with appender: %v", err)
	}

	for _, name := range tableNames {
		spec := bronze.TypedTableSpecs[name]
		var count int
		query := fmt.Sprintf(
			"SELECT count(*) FROM memory.bronze.%s WHERE %s = ?",
			name, bronze.QuoteIdentifier(spec.LedgerColumn),
		)
		if err := conn.QueryRowContext(ctx, query, ledgerSequence).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", name, err)
		}
		if count != 1 {
			t.Fatalf("%s appender count = %d, want 1", name, count)
		}
	}

	var envelope string
	if err := conn.QueryRowContext(ctx,
		"SELECT tx_envelope FROM memory.bronze.transactions_row_v2 WHERE ledger_sequence = ?",
		ledgerSequence,
	).Scan(&envelope); err != nil {
		t.Fatalf("read enrichment: %v", err)
	}
	if envelope != "envelope-xdr" {
		t.Fatalf("tx_envelope = %q, want XDR enrichment preserved through appender", envelope)
	}
}

func appenderFixtureJSON(ledgerSequence uint32) string {
	return fmt.Sprintf(`{
		"Sequence": %[1]d,
		"LedgerSequence": %[1]d,
		"CreatedLedger": %[1]d,
		"LedgerRange": 770000,
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
		"TxEnvelope": "envelope-xdr",
		"TxResult": "result-xdr",
		"TxMeta": "meta-xdr",
		"TxFeeMeta": "fee-meta-xdr",
		"TxSigners": "signers-json",
		"ExtraSigners": "extra-signers-json",
		"SorobanDataSizeBytes": 64,
		"SorobanFeeCharged": 100,
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
