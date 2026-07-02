package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"google.golang.org/protobuf/proto"
)

func TestHandleLedgerBatchEventRetriesWriteFailures(t *testing.T) {
	previousBackoff := writeRetryBackoff
	writeRetryBackoff = time.Millisecond
	t.Cleanup(func() { writeRetryBackoff = previousBackoff })

	payload, err := proto.Marshal(&componentsv1.LedgerBatch{
		NetworkPassphrase: "testnet",
		LedgerSequence:    123,
	})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	writer := &retryWriter{failuresRemaining: 2}
	err = handleLedgerBatchEvent(context.Background(), &flowctlv1.Event{
		Type:    contracts.LedgerBatchEventType,
		Payload: payload,
	}, writer)
	if err != nil {
		t.Fatalf("handle event: %v", err)
	}
	if writer.calls != 3 {
		t.Fatalf("writer calls = %d, want 3", writer.calls)
	}
}

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

	var watermarkCount int
	if err := sink.db.QueryRow(
		"SELECT count(*) FROM ingest_watermarks WHERE network_passphrase = ? AND ledger_sequence = ?",
		"testnet",
		ledgerSequence,
	).Scan(&watermarkCount); err != nil {
		t.Fatalf("count ingest watermarks: %v", err)
	}
	if watermarkCount != 1 {
		t.Fatalf("watermark count = %d, want 1", watermarkCount)
	}

	var txEnvelope string
	if err := sink.db.QueryRow(
		"SELECT tx_envelope FROM bronze.transactions_row_v2 WHERE ledger_sequence = ?",
		ledgerSequence,
	).Scan(&txEnvelope); err != nil {
		t.Fatalf("select tx_envelope: %v", err)
	}
	if txEnvelope != "envelope-xdr" {
		t.Fatalf("tx_envelope = %q, want envelope-xdr", txEnvelope)
	}
}

func TestDuckLakeSinkEnrichesTransactionXDRFromBatchRows(t *testing.T) {
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

	const ledgerSequence = 54321
	if err := sink.WriteBatch(&componentsv1.LedgerBatch{
		NetworkPassphrase: "testnet",
		LedgerSequence:    ledgerSequence,
		ClosedAtUnix:      1782900000,
		SchemaVersion:     contracts.SchemaVersion,
		ExtractionVersion: contracts.ExtractionVersion,
		Transactions: []*componentsv1.TransactionRow{{
			LedgerSequence:  ledgerSequence,
			TransactionHash: "tx-hash",
			EnvelopeXdr:     "batch-envelope-xdr",
			ResultXdr:       "batch-result-xdr",
			MetaXdr:         "batch-meta-xdr",
		}},
		BronzeRows: []*componentsv1.BronzeRow{{
			Id:                "row-1",
			TableName:         "transactions_row_v2",
			NetworkPassphrase: "testnet",
			LedgerSequence:    ledgerSequence,
			LedgerRange:       50000,
			RowJson:           transactionRowJSONWithoutXDR(ledgerSequence, "tx-hash"),
		}},
	}); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	var envelope, result, meta string
	if err := sink.db.QueryRow(
		"SELECT tx_envelope, tx_result, tx_meta FROM bronze.transactions_row_v2 WHERE ledger_sequence = ?",
		ledgerSequence,
	).Scan(&envelope, &result, &meta); err != nil {
		t.Fatalf("select transaction xdr fields: %v", err)
	}
	if envelope != "batch-envelope-xdr" || result != "batch-result-xdr" || meta != "batch-meta-xdr" {
		t.Fatalf("xdr enrichment = %q/%q/%q", envelope, result, meta)
	}
}

func TestDuckLakeSinkWatermarkGapQueryAfterRestart(t *testing.T) {
	tmp := t.TempDir()
	cfg := DuckLakeConfig{
		CatalogPath: filepath.Join(tmp, "stellar.ducklake"),
		DataPath:    filepath.Join(tmp, "data"),
		AttachName:  "test_lake",
	}
	sink, err := NewDuckLakeSink(cfg)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	const startLedger = 1000
	const ledgerCount = 1000
	tx, err := sink.db.Begin()
	if err != nil {
		t.Fatalf("begin watermark seed transaction: %v", err)
	}
	for i := uint32(0); i < ledgerCount; i++ {
		if _, err := tx.Exec(
			"INSERT INTO ingest_watermarks (network_passphrase, ledger_sequence, written_at) VALUES (?, ?, current_timestamp)",
			"testnet",
			startLedger+i,
		); err != nil {
			_ = tx.Rollback()
			t.Fatalf("seed watermark %d: %v", startLedger+i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit watermark seed transaction: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close before restart: %v", err)
	}

	restarted, err := NewDuckLakeSink(cfg)
	if err != nil {
		t.Fatalf("restart sink: %v", err)
	}
	t.Cleanup(func() {
		if err := restarted.Close(); err != nil {
			t.Fatalf("close restarted sink: %v", err)
		}
	})

	var watermarkCount int
	if err := restarted.db.QueryRow(
		"SELECT count(*) FROM ingest_watermarks WHERE network_passphrase = ?",
		"testnet",
	).Scan(&watermarkCount); err != nil {
		t.Fatalf("count watermarks after restart: %v", err)
	}
	if watermarkCount != ledgerCount {
		t.Fatalf("watermark count = %d, want %d", watermarkCount, ledgerCount)
	}

	var gapCount int
	if err := restarted.db.QueryRow(`
WITH bounds AS (
  SELECT
    min(ledger_sequence) AS min_seq,
    max(ledger_sequence) AS max_seq
  FROM ingest_watermarks
  WHERE network_passphrase = ?
),
expected AS (
  SELECT range AS ledger_sequence
  FROM bounds, range(CAST(min_seq AS BIGINT), CAST(max_seq AS BIGINT) + 1)
)
SELECT count(*)
FROM expected
LEFT JOIN ingest_watermarks USING (ledger_sequence)
WHERE ingest_watermarks.ledger_sequence IS NULL
`, "testnet").Scan(&gapCount); err != nil {
		t.Fatalf("gap query after restart: %v", err)
	}
	if gapCount != 0 {
		t.Fatalf("gap count = %d, want 0", gapCount)
	}
}

func TestDuckLakeSinkThousandLedgerIngestGapAfterRestart(t *testing.T) {
	if os.Getenv("DUCKLAKE_SINK_SLOW_1K") != "1" {
		t.Skip("set DUCKLAKE_SINK_SLOW_1K=1 to run the 1k-ledger ingest acceptance test")
	}
	tmp := t.TempDir()
	cfg := DuckLakeConfig{
		CatalogPath: filepath.Join(tmp, "stellar.ducklake"),
		DataPath:    filepath.Join(tmp, "data"),
		AttachName:  "test_lake",
	}
	sink, err := NewDuckLakeSink(cfg)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	const startLedger = 2000
	const ledgerCount = 1000
	for i := uint32(0); i < ledgerCount; i++ {
		if err := sink.WriteBatch(&componentsv1.LedgerBatch{
			NetworkPassphrase: "testnet",
			LedgerSequence:    startLedger + i,
			ClosedAtUnix:      int64(1782910000 + i),
			SchemaVersion:     contracts.SchemaVersion,
			ExtractionVersion: contracts.ExtractionVersion,
		}); err != nil {
			t.Fatalf("write batch %d: %v", startLedger+i, err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close before restart: %v", err)
	}

	restarted, err := NewDuckLakeSink(cfg)
	if err != nil {
		t.Fatalf("restart sink: %v", err)
	}
	t.Cleanup(func() {
		if err := restarted.Close(); err != nil {
			t.Fatalf("close restarted sink: %v", err)
		}
	})

	var gapCount int
	if err := restarted.db.QueryRow(`
WITH bounds AS (
  SELECT
    min(ledger_sequence) AS min_seq,
    max(ledger_sequence) AS max_seq
  FROM ingest_watermarks
  WHERE network_passphrase = ?
),
expected AS (
  SELECT range AS ledger_sequence
  FROM bounds, range(CAST(min_seq AS BIGINT), CAST(max_seq AS BIGINT) + 1)
)
SELECT count(*)
FROM expected
LEFT JOIN ingest_watermarks USING (ledger_sequence)
WHERE ingest_watermarks.ledger_sequence IS NULL
`, "testnet").Scan(&gapCount); err != nil {
		t.Fatalf("gap query after restart: %v", err)
	}
	if gapCount != 0 {
		t.Fatalf("gap count = %d, want 0", gapCount)
	}
}

func TestRemoteWriteSQLIncludesWatermarkAndNetworkCheck(t *testing.T) {
	sink := &DuckLakeSink{remoteCatalog: "stellar_lake"}
	sqlText, err := sink.remoteWriteSQL(&componentsv1.LedgerBatch{
		NetworkPassphrase: "testnet",
		LedgerSequence:    987,
		ClosedAtUnix:      1782900000,
		SchemaVersion:     contracts.SchemaVersion,
		ExtractionVersion: contracts.ExtractionVersion,
		Transactions: []*componentsv1.TransactionRow{{
			LedgerSequence:  987,
			TransactionHash: "tx-hash",
			EnvelopeXdr:     "batch-envelope-xdr",
			ResultXdr:       "batch-result-xdr",
			MetaXdr:         "batch-meta-xdr",
		}},
		BronzeRows: []*componentsv1.BronzeRow{{
			Id:                "row-1",
			TableName:         "transactions_row_v2",
			NetworkPassphrase: "testnet",
			LedgerSequence:    987,
			LedgerRange:       0,
			RowJson:           transactionRowJSONWithoutXDR(987, "tx-hash"),
		}},
	})
	if err != nil {
		t.Fatalf("remote write SQL: %v", err)
	}
	for _, want := range []string{
		"BEGIN TRANSACTION",
		"stellar_lake.catalog_metadata",
		"catalog network mismatch",
		"DELETE FROM stellar_lake.ingest_watermarks",
		"INSERT INTO stellar_lake.ingest_watermarks",
		"INSERT INTO stellar_lake.bronze.transactions_row_v2",
		"'batch-envelope-xdr'",
		"'batch-result-xdr'",
		"'batch-meta-xdr'",
		"COMMIT;",
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("remote write SQL missing %q in:\n%s", want, sqlText)
		}
	}
}

func TestDuckLakeSinkRejectsNetworkMismatch(t *testing.T) {
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

	if err := sink.WriteBatch(&componentsv1.LedgerBatch{
		NetworkPassphrase: "testnet",
		LedgerSequence:    1,
		SchemaVersion:     contracts.SchemaVersion,
		ExtractionVersion: contracts.ExtractionVersion,
	}); err != nil {
		t.Fatalf("write first batch: %v", err)
	}
	if err := sink.WriteBatch(&componentsv1.LedgerBatch{
		NetworkPassphrase: "pubnet",
		LedgerSequence:    2,
		SchemaVersion:     contracts.SchemaVersion,
		ExtractionVersion: contracts.ExtractionVersion,
	}); err == nil {
		t.Fatalf("write mismatched network succeeded")
	}
}

func TestTypedTableSpecsResolveAllColumns(t *testing.T) {
	if err := validateTypedTableSpecs(); err != nil {
		t.Fatalf("validate typed table specs: %v", err)
	}
}

func TestSinkHealthSnapshotReportsLastWrite(t *testing.T) {
	sink := &DuckLakeSink{}
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)

	if snapshot := sink.healthSnapshot(now); !snapshot.Healthy || !snapshot.LastWriteAt.IsZero() {
		t.Fatalf("initial snapshot = %+v, want healthy with no write", snapshot)
	}

	sink.recordWriteHealth(10, errors.New("write failed"))
	failed := sink.healthSnapshot(time.Now().UTC())
	if failed.Healthy {
		t.Fatalf("failed snapshot = %+v, want unhealthy", failed)
	}
	if failed.LastLedger != 10 || !strings.Contains(failed.LastError, "write failed") {
		t.Fatalf("failed snapshot = %+v, want ledger/error", failed)
	}

	sink.recordWriteHealth(11, nil)
	healthy := sink.healthSnapshot(time.Now().UTC())
	if !healthy.Healthy || healthy.LastLedger != 11 || healthy.LastError != "" {
		t.Fatalf("healthy snapshot = %+v, want recovered write state", healthy)
	}
}

type retryWriter struct {
	failuresRemaining int
	calls             int
}

func (w *retryWriter) WriteBatch(*componentsv1.LedgerBatch) error {
	w.calls++
	if w.failuresRemaining > 0 {
		w.failuresRemaining--
		return errors.New("temporary write failure")
	}
	return nil
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

func transactionRowJSONWithoutXDR(ledgerSequence uint32, transactionHash string) string {
	return fmt.Sprintf(`{
		"LedgerSequence": %[1]d,
		"TransactionHash": %[2]q,
		"SourceAccount": "source",
		"FeeCharged": 400,
		"MaxFee": 1000,
		"Successful": true,
		"TransactionResultCode": "TransactionResultCodeTxSuccess",
		"OperationCount": 1,
		"MemoType": "none",
		"Memo": "",
		"CreatedAt": "2026-01-01T00:00:00Z",
		"AccountSequence": 1,
		"LedgerRange": 50000,
		"SignaturesCount": 1,
		"NewAccount": false,
		"TransactionID": 1
	}`, ledgerSequence, transactionHash)
}
