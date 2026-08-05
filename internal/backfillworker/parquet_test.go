package backfillworker

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

func TestWriteParquetShardUsesTypedSchemaAndStableInputOrder(t *testing.T) {
	decoded := decodeLedgerRows(102, 100)
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	cfg := ParquetConfig{OutputDir: firstDir, LedgerStart: 100, LedgerEnd: 102, Compression: "zstd"}
	first, err := WriteParquetShard(context.Background(), cfg, decoded)
	if err != nil {
		t.Fatalf("write first shard: %v", err)
	}
	cfg.OutputDir = secondDir
	second, err := WriteParquetShard(context.Background(), cfg, decoded)
	if err != nil {
		t.Fatalf("write second shard: %v", err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("files = %d and %d, want one each", len(first), len(second))
	}
	if first[0].SHA256 != second[0].SHA256 {
		t.Fatalf("Parquet hashes differ: %s != %s", first[0].SHA256, second[0].SHA256)
	}
	if first[0].ParquetSchemaFingerprint != second[0].ParquetSchemaFingerprint {
		t.Fatalf("schema fingerprints differ")
	}
	if first[0].Table != "bronze.ledgers_row_v2" || first[0].Rows != 2 || first[0].MinLedger != 100 || first[0].MaxLedger != 102 {
		t.Fatalf("file artifact = %+v", first[0])
	}

	parsed, err := url.Parse(first[0].URI)
	if err != nil {
		t.Fatalf("parse file URI: %v", err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open verifier DuckDB: %v", err)
	}
	defer db.Close()
	var count int
	var minLedger, maxLedger uint32
	if err := db.QueryRow(
		"SELECT count(*), min(sequence), max(sequence) FROM read_parquet("+bronze.SQLLiteral(parsed.Path)+")",
	).Scan(&count, &minLedger, &maxLedger); err != nil {
		t.Fatalf("query output Parquet: %v", err)
	}
	if count != 2 || minLedger != 100 || maxLedger != 102 {
		t.Fatalf("Parquet coverage = count %d range %d-%d", count, minLedger, maxLedger)
	}
	var ordinalColumns int
	if err := db.QueryRow(
		"SELECT count(*) FROM (DESCRIBE SELECT * FROM read_parquet("+bronze.SQLLiteral(parsed.Path)+")) WHERE column_name = ?",
		ordinalColumn,
	).Scan(&ordinalColumns); err != nil {
		t.Fatalf("inspect Parquet schema: %v", err)
	}
	if ordinalColumns != 0 {
		t.Fatalf("stable ordinal leaked into published schema")
	}
}

func TestWriteParquetShardNeverOverwritesFinalFile(t *testing.T) {
	outputDir := t.TempDir()
	cfg := ParquetConfig{OutputDir: outputDir, LedgerStart: 100, LedgerEnd: 102}
	if _, err := WriteParquetShard(context.Background(), cfg, decodeLedgerRows(100, 102)); err != nil {
		t.Fatalf("write first shard: %v", err)
	}
	_, err := WriteParquetShard(context.Background(), cfg, decodeLedgerRows(100, 102))
	if err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("second write error = %v, want overwrite rejection", err)
	}
	matches, globErr := filepath.Glob(filepath.Join(outputDir, ".*.partial-*"))
	if globErr != nil {
		t.Fatalf("glob partial outputs: %v", globErr)
	}
	if len(matches) != 0 {
		t.Fatalf("partial outputs remain: %v", matches)
	}
}

func TestWriteParquetShardRejectsRowsOutsideShard(t *testing.T) {
	_, err := WriteParquetShard(context.Background(), ParquetConfig{
		OutputDir:   t.TempDir(),
		LedgerStart: 100,
		LedgerEnd:   101,
	}, decodeLedgerRows(100, 102))
	if err == nil || !strings.Contains(err.Error(), "falls outside shard") {
		t.Fatalf("WriteParquetShard error = %v, want range rejection", err)
	}
}

func TestWriteParquetShardCoversEveryTypedBronzeTable(t *testing.T) {
	const ledgerSequence = 777001
	tableNames := make([]string, 0, len(bronze.TypedTableSpecs))
	for name := range bronze.TypedTableSpecs {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	rows := make([]*componentsv1.BronzeRow, 0, len(tableNames))
	for index, name := range tableNames {
		rows = append(rows, &componentsv1.BronzeRow{
			Id:                fmt.Sprintf("row-%d", index),
			TableName:         name,
			NetworkPassphrase: "testnet",
			LedgerSequence:    ledgerSequence,
			LedgerRange:       770000,
			RowJson:           allTableFixtureJSON(ledgerSequence),
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
	files, err := WriteParquetShard(context.Background(), ParquetConfig{
		OutputDir:   t.TempDir(),
		LedgerStart: ledgerSequence,
		LedgerEnd:   ledgerSequence,
	}, bronze.DecodeTypedRows(batch))
	if err != nil {
		t.Fatalf("write all-table shard: %v", err)
	}
	if len(files) != len(tableNames) {
		t.Fatalf("published files = %d, want %d", len(files), len(tableNames))
	}
	for index, file := range files {
		if file.Table != "bronze."+tableNames[index] || file.Rows != 1 {
			t.Fatalf("file %d = %+v, want table %s with one row", index, file, tableNames[index])
		}
	}
}

func decodeLedgerRows(sequences ...uint32) []bronze.DecodedRow {
	rows := make([]*componentsv1.BronzeRow, 0, len(sequences))
	for _, sequence := range sequences {
		rows = append(rows, &componentsv1.BronzeRow{
			Id:             fmt.Sprintf("ledger-%d", sequence),
			TableName:      "ledgers_row_v2",
			LedgerSequence: sequence,
			LedgerRange:    sequence,
			RowJson: fmt.Sprintf(`{
				"Sequence": %d,
				"LedgerRange": %d,
				"LedgerHash": "hash-%d",
				"PreviousLedgerHash": "previous-%d"
			}`, sequence, sequence, sequence, sequence),
		})
	}
	return bronze.DecodeTypedRows(&componentsv1.LedgerBatch{BronzeRows: rows})
}

func allTableFixtureJSON(ledgerSequence uint32) string {
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
