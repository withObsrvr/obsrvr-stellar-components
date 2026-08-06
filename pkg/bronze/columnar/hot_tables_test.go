package columnar

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	extract "github.com/withObsrvr/stellar-extract"
)

func TestGeneratedHotTableBuildersMatchGenericProjection(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	closedAt := time.Unix(1_700_000_000, 123_456_000).UTC()
	text := "value"
	integer := 7
	integer32 := int32(8)
	integer64 := int64(9)
	boolean := true
	data := &extract.LedgerData{
		Transactions: []extract.TransactionData{{
			LedgerSequence: 123, TransactionHash: "tx", SourceAccount: "GABC",
			FeeCharged: 10, MaxFee: 20, Successful: true, TransactionResultCode: "txSUCCESS",
			OperationCount: 2, MemoType: &text, Memo: &text, CreatedAt: closedAt,
			AccountSequence: 99, LedgerRange: 123, SourceAccountMuxed: &text,
			TimeboundsMinTime: &integer64, SorobanResourcesInstructions: &integer64,
			SorobanHostFunctionType: &text, SorobanContractID: &text,
			SignaturesCount: 3, NewAccount: true, RentFeeCharged: &integer64,
			TransactionID: 123000001,
		}},
		Operations: []extract.OperationData{{
			TransactionHash: "tx", TransactionIndex: 1, OperationIndex: 2,
			LedgerSequence: 123, SourceAccount: "GABC", SourceAccountMuxed: &text,
			OpType: 1, TypeString: "payment", CreatedAt: closedAt,
			TransactionSuccessful: true, OperationResultCode: &text, LedgerRange: 123,
			Amount: &integer64, Asset: &text, Destination: &text, SetFlags: &integer,
			SorobanAuthRequired: &boolean, ContractsInvolved: []string{"CA", "CB"},
			MaxCallDepth: &integer, TransactionID: 123000001, OperationID: 123000002,
		}},
		Effects: []extract.EffectData{{
			LedgerSequence: 123, TransactionHash: "tx", OperationIndex: 2,
			EffectIndex: 3, EffectType: 4, EffectTypeString: "account_created",
			AccountID: &text, Amount: &text, AuthorizeFlag: &boolean,
			SignerWeight: &integer, OfferID: &integer64, CreatedAt: closedAt,
			LedgerRange: 123, DetailsJSON: &text, OperationID: &integer64,
		}},
		TokenTransfers: []extract.TokenTransferData{{
			LedgerSequence: 123, TransactionHash: "tx", TransactionID: 123000001,
			OperationID: &integer64, OperationIndex: &integer32, EventType: "transfer",
			From: &text, To: &text, Asset: "native", AssetType: "native",
			Amount: 1.25, AmountRaw: "12500000", ContractID: "",
			ClosedAt: closedAt, CreatedAt: closedAt, LedgerRange: 123,
		}},
	}
	overrides := bronze.TransactionOverrides{"tx": {
		"tx_envelope": "envelope-xdr",
		"tx_result":   "result-xdr",
		"tx_meta":     "meta-xdr",
	}}
	projected := bronze.ProjectLedgerData(data, overrides)
	byTable := make(map[string]bronze.DecodedRow, len(projected))
	for _, row := range projected {
		if row.Err != nil || !row.OK {
			t.Fatalf("project %s: %v", row.Spec.TableName, row.Err)
		}
		byTable[row.Spec.TableName] = row
	}

	tests := []struct {
		table  string
		direct func() arrow.RecordBatch
	}{
		{table: "transactions_row_v2", direct: func() arrow.RecordBatch {
			builder := NewTransactionsBuilder(allocator, 1)
			if err := builder.Append(data.Transactions[0], overrides["tx"]); err != nil {
				t.Fatal(err)
			}
			record := builder.NewRecordBatch()
			builder.Release()
			return record
		}},
		{table: "operations_row_v2", direct: func() arrow.RecordBatch {
			builder := NewOperationsBuilder(allocator, 1)
			if err := builder.Append(data.Operations[0]); err != nil {
				t.Fatal(err)
			}
			record := builder.NewRecordBatch()
			builder.Release()
			return record
		}},
		{table: "effects_row_v1", direct: func() arrow.RecordBatch {
			builder := NewEffectsBuilder(allocator, 1)
			if err := builder.Append(data.Effects[0]); err != nil {
				t.Fatal(err)
			}
			record := builder.NewRecordBatch()
			builder.Release()
			return record
		}},
		{table: "token_transfers_stream_v1", direct: func() arrow.RecordBatch {
			builder := NewTokenTransfersBuilder(allocator, 1)
			if err := builder.Append(data.TokenTransfers[0]); err != nil {
				t.Fatal(err)
			}
			record := builder.NewRecordBatch()
			builder.Release()
			return record
		}},
	}

	for _, test := range tests {
		t.Run(test.table, func(t *testing.T) {
			row, ok := byTable[test.table]
			if !ok {
				t.Fatalf("generic projection omitted %s", test.table)
			}
			layout, err := LayoutFor(row.Spec)
			if err != nil {
				t.Fatal(err)
			}
			generic, err := NewRecordBuilder(allocator, layout.Schema, 1)
			if err != nil {
				t.Fatal(err)
			}
			if err := generic.Append(row.Values); err != nil {
				t.Fatal(err)
			}
			genericRecord := generic.NewRecordBatch()
			generic.Release()
			directRecord := test.direct()
			if !array.RecordEqual(genericRecord, directRecord) {
				t.Fatalf("generated Arrow record differs from generic projection:\ngeneric=%s\ndirect=%s", genericRecord, directRecord)
			}
			genericRecord.Release()
			directRecord.Release()
		})
	}
}
