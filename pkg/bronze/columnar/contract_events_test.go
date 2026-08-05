package columnar

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	extract "github.com/withObsrvr/stellar-extract"
)

func TestContractEventsArrowMatchesTypedProjection(t *testing.T) {
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	contractID := "CAAAA"
	topic0 := "transfer"
	eraID := "protocol-23"
	closedAt := time.Unix(1_700_000_000, 123_456_000).UTC()
	createdAt := time.Unix(1_800_000_000, 654_321_000).UTC()
	rows := []extract.ContractEventData{
		{
			EventID: "event-2", ContractID: &contractID, LedgerSequence: 123,
			TransactionHash: "tx-b", ClosedAt: closedAt, EventType: "contract",
			InSuccessfulContractCall: true, Successful: true,
			ContractEventXDR: "event-xdr", TopicsJSON: `["a"]`, TopicsDecoded: `["transfer"]`,
			DataXDR: "data-xdr", DataDecoded: `{"amount":"1"}`, TopicCount: 1,
			OperationIndex: 2, EventIndex: 3, Topic0Decoded: &topic0,
			CreatedAt: createdAt, LedgerRange: 123, EraID: &eraID,
		},
		{
			EventID: "event-1", LedgerSequence: 123, TransactionHash: "tx-a",
			ClosedAt: closedAt, EventType: "diagnostic", CreatedAt: createdAt,
			LedgerRange: 123,
		},
	}
	SortContractEvents(rows)
	if rows[0].EventID != "event-1" {
		t.Fatalf("first sorted event = %s", rows[0].EventID)
	}

	builder := NewContractEventsBuilder(allocator, len(rows))
	for _, row := range rows {
		if err := builder.Append(row); err != nil {
			t.Fatalf("append event: %v", err)
		}
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	if record.NumRows() != 2 || record.NumCols() != int64(len(ContractEventsSchema.Fields())) {
		t.Fatalf("record dimensions = %d x %d", record.NumRows(), record.NumCols())
	}
	if got := record.Column(0).(*array.String).Value(0); got != "event-1" {
		t.Fatalf("first event_id = %q", got)
	}
	if !record.Column(1).IsNull(0) {
		t.Fatal("nil contract_id was not encoded as Arrow null")
	}
	if got := record.Column(2).(*array.Int64).Value(1); got != 123 {
		t.Fatalf("ledger_sequence = %d", got)
	}
	if got := record.Column(4).(*array.Timestamp).Value(1).ToTime(arrow.Microsecond); !got.Equal(closedAt) {
		t.Fatalf("closed_at = %v, want %v", got, closedAt)
	}
	if got := record.Column(23).(*array.String).Value(1); got != contracts.ExtractionVersion {
		t.Fatalf("version_label = %q", got)
	}

	direct := bronze.ProjectLedgerData(&extract.LedgerData{ContractEvents: rows}, nil)
	if len(direct) != len(rows) {
		t.Fatalf("direct rows = %d, Arrow rows = %d", len(direct), record.NumRows())
	}
	for index := range direct {
		if direct[index].Err != nil || !direct[index].OK {
			t.Fatalf("direct row %d: %+v", index, direct[index])
		}
		if direct[index].Spec.TableName != ContractEventsTable {
			t.Fatalf("direct table = %q", direct[index].Spec.TableName)
		}
	}
}

func TestContractEventsParquetIsDeterministicAndDuckDBCompatible(t *testing.T) {
	rows := []extract.ContractEventData{
		{EventID: "b", LedgerSequence: 124, TransactionHash: "tx-b", ClosedAt: time.Unix(1_700_000_005, 0).UTC(), EventType: "contract", OperationIndex: 1, EventIndex: 1, CreatedAt: time.Unix(1_800_000_000, 0).UTC(), LedgerRange: 124},
		{EventID: "a", LedgerSequence: 123, TransactionHash: "tx-a", ClosedAt: time.Unix(1_700_000_000, 0).UTC(), EventType: "diagnostic", OperationIndex: 0, EventIndex: 0, CreatedAt: time.Unix(1_800_000_000, 0).UTC(), LedgerRange: 123},
	}
	SortContractEvents(rows)
	makeRecord := func() []byte {
		allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
		builder := NewContractEventsBuilder(allocator, len(rows))
		for _, row := range rows {
			if err := builder.Append(row); err != nil {
				t.Fatal(err)
			}
		}
		record := builder.NewRecordBatch()
		builder.Release()
		var output bytes.Buffer
		if err := WriteContractEventsParquet(&output, []arrow.RecordBatch{record}, ParquetWriterOptions{Allocator: allocator, RowGroupRows: 2048}); err != nil {
			t.Fatalf("write Parquet: %v", err)
		}
		record.Release()
		allocator.AssertSize(t, 0)
		return output.Bytes()
	}
	first := makeRecord()
	second := makeRecord()
	if !bytes.Equal(first, second) {
		t.Fatal("identical Arrow records produced different Parquet bytes")
	}

	path := filepath.Join(t.TempDir(), "contract-events.parquet")
	if err := os.WriteFile(path, first, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	var minLedger, maxLedger int64
	query := "SELECT count(*), min(ledger_sequence), max(ledger_sequence) FROM read_parquet(" + bronze.SQLLiteral(path) + ")"
	if err := db.QueryRow(query).Scan(&count, &minLedger, &maxLedger); err != nil {
		t.Fatalf("query Arrow Parquet: %v", err)
	}
	if count != 2 || minLedger != 123 || maxLedger != 124 {
		t.Fatalf("Parquet coverage = %d rows / %d-%d", count, minLedger, maxLedger)
	}

	rowsDescription, err := db.Query("DESCRIBE SELECT * FROM read_parquet(" + bronze.SQLLiteral(path) + ")")
	if err != nil {
		t.Fatal(err)
	}
	defer rowsDescription.Close()
	var names, types []string
	for rowsDescription.Next() {
		var name, dataType, nullable string
		var key, defaultValue, extra sql.NullString
		if err := rowsDescription.Scan(&name, &dataType, &nullable, &key, &defaultValue, &extra); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
		types = append(types, dataType)
	}
	if err := rowsDescription.Err(); err != nil {
		t.Fatal(err)
	}
	wantNames := bronze.TypedTableSpecs[ContractEventsTable].Columns
	if !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("Parquet columns:\n got %v\nwant %v", names, wantNames)
	}
	if types[2] != "BIGINT" || types[4] != "TIMESTAMP" || types[13] != "INTEGER" || types[6] != "BOOLEAN" {
		t.Fatalf("key Parquet types = ledger:%s closed:%s topic_count:%s success:%s", types[2], types[4], types[13], types[6])
	}
}
