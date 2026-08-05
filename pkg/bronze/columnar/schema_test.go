package columnar

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

func TestEveryTypedTableHasEquivalentArrowLayout(t *testing.T) {
	for name, spec := range bronze.TypedTableSpecs {
		layout, err := LayoutFor(spec)
		if err != nil {
			t.Fatalf("layout %s: %v", name, err)
		}
		if got, want := len(layout.SQLTypes), len(spec.Columns); got != want {
			t.Fatalf("layout %s types = %d, want %d", name, got, want)
		}
	}
}

func TestRecordBuilderConvertsSQLValuesAndPreservesNulls(t *testing.T) {
	layout, err := LayoutFor(bronze.TypedTableSpecs["token_transfers_stream_v1"])
	if err != nil {
		t.Fatal(err)
	}
	allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer allocator.AssertSize(t, 0)
	builder, err := NewRecordBuilder(allocator, layout.Schema, 1)
	if err != nil {
		t.Fatal(err)
	}
	values := []any{
		uint32(123), "tx", int64(456), int64(789), uint32(2), "transfer", nil, "to",
		"asset", "contract", nil, nil, 1.25, "12500000", "CAAAA",
		time.Unix(1_700_000_000, 123_456_000).UTC(), time.Unix(1_800_000_000, 0).UTC(), uint32(123), nil, "v1",
	}
	if err := builder.Append(values); err != nil {
		t.Fatal(err)
	}
	record := builder.NewRecordBatch()
	builder.Release()
	defer record.Release()
	if got := record.Column(0).(*array.Int32).Value(0); got != 123 {
		t.Fatalf("ledger_sequence = %d", got)
	}
	if !record.Column(6).IsNull(0) || !record.Column(18).IsNull(0) {
		t.Fatal("nil values were not preserved as Arrow nulls")
	}
	if got := record.Column(12).(*array.Float64).Value(0); got != 1.25 {
		t.Fatalf("amount = %f", got)
	}
}

func TestRecordBuilderRejectsNarrowingOverflow(t *testing.T) {
	layout, err := LayoutFor(bronze.TypedTableSpecs["token_transfers_stream_v1"])
	if err != nil {
		t.Fatal(err)
	}
	builder, err := NewRecordBuilder(nil, layout.Schema, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Release()
	values := make([]any, len(layout.Schema.Fields()))
	values[0] = uint64(1 << 40)
	if err := builder.Append(values); err == nil {
		t.Fatal("expected INTEGER overflow rejection")
	}
}
