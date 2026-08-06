package columnar

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

const (
	TransactionsTable   = "transactions_row_v2"
	OperationsTable     = "operations_row_v2"
	EffectsTable        = "effects_row_v1"
	TokenTransfersTable = "token_transfers_stream_v1"
)

type extractRecordBuilder struct {
	record *array.RecordBuilder
}

func newExtractRecordBuilder(schema *arrow.Schema, allocator memory.Allocator, reserve int) *extractRecordBuilder {
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	record := array.NewRecordBuilder(allocator, schema)
	if reserve > 0 {
		record.Reserve(reserve)
	}
	return &extractRecordBuilder{record: record}
}

func (builder *extractRecordBuilder) Release() {
	if builder == nil || builder.record == nil {
		return
	}
	builder.record.Release()
	builder.record = nil
}

func (builder *extractRecordBuilder) Len() int {
	if builder == nil || builder.record == nil {
		return 0
	}
	return builder.record.Field(0).Len()
}

func (builder *extractRecordBuilder) NewRecordBatch() arrow.RecordBatch {
	if builder == nil || builder.record == nil || builder.Len() == 0 {
		return nil
	}
	return builder.record.NewRecordBatch()
}

func mustTypedLayout(tableName string) TypedTableLayout {
	spec, ok := bronze.TypedTableSpecs[tableName]
	if !ok {
		panic(fmt.Sprintf("typed Bronze table %s is not registered", tableName))
	}
	layout, err := LayoutFor(spec)
	if err != nil {
		panic(err)
	}
	return layout
}

func appendIntAsInt32(builder *array.Int32Builder, value int) error {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return fmt.Errorf("%d exceeds Arrow int32", value)
	}
	builder.Append(int32(value))
	return nil
}

func appendUint32AsInt32(builder *array.Int32Builder, value uint32) error {
	if value > math.MaxInt32 {
		return fmt.Errorf("%d exceeds Arrow int32", value)
	}
	builder.Append(int32(value))
	return nil
}

func appendOptionalIntAsInt32(builder *array.Int32Builder, value *int) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}
	return appendIntAsInt32(builder, *value)
}

func appendOptionalInt32(builder *array.Int32Builder, value *int32) {
	if value == nil {
		builder.AppendNull()
		return
	}
	builder.Append(*value)
}

func appendOptionalInt64(builder *array.Int64Builder, value *int64) {
	if value == nil {
		builder.AppendNull()
		return
	}
	builder.Append(*value)
}

func appendOptionalBool(builder *array.BooleanBuilder, value *bool) {
	if value == nil {
		builder.AppendNull()
		return
	}
	builder.Append(*value)
}

func appendStringSliceJSON(builder *array.StringBuilder, value []string) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	builder.Append(string(data))
	return nil
}

func appendRawTransactionString(builder *array.StringBuilder, overrides map[string]any, column string) error {
	value, ok := overrides[column]
	if !ok || value == nil {
		builder.AppendNull()
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return fmt.Errorf("raw transaction override %s is %T, want string", column, value)
	}
	builder.Append(text)
	return nil
}
