package columnar

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RecordBuilder converts already-projected SQL values into persistent Arrow
// column buffers. It is deliberately strict about narrowing conversions.
type RecordBuilder struct {
	schema *arrow.Schema
	record *array.RecordBuilder
}

func NewRecordBuilder(allocator memory.Allocator, schema *arrow.Schema, reserve int) (*RecordBuilder, error) {
	if schema == nil || len(schema.Fields()) == 0 {
		return nil, fmt.Errorf("Arrow schema is required")
	}
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	record := array.NewRecordBuilder(allocator, schema)
	if reserve > 0 {
		record.Reserve(reserve)
	}
	return &RecordBuilder{schema: schema, record: record}, nil
}

func (builder *RecordBuilder) Release() {
	if builder == nil || builder.record == nil {
		return
	}
	builder.record.Release()
	builder.record = nil
}

func (builder *RecordBuilder) Len() int {
	if builder == nil || builder.record == nil {
		return 0
	}
	return builder.record.Field(0).Len()
}

func (builder *RecordBuilder) Append(values []any) error {
	if builder == nil || builder.record == nil {
		return fmt.Errorf("Arrow record builder is closed")
	}
	if len(values) != len(builder.schema.Fields()) {
		return fmt.Errorf("Arrow row has %d values, schema has %d", len(values), len(builder.schema.Fields()))
	}
	fields := builder.record.Fields()
	for index, value := range values {
		if value == nil {
			fields[index].AppendNull()
			continue
		}
		var err error
		switch target := fields[index].(type) {
		case *array.StringBuilder:
			err = appendString(target, value)
		case *array.Int64Builder:
			err = appendInt64(target, value)
		case *array.Uint64Builder:
			err = appendUint64(target, value)
		case *array.Int32Builder:
			err = appendInt32(target, value)
		case *array.Float64Builder:
			err = appendFloat64(target, value)
		case *array.BooleanBuilder:
			err = appendBool(target, value)
		case *array.TimestampBuilder:
			err = appendTimestamp(target, value)
		default:
			err = fmt.Errorf("unsupported Arrow builder %T", target)
		}
		if err != nil {
			return fmt.Errorf("column %s: %w", builder.schema.Field(index).Name, err)
		}
	}
	return nil
}

func (builder *RecordBuilder) NewRecordBatch() arrow.RecordBatch {
	if builder == nil || builder.record == nil || builder.Len() == 0 {
		return nil
	}
	return builder.record.NewRecordBatch()
}

func appendString(builder *array.StringBuilder, value any) error {
	switch typed := value.(type) {
	case string:
		builder.Append(typed)
	case []byte:
		builder.Append(string(typed))
	case json.RawMessage:
		builder.Append(string(typed))
	default:
		return fmt.Errorf("cannot append %T as VARCHAR", value)
	}
	return nil
}

func appendInt64(builder *array.Int64Builder, value any) error {
	integer, err := signedInteger(value, 64)
	if err != nil {
		return err
	}
	builder.Append(integer)
	return nil
}

func appendUint64(builder *array.Uint64Builder, value any) error {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		builder.Append(reflected.Uint())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		integer := reflected.Int()
		if integer < 0 {
			return fmt.Errorf("%d is negative", integer)
		}
		builder.Append(uint64(integer))
	default:
		return fmt.Errorf("cannot append %T as UBIGINT", value)
	}
	return nil
}

func appendInt32(builder *array.Int32Builder, value any) error {
	integer, err := signedInteger(value, 32)
	if err != nil {
		return err
	}
	builder.Append(int32(integer))
	return nil
}

func signedInteger(value any, bits int) (int64, error) {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		integer := reflected.Int()
		if bits == 32 && (integer < math.MinInt32 || integer > math.MaxInt32) {
			return 0, fmt.Errorf("%d exceeds INTEGER", integer)
		}
		return integer, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		integer := reflected.Uint()
		maximum := uint64(math.MaxInt64)
		if bits == 32 {
			maximum = math.MaxInt32
		}
		if integer > maximum {
			return 0, fmt.Errorf("%d exceeds signed %d-bit integer", integer, bits)
		}
		return int64(integer), nil
	case reflect.String:
		integer, err := strconv.ParseInt(reflected.String(), 10, bits)
		if err != nil {
			return 0, fmt.Errorf("parse integer %q: %w", reflected.String(), err)
		}
		return integer, nil
	default:
		return 0, fmt.Errorf("cannot append %T as signed integer", value)
	}
}

func appendFloat64(builder *array.Float64Builder, value any) error {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Float32, reflect.Float64:
		builder.Append(reflected.Convert(reflect.TypeOf(float64(0))).Float())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		builder.Append(float64(reflected.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		builder.Append(float64(reflected.Uint()))
	default:
		return fmt.Errorf("cannot append %T as DOUBLE", value)
	}
	return nil
}

func appendBool(builder *array.BooleanBuilder, value any) error {
	boolean, ok := value.(bool)
	if !ok {
		return fmt.Errorf("cannot append %T as BOOLEAN", value)
	}
	builder.Append(boolean)
	return nil
}

func appendTimestamp(builder *array.TimestampBuilder, value any) error {
	timestamp, ok := value.(time.Time)
	if !ok {
		return fmt.Errorf("cannot append %T as TIMESTAMP", value)
	}
	builder.Append(arrow.Timestamp(timestamp.UnixMicro()))
	return nil
}
