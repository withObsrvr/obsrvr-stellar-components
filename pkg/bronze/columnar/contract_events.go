// Package columnar converts typed Stellar extraction rows directly into
// bounded Apache Arrow records. It is the columnar handoff used by historical
// file workers; live ingest keeps its durable row-oriented RPC contract.
package columnar

import (
	"cmp"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	extract "github.com/withObsrvr/stellar-extract"
)

const (
	ContractEventsTable = "contract_events_stream_v1"
	parquetCreatedBy    = "obsrvr-stellar-components/columnar-v1"
)

var timestampWithoutTimeZone = &arrow.TimestampType{Unit: arrow.Microsecond}

// ContractEventsSchema is intentionally equivalent to the public DuckLake
// Bronze table, without a private staging ordinal.
var ContractEventsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "event_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "contract_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "ledger_sequence", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "transaction_hash", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "closed_at", Type: timestampWithoutTimeZone, Nullable: true},
	{Name: "event_type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "in_successful_contract_call", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	{Name: "successful", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	{Name: "contract_event_xdr", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "topics_json", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "topics_decoded", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "data_xdr", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "data_decoded", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "topic_count", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "operation_index", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "event_index", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "topic0_decoded", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "topic1_decoded", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "topic2_decoded", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "topic3_decoded", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "created_at", Type: timestampWithoutTimeZone, Nullable: true},
	{Name: "ledger_range", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "era_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "version_label", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// ContractEventsBuilder owns Arrow buffers until Release. NewRecordBatch
// transfers the current arrays into an immutable record and resets the builder.
type ContractEventsBuilder struct {
	record *array.RecordBuilder
}

func NewContractEventsBuilder(allocator memory.Allocator, reserve int) *ContractEventsBuilder {
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	record := array.NewRecordBuilder(allocator, ContractEventsSchema)
	if reserve > 0 {
		record.Reserve(reserve)
	}
	return &ContractEventsBuilder{record: record}
}

func (builder *ContractEventsBuilder) Schema() *arrow.Schema {
	return ContractEventsSchema
}

func (builder *ContractEventsBuilder) Release() {
	if builder == nil || builder.record == nil {
		return
	}
	builder.record.Release()
	builder.record = nil
}

func (builder *ContractEventsBuilder) Len() int {
	if builder == nil || builder.record == nil {
		return 0
	}
	return builder.record.Field(0).Len()
}

func (builder *ContractEventsBuilder) Append(row extract.ContractEventData) error {
	if builder == nil || builder.record == nil {
		return fmt.Errorf("contract event builder is closed")
	}
	if row.OperationIndex > math.MaxInt32 {
		return fmt.Errorf("operation_index %d exceeds Arrow int32", row.OperationIndex)
	}
	if row.EventIndex > math.MaxInt32 {
		return fmt.Errorf("event_index %d exceeds Arrow int32", row.EventIndex)
	}
	fields := builder.record.Fields()
	fields[0].(*array.StringBuilder).Append(row.EventID)
	appendOptionalString(fields[1].(*array.StringBuilder), row.ContractID)
	fields[2].(*array.Int64Builder).Append(int64(row.LedgerSequence))
	fields[3].(*array.StringBuilder).Append(row.TransactionHash)
	fields[4].(*array.TimestampBuilder).Append(arrow.Timestamp(row.ClosedAt.UnixMicro()))
	fields[5].(*array.StringBuilder).Append(row.EventType)
	fields[6].(*array.BooleanBuilder).Append(row.InSuccessfulContractCall)
	fields[7].(*array.BooleanBuilder).Append(row.Successful)
	fields[8].(*array.StringBuilder).Append(row.ContractEventXDR)
	fields[9].(*array.StringBuilder).Append(row.TopicsJSON)
	fields[10].(*array.StringBuilder).Append(row.TopicsDecoded)
	fields[11].(*array.StringBuilder).Append(row.DataXDR)
	fields[12].(*array.StringBuilder).Append(row.DataDecoded)
	fields[13].(*array.Int32Builder).Append(row.TopicCount)
	fields[14].(*array.Int32Builder).Append(int32(row.OperationIndex))
	fields[15].(*array.Int32Builder).Append(int32(row.EventIndex))
	appendOptionalString(fields[16].(*array.StringBuilder), row.Topic0Decoded)
	appendOptionalString(fields[17].(*array.StringBuilder), row.Topic1Decoded)
	appendOptionalString(fields[18].(*array.StringBuilder), row.Topic2Decoded)
	appendOptionalString(fields[19].(*array.StringBuilder), row.Topic3Decoded)
	fields[20].(*array.TimestampBuilder).Append(arrow.Timestamp(row.CreatedAt.UnixMicro()))
	fields[21].(*array.Int64Builder).Append(int64(row.LedgerRange))
	appendOptionalString(fields[22].(*array.StringBuilder), row.EraID)
	fields[23].(*array.StringBuilder).Append(contracts.ExtractionVersion)
	return nil
}

func (builder *ContractEventsBuilder) NewRecordBatch() arrow.RecordBatch {
	if builder == nil || builder.record == nil {
		return nil
	}
	return builder.record.NewRecordBatch()
}

func appendOptionalString(builder *array.StringBuilder, value *string) {
	if value == nil {
		builder.AppendNull()
		return
	}
	builder.Append(*value)
}

// SortContractEvents establishes deterministic order within one ledger. The
// archive stream already provides ledger order, so this avoids a shard-wide
// sort while making extractor map traversal irrelevant.
func SortContractEvents(rows []extract.ContractEventData) {
	slices.SortStableFunc(rows, func(left, right extract.ContractEventData) int {
		for _, comparison := range []int{
			cmp.Compare(left.LedgerSequence, right.LedgerSequence),
			cmp.Compare(left.TransactionHash, right.TransactionHash),
			cmp.Compare(left.OperationIndex, right.OperationIndex),
			cmp.Compare(left.EventIndex, right.EventIndex),
			cmp.Compare(left.EventType, right.EventType),
			cmp.Compare(left.EventID, right.EventID),
			cmp.Compare(left.ContractEventXDR, right.ContractEventXDR),
			cmp.Compare(left.TopicsJSON, right.TopicsJSON),
			cmp.Compare(left.DataXDR, right.DataXDR),
		} {
			if comparison != 0 {
				return comparison
			}
		}
		return 0
	})
}

type ParquetWriterOptions struct {
	RowGroupRows int64
	ZstdLevel    int
	Allocator    memory.Allocator
}

// WriteContractEventsParquet writes already ordered records directly from
// Arrow buffers. Every option affecting physical bytes is pinned here so two
// attempts with the same records produce the same artifact.
func WriteContractEventsParquet(output io.Writer, records []arrow.RecordBatch, opts ParquetWriterOptions) error {
	if output == nil {
		return fmt.Errorf("Parquet output is required")
	}
	if opts.RowGroupRows <= 0 {
		opts.RowGroupRows = 16_384
	}
	if opts.ZstdLevel == 0 {
		opts.ZstdLevel = 3
	}
	if opts.Allocator == nil {
		opts.Allocator = memory.DefaultAllocator
	}
	properties := parquet.NewWriterProperties(
		parquet.WithAllocator(opts.Allocator),
		parquet.WithCreatedBy(parquetCreatedBy),
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(opts.ZstdLevel),
		parquet.WithDictionaryDefault(true),
		parquet.WithMaxRowGroupLength(opts.RowGroupRows),
		parquet.WithStats(true),
	)
	arrowProperties := pqarrow.NewArrowWriterProperties(
		pqarrow.WithAllocator(opts.Allocator),
	)
	writer, err := pqarrow.NewFileWriter(ContractEventsSchema, output, properties, arrowProperties)
	if err != nil {
		return fmt.Errorf("create contract-events Parquet writer: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
		}
	}()
	for index, record := range records {
		if record == nil {
			return fmt.Errorf("contract-events record %d is nil", index)
		}
		if !record.Schema().Equal(ContractEventsSchema) {
			return fmt.Errorf("contract-events record %d has divergent schema", index)
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("write contract-events record %d: %w", index, err)
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close contract-events Parquet writer: %w", err)
	}
	closed = true
	return nil
}
