package backfillworker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	parquetFile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	bronzeColumnar "github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze/columnar"
	extract "github.com/withObsrvr/stellar-extract"
)

const columnarParquetCreatedBy = "obsrvr-stellar-components/backfill-columnar-v1"

type columnarShardWriter struct {
	cfg       ParquetConfig
	outputDir string
	allocator memory.Allocator
	tables    map[string]*columnarTableWriter
	files     []backfillmanifest.File
	closed    bool
}

type columnarTableWriter struct {
	owner          *columnarShardWriter
	publicName     string
	tableName      string
	ledgerColumn   int
	layout         bronzeColumnar.TypedTableLayout
	builder        *bronzeColumnar.RecordBuilder
	contractEvents *bronzeColumnar.ContractEventsBuilder
	part           *columnarPart
	partIndex      int
	files          []backfillmanifest.File
}

type columnarPart struct {
	temporaryPath string
	file          *os.File
	writer        *pqarrow.FileWriter
	rows          uint64
	minLedger     uint32
	maxLedger     uint32
}

// pqarrow closes an output that implements io.Closer. The shard writer owns
// fsync and publication, so expose only Write and retain the file lifetime.
type nonClosingFileWriter struct {
	file *os.File
}

func (writer nonClosingFileWriter) Write(data []byte) (int, error) {
	return writer.file.Write(data)
}

func newColumnarShardWriter(cfg ParquetConfig, outputDir string) *columnarShardWriter {
	if cfg.RowGroupRows == 0 {
		cfg.RowGroupRows = 16_384
	}
	return &columnarShardWriter{
		cfg:       cfg,
		outputDir: outputDir,
		allocator: memory.DefaultAllocator,
		tables:    make(map[string]*columnarTableWriter, len(bronze.TypedTableSpecs)+2),
	}
}

func (writer *columnarShardWriter) appendDecodedLedger(sequence uint32, decoded []bronze.DecodedRow) error {
	grouped := make(map[string][]bronze.DecodedRow)
	for index, row := range decoded {
		if row.Err != nil {
			return fmt.Errorf("ledger %d Bronze row %d (%s): %w", sequence, index, row.Spec.TableName, row.Err)
		}
		if !row.OK {
			return fmt.Errorf("ledger %d Bronze row %d targets unsupported table %q", sequence, index, row.Spec.TableName)
		}
		known, ok := bronze.TypedTableSpecs[row.Spec.TableName]
		if !ok || !slices.Equal(known.Columns, row.Spec.Columns) || known.LedgerColumn != row.Spec.LedgerColumn {
			return fmt.Errorf("ledger %d Bronze row %d has divergent table spec %q", sequence, index, row.Spec.TableName)
		}
		grouped[row.Spec.TableName] = append(grouped[row.Spec.TableName], row)
	}
	tableNames := make([]string, 0, len(grouped))
	for tableName := range grouped {
		tableNames = append(tableNames, tableName)
	}
	slices.Sort(tableNames)
	for _, tableName := range tableNames {
		rows := grouped[tableName]
		slices.SortStableFunc(rows, func(left, right bronze.DecodedRow) int {
			return compareProjectedRows(left.Values, right.Values)
		})
		table, err := writer.typedTable(tableName)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := table.append(sequence, row.Values); err != nil {
				return fmt.Errorf("append ledger %d to %s: %w", sequence, tableName, err)
			}
		}
	}
	return nil
}

func (writer *columnarShardWriter) appendContractEvents(sequence uint32, rows []extract.ContractEventData) error {
	if len(rows) == 0 {
		return nil
	}
	table, err := writer.typedTable(bronzeColumnar.ContractEventsTable)
	if err != nil {
		return err
	}
	if !table.layout.Schema.Equal(bronzeColumnar.ContractEventsSchema) {
		return fmt.Errorf("generated contract-events schema diverges from Bronze DDL")
	}
	if table.builder.Len() != 0 {
		return fmt.Errorf("contract-events table mixed generic and direct rows")
	}
	if table.contractEvents == nil {
		table.contractEvents = bronzeColumnar.NewContractEventsBuilder(writer.allocator, int(writer.cfg.RowGroupRows))
	}
	bronzeColumnar.SortContractEvents(rows)
	for _, row := range rows {
		if row.LedgerSequence != sequence {
			return fmt.Errorf("contract event ledger %d does not match source ledger %d", row.LedgerSequence, sequence)
		}
		if err := table.contractEvents.Append(row); err != nil {
			return err
		}
		if uint64(table.contractEvents.Len()) >= writer.cfg.RowGroupRows {
			if err := table.flushContractEvents(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (writer *columnarShardWriter) appendEnvelope(sequence uint32, values ledgerEnvelopeValues) error {
	ledgerBatches, err := writer.envelopeTable(
		"ledger_batches",
		[]string{"network_passphrase", "ledger_sequence", "closed_at_unix", "schema_version", "extraction_version", "transaction_count", "operation_count", "bronze_row_count", "payload_json"},
		[]string{"VARCHAR", "UBIGINT", "BIGINT", "VARCHAR", "VARCHAR", "INTEGER", "INTEGER", "INTEGER", "VARCHAR"},
		1,
	)
	if err != nil {
		return err
	}
	if err := ledgerBatches.append(sequence, []any{
		values.networkPassphrase, sequence, values.closedAtUnix, values.schemaVersion,
		values.extractionVersion, values.transactionCount, values.operationCount,
		values.bronzeRowCount, nil,
	}); err != nil {
		return fmt.Errorf("append ledger %d metadata: %w", sequence, err)
	}
	watermarks, err := writer.envelopeTable(
		"ingest_watermarks",
		[]string{"network_passphrase", "ledger_sequence", "written_at"},
		[]string{"VARCHAR", "UBIGINT", "TIMESTAMP"},
		1,
	)
	if err != nil {
		return err
	}
	if err := watermarks.append(sequence, []any{values.networkPassphrase, sequence, values.writtenAt.UTC()}); err != nil {
		return fmt.Errorf("append ledger %d watermark: %w", sequence, err)
	}
	return nil
}

type ledgerEnvelopeValues struct {
	networkPassphrase string
	closedAtUnix      int64
	schemaVersion     string
	extractionVersion string
	transactionCount  int
	operationCount    int
	bronzeRowCount    int
	writtenAt         time.Time
}

func (writer *columnarShardWriter) typedTable(tableName string) (*columnarTableWriter, error) {
	if table, ok := writer.tables["bronze."+tableName]; ok {
		return table, nil
	}
	spec, ok := bronze.TypedTableSpecs[tableName]
	if !ok {
		return nil, fmt.Errorf("unsupported typed table %q", tableName)
	}
	layout, err := bronzeColumnar.LayoutFor(spec)
	if err != nil {
		return nil, err
	}
	ledgerColumn := slices.Index(spec.Columns, spec.LedgerColumn)
	if ledgerColumn < 0 {
		return nil, fmt.Errorf("typed table %s has no ledger column %s", tableName, spec.LedgerColumn)
	}
	return writer.addTable("bronze."+tableName, tableName, ledgerColumn, layout)
}

func (writer *columnarShardWriter) envelopeTable(tableName string, columns, sqlTypes []string, ledgerColumn int) (*columnarTableWriter, error) {
	if table, ok := writer.tables["main."+tableName]; ok {
		return table, nil
	}
	fields := make([]arrow.Field, len(columns))
	for index, column := range columns {
		dataType, _, err := arrowTypeForManifestSQL(sqlTypes[index])
		if err != nil {
			return nil, err
		}
		fields[index] = arrow.Field{Name: column, Type: dataType, Nullable: true}
	}
	return writer.addTable("main."+tableName, tableName, ledgerColumn, bronzeColumnar.TypedTableLayout{
		Schema:   arrow.NewSchema(fields, nil),
		SQLTypes: slices.Clone(sqlTypes),
	})
}

func (writer *columnarShardWriter) addTable(publicName, tableName string, ledgerColumn int, layout bronzeColumnar.TypedTableLayout) (*columnarTableWriter, error) {
	builder, err := bronzeColumnar.NewRecordBuilder(writer.allocator, layout.Schema, int(writer.cfg.RowGroupRows))
	if err != nil {
		return nil, err
	}
	table := &columnarTableWriter{
		owner: writer, publicName: publicName, tableName: tableName,
		ledgerColumn: ledgerColumn, layout: layout, builder: builder,
	}
	writer.tables[publicName] = table
	return table, nil
}

func (table *columnarTableWriter) append(sequence uint32, values []any) error {
	if table.ledgerColumn < 0 || table.ledgerColumn >= len(values) {
		return fmt.Errorf("ledger column index %d is outside %d values", table.ledgerColumn, len(values))
	}
	rowLedger, err := uint32Value(values[table.ledgerColumn])
	if err != nil {
		return fmt.Errorf("ledger column %s: %w", table.layout.Schema.Field(table.ledgerColumn).Name, err)
	}
	if rowLedger != sequence {
		return fmt.Errorf("row ledger %d does not match source ledger %d", rowLedger, sequence)
	}
	if err := table.builder.Append(values); err != nil {
		return err
	}
	if uint64(table.builder.Len()) >= table.owner.cfg.RowGroupRows {
		return table.flush()
	}
	return nil
}

func (table *columnarTableWriter) flush() error {
	record := table.builder.NewRecordBatch()
	if record == nil {
		return nil
	}
	defer record.Release()
	return table.writeRecord(record)
}

func (table *columnarTableWriter) flushContractEvents() error {
	if table.contractEvents == nil {
		return nil
	}
	record := table.contractEvents.NewRecordBatch()
	if record == nil || record.NumRows() == 0 {
		if record != nil {
			record.Release()
		}
		return nil
	}
	defer record.Release()
	return table.writeRecord(record)
}

func (table *columnarTableWriter) writeRecord(record arrow.RecordBatch) error {
	if table.part == nil {
		if err := table.openPart(); err != nil {
			return err
		}
	}
	ledgerValues := record.Column(table.ledgerColumn)
	minLedger, maxLedger, err := ledgerRange(ledgerValues)
	if err != nil {
		return err
	}
	if err := table.part.writer.Write(record); err != nil {
		return fmt.Errorf("write Arrow record for %s: %w", table.publicName, err)
	}
	if table.part.rows == 0 || minLedger < table.part.minLedger {
		table.part.minLedger = minLedger
	}
	if table.part.rows == 0 || maxLedger > table.part.maxLedger {
		table.part.maxLedger = maxLedger
	}
	table.part.rows += uint64(record.NumRows())
	if table.owner.cfg.FileTargetBytes > 0 && uint64(table.part.writer.TotalCompressedBytes()) >= table.owner.cfg.FileTargetBytes {
		return table.closePart()
	}
	return nil
}

func (table *columnarTableWriter) openPart() error {
	if table.part != nil {
		return fmt.Errorf("columnar part for %s is already open", table.publicName)
	}
	temporaryPath, err := reserveTemporaryPath(table.owner.outputDir, "."+table.tableName+"-*.parquet.partial")
	if err != nil {
		return fmt.Errorf("reserve columnar part for %s: %w", table.publicName, err)
	}
	file, err := os.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create columnar part for %s: %w", table.publicName, err)
	}
	properties, err := columnarWriterProperties(table.owner.cfg, table.owner.allocator)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(temporaryPath)
		return err
	}
	arrowProperties := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(table.owner.allocator))
	parquetWriter, err := pqarrow.NewFileWriter(table.layout.Schema, nonClosingFileWriter{file: file}, properties, arrowProperties)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(temporaryPath)
		return fmt.Errorf("create columnar Parquet writer for %s: %w", table.publicName, err)
	}
	table.part = &columnarPart{temporaryPath: temporaryPath, file: file, writer: parquetWriter}
	return nil
}

func (table *columnarTableWriter) closePart() (resultErr error) {
	part := table.part
	if part == nil {
		return nil
	}
	table.part = nil
	published := false
	fileClosed := false
	defer func() {
		if !fileClosed {
			closeErr := part.file.Close()
			resultErr = errors.Join(resultErr, closeErr)
		}
		if !published {
			_ = os.Remove(part.temporaryPath)
		}
	}()
	if err := part.writer.Close(); err != nil {
		return fmt.Errorf("close columnar Parquet writer for %s: %w", table.publicName, err)
	}
	if err := part.file.Sync(); err != nil {
		return fmt.Errorf("sync columnar Parquet for %s: %w", table.publicName, err)
	}
	if err := part.file.Close(); err != nil {
		return fmt.Errorf("close columnar Parquet file for %s: %w", table.publicName, err)
	}
	fileClosed = true
	if err := canonicalizeParquetFooter(part.temporaryPath); err != nil {
		return fmt.Errorf("canonicalize columnar Parquet footer for %s: %w", table.publicName, err)
	}
	if err := verifyColumnarParquet(part.temporaryPath, table.layout.Schema, part.rows, table.owner.allocator); err != nil {
		return fmt.Errorf("verify columnar Parquet for %s: %w", table.publicName, err)
	}
	sha, bytes, err := hashFile(part.temporaryPath)
	if err != nil {
		return fmt.Errorf("hash columnar Parquet for %s: %w", table.publicName, err)
	}
	if table.owner.cfg.FileMaxBytes > 0 && bytes > table.owner.cfg.FileMaxBytes {
		return fmt.Errorf("Parquet for %s part %d is %d bytes, exceeds hard maximum %d", table.publicName, table.partIndex, bytes, table.owner.cfg.FileMaxBytes)
	}
	if part.minLedger < table.owner.cfg.LedgerStart || part.maxLedger > table.owner.cfg.LedgerEnd {
		return fmt.Errorf("Parquet part for %s has range %d-%d outside shard %d-%d", table.publicName, part.minLedger, part.maxLedger, table.owner.cfg.LedgerStart, table.owner.cfg.LedgerEnd)
	}
	fingerprint, err := schemaFingerprint(table.layout)
	if err != nil {
		return fmt.Errorf("fingerprint columnar schema for %s: %w", table.publicName, err)
	}
	finalName := fmt.Sprintf("%010d-%010d-%s-%05d.parquet", table.owner.cfg.LedgerStart, table.owner.cfg.LedgerEnd, table.tableName, table.partIndex)
	finalPath := table.owner.outputDir + string(os.PathSeparator) + finalName
	if err := publishNoReplace(part.temporaryPath, finalPath); err != nil {
		return fmt.Errorf("publish columnar Parquet for %s: %w", table.publicName, err)
	}
	published = true
	artifact := backfillmanifest.File{
		Table: table.publicName, URI: (&url.URL{Scheme: "file", Path: finalPath}).String(),
		SHA256: sha, Bytes: bytes, Rows: part.rows, MinLedger: part.minLedger, MaxLedger: part.maxLedger,
		ParquetSchemaFingerprint: fingerprint,
	}
	table.files = append(table.files, artifact)
	table.owner.files = append(table.owner.files, artifact)
	table.partIndex++
	return nil
}

// Arrow Go accumulates page encoding statistics in maps and emits those maps
// into a Thrift list. Map iteration changes the physical footer even when rows,
// order, and logical metadata are identical. Sort that list and rewrite only
// the fixed-length footer before hashing; data pages remain untouched.
func canonicalizeParquetFooter(path string) error {
	reader, err := parquetFile.OpenParquetFile(path, false)
	if err != nil {
		return fmt.Errorf("open Parquet footer: %w", err)
	}
	metadata := reader.MetaData()
	for _, rowGroup := range metadata.RowGroups {
		for _, column := range rowGroup.Columns {
			if column.MetaData == nil {
				continue
			}
			stats := column.MetaData.EncodingStats
			sort.Slice(stats, func(left, right int) bool {
				if stats[left].PageType != stats[right].PageType {
					return stats[left].PageType < stats[right].PageType
				}
				if stats[left].Encoding != stats[right].Encoding {
					return stats[left].Encoding < stats[right].Encoding
				}
				return stats[left].Count < stats[right].Count
			})
		}
	}
	serialized, err := metadata.Serialize(context.Background())
	if err != nil {
		_ = reader.Close()
		return fmt.Errorf("serialize canonical footer: %w", err)
	}
	metadataSize := metadata.Size()
	if len(serialized) != metadataSize {
		_ = reader.Close()
		return fmt.Errorf("canonical footer changed size from %d to %d", metadataSize, len(serialized))
	}
	info, err := os.Stat(path)
	if err != nil {
		_ = reader.Close()
		return err
	}
	footerOffset := info.Size() - 8 - int64(metadataSize)
	if footerOffset < 4 {
		_ = reader.Close()
		return fmt.Errorf("invalid Parquet footer offset %d", footerOffset)
	}
	if err := reader.Close(); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	written, err := file.WriteAt(serialized, footerOffset)
	if err != nil {
		return err
	}
	if written != len(serialized) {
		return fmt.Errorf("canonical footer write = %d bytes, want %d", written, len(serialized))
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	closed = true
	return nil
}

func verifyColumnarParquet(path string, expectedSchema *arrow.Schema, expectedRows uint64, allocator memory.Allocator) error {
	reader, err := parquetFile.OpenParquetFile(path, false)
	if err != nil {
		return fmt.Errorf("open footer: %w", err)
	}
	defer reader.Close()
	if reader.NumRows() < 0 || uint64(reader.NumRows()) != expectedRows {
		return fmt.Errorf("footer rows = %d, want %d", reader.NumRows(), expectedRows)
	}
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, allocator)
	if err != nil {
		return fmt.Errorf("read Arrow footer schema: %w", err)
	}
	actualSchema, err := arrowReader.Schema()
	if err != nil {
		return fmt.Errorf("convert footer schema: %w", err)
	}
	if !schemasLogicallyEqual(actualSchema, expectedSchema) {
		return fmt.Errorf("footer schema diverges:\nactual: %s\nexpected: %s", actualSchema, expectedSchema)
	}
	return nil
}

func schemasLogicallyEqual(actual, expected *arrow.Schema) bool {
	if actual == nil || expected == nil {
		return actual == expected
	}
	actualFields := actual.Fields()
	expectedFields := expected.Fields()
	if len(actualFields) != len(expectedFields) {
		return false
	}
	for index := range actualFields {
		if actualFields[index].Name != expectedFields[index].Name ||
			actualFields[index].Nullable != expectedFields[index].Nullable ||
			!arrow.TypeEqual(actualFields[index].Type, expectedFields[index].Type) {
			return false
		}
	}
	return true
}

func (writer *columnarShardWriter) close() ([]backfillmanifest.File, error) {
	if writer.closed {
		return nil, fmt.Errorf("columnar shard writer is already closed")
	}
	writer.closed = true
	names := make([]string, 0, len(writer.tables))
	for name := range writer.tables {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		table := writer.tables[name]
		if err := table.flush(); err != nil {
			writer.abort()
			return nil, err
		}
		if err := table.flushContractEvents(); err != nil {
			writer.abort()
			return nil, err
		}
		if err := table.closePart(); err != nil {
			writer.abort()
			return nil, err
		}
		table.builder.Release()
		if table.contractEvents != nil {
			table.contractEvents.Release()
			table.contractEvents = nil
		}
	}
	slices.SortFunc(writer.files, func(left, right backfillmanifest.File) int {
		if comparison := cmp.Compare(left.Table, right.Table); comparison != 0 {
			return comparison
		}
		return cmp.Compare(left.URI, right.URI)
	})
	return slices.Clone(writer.files), nil
}

func (writer *columnarShardWriter) abort() {
	for _, table := range writer.tables {
		table.builder.Release()
		if table.contractEvents != nil {
			table.contractEvents.Release()
			table.contractEvents = nil
		}
		if table.part != nil {
			_ = table.part.writer.Close()
			_ = table.part.file.Close()
			_ = os.Remove(table.part.temporaryPath)
			table.part = nil
		}
	}
	removeLocalArtifacts(writer.files)
}

func columnarWriterProperties(cfg ParquetConfig, allocator memory.Allocator) (*parquet.WriterProperties, error) {
	options := []parquet.WriterProperty{
		parquet.WithAllocator(allocator),
		parquet.WithCreatedBy(columnarParquetCreatedBy),
		parquet.WithDictionaryDefault(true),
		parquet.WithMaxRowGroupLength(int64(cfg.RowGroupRows)),
		parquet.WithStats(true),
	}
	switch cfg.Compression {
	case "", "zstd":
		options = append(options, parquet.WithCompression(compress.Codecs.Zstd), parquet.WithCompressionLevel(3))
	case "snappy":
		options = append(options, parquet.WithCompression(compress.Codecs.Snappy))
	case "uncompressed":
		options = append(options, parquet.WithCompression(compress.Codecs.Uncompressed))
	default:
		return nil, fmt.Errorf("unsupported columnar compression %q", cfg.Compression)
	}
	return parquet.NewWriterProperties(options...), nil
}

func schemaFingerprint(layout bronzeColumnar.TypedTableLayout) (string, error) {
	fields := layout.Schema.Fields()
	if len(fields) != len(layout.SQLTypes) {
		return "", fmt.Errorf("Arrow schema has %d fields and %d SQL types", len(fields), len(layout.SQLTypes))
	}
	columns := make([]schemaColumn, len(fields))
	for index, field := range fields {
		columns[index] = schemaColumn{Name: field.Name, Type: layout.SQLTypes[index], Nullable: "YES"}
	}
	return backfillmanifest.CanonicalDigest(columns)
}

func arrowTypeForManifestSQL(sqlType string) (arrow.DataType, string, error) {
	switch sqlType {
	case "VARCHAR":
		return arrow.BinaryTypes.String, sqlType, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, sqlType, nil
	case "UBIGINT":
		return arrow.PrimitiveTypes.Uint64, sqlType, nil
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32, sqlType, nil
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, sqlType, nil
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, sqlType, nil
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond}, sqlType, nil
	default:
		return nil, "", fmt.Errorf("unsupported manifest SQL type %q", sqlType)
	}
}

func uint32Value(value any) (uint32, error) {
	integer, err := numericUint64(value)
	if err != nil {
		return 0, err
	}
	if integer > math.MaxUint32 {
		return 0, fmt.Errorf("%d exceeds uint32", integer)
	}
	return uint32(integer), nil
}

func numericUint64(value any) (uint64, error) {
	switch typed := value.(type) {
	case int:
		if typed < 0 {
			return 0, fmt.Errorf("negative integer %d", typed)
		}
		return uint64(typed), nil
	case int32:
		if typed < 0 {
			return 0, fmt.Errorf("negative integer %d", typed)
		}
		return uint64(typed), nil
	case int64:
		if typed < 0 {
			return 0, fmt.Errorf("negative integer %d", typed)
		}
		return uint64(typed), nil
	case uint:
		return uint64(typed), nil
	case uint32:
		return uint64(typed), nil
	case uint64:
		return typed, nil
	default:
		return 0, fmt.Errorf("cannot read %T as ledger sequence", value)
	}
}

func ledgerRange(values arrow.Array) (uint32, uint32, error) {
	if values.Len() == 0 {
		return 0, 0, fmt.Errorf("ledger array is empty")
	}
	var minLedger, maxLedger uint32
	for index := 0; index < values.Len(); index++ {
		if values.IsNull(index) {
			return 0, 0, fmt.Errorf("ledger array contains null at row %d", index)
		}
		var raw uint64
		switch typed := values.(type) {
		case interface{ Value(int) int64 }:
			value := typed.Value(index)
			if value < 0 {
				return 0, 0, fmt.Errorf("ledger array contains negative value %d", value)
			}
			raw = uint64(value)
		case interface{ Value(int) int32 }:
			value := typed.Value(index)
			if value < 0 {
				return 0, 0, fmt.Errorf("ledger array contains negative value %d", value)
			}
			raw = uint64(value)
		case interface{ Value(int) uint64 }:
			raw = typed.Value(index)
		default:
			return 0, 0, fmt.Errorf("unsupported ledger Arrow array %T", values)
		}
		if raw > math.MaxUint32 {
			return 0, 0, fmt.Errorf("ledger %d exceeds uint32", raw)
		}
		ledger := uint32(raw)
		if index == 0 || ledger < minLedger {
			minLedger = ledger
		}
		if index == 0 || ledger > maxLedger {
			maxLedger = ledger
		}
	}
	return minLedger, maxLedger, nil
}

func compareProjectedRows(left, right []any) int {
	limit := min(len(left), len(right))
	for index := 0; index < limit; index++ {
		if comparison := compareProjectedValue(left[index], right[index]); comparison != 0 {
			return comparison
		}
	}
	return cmp.Compare(len(left), len(right))
}

func compareProjectedValue(left, right any) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	if leftTime, ok := left.(time.Time); ok {
		if rightTime, ok := right.(time.Time); ok {
			return cmp.Compare(leftTime.UnixNano(), rightTime.UnixNano())
		}
	}
	if leftString, ok := left.(string); ok {
		if rightString, ok := right.(string); ok {
			return cmp.Compare(leftString, rightString)
		}
	}
	if leftBool, ok := left.(bool); ok {
		if rightBool, ok := right.(bool); ok {
			if leftBool == rightBool {
				return 0
			}
			if !leftBool {
				return -1
			}
			return 1
		}
	}
	// Projected values for a column normally have one concrete type. Compare
	// integer types exactly before the numeric fallback so BIGINT values above
	// 2^53 do not collapse to the same float64 ordering key.
	switch leftTyped := left.(type) {
	case int:
		if rightTyped, ok := right.(int); ok {
			return cmp.Compare(leftTyped, rightTyped)
		}
	case int32:
		if rightTyped, ok := right.(int32); ok {
			return cmp.Compare(leftTyped, rightTyped)
		}
	case int64:
		if rightTyped, ok := right.(int64); ok {
			return cmp.Compare(leftTyped, rightTyped)
		}
	case uint:
		if rightTyped, ok := right.(uint); ok {
			return cmp.Compare(leftTyped, rightTyped)
		}
	case uint32:
		if rightTyped, ok := right.(uint32); ok {
			return cmp.Compare(leftTyped, rightTyped)
		}
	case uint64:
		if rightTyped, ok := right.(uint64); ok {
			return cmp.Compare(leftTyped, rightTyped)
		}
	case float32:
		if rightTyped, ok := right.(float32); ok {
			return cmp.Compare(leftTyped, rightTyped)
		}
	case float64:
		if rightTyped, ok := right.(float64); ok {
			return cmp.Compare(leftTyped, rightTyped)
		}
	}
	leftNumber, leftNumeric := numericFloat64(left)
	rightNumber, rightNumeric := numericFloat64(right)
	if leftNumeric && rightNumeric {
		return cmp.Compare(leftNumber, rightNumber)
	}
	return cmp.Compare(fmt.Sprintf("%T:%v", left, left), fmt.Sprintf("%T:%v", right, right))
}

func numericFloat64(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}
