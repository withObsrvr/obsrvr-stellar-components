package backfillworker

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/ingestbatch"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
)

// LedgerBatchSource yields an ordered shard and returns io.EOF after the exact
// configured range. A source may reuse its own read buffers after Next returns;
// the worker does not retain a batch after it has been decoded and appended.
type LedgerBatchSource func() (*componentsv1.LedgerBatch, error)

type StreamResult struct {
	Files                 []backfillmanifest.File
	Descriptor            ingestbatch.Descriptor
	PeakBatchEncodedBytes uint64
	PeakBatchBronzeRows   uint64
	StagingDuration       time.Duration
	ExportDuration        time.Duration
	SetupDuration         time.Duration
	SourceDuration        time.Duration
	DigestDuration        time.Duration
	DecodeDuration        time.Duration
	AppendDuration        time.Duration
}

type streamingAppender struct {
	name     string
	appender *duckdb.Appender
	ordinal  uint64
}

var duckDBMemoryLimitPattern = regexp.MustCompile(`^[1-9][0-9]*(KB|MB|GB|TB|KiB|MiB|GiB|TiB)$`)

// WriteLedgerBatchStream keeps at most one source batch and its decoded rows
// in Go memory. Rows are appended immediately to a disposable worker-local
// DuckDB database; only closed, verified Parquet files leave that boundary.
func WriteLedgerBatchStream(ctx context.Context, cfg LedgerBatchConfig, next LedgerBatchSource) (result StreamResult, resultErr error) {
	stagingStarted := time.Now()
	if err := validateStreamingConfig(cfg, next); err != nil {
		return StreamResult{}, err
	}
	if err := os.MkdirAll(cfg.Parquet.OutputDir, 0o750); err != nil {
		return StreamResult{}, fmt.Errorf("create shard output directory: %w", err)
	}
	absOutputDir, err := filepath.Abs(cfg.Parquet.OutputDir)
	if err != nil {
		return StreamResult{}, fmt.Errorf("resolve shard output directory: %w", err)
	}
	databasePath, err := reserveTemporaryPath(absOutputDir, ".backfill-worker-*.duckdb")
	if err != nil {
		return StreamResult{}, fmt.Errorf("reserve worker database path: %w", err)
	}
	defer func() {
		_ = os.Remove(databasePath)
		_ = os.Remove(databasePath + ".wal")
		_ = os.RemoveAll(databasePath + ".tmp")
	}()

	db, err := sql.Open("duckdb", databasePath)
	if err != nil {
		return StreamResult{}, fmt.Errorf("open worker DuckDB: %w", err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return StreamResult{}, fmt.Errorf("open worker DuckDB connection: %w", err)
	}
	defer conn.Close()
	if cfg.MemoryLimit != "" {
		if _, err := conn.ExecContext(ctx, "SET memory_limit = '"+cfg.MemoryLimit+"'"); err != nil {
			return StreamResult{}, fmt.Errorf("set DuckDB memory limit: %w", err)
		}
	}

	tableNames := sortedTypedTableNames()
	if err := createStreamingSchema(ctx, conn, tableNames); err != nil {
		return StreamResult{}, err
	}

	tableCounts := map[string]uint64{
		"main.ingest_watermarks": 0,
		"main.ledger_batches":    0,
	}
	accumulator := ingestbatch.NewAccumulator()
	result.SetupDuration = time.Since(stagingStarted)
	err = conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected DuckDB driver connection type %T", driverConn)
		}
		appenders, err := openStreamingAppenders(dc, tableNames)
		if err != nil {
			return err
		}
		closed := false
		defer func() {
			if !closed {
				_ = closeStreamingAppenders(context.Background(), appenders)
			}
		}()

		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			sourceStarted := time.Now()
			batch, err := next()
			result.SourceDuration += time.Since(sourceStarted)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("read shard source: %w", err)
			}
			if err := validateNextStreamingBatch(cfg, accumulator, batch); err != nil {
				return err
			}
			digestStarted := time.Now()
			if err := accumulator.Add(batch); err != nil {
				return err
			}
			result.DigestDuration += time.Since(digestStarted)
			descriptor := accumulator.Totals()
			batchBytes := uint64(0)
			if descriptor.EncodedBytes >= result.Descriptor.EncodedBytes {
				batchBytes = descriptor.EncodedBytes - result.Descriptor.EncodedBytes
			}
			batchRows := uint64(len(batch.BronzeRows))
			if batchBytes > result.PeakBatchEncodedBytes {
				result.PeakBatchEncodedBytes = batchBytes
			}
			if batchRows > result.PeakBatchBronzeRows {
				result.PeakBatchBronzeRows = batchRows
			}
			result.Descriptor = descriptor
			if cfg.MaxEncodedBytes > 0 && descriptor.EncodedBytes > cfg.MaxEncodedBytes {
				return fmt.Errorf("selected range exceeds encoded byte bound: %d > %d", descriptor.EncodedBytes, cfg.MaxEncodedBytes)
			}
			if cfg.MaxBronzeRows > 0 && descriptor.BronzeRows > cfg.MaxBronzeRows {
				return fmt.Errorf("selected range exceeds Bronze row bound: %d > %d", descriptor.BronzeRows, cfg.MaxBronzeRows)
			}

			decodeStarted := time.Now()
			decoded := bronze.DecodeTypedRowsBatches([]*componentsv1.LedgerBatch{batch}, cfg.DecodeWorkers)
			result.DecodeDuration += time.Since(decodeStarted)
			if len(decoded) != len(batch.BronzeRows) {
				return fmt.Errorf("ledger %d decoded %d Bronze rows, want %d", batch.LedgerSequence, len(decoded), len(batch.BronzeRows))
			}
			appendStarted := time.Now()
			for index, decodedRow := range decoded {
				sourceRow := batch.BronzeRows[index]
				if decodedRow.Err != nil {
					return fmt.Errorf("ledger %d Bronze row %d (%s): %w", batch.LedgerSequence, index, sourceRow.TableName, decodedRow.Err)
				}
				if !decodedRow.OK {
					return fmt.Errorf("ledger %d Bronze row %d targets unsupported table %q", batch.LedgerSequence, index, sourceRow.TableName)
				}
				known, ok := bronze.TypedTableSpecs[decodedRow.Spec.TableName]
				if !ok || !slices.Equal(known.Columns, decodedRow.Spec.Columns) || known.LedgerColumn != decodedRow.Spec.LedgerColumn {
					return fmt.Errorf("ledger %d Bronze row %d has divergent table spec %q", batch.LedgerSequence, index, decodedRow.Spec.TableName)
				}
				target := appenders["bronze."+known.TableName]
				values := make([]driver.Value, len(decodedRow.Values)+1)
				for column, value := range decodedRow.Values {
					values[column] = value
				}
				values[len(values)-1] = target.ordinal
				if err := target.appender.AppendRow(values...); err != nil {
					return fmt.Errorf("append ledger %d Bronze row %d to %s: %w", batch.LedgerSequence, index, known.TableName, err)
				}
				target.ordinal++
				tableCounts["bronze."+known.TableName]++
			}

			batchOrdinal := uint64(descriptor.LedgerCount - 1)
			if err := appendLedgerEnvelope(appenders, cfg, batch, batchOrdinal); err != nil {
				return err
			}
			tableCounts["main.ingest_watermarks"]++
			tableCounts["main.ledger_batches"]++
			result.AppendDuration += time.Since(appendStarted)
		}
		closed = true
		return closeStreamingAppenders(ctx, appenders)
	})
	if err != nil {
		return StreamResult{}, err
	}
	result.StagingDuration = time.Since(stagingStarted)

	descriptor, err := accumulator.Descriptor()
	if err != nil {
		return StreamResult{}, err
	}
	if descriptor.LedgerStart != cfg.Parquet.LedgerStart || descriptor.LedgerEnd != cfg.Parquet.LedgerEnd || descriptor.LedgerCount != cfg.Parquet.LedgerEnd-cfg.Parquet.LedgerStart+1 {
		return StreamResult{}, fmt.Errorf("stream produced range %d-%d (%d ledgers), want %d-%d", descriptor.LedgerStart, descriptor.LedgerEnd, descriptor.LedgerCount, cfg.Parquet.LedgerStart, cfg.Parquet.LedgerEnd)
	}
	result.Descriptor = descriptor

	exportStarted := time.Now()
	created := make([]backfillmanifest.File, 0, len(tableCounts))
	defer func() {
		if resultErr != nil {
			removeLocalArtifacts(created)
		}
	}()
	for _, tableName := range tableNames {
		if tableCounts["bronze."+tableName] == 0 {
			continue
		}
		artifacts, err := writeTypedParquetFiles(ctx, conn, absOutputDir, cfg.Parquet, bronze.TypedTableSpecs[tableName])
		if err != nil {
			return StreamResult{}, err
		}
		created = append(created, artifacts...)
	}
	for _, table := range envelopeTableDefinitions() {
		artifacts, err := writeEnvelopeParquetFiles(ctx, conn, absOutputDir, cfg.Parquet, table)
		if err != nil {
			return StreamResult{}, err
		}
		created = append(created, artifacts...)
	}
	sort.Slice(created, func(i, j int) bool {
		if created[i].Table != created[j].Table {
			return created[i].Table < created[j].Table
		}
		return created[i].URI < created[j].URI
	})
	if err := validateCompleteTableCounts(tableCounts, created); err != nil {
		return StreamResult{}, err
	}
	result.ExportDuration = time.Since(exportStarted)
	result.Files = created
	return result, nil
}

func validateStreamingConfig(cfg LedgerBatchConfig, next LedgerBatchSource) error {
	if next == nil {
		return fmt.Errorf("ledger batch source is required")
	}
	if err := validateConfig(cfg.Parquet); err != nil {
		return err
	}
	if cfg.DecodeWorkers <= 0 {
		return fmt.Errorf("decode workers must be positive")
	}
	if cfg.WatermarkWrittenAt.IsZero() {
		return fmt.Errorf("pinned watermark timestamp is required")
	}
	if cfg.MemoryLimit != "" && !duckDBMemoryLimitPattern.MatchString(cfg.MemoryLimit) {
		return fmt.Errorf("invalid DuckDB memory limit %q", cfg.MemoryLimit)
	}
	return nil
}

func validateNextStreamingBatch(cfg LedgerBatchConfig, accumulator *ingestbatch.Accumulator, batch *componentsv1.LedgerBatch) error {
	if batch == nil {
		return fmt.Errorf("stream returned a nil LedgerBatch")
	}
	descriptor := accumulator.Totals()
	want := cfg.Parquet.LedgerStart
	if descriptor.LedgerCount > 0 {
		want = descriptor.LedgerEnd + 1
	}
	if batch.LedgerSequence != want {
		return fmt.Errorf("stream ledger %d is out of order, want %d", batch.LedgerSequence, want)
	}
	if batch.LedgerSequence > cfg.Parquet.LedgerEnd {
		return fmt.Errorf("stream ledger %d exceeds shard end %d", batch.LedgerSequence, cfg.Parquet.LedgerEnd)
	}
	if strings.TrimSpace(batch.NetworkPassphrase) == "" {
		return fmt.Errorf("ledger %d has empty network passphrase", batch.LedgerSequence)
	}
	return nil
}

func sortedTypedTableNames() []string {
	names := make([]string, 0, len(bronze.TypedTableSpecs))
	for name := range bronze.TypedTableSpecs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func createStreamingSchema(ctx context.Context, conn *sql.Conn, tableNames []string) error {
	if err := createLocalSchema(ctx, conn, tableNames); err != nil {
		return err
	}
	for _, statement := range []string{bronze.CreateLedgerBatchesSQL, bronze.CreateIngestWatermarksSQL} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create envelope table: %w", err)
		}
	}
	for _, table := range envelopeTableDefinitions() {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(
			"ALTER TABLE main.%s ADD COLUMN %s UBIGINT",
			bronze.QuoteIdentifier(table.Name),
			bronze.QuoteIdentifier(ordinalColumn),
		)); err != nil {
			return fmt.Errorf("add stable ordinal to %s: %w", table.Name, err)
		}
	}
	return nil
}

func openStreamingAppenders(dc driver.Conn, tableNames []string) (map[string]*streamingAppender, error) {
	appenders := make(map[string]*streamingAppender, len(tableNames)+2)
	for _, tableName := range tableNames {
		spec := bronze.TypedTableSpecs[tableName]
		columns := append(slices.Clone(spec.Columns), ordinalColumn)
		appender, err := duckdb.NewAppenderWithColumns(dc, "", "bronze", tableName, columns)
		if err != nil {
			_ = closeStreamingAppenders(context.Background(), appenders)
			return nil, fmt.Errorf("create streaming appender for %s: %w", tableName, err)
		}
		appenders["bronze."+tableName] = &streamingAppender{name: "bronze." + tableName, appender: appender}
	}
	for _, table := range envelopeTableDefinitions() {
		appender, err := duckdb.NewAppenderWithColumns(dc, "", "main", table.Name, table.Columns)
		if err != nil {
			_ = closeStreamingAppenders(context.Background(), appenders)
			return nil, fmt.Errorf("create streaming appender for %s: %w", table.Name, err)
		}
		appenders["main."+table.Name] = &streamingAppender{name: "main." + table.Name, appender: appender}
	}
	return appenders, nil
}

func closeStreamingAppenders(ctx context.Context, appenders map[string]*streamingAppender) error {
	names := make([]string, 0, len(appenders))
	for name := range appenders {
		names = append(names, name)
	}
	sort.Strings(names)
	var result error
	for _, name := range names {
		appender := appenders[name]
		if err := appender.appender.CloseWithCancel(ctx); err != nil {
			result = errors.Join(result, fmt.Errorf("close streaming appender for %s: %w", name, err))
		}
	}
	return result
}

func appendLedgerEnvelope(appenders map[string]*streamingAppender, cfg LedgerBatchConfig, batch *componentsv1.LedgerBatch, ordinal uint64) error {
	metadata := []driver.Value{
		batch.NetworkPassphrase,
		batch.LedgerSequence,
		batch.ClosedAtUnix,
		batch.SchemaVersion,
		batch.ExtractionVersion,
		len(batch.Transactions),
		len(batch.Operations),
		len(batch.BronzeRows),
		nil,
		ordinal,
	}
	if err := appenders["main.ledger_batches"].appender.AppendRow(metadata...); err != nil {
		return fmt.Errorf("append ledger %d metadata: %w", batch.LedgerSequence, err)
	}
	watermark := []driver.Value{
		batch.NetworkPassphrase,
		batch.LedgerSequence,
		cfg.WatermarkWrittenAt.UTC(),
		ordinal,
	}
	if err := appenders["main.ingest_watermarks"].appender.AppendRow(watermark...); err != nil {
		return fmt.Errorf("append ledger %d watermark: %w", batch.LedgerSequence, err)
	}
	return nil
}

func envelopeTableDefinitions() []envelopeTable {
	return []envelopeTable{
		{
			Name: "ingest_watermarks",
			Columns: []string{
				"network_passphrase", "ledger_sequence", "written_at", ordinalColumn,
			},
			LedgerColumn: "ledger_sequence",
		},
		{
			Name: "ledger_batches",
			Columns: []string{
				"network_passphrase", "ledger_sequence", "closed_at_unix", "schema_version",
				"extraction_version", "transaction_count", "operation_count", "bronze_row_count",
				"payload_json", ordinalColumn,
			},
			LedgerColumn: "ledger_sequence",
		},
	}
}
