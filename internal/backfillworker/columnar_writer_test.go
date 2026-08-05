package backfillworker

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	extract "github.com/withObsrvr/stellar-extract"
)

func TestArrowParquetWriterCoversEveryTableAndMatchesAppenderLogically(t *testing.T) {
	const sequence = uint32(777001)
	batch := allTableLedgerBatch(sequence)
	writtenAt := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	base := LedgerBatchConfig{
		Parquet: ParquetConfig{
			LedgerStart: sequence, LedgerEnd: sequence, Compression: "zstd",
			FileTargetBytes: 16 << 20, FileMaxBytes: 32 << 20, RowGroupRows: 2048,
		},
		DecodeWorkers: 2, WatermarkWrittenAt: writtenAt,
		MaxEncodedBytes: 1 << 20, MaxBronzeRows: 100,
	}
	appenderConfig := base
	appenderConfig.Parquet.OutputDir = t.TempDir()
	appenderConfig.WriterMode = WriterDuckDBAppender
	appenderFiles, err := WriteLedgerBatchShard(context.Background(), appenderConfig, []*componentsv1.LedgerBatch{batch})
	if err != nil {
		t.Fatalf("write Appender oracle: %v", err)
	}
	arrowConfig := base
	arrowConfig.Parquet.OutputDir = t.TempDir()
	arrowConfig.WriterMode = WriterArrowParquet
	arrowFiles, err := WriteLedgerBatchShard(context.Background(), arrowConfig, []*componentsv1.LedgerBatch{batch})
	if err != nil {
		t.Fatalf("write Arrow candidate: %v", err)
	}
	if got, want := len(arrowFiles), len(bronze.TypedTableSpecs)+2; got != want {
		t.Fatalf("Arrow files = %d, want %d", got, want)
	}

	appenderByTable := filesByTable(t, appenderFiles)
	arrowByTable := filesByTable(t, arrowFiles)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for table, oracle := range appenderByTable {
		candidate, ok := arrowByTable[table]
		if !ok {
			t.Fatalf("Arrow output is missing %s", table)
		}
		if candidate.Rows != oracle.Rows || candidate.MinLedger != oracle.MinLedger || candidate.MaxLedger != oracle.MaxLedger {
			t.Fatalf("coverage %s: Arrow=%+v Appender=%+v", table, candidate, oracle)
		}
		if candidate.ParquetSchemaFingerprint != oracle.ParquetSchemaFingerprint {
			t.Fatalf("schema fingerprint %s: Arrow=%s Appender=%s", table, candidate.ParquetSchemaFingerprint, oracle.ParquetSchemaFingerprint)
		}
		candidatePath := artifactPath(t, candidate)
		oraclePath := artifactPath(t, oracle)
		query := "SELECT count(*) FROM ((SELECT * FROM read_parquet(" + bronze.SQLLiteral(candidatePath) + ") EXCEPT ALL SELECT * FROM read_parquet(" + bronze.SQLLiteral(oraclePath) + ")) UNION ALL (SELECT * FROM read_parquet(" + bronze.SQLLiteral(oraclePath) + ") EXCEPT ALL SELECT * FROM read_parquet(" + bronze.SQLLiteral(candidatePath) + ")))"
		var differences int
		if err := db.QueryRow(query).Scan(&differences); err != nil {
			t.Fatalf("compare %s: %v", table, err)
		}
		if differences != 0 {
			t.Fatalf("logical differences for %s = %d", table, differences)
		}
	}
}

func TestArrowParquetWriterIsByteStable(t *testing.T) {
	const sequence = uint32(777001)
	write := func(outputDir string) []backfillmanifest.File {
		files, err := WriteLedgerBatchShard(context.Background(), LedgerBatchConfig{
			Parquet: ParquetConfig{
				OutputDir: outputDir, LedgerStart: sequence, LedgerEnd: sequence,
				Compression: "zstd", FileTargetBytes: 16 << 20, FileMaxBytes: 32 << 20, RowGroupRows: 2048,
			},
			WriterMode: WriterArrowParquet, DecodeWorkers: 2,
			WatermarkWrittenAt: time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC),
		}, []*componentsv1.LedgerBatch{allTableLedgerBatch(sequence)})
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	first := fileLogicalIdentities(write(t.TempDir()))
	second := fileLogicalIdentities(write(t.TempDir()))
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("Arrow artifacts are not byte-stable:\nfirst=%v\nsecond=%v", first, second)
	}
}

func TestDirectContractEventsBuilderMatchesGenericColumnarValues(t *testing.T) {
	const sequence = uint32(777001)
	events := []extract.ContractEventData{{
		EventID: "event", LedgerSequence: sequence, TransactionHash: "tx",
		ClosedAt:  time.Date(2026, 8, 5, 12, 0, 0, 123456000, time.UTC),
		EventType: "contract", InSuccessfulContractCall: true, Successful: true,
		ContractEventXDR: "event-xdr", TopicsJSON: `[]`, TopicsDecoded: `[]`,
		DataXDR: "data-xdr", DataDecoded: `{}`, TopicCount: 0,
		OperationIndex: 2, EventIndex: 3,
		CreatedAt:   time.Date(2026, 8, 5, 13, 0, 0, 654321000, time.UTC),
		LedgerRange: sequence,
	}}
	cfg := ParquetConfig{
		LedgerStart: sequence, LedgerEnd: sequence, Compression: "zstd",
		FileTargetBytes: 16 << 20, FileMaxBytes: 32 << 20, RowGroupRows: 2048,
	}
	write := func(outputDir string, direct bool) backfillmanifest.File {
		cfg.OutputDir = outputDir
		writer := newColumnarShardWriter(cfg, outputDir)
		if direct {
			if err := writer.appendContractEvents(sequence, append([]extract.ContractEventData(nil), events...)); err != nil {
				t.Fatal(err)
			}
		} else {
			rows := bronze.ProjectLedgerData(&extract.LedgerData{ContractEvents: append([]extract.ContractEventData(nil), events...)}, nil)
			if err := writer.appendDecodedLedger(sequence, rows); err != nil {
				t.Fatal(err)
			}
		}
		files, err := writer.close()
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 1 {
			t.Fatalf("files = %d, want 1", len(files))
		}
		return files[0]
	}
	direct := write(t.TempDir(), true)
	generic := write(t.TempDir(), false)
	if direct.SHA256 != generic.SHA256 || direct.ParquetSchemaFingerprint != generic.ParquetSchemaFingerprint {
		t.Fatalf("direct artifact diverges from generic artifact:\ndirect=%+v\ngeneric=%+v", direct, generic)
	}
}

func TestArrowParquetFooterCanonicalizesEncodingStats(t *testing.T) {
	const sequence = uint32(777001)
	events := make([]extract.ContractEventData, 6_000)
	for index := range events {
		events[index] = extract.ContractEventData{
			EventID: fmt.Sprintf("event-%05d", index), LedgerSequence: sequence,
			TransactionHash: fmt.Sprintf("tx-%05d", index),
			ClosedAt:        time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC),
			EventType:       "contract", ContractEventXDR: fmt.Sprintf("%05d-%s", index, strings.Repeat("x", 256)),
			CreatedAt: time.Date(2026, 8, 5, 13, 0, 0, 0, time.UTC), LedgerRange: sequence,
		}
	}
	write := func(outputDir string) backfillmanifest.File {
		writer := newColumnarShardWriter(ParquetConfig{
			OutputDir: outputDir, LedgerStart: sequence, LedgerEnd: sequence,
			Compression: "zstd", FileTargetBytes: 16 << 20, FileMaxBytes: 32 << 20, RowGroupRows: 8_192,
		}, outputDir)
		if err := writer.appendContractEvents(sequence, append([]extract.ContractEventData(nil), events...)); err != nil {
			t.Fatal(err)
		}
		files, err := writer.close()
		if err != nil {
			t.Fatal(err)
		}
		return files[0]
	}
	first := write(t.TempDir())
	second := write(t.TempDir())
	if first.SHA256 != second.SHA256 || first.Bytes != second.Bytes {
		t.Fatalf("canonical footer is not byte-stable:\nfirst=%+v\nsecond=%+v", first, second)
	}
}

func TestProjectedValueOrderingPreservesLargeIntegers(t *testing.T) {
	const lower = int64(1<<53 + 1)
	if comparison := compareProjectedValue(lower, lower+1); comparison >= 0 {
		t.Fatalf("compareProjectedValue(%d, %d) = %d, want negative", lower, lower+1, comparison)
	}
	if comparison := compareProjectedValue(uint64(lower+1), uint64(lower)); comparison <= 0 {
		t.Fatalf("compareProjectedValue(%d, %d) = %d, want positive", lower+1, lower, comparison)
	}
}

func allTableLedgerBatch(sequence uint32) *componentsv1.LedgerBatch {
	tableNames := make([]string, 0, len(bronze.TypedTableSpecs))
	for name := range bronze.TypedTableSpecs {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	rows := make([]*componentsv1.BronzeRow, 0, len(tableNames))
	for index, name := range tableNames {
		rows = append(rows, &componentsv1.BronzeRow{
			Id: fmt.Sprintf("row-%d", index), TableName: name,
			NetworkPassphrase: "testnet", LedgerSequence: sequence,
			LedgerRange: 770000, RowJson: allTableFixtureJSON(sequence),
		})
	}
	return &componentsv1.LedgerBatch{
		NetworkPassphrase: "testnet", LedgerSequence: sequence,
		ClosedAtUnix: 1_700_000_000, SchemaVersion: "v1", ExtractionVersion: "v1",
		Transactions: []*componentsv1.TransactionRow{{
			LedgerSequence: sequence, TransactionHash: "tx-hash",
			EnvelopeXdr: "envelope-xdr", ResultXdr: "result-xdr", MetaXdr: "meta-xdr",
		}},
		BronzeRows: rows,
	}
}

func filesByTable(t *testing.T, files []backfillmanifest.File) map[string]backfillmanifest.File {
	t.Helper()
	result := make(map[string]backfillmanifest.File, len(files))
	for _, file := range files {
		if _, duplicate := result[file.Table]; duplicate {
			t.Fatalf("test expected one file for %s", file.Table)
		}
		result[file.Table] = file
	}
	return result
}

func artifactPath(t *testing.T, file backfillmanifest.File) string {
	t.Helper()
	parsed, err := url.Parse(file.URI)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Path
}
