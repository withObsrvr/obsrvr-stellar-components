package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
	componentsv1 "github.com/withObsrvr/obsrvr-stellar-components/gen/go/stellar/components/v1"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/ledgerfixture"
	"github.com/withObsrvr/stellar-raw-ledger-origin/source/ledgerstream"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRunWritesValidatedCompleteShardAndStableFileHashes(t *testing.T) {
	fixturePath := recordTestFixture(t, 100, 101)
	firstOutput := filepath.Join(t.TempDir(), "first")
	secondOutput := filepath.Join(t.TempDir(), "second")
	firstSummary := runWorker(t, fixturePath, firstOutput)
	secondSummary := runWorker(t, fixturePath, secondOutput)
	if firstSummary.Ledgers != 2 || firstSummary.ParquetFiles != 3 || firstSummary.OutputRows != 6 {
		t.Fatalf("first summary = %+v", firstSummary)
	}

	firstJob := readJSON[backfillmanifest.JobManifest](t, firstSummary.JobManifest)
	firstResult := readJSON[backfillmanifest.ShardResultManifest](t, firstSummary.ResultManifest)
	if err := backfillmanifest.ValidateJob(firstJob); err != nil {
		t.Fatalf("validate job: %v", err)
	}
	if err := backfillmanifest.ValidateShardResult(firstJob, firstJob.Shards[0], firstResult); err != nil {
		t.Fatalf("validate result: %v", err)
	}
	secondResult := readJSON[backfillmanifest.ShardResultManifest](t, secondSummary.ResultManifest)
	firstHashes := fileHashes(firstResult.Files)
	secondHashes := fileHashes(secondResult.Files)
	if !reflect.DeepEqual(firstHashes, secondHashes) {
		t.Fatalf("file hashes differ: %v != %v", firstHashes, secondHashes)
	}
}

func TestRunEnforcesInputRowBoundBeforeWritingParquet(t *testing.T) {
	fixturePath := recordTestFixture(t, 100, 101)
	output := filepath.Join(t.TempDir(), "bounded")
	var stdout bytes.Buffer
	err := run(context.Background(), []string{
		"--fixtures", fixturePath,
		"--output", output,
		"--max-bronze-rows", "1",
	}, &stdout)
	if err == nil {
		t.Fatal("run succeeded, want Bronze row bound failure")
	}
	matches, globErr := filepath.Glob(filepath.Join(output, "*.parquet"))
	if globErr != nil {
		t.Fatalf("glob output: %v", globErr)
	}
	if len(matches) != 0 {
		t.Fatalf("Parquet files written after resource rejection: %v", matches)
	}
}

func TestParseConfigAllowsLedgerStreamWithoutFixtures(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--source", "ledger-stream",
		"--output", t.TempDir(),
		"--start-ledger", "100",
		"--end-ledger", "101",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parse ledger-stream config: %v", err)
	}
	start, end, err := selectedStreamRange(cfg)
	if err != nil {
		t.Fatalf("select ledger-stream range: %v", err)
	}
	if start != 100 || end != 101 {
		t.Fatalf("selected range = %d-%d, want 100-101", start, end)
	}
}

func TestParseConfigAcceptsArrowThroughputProfile(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--source", "ledger-stream",
		"--output", t.TempDir(),
		"--start-ledger", "100",
		"--end-ledger", "101",
		"--writer", "ARROW-PARQUET",
		"--compression", "SNAPPY",
		"--extract-workers", "4",
		"--max-inflight-ledgers", "8",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parse Arrow throughput profile: %v", err)
	}
	if cfg.Writer != "arrow-parquet" || cfg.Compression != "snappy" || cfg.ExtractWorkers != 4 || cfg.MaxInFlight != 8 {
		t.Fatalf("profile = writer %q compression %q extract workers %d max in-flight %d", cfg.Writer, cfg.Compression, cfg.ExtractWorkers, cfg.MaxInFlight)
	}
}

func TestParseConfigRejectsPipelineBoundBelowWorkers(t *testing.T) {
	_, err := parseConfig([]string{
		"--source", "ledger-stream",
		"--output", t.TempDir(),
		"--start-ledger", "100",
		"--end-ledger", "101",
		"--extract-workers", "4",
		"--max-inflight-ledgers", "3",
	}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("parseConfig succeeded with fewer in-flight ledgers than extract workers")
	}
}

func TestSelectedStreamRangeRequiresExplicitBoundedRange(t *testing.T) {
	for _, cfg := range []config{
		{LedgerStart: 100},
		{LedgerEnd: 100},
		{LedgerStart: 101, LedgerEnd: 100},
	} {
		if _, _, err := selectedStreamRange(cfg); err == nil {
			t.Fatalf("selectedStreamRange(%+v) succeeded", cfg)
		}
	}
}

func TestLedgerStreamJobSourceRecordsArchiveIdentity(t *testing.T) {
	source, err := ledgerStreamJobSource(ledgerstream.Config{
		Type:               ledgerstream.TypeArchive,
		ArchiveStorageType: "S3",
		ArchiveBucketName:  "aws-public-blockchain",
		ArchivePath:        "v1.1/stellar/ledgers/pubnet",
		NetworkPassphrase:  network.PublicNetworkPassphrase,
	})
	if err != nil {
		t.Fatalf("ledger stream source: %v", err)
	}
	if source.Kind != "history-archive" || source.URI != "s3://aws-public-blockchain/v1.1/stellar/ledgers/pubnet" {
		t.Fatalf("source identity = %+v", source)
	}
	if source.ExtractorVersion == "" || source.ExtractorVersion == "unknown" {
		t.Fatalf("extractor version = %q", source.ExtractorVersion)
	}
}

func runWorker(t *testing.T, fixturePath, output string) runSummary {
	t.Helper()
	var stdout bytes.Buffer
	if err := run(context.Background(), []string{
		"--fixtures", fixturePath,
		"--output", output,
		"--code-revision", "test-revision",
		"--watermark-time", "2026-08-05T00:00:00Z",
	}, &stdout); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	var summary runSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary %q: %v", stdout.String(), err)
	}
	return summary
}

func recordTestFixture(t *testing.T, start, end uint32) string {
	t.Helper()
	var input bytes.Buffer
	for sequence := start; sequence <= end; sequence++ {
		batch := &componentsv1.LedgerBatch{
			NetworkPassphrase: "testnet",
			LedgerSequence:    sequence,
			ClosedAtUnix:      int64(sequence),
			SchemaVersion:     "v1",
			ExtractionVersion: "v1",
			BronzeRows: []*componentsv1.BronzeRow{{
				Id:                fmt.Sprintf("ledger-%d", sequence),
				TableName:         "ledgers_row_v2",
				NetworkPassphrase: "testnet",
				LedgerSequence:    sequence,
				LedgerRange:       sequence,
				RowJson: fmt.Sprintf(`{
					"Sequence": %d,
					"LedgerRange": %d,
					"LedgerHash": "hash-%d",
					"PreviousLedgerHash": "previous-%d"
				}`, sequence, sequence, sequence, sequence),
			}},
		}
		encoded, err := protojson.Marshal(batch)
		if err != nil {
			t.Fatalf("marshal fixture batch: %v", err)
		}
		input.Write(encoded)
		input.WriteByte('\n')
	}
	fixtureDir := t.TempDir()
	manifestPath := filepath.Join(fixtureDir, "fixture.manifest.json")
	if _, err := ledgerfixture.RecordJSONL(&input, ledgerfixture.RecordOptions{
		ManifestPath:   manifestPath,
		BatchesPerFile: 10,
	}); err != nil {
		t.Fatalf("record fixture: %v", err)
	}
	return manifestPath
}

func readJSON[T any](t *testing.T, name string) T {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return value
}

func fileHashes(files []backfillmanifest.File) map[string]string {
	hashes := make(map[string]string, len(files))
	for _, file := range files {
		hashes[file.Table] = file.SHA256
	}
	return hashes
}
