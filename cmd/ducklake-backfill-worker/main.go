// Command ducklake-backfill-worker materializes one fixture-backed historical
// shard into immutable Parquet plus validated job and result manifests. It is
// the local/file smoke entrypoint; catalog registration remains a separate
// authority boundary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"iter"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillmanifest"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/backfillworker"
	"github.com/withObsrvr/obsrvr-stellar-components/internal/ledgerfixture"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/bronze"
	"github.com/withObsrvr/obsrvr-stellar-components/pkg/contracts"
	"github.com/withObsrvr/stellar-raw-ledger-origin/source/ledgerstream"
)

type config struct {
	Source          string
	Fixtures        string
	OutputDir       string
	LedgerStart     uint
	LedgerEnd       uint
	JobID           string
	WorkerID        string
	Attempt         uint
	DecodeWorkers   int
	Compression     string
	FileTargetBytes uint64
	FileMaxBytes    uint64
	RowGroupRows    uint64
	MaxEncodedBytes uint64
	MaxBronzeRows   uint64
	MemoryLimit     string
	CodeRevision    string
	ImageDigest     string
	WatermarkTime   string
}

type runSummary struct {
	Source             string  `json:"source"`
	JobID              string  `json:"job_id"`
	ShardID            string  `json:"shard_id"`
	GenerationDigest   string  `json:"generation_digest"`
	LedgerStart        uint32  `json:"ledger_start"`
	LedgerEnd          uint32  `json:"ledger_end"`
	Ledgers            uint32  `json:"ledgers"`
	InputBytes         uint64  `json:"input_bytes"`
	BronzeRows         uint64  `json:"bronze_rows"`
	ParquetFiles       int     `json:"parquet_files"`
	ParquetBytes       uint64  `json:"parquet_bytes"`
	OutputRows         uint64  `json:"output_rows"`
	ElapsedSeconds     float64 `json:"elapsed_seconds"`
	LedgersPerSecond   float64 `json:"ledgers_per_second"`
	RowsPerSecond      float64 `json:"rows_per_second"`
	BytesPerSecond     float64 `json:"input_bytes_per_second"`
	PeakBatchBytes     uint64  `json:"peak_batch_bytes"`
	PeakBatchRows      uint64  `json:"peak_batch_bronze_rows"`
	StagingSeconds     float64 `json:"staging_seconds"`
	ExportSeconds      float64 `json:"export_seconds"`
	SetupSeconds       float64 `json:"setup_seconds"`
	SourceSeconds      float64 `json:"source_seconds"`
	DigestSeconds      float64 `json:"digest_seconds"`
	DecodeSeconds      float64 `json:"decode_seconds"`
	ExtractionSeconds  float64 `json:"extraction_seconds"`
	RawViewSeconds     float64 `json:"raw_view_seconds"`
	RawDecodeSeconds   float64 `json:"raw_decode_seconds"`
	RawExtractSeconds  float64 `json:"raw_extract_seconds"`
	RawPinSeconds      float64 `json:"raw_pin_seconds"`
	RawEnvelopeSeconds float64 `json:"raw_envelope_seconds"`
	RawProjectSeconds  float64 `json:"raw_project_seconds"`
	AppendSeconds      float64 `json:"append_seconds"`
	JobManifest        string  `json:"job_manifest"`
	ResultManifest     string  `json:"result_manifest"`
}

type jobSource struct {
	NetworkPassphrase string
	Kind              string
	URI               string
	ExtractorVersion  string
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer) error {
	cfg, err := parseConfig(args, stdout)
	if err != nil {
		return err
	}
	writtenAt, err := time.Parse(time.RFC3339Nano, cfg.WatermarkTime)
	if err != nil {
		return fmt.Errorf("parse --watermark-time: %w", err)
	}
	if writtenAt.Location() != time.UTC {
		return fmt.Errorf("--watermark-time must use UTC (Z)")
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o750); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	var (
		start       uint32
		end         uint32
		source      jobSource
		batchNext   backfillworker.LedgerBatchSource
		rawNext     backfillworker.RawLedgerSource
		closeSource func()
	)
	switch cfg.Source {
	case "fixture":
		fixture, err := ledgerfixture.ReadManifest(cfg.Fixtures)
		if err != nil {
			return err
		}
		start, end, err = selectedFixtureRange(cfg, fixture)
		if err != nil {
			return err
		}
		reader, err := ledgerfixture.NewRangeReader(cfg.Fixtures, fixture, start, end)
		if err != nil {
			return err
		}
		batchNext = reader.Next
		closeSource = func() { _ = reader.Close() }
		absFixtures, err := filepath.Abs(cfg.Fixtures)
		if err != nil {
			closeSource()
			return fmt.Errorf("resolve fixture manifest: %w", err)
		}
		source = jobSource{
			NetworkPassphrase: fixture.NetworkPassphrase,
			Kind:              "fixture",
			URI:               (&url.URL{Scheme: "file", Path: absFixtures}).String(),
			ExtractorVersion:  fixture.ExtractionVersion,
		}
	case "ledger-stream":
		start, end, err = selectedStreamRange(cfg)
		if err != nil {
			return err
		}
		streamConfig := ledgerstream.ConfigFromEnv()
		stream, err := ledgerstream.New(streamConfig)
		if err != nil {
			return fmt.Errorf("create ledger stream: %w", err)
		}
		source, err = ledgerStreamJobSource(streamConfig)
		if err != nil {
			return err
		}
		next, stop := iter.Pull2(stream.RawLedgers(ctx, ledgerbackend.BoundedRange(start, end)))
		closeSource = stop
		rawNext = func() ([]byte, error) {
			raw, streamErr, ok := next()
			if !ok {
				return nil, io.EOF
			}
			return raw, streamErr
		}
	default:
		return fmt.Errorf("unsupported --source %q", cfg.Source)
	}
	defer closeSource()

	job, shard, err := buildJob(cfg, source, start, end)
	if err != nil {
		return err
	}

	started := time.Now().UTC()
	workerConfig := backfillworker.LedgerBatchConfig{
		Parquet: backfillworker.ParquetConfig{
			OutputDir:       cfg.OutputDir,
			LedgerStart:     start,
			LedgerEnd:       end,
			Compression:     cfg.Compression,
			FileTargetBytes: cfg.FileTargetBytes,
			FileMaxBytes:    cfg.FileMaxBytes,
			RowGroupRows:    cfg.RowGroupRows,
		},
		DecodeWorkers:      cfg.DecodeWorkers,
		WatermarkWrittenAt: writtenAt,
		MaxEncodedBytes:    cfg.MaxEncodedBytes,
		MaxBronzeRows:      cfg.MaxBronzeRows,
		MemoryLimit:        cfg.MemoryLimit,
	}
	var stream backfillworker.StreamResult
	if batchNext != nil {
		stream, err = backfillworker.WriteLedgerBatchStream(ctx, workerConfig, batchNext)
	} else {
		stream, err = backfillworker.WriteRawLedgerStream(ctx, workerConfig, backfillworker.RawLedgerOptions{
			NetworkPassphrase: source.NetworkPassphrase,
			SchemaVersion:     contracts.SchemaVersion,
			ExtractionVersion: source.ExtractorVersion,
			MaterializedAt:    writtenAt,
			ProjectWorkers:    cfg.DecodeWorkers,
		}, rawNext)
	}
	if err != nil {
		return err
	}
	completed := time.Now().UTC()
	files := stream.Files
	descriptor := stream.Descriptor

	tableCounts := make(map[string]uint64)
	var parquetBytes, outputRows uint64
	for _, file := range files {
		tableCounts[file.Table] += file.Rows
		parquetBytes += file.Bytes
		outputRows += file.Rows
	}
	schemaFingerprint, err := aggregateSchemaFingerprint(files)
	if err != nil {
		return err
	}
	result := backfillmanifest.ShardResultManifest{
		FormatVersion:     backfillmanifest.FormatVersion,
		JobID:             job.JobID,
		ShardID:           shard.ShardID,
		LedgerStart:       start,
		LedgerEnd:         end,
		LedgerCount:       descriptor.LedgerCount,
		SourceDigest:      "sha256:" + descriptor.PayloadSHA256,
		SchemaFingerprint: schemaFingerprint,
		Files:             files,
		TableCounts:       tableCounts,
		Worker: backfillmanifest.Worker{
			ID:      cfg.WorkerID,
			Attempt: uint32(cfg.Attempt),
		},
		StartedAt:   started.Format(time.RFC3339Nano),
		CompletedAt: completed.Format(time.RFC3339Nano),
	}
	result.GenerationDigest, err = backfillmanifest.GenerationDigest(result)
	if err != nil {
		return err
	}
	if err := backfillmanifest.ValidateShardResult(job, shard, result); err != nil {
		return fmt.Errorf("validate produced shard manifest: %w", err)
	}

	jobPath := filepath.Join(cfg.OutputDir, "job.manifest.json")
	resultPath := filepath.Join(cfg.OutputDir, "shard-result.manifest.json")
	if err := writeJSONExclusive(jobPath, job); err != nil {
		return err
	}
	if err := writeJSONExclusive(resultPath, result); err != nil {
		return err
	}
	elapsed := completed.Sub(started)
	summary := runSummary{
		Source:             cfg.Source,
		JobID:              job.JobID,
		ShardID:            shard.ShardID,
		GenerationDigest:   result.GenerationDigest,
		LedgerStart:        start,
		LedgerEnd:          end,
		Ledgers:            descriptor.LedgerCount,
		InputBytes:         descriptor.EncodedBytes,
		BronzeRows:         descriptor.BronzeRows,
		ParquetFiles:       len(files),
		ParquetBytes:       parquetBytes,
		OutputRows:         outputRows,
		ElapsedSeconds:     elapsed.Seconds(),
		PeakBatchBytes:     stream.PeakBatchEncodedBytes,
		PeakBatchRows:      stream.PeakBatchBronzeRows,
		StagingSeconds:     stream.StagingDuration.Seconds(),
		ExportSeconds:      stream.ExportDuration.Seconds(),
		SetupSeconds:       stream.SetupDuration.Seconds(),
		SourceSeconds:      stream.SourceDuration.Seconds(),
		DigestSeconds:      stream.DigestDuration.Seconds(),
		DecodeSeconds:      stream.DecodeDuration.Seconds(),
		ExtractionSeconds:  stream.ExtractionDuration.Seconds(),
		RawViewSeconds:     stream.RawViewDuration.Seconds(),
		RawDecodeSeconds:   stream.RawDecodeDuration.Seconds(),
		RawExtractSeconds:  stream.RawExtractDuration.Seconds(),
		RawPinSeconds:      stream.RawPinDuration.Seconds(),
		RawEnvelopeSeconds: stream.RawEnvelopeDuration.Seconds(),
		RawProjectSeconds:  stream.RawProjectDuration.Seconds(),
		AppendSeconds:      stream.AppendDuration.Seconds(),
		JobManifest:        jobPath,
		ResultManifest:     resultPath,
	}
	if elapsed > 0 {
		summary.LedgersPerSecond = float64(summary.Ledgers) / elapsed.Seconds()
		summary.RowsPerSecond = float64(summary.OutputRows) / elapsed.Seconds()
		summary.BytesPerSecond = float64(summary.InputBytes) / elapsed.Seconds()
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(summary)
}

func parseConfig(args []string, output io.Writer) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("ducklake-backfill-worker", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.Source, "source", "fixture", "input source: fixture or ledger-stream")
	flags.StringVar(&cfg.Fixtures, "fixtures", "", "LedgerBatch fixture manifest (required for --source=fixture)")
	flags.StringVar(&cfg.OutputDir, "output", "", "new shard output directory (required)")
	flags.UintVar(&cfg.LedgerStart, "start-ledger", 0, "inclusive ledger start; defaults to fixture start")
	flags.UintVar(&cfg.LedgerEnd, "end-ledger", 0, "inclusive ledger end; defaults to fixture end")
	flags.StringVar(&cfg.JobID, "job-id", "", "stable job ID; defaults to a range-derived local ID")
	flags.StringVar(&cfg.WorkerID, "worker-id", "local-worker", "worker evidence ID")
	flags.UintVar(&cfg.Attempt, "attempt", 1, "one-based worker attempt")
	flags.IntVar(&cfg.DecodeWorkers, "decode-workers", min(runtime.NumCPU(), 8), "bounded decode worker count")
	flags.StringVar(&cfg.Compression, "compression", "zstd", "Parquet compression: zstd, snappy, or uncompressed")
	flags.Uint64Var(&cfg.FileTargetBytes, "file-target-bytes", 256<<20, "target Parquet part bytes before DuckDB rolls at a row-group boundary")
	flags.Uint64Var(&cfg.FileMaxBytes, "file-max-bytes", 512<<20, "hard maximum bytes for any rolled Parquet part")
	flags.Uint64Var(&cfg.RowGroupRows, "row-group-rows", 16_384, "Parquet rows per row group; minimum 2048")
	flags.Uint64Var(&cfg.MaxEncodedBytes, "max-encoded-bytes", 512<<20, "hard selected-range source payload byte limit")
	flags.Uint64Var(&cfg.MaxBronzeRows, "max-bronze-rows", 500_000, "hard selected-range Bronze row limit")
	flags.StringVar(&cfg.MemoryLimit, "memory-limit", "1GB", "hard DuckDB buffer-manager memory limit per worker")
	flags.StringVar(&cfg.CodeRevision, "code-revision", "local", "code revision recorded in the job manifest")
	flags.StringVar(&cfg.ImageDigest, "image-digest", backfillmanifest.Digest([]byte("local-unpinned-image")), "image SHA-256 recorded in the job manifest")
	flags.StringVar(&cfg.WatermarkTime, "watermark-time", "2026-08-05T00:00:00Z", "pinned deterministic UTC watermark timestamp")
	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	if flags.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	cfg.Source = strings.ToLower(strings.TrimSpace(cfg.Source))
	if cfg.OutputDir == "" {
		return cfg, fmt.Errorf("--output is required")
	}
	if cfg.Source != "fixture" && cfg.Source != "ledger-stream" {
		return cfg, fmt.Errorf("--source must be fixture or ledger-stream")
	}
	if cfg.Source == "fixture" && cfg.Fixtures == "" {
		return cfg, fmt.Errorf("--fixtures is required for --source=fixture")
	}
	if cfg.Attempt == 0 || cfg.Attempt > uint(^uint32(0)) {
		return cfg, fmt.Errorf("--attempt must fit a positive uint32")
	}
	if cfg.DecodeWorkers <= 0 || cfg.MaxEncodedBytes == 0 || cfg.MaxBronzeRows == 0 || cfg.FileTargetBytes == 0 || cfg.FileMaxBytes == 0 || cfg.RowGroupRows < 2048 {
		return cfg, fmt.Errorf("decode, byte, row, and file bounds must be positive, and row-group-rows must be at least 2048")
	}
	if cfg.FileMaxBytes < cfg.FileTargetBytes {
		return cfg, fmt.Errorf("--file-max-bytes must be greater than or equal to --file-target-bytes")
	}
	return cfg, nil
}

func selectedFixtureRange(cfg config, fixture *ledgerfixture.Manifest) (uint32, uint32, error) {
	start := uint32(cfg.LedgerStart)
	end := uint32(cfg.LedgerEnd)
	if cfg.LedgerStart == 0 {
		start = fixture.LedgerStart
	}
	if cfg.LedgerEnd == 0 {
		end = fixture.LedgerEnd
	}
	if uint(start) != cfg.LedgerStart && cfg.LedgerStart != 0 || uint(end) != cfg.LedgerEnd && cfg.LedgerEnd != 0 {
		return 0, 0, fmt.Errorf("ledger range must fit uint32")
	}
	if start < fixture.LedgerStart || end > fixture.LedgerEnd || end < start {
		return 0, 0, fmt.Errorf("selected range %d-%d falls outside fixture %d-%d", start, end, fixture.LedgerStart, fixture.LedgerEnd)
	}
	return start, end, nil
}

func selectedStreamRange(cfg config) (uint32, uint32, error) {
	if cfg.LedgerStart == 0 || cfg.LedgerEnd == 0 {
		return 0, 0, fmt.Errorf("--source=ledger-stream requires explicit --start-ledger and --end-ledger")
	}
	start := uint32(cfg.LedgerStart)
	end := uint32(cfg.LedgerEnd)
	if uint(start) != cfg.LedgerStart || uint(end) != cfg.LedgerEnd {
		return 0, 0, fmt.Errorf("ledger range must fit uint32")
	}
	if end < start {
		return 0, 0, fmt.Errorf("ledger end %d precedes start %d", end, start)
	}
	return start, end, nil
}

func buildJob(cfg config, source jobSource, start, end uint32) (backfillmanifest.JobManifest, backfillmanifest.ShardSpec, error) {
	jobID := cfg.JobID
	if jobID == "" {
		jobID = fmt.Sprintf("local-bronze-%d-%d-schema%d", start, end, len(bronze.Migrations))
	}
	job := backfillmanifest.JobManifest{
		FormatVersion:     backfillmanifest.FormatVersion,
		JobID:             jobID,
		NetworkPassphrase: source.NetworkPassphrase,
		LedgerStart:       start,
		LedgerEnd:         end,
		Source: backfillmanifest.Source{
			Kind: source.Kind,
			URI:  source.URI,
		},
		SchemaVersion:    uint32(len(bronze.Migrations)),
		ExtractorVersion: source.ExtractorVersion,
		CodeRevision:     cfg.CodeRevision,
		ImageDigest:      cfg.ImageDigest,
		DuckDBVersion:    "1.5.5",
		DuckLakeVersion:  "not-loaded-by-worker",
		FileTargetBytes:  cfg.FileTargetBytes,
	}
	shardID, err := backfillmanifest.DeriveShardID(job.JobID, job.NetworkPassphrase, job.SchemaVersion, start, end)
	if err != nil {
		return backfillmanifest.JobManifest{}, backfillmanifest.ShardSpec{}, err
	}
	shard := backfillmanifest.ShardSpec{
		JobID:               job.JobID,
		ShardID:             shardID,
		LedgerStart:         start,
		LedgerEnd:           end,
		ExpectedPredecessor: start - 1,
		AttemptPolicy:       backfillmanifest.AttemptPolicy{MaxAttempts: 5, LeaseSeconds: 900},
	}
	job.Shards = []backfillmanifest.ShardSpec{shard}
	if err := backfillmanifest.ValidateJob(job); err != nil {
		return backfillmanifest.JobManifest{}, backfillmanifest.ShardSpec{}, err
	}
	return job, shard, nil
}

func ledgerStreamJobSource(cfg ledgerstream.Config) (jobSource, error) {
	source := jobSource{
		NetworkPassphrase: cfg.NetworkPassphrase,
		ExtractorVersion:  dependencyVersion("github.com/withObsrvr/stellar-extract"),
	}
	switch cfg.Type {
	case ledgerstream.TypeArchive:
		scheme := strings.ToLower(cfg.ArchiveStorageType)
		if scheme == "gcs" {
			scheme = "gs"
		}
		source.Kind = "history-archive"
		source.URI = (&url.URL{
			Scheme: scheme,
			Host:   cfg.ArchiveBucketName,
			Path:   "/" + strings.Trim(cfg.ArchivePath, "/"),
		}).String()
	case ledgerstream.TypeRPC:
		source.Kind = "stellar-rpc"
		source.URI = cfg.RPCServerURL
	case ledgerstream.TypeCaptiveCore:
		source.Kind = "captive-core"
		if len(cfg.HistoryArchiveURLs) > 0 {
			source.URI = cfg.HistoryArchiveURLs[0]
		}
	default:
		return jobSource{}, fmt.Errorf("unsupported ledger stream type %q", cfg.Type)
	}
	if source.NetworkPassphrase == "" || source.URI == "" || source.ExtractorVersion == "" {
		return jobSource{}, fmt.Errorf("ledger stream source identity is incomplete: %+v", source)
	}
	return source, nil
}

func dependencyVersion(modulePath string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dependency := range info.Deps {
		if dependency.Path != modulePath {
			continue
		}
		if dependency.Replace != nil {
			dependency = dependency.Replace
		}
		if dependency.Version == "" {
			return dependency.Path
		}
		return dependency.Path + "@" + dependency.Version
	}
	return "unknown"
}

func aggregateSchemaFingerprint(files []backfillmanifest.File) (string, error) {
	type entry struct {
		Table       string `json:"table"`
		Fingerprint string `json:"fingerprint"`
	}
	entries := make([]entry, 0, len(files))
	for _, file := range files {
		entries = append(entries, entry{Table: file.Table, Fingerprint: file.ParquetSchemaFingerprint})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Table < entries[j].Table })
	return backfillmanifest.CanonicalDigest(entries)
}

func writeJSONExclusive(name string, value any) (resultErr error) {
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create manifest %s: %w", name, err)
	}
	complete := false
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
		}
		if !complete {
			_ = os.Remove(name)
		}
	}()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("write manifest %s: %w", name, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync manifest %s: %w", name, err)
	}
	complete = true
	return nil
}
