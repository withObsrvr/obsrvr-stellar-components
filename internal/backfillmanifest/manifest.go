// Package backfillmanifest defines the immutable contract between parallel
// backfill workers and the process that is allowed to mutate a DuckLake
// catalog. It deliberately contains no DuckDB or object-store operations.
package backfillmanifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
)

// FormatVersion 2 pins writer and physical Parquet policy in the immutable job
// identity. Version 1 jobs did not distinguish Appender, Arrow, or codec.
const FormatVersion = 2

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	tablePattern      = regexp.MustCompile(`^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$`)
	digestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type Source struct {
	Kind string `json:"kind"`
	URI  string `json:"uri"`
}

type AttemptPolicy struct {
	MaxAttempts  uint32 `json:"max_attempts"`
	LeaseSeconds uint32 `json:"lease_seconds"`
}

type ShardSpec struct {
	JobID               string        `json:"job_id"`
	ShardID             string        `json:"shard_id"`
	LedgerStart         uint32        `json:"ledger_start"`
	LedgerEnd           uint32        `json:"ledger_end"`
	ExpectedPredecessor uint32        `json:"expected_predecessor"`
	AttemptPolicy       AttemptPolicy `json:"attempt_policy"`
}

type JobManifest struct {
	FormatVersion     uint32      `json:"format_version"`
	JobID             string      `json:"job_id"`
	NetworkPassphrase string      `json:"network_passphrase"`
	LedgerStart       uint32      `json:"ledger_start"`
	LedgerEnd         uint32      `json:"ledger_end"`
	Source            Source      `json:"source"`
	SchemaVersion     uint32      `json:"schema_version"`
	ExtractorVersion  string      `json:"extractor_version"`
	CodeRevision      string      `json:"code_revision"`
	ImageDigest       string      `json:"image_digest"`
	DuckDBVersion     string      `json:"duckdb_version"`
	DuckLakeVersion   string      `json:"ducklake_version"`
	Writer            string      `json:"writer"`
	Compression       string      `json:"compression"`
	FileTargetBytes   uint64      `json:"file_target_bytes"`
	FileMaxBytes      uint64      `json:"file_max_bytes"`
	RowGroupRows      uint64      `json:"row_group_rows"`
	Shards            []ShardSpec `json:"shards"`
}

type File struct {
	Table                    string `json:"table"`
	URI                      string `json:"uri"`
	SHA256                   string `json:"sha256"`
	Bytes                    uint64 `json:"bytes"`
	Rows                     uint64 `json:"rows"`
	MinLedger                uint32 `json:"min_ledger"`
	MaxLedger                uint32 `json:"max_ledger"`
	ParquetSchemaFingerprint string `json:"parquet_schema_fingerprint"`
}

type Worker struct {
	ID      string `json:"id"`
	Attempt uint32 `json:"attempt"`
}

type ShardResultManifest struct {
	FormatVersion     uint32            `json:"format_version"`
	JobID             string            `json:"job_id"`
	ShardID           string            `json:"shard_id"`
	GenerationDigest  string            `json:"generation_digest"`
	LedgerStart       uint32            `json:"ledger_start"`
	LedgerEnd         uint32            `json:"ledger_end"`
	LedgerCount       uint32            `json:"ledger_count"`
	SourceDigest      string            `json:"source_digest"`
	SchemaFingerprint string            `json:"schema_fingerprint"`
	Files             []File            `json:"files"`
	TableCounts       map[string]uint64 `json:"table_counts"`
	Worker            Worker            `json:"worker"`
	StartedAt         string            `json:"started_at"`
	CompletedAt       string            `json:"completed_at"`
}

// Digest returns the repository's canonical digest representation.
func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// CanonicalJSON returns the stable, whitespace-free JSON representation used
// for manifest hashing. These contracts contain only structs, slices, string
// maps, integers, and strings; encoding/json sorts string map keys.
func CanonicalJSON(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical manifest JSON: %w", err)
	}
	return data, nil
}

func CanonicalDigest(value any) (string, error) {
	data, err := CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	return Digest(data), nil
}

// GenerationDigest identifies the deterministic output of a shard while
// deliberately excluding worker attempt and timing evidence. Two at-least-once
// attempts may have different evidence but must produce the same generation.
func GenerationDigest(result ShardResultManifest) (string, error) {
	generation := struct {
		FormatVersion     uint32            `json:"format_version"`
		JobID             string            `json:"job_id"`
		ShardID           string            `json:"shard_id"`
		LedgerStart       uint32            `json:"ledger_start"`
		LedgerEnd         uint32            `json:"ledger_end"`
		LedgerCount       uint32            `json:"ledger_count"`
		SourceDigest      string            `json:"source_digest"`
		SchemaFingerprint string            `json:"schema_fingerprint"`
		Files             []File            `json:"files"`
		TableCounts       map[string]uint64 `json:"table_counts"`
	}{
		FormatVersion:     result.FormatVersion,
		JobID:             result.JobID,
		ShardID:           result.ShardID,
		LedgerStart:       result.LedgerStart,
		LedgerEnd:         result.LedgerEnd,
		LedgerCount:       result.LedgerCount,
		SourceDigest:      result.SourceDigest,
		SchemaFingerprint: result.SchemaFingerprint,
		Files:             result.Files,
		TableCounts:       result.TableCounts,
	}
	return CanonicalDigest(generation)
}

func ValidateRetry(existing, candidate ShardResultManifest) error {
	existingDigest, err := GenerationDigest(existing)
	if err != nil {
		return fmt.Errorf("digest existing generation: %w", err)
	}
	candidateDigest, err := GenerationDigest(candidate)
	if err != nil {
		return fmt.Errorf("digest candidate generation: %w", err)
	}
	if existingDigest != candidateDigest {
		return fmt.Errorf("shard retry diverged: existing generation %s, candidate %s", existingDigest, candidateDigest)
	}
	return nil
}

// DeriveShardID makes shard identity independent of attempts, workers, and
// completion timestamps.
func DeriveShardID(jobID, networkPassphrase string, schemaVersion, ledgerStart, ledgerEnd uint32) (string, error) {
	identity := struct {
		JobID             string `json:"job_id"`
		NetworkPassphrase string `json:"network_passphrase"`
		SchemaVersion     uint32 `json:"schema_version"`
		LedgerStart       uint32 `json:"ledger_start"`
		LedgerEnd         uint32 `json:"ledger_end"`
	}{
		JobID:             jobID,
		NetworkPassphrase: networkPassphrase,
		SchemaVersion:     schemaVersion,
		LedgerStart:       ledgerStart,
		LedgerEnd:         ledgerEnd,
	}
	return CanonicalDigest(identity)
}

func ValidateJob(job JobManifest) error {
	if job.FormatVersion != FormatVersion {
		return fmt.Errorf("job format_version = %d, want %d", job.FormatVersion, FormatVersion)
	}
	if !identifierPattern.MatchString(job.JobID) {
		return fmt.Errorf("job_id %q is invalid", job.JobID)
	}
	if strings.TrimSpace(job.NetworkPassphrase) == "" {
		return fmt.Errorf("network_passphrase is required")
	}
	if err := validateRange(job.LedgerStart, job.LedgerEnd); err != nil {
		return fmt.Errorf("job ledger range: %w", err)
	}
	if strings.TrimSpace(job.Source.Kind) == "" {
		return fmt.Errorf("source kind is required")
	}
	if err := validateURI(job.Source.URI); err != nil {
		return fmt.Errorf("source URI: %w", err)
	}
	if job.SchemaVersion == 0 {
		return fmt.Errorf("schema_version must be positive")
	}
	for name, value := range map[string]string{
		"extractor_version": job.ExtractorVersion,
		"code_revision":     job.CodeRevision,
		"duckdb_version":    job.DuckDBVersion,
		"ducklake_version":  job.DuckLakeVersion,
		"writer":            job.Writer,
		"compression":       job.Compression,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if job.Writer != "duckdb-appender" && job.Writer != "arrow-parquet" {
		return fmt.Errorf("writer %q is unsupported", job.Writer)
	}
	if job.Compression != "zstd" && job.Compression != "snappy" && job.Compression != "uncompressed" {
		return fmt.Errorf("compression %q is unsupported", job.Compression)
	}
	if !digestPattern.MatchString(job.ImageDigest) {
		return fmt.Errorf("image_digest %q is not a canonical SHA-256 digest", job.ImageDigest)
	}
	if job.FileTargetBytes == 0 {
		return fmt.Errorf("file_target_bytes must be positive")
	}
	if job.FileMaxBytes < job.FileTargetBytes {
		return fmt.Errorf("file_max_bytes must be at least file_target_bytes")
	}
	if job.RowGroupRows < 2048 {
		return fmt.Errorf("row_group_rows must be at least 2048")
	}
	if len(job.Shards) == 0 {
		return fmt.Errorf("job must contain at least one shard")
	}

	next := job.LedgerStart
	seen := make(map[string]struct{}, len(job.Shards))
	for index, shard := range job.Shards {
		if err := ValidateShardSpec(job, shard); err != nil {
			return fmt.Errorf("shard %d: %w", index, err)
		}
		if shard.LedgerStart != next {
			return fmt.Errorf("shard %d starts at %d, want continuous coverage at %d", index, shard.LedgerStart, next)
		}
		if _, ok := seen[shard.ShardID]; ok {
			return fmt.Errorf("shard %d duplicates shard_id %s", index, shard.ShardID)
		}
		seen[shard.ShardID] = struct{}{}
		if shard.LedgerEnd == job.LedgerEnd {
			next = 0
		} else {
			next = shard.LedgerEnd + 1
		}
	}
	last := job.Shards[len(job.Shards)-1]
	if last.LedgerEnd != job.LedgerEnd {
		return fmt.Errorf("last shard ends at %d, want job end %d", last.LedgerEnd, job.LedgerEnd)
	}
	return nil
}

func ValidateShardSpec(job JobManifest, shard ShardSpec) error {
	if shard.JobID != job.JobID {
		return fmt.Errorf("job_id %q does not match %q", shard.JobID, job.JobID)
	}
	if err := validateRange(shard.LedgerStart, shard.LedgerEnd); err != nil {
		return fmt.Errorf("ledger range: %w", err)
	}
	if shard.LedgerStart < job.LedgerStart || shard.LedgerEnd > job.LedgerEnd {
		return fmt.Errorf("range %d-%d falls outside job range %d-%d", shard.LedgerStart, shard.LedgerEnd, job.LedgerStart, job.LedgerEnd)
	}
	if shard.ExpectedPredecessor != shard.LedgerStart-1 {
		return fmt.Errorf("expected_predecessor = %d, want %d", shard.ExpectedPredecessor, shard.LedgerStart-1)
	}
	if shard.AttemptPolicy.MaxAttempts == 0 {
		return fmt.Errorf("attempt_policy.max_attempts must be positive")
	}
	if shard.AttemptPolicy.LeaseSeconds == 0 {
		return fmt.Errorf("attempt_policy.lease_seconds must be positive")
	}
	wantID, err := DeriveShardID(job.JobID, job.NetworkPassphrase, job.SchemaVersion, shard.LedgerStart, shard.LedgerEnd)
	if err != nil {
		return err
	}
	if shard.ShardID != wantID {
		return fmt.Errorf("shard_id %q does not match derived identity %q", shard.ShardID, wantID)
	}
	return nil
}

func ValidateShardResult(job JobManifest, shard ShardSpec, result ShardResultManifest) error {
	if err := ValidateShardSpec(job, shard); err != nil {
		return fmt.Errorf("shard specification: %w", err)
	}
	if result.FormatVersion != FormatVersion {
		return fmt.Errorf("result format_version = %d, want %d", result.FormatVersion, FormatVersion)
	}
	if result.JobID != shard.JobID || result.ShardID != shard.ShardID {
		return fmt.Errorf("result identity does not match shard")
	}
	wantGeneration, err := GenerationDigest(result)
	if err != nil {
		return err
	}
	if result.GenerationDigest != wantGeneration {
		return fmt.Errorf("generation_digest %q does not match deterministic output %q", result.GenerationDigest, wantGeneration)
	}
	if result.LedgerStart != shard.LedgerStart || result.LedgerEnd != shard.LedgerEnd {
		return fmt.Errorf("result range %d-%d does not match shard range %d-%d", result.LedgerStart, result.LedgerEnd, shard.LedgerStart, shard.LedgerEnd)
	}
	wantCount := uint64(shard.LedgerEnd) - uint64(shard.LedgerStart) + 1
	if uint64(result.LedgerCount) != wantCount {
		return fmt.Errorf("ledger_count = %d, want %d", result.LedgerCount, wantCount)
	}
	if !digestPattern.MatchString(result.SourceDigest) {
		return fmt.Errorf("source_digest %q is not a canonical SHA-256 digest", result.SourceDigest)
	}
	if !digestPattern.MatchString(result.SchemaFingerprint) {
		return fmt.Errorf("schema_fingerprint %q is not a canonical SHA-256 digest", result.SchemaFingerprint)
	}
	if len(result.Files) == 0 {
		return fmt.Errorf("result must contain at least one file")
	}
	if strings.TrimSpace(result.Worker.ID) == "" {
		return fmt.Errorf("worker.id is required")
	}
	if result.Worker.Attempt == 0 || result.Worker.Attempt > shard.AttemptPolicy.MaxAttempts {
		return fmt.Errorf("worker.attempt = %d, want 1-%d", result.Worker.Attempt, shard.AttemptPolicy.MaxAttempts)
	}
	started, err := time.Parse(time.RFC3339Nano, result.StartedAt)
	if err != nil {
		return fmt.Errorf("started_at: %w", err)
	}
	completed, err := time.Parse(time.RFC3339Nano, result.CompletedAt)
	if err != nil {
		return fmt.Errorf("completed_at: %w", err)
	}
	if completed.Before(started) {
		return fmt.Errorf("completed_at precedes started_at")
	}

	fileRows := make(map[string]uint64, len(result.TableCounts))
	seenURIs := make(map[string]struct{}, len(result.Files))
	for index, file := range result.Files {
		if err := validateFile(shard, file); err != nil {
			return fmt.Errorf("file %d: %w", index, err)
		}
		if index > 0 && compareFiles(result.Files[index-1], file) >= 0 {
			return fmt.Errorf("files must be strictly ordered by table, range, and URI")
		}
		if _, ok := seenURIs[file.URI]; ok {
			return fmt.Errorf("file %d duplicates URI %q", index, file.URI)
		}
		seenURIs[file.URI] = struct{}{}
		fileRows[file.Table] += file.Rows
	}
	for table, count := range result.TableCounts {
		if !tablePattern.MatchString(table) {
			return fmt.Errorf("table_counts key %q is not schema-qualified", table)
		}
		if fileRows[table] != count {
			return fmt.Errorf("table %s file rows = %d, declared %d", table, fileRows[table], count)
		}
		delete(fileRows, table)
	}
	if len(fileRows) > 0 {
		tables := make([]string, 0, len(fileRows))
		for table := range fileRows {
			tables = append(tables, table)
		}
		sort.Strings(tables)
		return fmt.Errorf("file tables missing from table_counts: %s", strings.Join(tables, ", "))
	}
	return nil
}

func validateRange(start, end uint32) error {
	if start == 0 {
		return fmt.Errorf("start must be positive")
	}
	if end < start {
		return fmt.Errorf("end %d precedes start %d", end, start)
	}
	return nil
}

func validateFile(shard ShardSpec, file File) error {
	if !tablePattern.MatchString(file.Table) {
		return fmt.Errorf("table %q is not schema-qualified", file.Table)
	}
	if err := validateURI(file.URI); err != nil {
		return fmt.Errorf("URI: %w", err)
	}
	if !digestPattern.MatchString(file.SHA256) {
		return fmt.Errorf("sha256 %q is not canonical", file.SHA256)
	}
	if !digestPattern.MatchString(file.ParquetSchemaFingerprint) {
		return fmt.Errorf("parquet_schema_fingerprint %q is not canonical", file.ParquetSchemaFingerprint)
	}
	if file.Bytes == 0 || file.Rows == 0 {
		return fmt.Errorf("bytes and rows must be positive")
	}
	if err := validateRange(file.MinLedger, file.MaxLedger); err != nil {
		return fmt.Errorf("ledger range: %w", err)
	}
	if file.MinLedger < shard.LedgerStart || file.MaxLedger > shard.LedgerEnd {
		return fmt.Errorf("range %d-%d falls outside shard range %d-%d", file.MinLedger, file.MaxLedger, shard.LedgerStart, shard.LedgerEnd)
	}
	return nil
}

func validateURI(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.Scheme == "" {
		return fmt.Errorf("absolute URI with a scheme is required")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return fmt.Errorf("credentials, query, and fragment are not allowed")
	}
	if parsed.Scheme != "file" && parsed.Host == "" {
		return fmt.Errorf("host is required for %s URI", parsed.Scheme)
	}
	if parsed.Scheme == "file" && !strings.HasPrefix(parsed.Path, "/") {
		return fmt.Errorf("file URI path must be absolute")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return fmt.Errorf("object path is required")
	}
	if cleaned := path.Clean(parsed.Path); cleaned != parsed.Path || strings.Contains(parsed.Path, "/../") {
		return fmt.Errorf("object path must be clean")
	}
	return nil
}

func compareFiles(left, right File) int {
	if value := strings.Compare(left.Table, right.Table); value != 0 {
		return value
	}
	if left.MinLedger < right.MinLedger {
		return -1
	}
	if left.MinLedger > right.MinLedger {
		return 1
	}
	if left.MaxLedger < right.MaxLedger {
		return -1
	}
	if left.MaxLedger > right.MaxLedger {
		return 1
	}
	return strings.Compare(left.URI, right.URI)
}
