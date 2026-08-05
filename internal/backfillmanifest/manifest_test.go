package backfillmanifest

import (
	"strings"
	"testing"
)

func TestValidateJobRequiresExactDeterministicCoverage(t *testing.T) {
	job := validJob(t)
	if err := ValidateJob(job); err != nil {
		t.Fatalf("validate job: %v", err)
	}

	job.Shards[1].LedgerStart++
	job.Shards[1].ExpectedPredecessor++
	job.Shards[1].ShardID = mustShardID(t, job, job.Shards[1].LedgerStart, job.Shards[1].LedgerEnd)
	if err := ValidateJob(job); err == nil || !strings.Contains(err.Error(), "continuous coverage") {
		t.Fatalf("ValidateJob gap error = %v, want continuous coverage", err)
	}
}

func TestShardIDExcludesAttemptAndWorkerEvidence(t *testing.T) {
	job := validJob(t)
	first := job.Shards[0]
	id, err := DeriveShardID(job.JobID, job.NetworkPassphrase, job.SchemaVersion, first.LedgerStart, first.LedgerEnd)
	if err != nil {
		t.Fatalf("derive shard ID: %v", err)
	}
	first.AttemptPolicy.MaxAttempts = 99
	again, err := DeriveShardID(job.JobID, job.NetworkPassphrase, job.SchemaVersion, first.LedgerStart, first.LedgerEnd)
	if err != nil {
		t.Fatalf("derive shard ID again: %v", err)
	}
	if id != again || id != job.Shards[0].ShardID {
		t.Fatalf("shard IDs differ: %q %q %q", id, again, job.Shards[0].ShardID)
	}
}

func TestCanonicalDigestIsStableAcrossMapInsertionOrder(t *testing.T) {
	left := map[string]uint64{"bronze.z": 2, "bronze.a": 1}
	right := map[string]uint64{}
	right["bronze.a"] = 1
	right["bronze.z"] = 2
	leftDigest, err := CanonicalDigest(left)
	if err != nil {
		t.Fatalf("left digest: %v", err)
	}
	rightDigest, err := CanonicalDigest(right)
	if err != nil {
		t.Fatalf("right digest: %v", err)
	}
	if leftDigest != rightDigest {
		t.Fatalf("canonical digests differ: %s != %s", leftDigest, rightDigest)
	}
}

func TestValidateShardResultChecksFilesCountsAndOrdering(t *testing.T) {
	job := validJob(t)
	shard := job.Shards[0]
	result := validResult(shard)
	if err := ValidateShardResult(job, shard, result); err != nil {
		t.Fatalf("validate result: %v", err)
	}

	result.TableCounts["bronze.ledgers_row_v2"]++
	result.GenerationDigest, _ = GenerationDigest(result)
	if err := ValidateShardResult(job, shard, result); err == nil || !strings.Contains(err.Error(), "declared") {
		t.Fatalf("ValidateShardResult count error = %v, want declared count mismatch", err)
	}

	result = validResult(shard)
	result.Files[0], result.Files[1] = result.Files[1], result.Files[0]
	result.GenerationDigest, _ = GenerationDigest(result)
	if err := ValidateShardResult(job, shard, result); err == nil || !strings.Contains(err.Error(), "strictly ordered") {
		t.Fatalf("ValidateShardResult ordering error = %v, want strict ordering", err)
	}
}

func TestValidateShardResultRejectsUnsafeURIAndDivergentIdentity(t *testing.T) {
	job := validJob(t)
	shard := job.Shards[0]
	result := validResult(shard)
	result.Files[0].URI = "file:///tmp/shards/../escape.parquet"
	result.GenerationDigest, _ = GenerationDigest(result)
	if err := ValidateShardResult(job, shard, result); err == nil || !strings.Contains(err.Error(), "clean") {
		t.Fatalf("ValidateShardResult URI error = %v, want clean path", err)
	}

	result = validResult(shard)
	result.ShardID = Digest([]byte("another-shard"))
	if err := ValidateShardResult(job, shard, result); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("ValidateShardResult identity error = %v, want identity mismatch", err)
	}
}

func TestValidateRetryIgnoresAttemptEvidenceButRejectsDifferentFiles(t *testing.T) {
	job := validJob(t)
	shard := job.Shards[0]
	existing := validResult(shard)
	candidate := validResult(shard)
	candidate.Worker = Worker{ID: "worker-9", Attempt: 2}
	candidate.StartedAt = "2026-08-05T13:00:00Z"
	candidate.CompletedAt = "2026-08-05T13:01:00Z"
	if err := ValidateRetry(existing, candidate); err != nil {
		t.Fatalf("identical generation retry: %v", err)
	}

	candidate.Files[0].SHA256 = Digest([]byte("different-file"))
	if err := ValidateRetry(existing, candidate); err == nil || !strings.Contains(err.Error(), "diverged") {
		t.Fatalf("ValidateRetry error = %v, want divergence", err)
	}
}

func validJob(t *testing.T) JobManifest {
	t.Helper()
	job := JobManifest{
		FormatVersion:     FormatVersion,
		JobID:             "pubnet-bronze-100-109-schema7",
		NetworkPassphrase: "Public Global Stellar Network ; September 2015",
		LedgerStart:       100,
		LedgerEnd:         109,
		Source:            Source{Kind: "fixture", URI: "file:///tmp/fixtures/manifest.json"},
		SchemaVersion:     7,
		ExtractorVersion:  "stellar-extract-v0.1.4",
		CodeRevision:      "0123456789abcdef",
		ImageDigest:       Digest([]byte("image")),
		DuckDBVersion:     "1.5.5",
		DuckLakeVersion:   "d8a1881e",
		FileTargetBytes:   256 << 20,
	}
	job.Shards = []ShardSpec{
		validShard(t, job, 100, 104),
		validShard(t, job, 105, 109),
	}
	return job
}

func validShard(t *testing.T, job JobManifest, start, end uint32) ShardSpec {
	t.Helper()
	return ShardSpec{
		JobID:               job.JobID,
		ShardID:             mustShardID(t, job, start, end),
		LedgerStart:         start,
		LedgerEnd:           end,
		ExpectedPredecessor: start - 1,
		AttemptPolicy:       AttemptPolicy{MaxAttempts: 5, LeaseSeconds: 900},
	}
}

func mustShardID(t *testing.T, job JobManifest, start, end uint32) string {
	t.Helper()
	id, err := DeriveShardID(job.JobID, job.NetworkPassphrase, job.SchemaVersion, start, end)
	if err != nil {
		t.Fatalf("derive shard ID: %v", err)
	}
	return id
}

func validResult(shard ShardSpec) ShardResultManifest {
	files := []File{
		{
			Table:                    "bronze.ledgers_row_v2",
			URI:                      "file:///tmp/shards/ledgers.parquet",
			SHA256:                   Digest([]byte("ledgers")),
			Bytes:                    1024,
			Rows:                     5,
			MinLedger:                shard.LedgerStart,
			MaxLedger:                shard.LedgerEnd,
			ParquetSchemaFingerprint: Digest([]byte("ledger-schema")),
		},
		{
			Table:                    "bronze.transactions_row_v2",
			URI:                      "file:///tmp/shards/transactions.parquet",
			SHA256:                   Digest([]byte("transactions")),
			Bytes:                    2048,
			Rows:                     12,
			MinLedger:                shard.LedgerStart,
			MaxLedger:                shard.LedgerEnd,
			ParquetSchemaFingerprint: Digest([]byte("transaction-schema")),
		},
	}
	result := ShardResultManifest{
		FormatVersion:     FormatVersion,
		JobID:             shard.JobID,
		ShardID:           shard.ShardID,
		LedgerStart:       shard.LedgerStart,
		LedgerEnd:         shard.LedgerEnd,
		LedgerCount:       shard.LedgerEnd - shard.LedgerStart + 1,
		SourceDigest:      Digest([]byte("source")),
		SchemaFingerprint: Digest([]byte("schema")),
		Files:             files,
		TableCounts: map[string]uint64{
			"bronze.ledgers_row_v2":      5,
			"bronze.transactions_row_v2": 12,
		},
		Worker:      Worker{ID: "worker-1", Attempt: 1},
		StartedAt:   "2026-08-05T12:00:00Z",
		CompletedAt: "2026-08-05T12:01:00Z",
	}
	result.GenerationDigest, _ = GenerationDigest(result)
	return result
}
