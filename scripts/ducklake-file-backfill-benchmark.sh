#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
worker_bin="${BACKFILL_WORKER_BIN:-$repo_root/bin/ducklake-backfill-worker}"
backfill_source="${BACKFILL_SOURCE:-fixture}"
fixture_manifest="${BACKFILL_FIXTURE_MANIFEST:-${1:-}}"
concurrency="${BACKFILL_CONCURRENCY:-1}"
minimum_lps="${BACKFILL_MIN_AGGREGATE_LPS:-0}"
file_target_bytes="${BACKFILL_FILE_TARGET_BYTES:-268435456}"
file_max_bytes="${BACKFILL_FILE_MAX_BYTES:-536870912}"
row_group_rows="${BACKFILL_ROW_GROUP_ROWS:-16384}"
decode_workers="${BACKFILL_DECODE_WORKERS:-8}"
writer_mode="${BACKFILL_WRITER:-duckdb-appender}"
compression="${BACKFILL_COMPRESSION:-zstd}"
max_encoded_bytes="${BACKFILL_MAX_ENCODED_BYTES:-4294967296}"
max_bronze_rows="${BACKFILL_MAX_BRONZE_ROWS:-2000000}"
memory_limit="${BACKFILL_MEMORY_LIMIT:-1GB}"
keep_outputs="${BACKFILL_KEEP_OUTPUTS:-false}"

if [[ ! -x "$worker_bin" ]]; then
  echo "backfill worker is not executable: $worker_bin" >&2
  exit 2
fi
if [[ ! "$concurrency" =~ ^[1-9][0-9]*$ ]]; then
  echo "BACKFILL_CONCURRENCY must be a positive integer" >&2
  exit 2
fi

case "$backfill_source" in
  fixture)
    if [[ -z "$fixture_manifest" ]]; then
      echo "set BACKFILL_FIXTURE_MANIFEST or pass a fixture manifest path" >&2
      exit 2
    fi
    ledger_start="$(jq -er '.ledger_start' "$fixture_manifest")"
    ledger_end="$(jq -er '.ledger_end' "$fixture_manifest")"
    ;;
  ledger-stream)
    ledger_start="${BACKFILL_LEDGER_START:-}"
    ledger_end="${BACKFILL_LEDGER_END:-}"
    if [[ ! "$ledger_start" =~ ^[1-9][0-9]*$ || ! "$ledger_end" =~ ^[1-9][0-9]*$ || "$ledger_end" -lt "$ledger_start" ]]; then
      echo "ledger-stream mode requires a valid BACKFILL_LEDGER_START/BACKFILL_LEDGER_END range" >&2
      exit 2
    fi
    ;;
  *)
    echo "BACKFILL_SOURCE must be fixture or ledger-stream" >&2
    exit 2
    ;;
esac
ledger_count=$((ledger_end - ledger_start + 1))
if (( concurrency > ledger_count )); then
  echo "concurrency $concurrency exceeds ledger count $ledger_count" >&2
  exit 2
fi

if [[ -n "${BACKFILL_RUNTIME_DIR:-}" ]]; then
  runtime_dir="$BACKFILL_RUNTIME_DIR"
  mkdir -p "$runtime_dir"
else
  runtime_dir="$(mktemp -d "${TMPDIR:-/tmp}/obsrvr-file-backfill-benchmark.XXXXXX")"
fi
runtime_dir="$(realpath "$runtime_dir")"
if [[ "$runtime_dir" == "/" || "$runtime_dir" == "$repo_root" || ( -n "${HOME:-}" && "$runtime_dir" == "$HOME" ) ]]; then
  echo "refusing unsafe benchmark runtime directory: $runtime_dir" >&2
  exit 2
fi
evidence_dir="$runtime_dir/evidence"
outputs_dir="$runtime_dir/outputs"
if [[ -e "$evidence_dir" || -e "$outputs_dir" ]]; then
  echo "benchmark runtime already contains evidence or outputs: $runtime_dir" >&2
  exit 2
fi
mkdir -p "$evidence_dir" "$outputs_dir"
touch "$runtime_dir/.obsrvr-file-backfill-benchmark"

time_bin="${TIME_BIN:-$(type -P time || true)}"
base_size=$((ledger_count / concurrency))
extra=$((ledger_count % concurrency))
next_start=$ledger_start
pids=()
worker_ids=()

started_ns="$(date +%s%N)"
for ((worker_index = 0; worker_index < concurrency; worker_index++)); do
  shard_size=$base_size
  if (( worker_index < extra )); then
    shard_size=$((shard_size + 1))
  fi
  shard_start=$next_start
  shard_end=$((shard_start + shard_size - 1))
  next_start=$((shard_end + 1))
  worker_id="worker-$(printf '%03d' "$worker_index")"
  worker_output="$outputs_dir/$worker_id"
  worker_summary="$evidence_dir/$worker_id.summary.json"
  worker_resource="$evidence_dir/$worker_id.resource.txt"
  mkdir -p "$worker_output"

  command=(
    "$worker_bin"
    --source "$backfill_source"
    --output "$worker_output"
    --start-ledger "$shard_start"
    --end-ledger "$shard_end"
    --job-id "benchmark-${ledger_start}-${ledger_end}-c${concurrency}"
    --worker-id "$worker_id"
    --decode-workers "$decode_workers"
    --writer "$writer_mode"
    --compression "$compression"
    --max-encoded-bytes "$max_encoded_bytes"
    --max-bronze-rows "$max_bronze_rows"
    --memory-limit "$memory_limit"
    --file-target-bytes "$file_target_bytes"
    --file-max-bytes "$file_max_bytes"
    --row-group-rows "$row_group_rows"
    --code-revision "$(git -C "$repo_root" rev-parse HEAD)"
    --watermark-time "2026-08-05T00:00:00Z"
  )
  if [[ "$backfill_source" == "fixture" ]]; then
    command+=(--fixtures "$fixture_manifest")
  fi
  if [[ -n "$time_bin" ]]; then
    "$time_bin" -f 'max_rss_kib=%M\nuser_seconds=%U\nsystem_seconds=%S' -o "$worker_resource" "${command[@]}" >"$worker_summary" &
  else
    "${command[@]}" >"$worker_summary" &
  fi
  pids+=("$!")
  worker_ids+=("$worker_id")
done

failed=0
for index in "${!pids[@]}"; do
  if ! wait "${pids[$index]}"; then
    echo "${worker_ids[$index]} failed; evidence retained at $runtime_dir" >&2
    failed=1
  fi
done
if (( failed != 0 )); then
  exit 1
fi
completed_ns="$(date +%s%N)"
wall_seconds="$(awk -v start="$started_ns" -v end="$completed_ns" 'BEGIN { printf "%.9f", (end-start)/1000000000 }')"

for worker_summary in "$evidence_dir"/worker-*.summary.json; do
  worker_id="$(basename "$worker_summary" .summary.json)"
  worker_resource="$evidence_dir/$worker_id.resource.txt"
  max_rss_kib=0
  if [[ -f "$worker_resource" ]]; then
    max_rss_kib="$(awk -F= '$1 == "max_rss_kib" { print $2 }' "$worker_resource")"
    max_rss_kib="${max_rss_kib:-0}"
  fi
  job_evidence="$evidence_dir/$worker_id.job.manifest.json"
  result_evidence="$evidence_dir/$worker_id.shard-result.manifest.json"
  cp "$outputs_dir/$worker_id/job.manifest.json" "$job_evidence"
  cp "$outputs_dir/$worker_id/shard-result.manifest.json" "$result_evidence"
  enriched_summary="$worker_summary.enriched"
  jq \
    --argjson max_rss_kib "$max_rss_kib" \
    --arg job_manifest "$job_evidence" \
    --arg result_manifest "$result_evidence" \
    '. + {max_rss_kib: $max_rss_kib, job_manifest: $job_manifest, result_manifest: $result_manifest}' \
    "$worker_summary" >"$enriched_summary"
  mv "$enriched_summary" "$worker_summary"
done

jq -s \
  --argjson workers "$concurrency" \
  --argjson wall_seconds "$wall_seconds" \
  --arg source "$backfill_source" \
  --arg writer "$writer_mode" \
  --arg compression "$compression" \
  --arg fixture_manifest "$fixture_manifest" \
  '
    {
      workers: $workers,
      source: $source,
      writer: $writer,
      compression: $compression,
      fixture_manifest: $fixture_manifest,
      wall_seconds: $wall_seconds,
      ledgers: (map(.ledgers) | add),
      input_bytes: (map(.input_bytes) | add),
      bronze_rows: (map(.bronze_rows) | add),
      output_rows: (map(.output_rows) | add),
      parquet_files: (map(.parquet_files) | add),
      parquet_bytes: (map(.parquet_bytes) | add),
      peak_batch_bytes: (map(.peak_batch_bytes) | max),
      peak_batch_bronze_rows: (map(.peak_batch_bronze_rows) | max),
      peak_worker_rss_kib: (map(.max_rss_kib) | max),
      critical_worker_staging_seconds: (map(.staging_seconds) | max),
      critical_worker_export_seconds: (map(.export_seconds) | max),
      critical_worker_setup_seconds: (map(.setup_seconds) | max),
      critical_worker_source_seconds: (map(.source_seconds) | max),
      critical_worker_digest_seconds: (map(.digest_seconds) | max),
      critical_worker_decode_seconds: (map(.decode_seconds) | max),
      critical_worker_extraction_seconds: (map(.extraction_seconds) | max),
      critical_worker_raw_view_seconds: (map(.raw_view_seconds) | max),
      critical_worker_raw_decode_seconds: (map(.raw_decode_seconds) | max),
      critical_worker_raw_extract_seconds: (map(.raw_extract_seconds) | max),
      critical_worker_raw_pin_seconds: (map(.raw_pin_seconds) | max),
      critical_worker_raw_envelope_seconds: (map(.raw_envelope_seconds) | max),
      critical_worker_raw_project_seconds: (map(.raw_project_seconds) | max),
      critical_worker_append_seconds: (map(.append_seconds) | max),
      workers_detail: .
    }
    | .aggregate_ledgers_per_second = (.ledgers / .wall_seconds)
    | .aggregate_rows_per_second = (.output_rows / .wall_seconds)
    | .aggregate_input_bytes_per_second = (.input_bytes / .wall_seconds)
  ' "$evidence_dir"/worker-*.summary.json >"$evidence_dir/aggregate.json"

if [[ "$keep_outputs" != "true" ]]; then
  expected_outputs="$runtime_dir/outputs"
  if [[ "$outputs_dir" != "$expected_outputs" || ! -f "$runtime_dir/.obsrvr-file-backfill-benchmark" ]]; then
    echo "refusing unsafe output cleanup: $outputs_dir" >&2
    exit 1
  fi
  rm -rf "$outputs_dir"
fi

cat "$evidence_dir/aggregate.json"
aggregate_lps="$(jq -r '.aggregate_ledgers_per_second' "$evidence_dir/aggregate.json")"
if ! awk -v actual="$aggregate_lps" -v minimum="$minimum_lps" 'BEGIN { exit !(actual >= minimum) }'; then
  echo "aggregate throughput $aggregate_lps ledgers/s is below required $minimum_lps" >&2
  exit 1
fi
echo "evidence: $evidence_dir" >&2
