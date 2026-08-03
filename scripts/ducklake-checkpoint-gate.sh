#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

target_mib="${CHECKPOINT_GATE_WAL_MIB:-64}"
start_ledger="${CHECKPOINT_GATE_START_LEDGER:-62080000}"
for value in "$target_mib" "$start_ledger"; do
  [[ "$value" =~ ^[0-9]+$ ]] || { echo "checkpoint gate numeric settings must be non-negative integers" >&2; exit 2; }
done
# The longer retained mainnet sample produced roughly 0.56 MiB of catalog WAL
# per ledger. Two ledgers per target MiB leaves modest headroom for row-shape
# variation while keeping candidate overshoot bounded.
ledger_count="${CHECKPOINT_GATE_LEDGER_COUNT:-$((target_mib * 2 + 4))}"
timeout_seconds="${CHECKPOINT_GATE_TIMEOUT:-$((ledger_count * 2 + 180))}"
runtime_dir="${CHECKPOINT_GATE_RUNTIME_DIR:-/tmp/obsrvr-ducklake-checkpoint-gate-${target_mib}mib}"
for value in "$ledger_count" "$timeout_seconds"; do
  [[ "$value" =~ ^[0-9]+$ ]] || { echo "checkpoint gate numeric settings must be non-negative integers" >&2; exit 2; }
done
end_ledger=$((start_ledger + ledger_count - 1))
target_bytes=$((target_mib * 1024 * 1024))

(( target_mib > 0 && ledger_count > 0 && timeout_seconds > 0 )) || { echo "checkpoint gate sizes/timeouts must be positive" >&2; exit 2; }
command -v duckdb >/dev/null 2>&1 || { echo "duckdb CLI is required" >&2; exit 2; }

baseline_dir="${runtime_dir}-baseline"
baseline_fingerprint="$runtime_dir/baseline-logical-fingerprint.txt"
checkpoint_fingerprint="$runtime_dir/checkpoint-logical-fingerprint.txt"
resume_fingerprint="$runtime_dir/resume-logical-fingerprint.txt"

TELEMETRY_GATE_RUNTIME_DIR="$baseline_dir" \
TELEMETRY_GATE_START_LEDGER="$start_ledger" \
TELEMETRY_GATE_END_LEDGER="$end_ledger" \
TELEMETRY_GATE_TIMEOUT="$timeout_seconds" \
TELEMETRY_GATE_MANUAL_CHECKPOINT=false \
scripts/ducklake-telemetry-gate.sh

TELEMETRY_GATE_RUNTIME_DIR="$runtime_dir" \
TELEMETRY_GATE_START_LEDGER="$start_ledger" \
TELEMETRY_GATE_END_LEDGER="$end_ledger" \
TELEMETRY_GATE_TIMEOUT="$timeout_seconds" \
TELEMETRY_GATE_MANUAL_CHECKPOINT=true \
TELEMETRY_GATE_MIN_WAL_BYTES="$target_bytes" \
scripts/ducklake-telemetry-gate.sh

summary="$runtime_dir/telemetry-summary.env"
# The gate writes a fixed-key, numeric-only summary. Source it only after
# rejecting unexpected keys/characters.
if grep -Ev '^[a-z_]+=(true|false|[0-9.]+)$' "$summary" | grep -q .; then
  echo "telemetry summary contained unexpected content" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$summary"
cp "$summary" "$runtime_dir/telemetry-summary-checkpoint.env"
cp "$runtime_dir/server.log" "$runtime_dir/server-checkpoint.log"
cp "$runtime_dir/pipeline.log" "$runtime_dir/pipeline-checkpoint.log"

scripts/ducklake-logical-fingerprint.sh \
  "$baseline_dir/stellar.ducklake" "$baseline_dir/data" \
  "$baseline_fingerprint" "$start_ledger" "$end_ledger" >/dev/null
scripts/ducklake-logical-fingerprint.sh \
  "$runtime_dir/stellar.ducklake" "$runtime_dir/data" \
  "$checkpoint_fingerprint" "$start_ledger" "$end_ledger" >/dev/null
if ! diff -u "$baseline_fingerprint" "$checkpoint_fingerprint" >"$runtime_dir/checkpoint-parity.diff"; then
  echo "logical parity differs from the no-checkpoint baseline" >&2
  cat "$runtime_dir/checkpoint-parity.diff" >&2
  exit 1
fi

resume_ledger=$((end_ledger + 1))
TELEMETRY_GATE_RUNTIME_DIR="$runtime_dir" \
TELEMETRY_GATE_RESET_RUNTIME=false \
TELEMETRY_GATE_START_LEDGER="$resume_ledger" \
TELEMETRY_GATE_END_LEDGER="$resume_ledger" \
TELEMETRY_GATE_TIMEOUT=180 \
TELEMETRY_GATE_MANUAL_CHECKPOINT=false \
scripts/ducklake-telemetry-gate.sh
resume_startup_seconds="$(awk -F= '$1 == "server_startup_seconds" { print $2 }' "$summary")"
scripts/ducklake-logical-fingerprint.sh \
  "$runtime_dir/stellar.ducklake" "$runtime_dir/data" \
  "$resume_fingerprint" "$start_ledger" "$end_ledger" >/dev/null
if ! diff -u "$baseline_fingerprint" "$resume_fingerprint" >"$runtime_dir/resume-parity.diff"; then
  echo "original logical contents changed after resume" >&2
  cat "$runtime_dir/resume-parity.diff" >&2
  exit 1
fi

network="Public Global Stellar Network ; September 2015"
read -r watermark_count watermark_min watermark_max watermark_gaps < <(
  duckdb :memory: -csv -noheader -c "
    INSTALL ducklake;
    LOAD ducklake;
    ATTACH 'ducklake:$runtime_dir/stellar.ducklake' AS stellar_lake
      (DATA_PATH '$runtime_dir/data');
    WITH bounds AS (
      SELECT count(*) AS count_value,
             min(ledger_sequence) AS min_value,
             max(ledger_sequence) AS max_value
      FROM stellar_lake.ingest_watermarks
      WHERE network_passphrase = '$network'
    ), gaps AS (
      SELECT count(*) AS gap_count
      FROM bounds,
           range(CAST(min_value AS BIGINT), CAST(max_value AS BIGINT) + 1) expected(sequence)
      LEFT JOIN stellar_lake.ingest_watermarks actual
        ON actual.network_passphrase = '$network'
       AND actual.ledger_sequence = expected.sequence
      WHERE actual.ledger_sequence IS NULL
    )
    SELECT count_value, min_value, max_value, gap_count FROM bounds, gaps;" \
  | tr ',' ' '
)

expected_final_count=$((ledger_count + 1))
[[ "$watermark_count" == "$expected_final_count" ]] || { echo "watermark count=$watermark_count, want $expected_final_count" >&2; exit 1; }
[[ "$watermark_min" == "$start_ledger" ]] || { echo "watermark min=$watermark_min, want $start_ledger" >&2; exit 1; }
[[ "$watermark_max" == "$resume_ledger" ]] || { echo "watermark max=$watermark_max, want $resume_ledger" >&2; exit 1; }
[[ "$watermark_gaps" == 0 ]] || { echo "watermark gaps=$watermark_gaps, want 0" >&2; exit 1; }

cat >"$runtime_dir/checkpoint-gate-summary.env" <<EOF
target_wal_mib=$target_mib
target_wal_bytes=$target_bytes
observed_wal_before_bytes=$manual_checkpoint_wal_before_bytes
observed_wal_after_bytes=$manual_checkpoint_wal_after_bytes
checkpoint_duration_seconds=$manual_checkpoint_duration_seconds
logical_parity_differences=0
resume_ledger=$resume_ledger
resume_startup_seconds=$resume_startup_seconds
watermark_count=$watermark_count
watermark_min=$watermark_min
watermark_max=$watermark_max
watermark_gaps=$watermark_gaps
EOF

cat "$runtime_dir/checkpoint-gate-summary.env"
echo "explicit checkpoint candidate gate passed; evidence retained in $runtime_dir"
