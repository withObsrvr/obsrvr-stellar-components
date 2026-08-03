#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

target_mib="${RECOVERY_GATE_WAL_MIB:-64}"
start_ledger="${RECOVERY_GATE_START_LEDGER:-62080000}"
ledger_count="${RECOVERY_GATE_LEDGER_COUNT:-$((target_mib * 2 + 4))}"
timeout_seconds="${RECOVERY_GATE_TIMEOUT:-$((ledger_count * 2 + 180))}"
runtime_dir="${RECOVERY_GATE_RUNTIME_DIR:-/tmp/obsrvr-ducklake-crash-recovery-${target_mib}mib}"
recovery_objective_seconds="${RECOVERY_GATE_MAX_STARTUP_SECONDS:-30}"

for value in "$target_mib" "$start_ledger" "$ledger_count" "$timeout_seconds" "$recovery_objective_seconds"; do
  [[ "$value" =~ ^[0-9]+$ ]] || { echo "recovery gate settings must be non-negative integers" >&2; exit 2; }
done
(( target_mib > 0 && ledger_count > 0 && timeout_seconds > 0 && recovery_objective_seconds > 0 )) || {
  echo "recovery gate sizes/timeouts must be positive" >&2
  exit 2
}
[[ "$runtime_dir" == /tmp/* ]] || { echo "RECOVERY_GATE_RUNTIME_DIR must be beneath /tmp" >&2; exit 2; }

end_ledger=$((start_ledger + ledger_count - 1))
resume_ledger=$((end_ledger + 1))
target_bytes=$((target_mib * 1024 * 1024))
baseline_dir="${runtime_dir}-baseline"
summary="$runtime_dir/telemetry-summary.env"
network="Public Global Stellar Network ; September 2015"

TELEMETRY_GATE_RUNTIME_DIR="$baseline_dir" \
TELEMETRY_GATE_START_LEDGER="$start_ledger" \
TELEMETRY_GATE_END_LEDGER="$end_ledger" \
TELEMETRY_GATE_TIMEOUT="$timeout_seconds" \
TELEMETRY_GATE_MANUAL_CHECKPOINT=false \
TELEMETRY_GATE_SERVER_SHUTDOWN=graceful \
scripts/ducklake-telemetry-gate.sh

TELEMETRY_GATE_RUNTIME_DIR="$runtime_dir" \
TELEMETRY_GATE_START_LEDGER="$start_ledger" \
TELEMETRY_GATE_END_LEDGER="$end_ledger" \
TELEMETRY_GATE_TIMEOUT="$timeout_seconds" \
TELEMETRY_GATE_MANUAL_CHECKPOINT=false \
TELEMETRY_GATE_SERVER_SHUTDOWN=kill \
scripts/ducklake-telemetry-gate.sh

if grep -Ev '^[a-z_]+=(true|false|[0-9.]+)$' "$summary" | grep -q .; then
  echo "crash telemetry summary contained unexpected content" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$summary"
cp "$summary" "$runtime_dir/telemetry-summary-before-crash-recovery.env"
cp "$runtime_dir/server.log" "$runtime_dir/server-before-crash-recovery.log"
cp "$runtime_dir/pipeline.log" "$runtime_dir/pipeline-before-crash-recovery.log"
wal_before_recovery="$catalog_wal_metric_bytes"
if ! awk -v observed="$wal_before_recovery" -v minimum="$target_bytes" 'BEGIN { exit !(observed >= minimum) }'; then
  echo "WAL did not reach recovery candidate: observed=$wal_before_recovery minimum=$target_bytes" >&2
  exit 1
fi

scripts/ducklake-logical-fingerprint.sh \
  "$baseline_dir/stellar.ducklake" "$baseline_dir/data" \
  "$runtime_dir/baseline-logical-fingerprint.txt" "$start_ledger" "$end_ledger" >/dev/null

TELEMETRY_GATE_RUNTIME_DIR="$runtime_dir" \
TELEMETRY_GATE_RESET_RUNTIME=false \
TELEMETRY_GATE_START_LEDGER="$resume_ledger" \
TELEMETRY_GATE_END_LEDGER="$resume_ledger" \
TELEMETRY_GATE_TIMEOUT=180 \
TELEMETRY_GATE_MANUAL_CHECKPOINT=false \
TELEMETRY_GATE_SERVER_SHUTDOWN=graceful \
scripts/ducklake-telemetry-gate.sh

recovery_startup_seconds="$(awk -F= '$1 == "server_startup_seconds" { print $2 }' "$summary")"
if ! awk -v observed="$recovery_startup_seconds" -v maximum="$recovery_objective_seconds" 'BEGIN { exit !(observed < maximum) }'; then
  echo "recovery startup exceeded objective: observed=${recovery_startup_seconds}s maximum=${recovery_objective_seconds}s" >&2
  exit 1
fi

scripts/ducklake-logical-fingerprint.sh \
  "$runtime_dir/stellar.ducklake" "$runtime_dir/data" \
  "$runtime_dir/recovered-logical-fingerprint.txt" "$start_ledger" "$end_ledger" >/dev/null
if ! diff -u \
  "$runtime_dir/baseline-logical-fingerprint.txt" \
  "$runtime_dir/recovered-logical-fingerprint.txt" \
  >"$runtime_dir/recovery-parity.diff"; then
  echo "recovered logical contents differ from baseline" >&2
  cat "$runtime_dir/recovery-parity.diff" >&2
  exit 1
fi

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
expected_count=$((ledger_count + 1))
[[ "$watermark_count" == "$expected_count" ]] || { echo "watermark count=$watermark_count, want $expected_count" >&2; exit 1; }
[[ "$watermark_min" == "$start_ledger" ]] || { echo "watermark min=$watermark_min, want $start_ledger" >&2; exit 1; }
[[ "$watermark_max" == "$resume_ledger" ]] || { echo "watermark max=$watermark_max, want $resume_ledger" >&2; exit 1; }
[[ "$watermark_gaps" == 0 ]] || { echo "watermark gaps=$watermark_gaps, want 0" >&2; exit 1; }

cat >"$runtime_dir/crash-recovery-summary.env" <<EOF
target_wal_mib=$target_mib
target_wal_bytes=$target_bytes
observed_wal_before_recovery_bytes=$wal_before_recovery
recovery_startup_seconds=$recovery_startup_seconds
recovery_objective_seconds=$recovery_objective_seconds
logical_parity_differences=0
partial_ledger_commits=0
resume_ledger=$resume_ledger
watermark_count=$watermark_count
watermark_min=$watermark_min
watermark_max=$watermark_max
watermark_gaps=$watermark_gaps
EOF

cat "$runtime_dir/crash-recovery-summary.env"
echo "crash recovery gate passed; evidence retained in $runtime_dir"
