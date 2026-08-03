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

[[ "$watermark_count" == "$ledger_count" ]] || { echo "watermark count=$watermark_count, want $ledger_count" >&2; exit 1; }
[[ "$watermark_min" == "$start_ledger" ]] || { echo "watermark min=$watermark_min, want $start_ledger" >&2; exit 1; }
[[ "$watermark_max" == "$end_ledger" ]] || { echo "watermark max=$watermark_max, want $end_ledger" >&2; exit 1; }
[[ "$watermark_gaps" == 0 ]] || { echo "watermark gaps=$watermark_gaps, want 0" >&2; exit 1; }

cat >"$runtime_dir/checkpoint-gate-summary.env" <<EOF
target_wal_mib=$target_mib
target_wal_bytes=$target_bytes
observed_wal_before_bytes=$manual_checkpoint_wal_before_bytes
observed_wal_after_bytes=$manual_checkpoint_wal_after_bytes
checkpoint_duration_seconds=$manual_checkpoint_duration_seconds
watermark_count=$watermark_count
watermark_min=$watermark_min
watermark_max=$watermark_max
watermark_gaps=$watermark_gaps
EOF

cat "$runtime_dir/checkpoint-gate-summary.env"
echo "explicit checkpoint candidate gate passed; evidence retained in $runtime_dir"
