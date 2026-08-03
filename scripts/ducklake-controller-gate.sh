#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

runtime_base="${CONTROLLER_GATE_RUNTIME_BASE:-/tmp/obsrvr-ducklake-controller-gate}"
start_ledger="${CONTROLLER_GATE_START_LEDGER:-62080000}"
end_ledger="${CONTROLLER_GATE_END_LEDGER:-62080005}"
[[ "$runtime_base" == /tmp/* ]] || { echo "CONTROLLER_GATE_RUNTIME_BASE must be beneath /tmp" >&2; exit 2; }
for value in "$start_ledger" "$end_ledger"; do
  [[ "$value" =~ ^[0-9]+$ ]] || { echo "controller gate ledgers must be non-negative integers" >&2; exit 2; }
done
(( end_ledger >= start_ledger )) || { echo "end ledger must be >= start ledger" >&2; exit 2; }

run_profile() {
  local profile="$1" soft_bytes="$2" hard_bytes="$3" idle_duration="$4" expected_trigger="$5"
  local runtime_dir="${runtime_base}-${profile}"
  TELEMETRY_GATE_RUNTIME_DIR="$runtime_dir" \
  TELEMETRY_GATE_START_LEDGER="$start_ledger" \
  TELEMETRY_GATE_END_LEDGER="$end_ledger" \
  TELEMETRY_GATE_CHECKPOINT_ENABLED=true \
  TELEMETRY_GATE_CHECKPOINT_CONTROLLER_ENABLED=true \
  TELEMETRY_GATE_CHECKPOINT_SOFT_WAL_BYTES="$soft_bytes" \
  TELEMETRY_GATE_CHECKPOINT_HARD_WAL_BYTES="$hard_bytes" \
  TELEMETRY_GATE_CHECKPOINT_POLL_INTERVAL=50ms \
  TELEMETRY_GATE_CHECKPOINT_IDLE_DURATION="$idle_duration" \
  TELEMETRY_GATE_WAIT_CONTROLLER_CHECKPOINTS=1 \
  scripts/ducklake-telemetry-gate.sh

  local expected_count other_trigger
  expected_count="$(awk -v trigger="$expected_trigger" '
    index($1, "obsrvr_ducklake_checkpoint_total{") == 1 &&
    index($1, "result=\"success\"") > 0 &&
    index($1, "trigger=\"" trigger "\"") > 0 { print $2 }
  ' "$runtime_dir/server-metrics.prom")"
  [[ "$expected_count" == 1 ]] || { echo "$profile trigger $expected_trigger count=$expected_count, want 1" >&2; exit 1; }
  if [[ "$expected_trigger" == idle ]]; then other_trigger=hard_limit; else other_trigger=idle; fi
  other_count="$(awk -v trigger="$other_trigger" '
    index($1, "obsrvr_ducklake_checkpoint_total{") == 1 &&
    index($1, "result=\"success\"") > 0 &&
    index($1, "trigger=\"" trigger "\"") > 0 { print $2 }
  ' "$runtime_dir/server-metrics.prom")"
  [[ "$other_count" == 0 ]] || { echo "$profile unexpected trigger $other_trigger count=$other_count" >&2; exit 1; }
}

run_profile idle 1048576 67108864 100ms idle
run_profile hard 524288 1048576 1h hard_limit

cat >"${runtime_base}-summary.env" <<EOF
idle_checkpoint_successes=1
hard_limit_checkpoint_successes=1
ingest_errors=0
ingest_retries=0
controller_default_enabled=false
EOF
cat "${runtime_base}-summary.env"
echo "checkpoint controller gate passed; evidence retained under ${runtime_base}-{idle,hard}"
