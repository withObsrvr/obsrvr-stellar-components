#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

target_mib="${KILL_CHECKPOINT_GATE_WAL_MIB:-64}"
start_ledger="${KILL_CHECKPOINT_GATE_START_LEDGER:-62080000}"
ledger_count="${KILL_CHECKPOINT_GATE_LEDGER_COUNT:-$((target_mib * 2 + 4))}"
timeout_seconds="${KILL_CHECKPOINT_GATE_TIMEOUT:-$((ledger_count * 2 + 180))}"
runtime_dir="${KILL_CHECKPOINT_GATE_RUNTIME_DIR:-/tmp/obsrvr-ducklake-kill-checkpoint-${target_mib}mib}"
recovery_objective_seconds="${KILL_CHECKPOINT_GATE_MAX_STARTUP_SECONDS:-30}"
for value in "$target_mib" "$start_ledger" "$ledger_count" "$timeout_seconds" "$recovery_objective_seconds"; do
  [[ "$value" =~ ^[0-9]+$ ]] || { echo "kill-checkpoint gate settings must be non-negative integers" >&2; exit 2; }
done
(( target_mib > 0 && ledger_count > 0 && timeout_seconds > 0 && recovery_objective_seconds > 0 )) || {
  echo "kill-checkpoint gate sizes/timeouts must be positive" >&2
  exit 2
}
[[ "$runtime_dir" == /tmp/* ]] || { echo "KILL_CHECKPOINT_GATE_RUNTIME_DIR must be beneath /tmp" >&2; exit 2; }

end_ledger=$((start_ledger + ledger_count - 1))
resume_ledger=$((end_ledger + 1))
target_bytes=$((target_mib * 1024 * 1024))
baseline_dir="${runtime_dir}-baseline"
summary="$runtime_dir/telemetry-summary.env"
# Must match the telemetry gate's isolated fixture token while the held server
# remains owned by that harness.
token="telemetry_gate_secret"
network="Public Global Stellar Network ; September 2015"
server_pid=""
checkpoint_pid=""
telemetry_pid=""

cleanup() {
  if [[ -n "$telemetry_pid" ]] && kill -0 "$telemetry_pid" 2>/dev/null; then
    kill "$telemetry_pid" 2>/dev/null || true
    wait "$telemetry_pid" 2>/dev/null || true
  fi
  if [[ -n "$checkpoint_pid" ]] && kill -0 "$checkpoint_pid" 2>/dev/null; then
    kill "$checkpoint_pid" 2>/dev/null || true
    wait "$checkpoint_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

port_range_is_free() {
  local base="$1" port
  for port in $(seq $((base + 1)) $((base + 10))); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&- 3<&-
      return 1
    fi
  done
  return 0
}
port_base="${KILL_CHECKPOINT_GATE_PORT_BASE:-}"
if [[ -z "$port_base" ]]; then
  for _ in $(seq 1 100); do
    candidate=$((30000 + RANDOM % 20000))
    if port_range_is_free "$candidate"; then port_base="$candidate"; break; fi
  done
fi
[[ "$port_base" =~ ^[0-9]+$ ]] && (( port_base >= 1024 && port_base <= 65525 )) || {
  echo "could not select a valid local port range" >&2
  exit 2
}
port_range_is_free "$port_base" || { echo "selected local port range is occupied" >&2; exit 2; }
quack_port=$((port_base + 1))
health_port=$((port_base + 2))
ingest_port=$((port_base + 3))

metric_value_from_url() {
  local metric="$1"
  curl -fsS "http://127.0.0.1:$health_port/metrics" | awk -v metric="$metric" '
    $1 == metric { value += $2; found=1 }
    END { if (!found) exit 1; printf "%.12g\n", value }
  '
}

start_server() {
  local log_file="$1"
  local start_ns ports_released=false
  for _ in $(seq 1 500); do
    if port_range_is_free "$port_base"; then
      ports_released=true
      break
    fi
    sleep 0.01
  done
  [[ "$ports_released" == true ]] || { echo "server ports were not released after process exit" >&2; return 1; }
  start_ns="$(date +%s%N)"
  QUACK_TOKEN="$token" \
  QUACK_URI="quack:127.0.0.1:$quack_port" \
  QUACK_HEALTH_ADDR="127.0.0.1:$health_port" \
  QUACK_INSECURE=true \
  QUACK_DISABLE_SSL=true \
  QUACK_ENABLE_EXTERNAL_ACCESS=true \
  QUACK_DISABLED_FILESYSTEMS=none \
  DUCKDB_CHECKPOINT_THRESHOLD=1GB \
  DUCKLAKE_INLINE_ROW_LIMIT=256 \
  CHECKPOINT_ENABLED=true \
  CHECKPOINT_TIMEOUT=30s \
  CHECKPOINT_ADMIN_TOKEN="$token" \
  INGEST_PORT="$ingest_port" \
  DUCKLAKE_CATALOG_PATH="$runtime_dir/stellar.ducklake" \
  DUCKLAKE_DATA_PATH="$runtime_dir/data" \
  bin/quack-ducklake-server >"$log_file" 2>&1 &
  server_pid="$!"
  for _ in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:$health_port/healthz" >/dev/null 2>&1; then
      local healthy_ns
      healthy_ns="$(date +%s%N)"
      server_startup_seconds="$(awk -v start="$start_ns" -v healthy="$healthy_ns" 'BEGIN { printf "%.6f", (healthy-start)/1000000000 }')"
      return 0
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      cat "$log_file" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "server did not become healthy" >&2
  return 1
}

stop_server() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid"
    wait "$server_pid" 2>/dev/null || true
  fi
  server_pid=""
}

TELEMETRY_GATE_RUNTIME_DIR="$baseline_dir" \
TELEMETRY_GATE_START_LEDGER="$start_ledger" \
TELEMETRY_GATE_END_LEDGER="$end_ledger" \
TELEMETRY_GATE_TIMEOUT="$timeout_seconds" \
TELEMETRY_GATE_MANUAL_CHECKPOINT=false \
scripts/ducklake-telemetry-gate.sh

rm -rf "$runtime_dir"
mkdir -p "$runtime_dir"
TELEMETRY_GATE_RUNTIME_DIR="$runtime_dir" \
TELEMETRY_GATE_RESET_RUNTIME=false \
TELEMETRY_GATE_PORT_BASE="$port_base" \
TELEMETRY_GATE_START_LEDGER="$start_ledger" \
TELEMETRY_GATE_END_LEDGER="$end_ledger" \
TELEMETRY_GATE_TIMEOUT="$timeout_seconds" \
TELEMETRY_GATE_MANUAL_CHECKPOINT=false \
TELEMETRY_GATE_CHECKPOINT_ENABLED=true \
TELEMETRY_GATE_HOLD_AFTER_INGEST=true \
scripts/ducklake-telemetry-gate.sh >"$runtime_dir/held-telemetry.log" 2>&1 &
telemetry_pid="$!"
ready_file="$runtime_dir/after-ingest-ready.env"
for _ in $(seq 1 "$timeout_seconds"); do
  [[ -f "$ready_file" ]] && break
  if ! kill -0 "$telemetry_pid" 2>/dev/null; then
    wait "$telemetry_pid" 2>/dev/null || true
    telemetry_pid=""
    cat "$runtime_dir/held-telemetry.log" >&2
    exit 1
  fi
  sleep 1
done
[[ -f "$ready_file" ]] || { echo "held telemetry gate did not reach post-ingest synchronization" >&2; exit 1; }
if grep -Ev '^(server_pid|health_port|server_startup_seconds)=[0-9.]+$' "$ready_file" | grep -q .; then
  echo "held telemetry synchronization file contained unexpected content" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$ready_file"
initial_startup_seconds="$server_startup_seconds"
wal_before_checkpoint="$(metric_value_from_url obsrvr_ducklake_catalog_wal_bytes)"
if ! awk -v observed="$wal_before_checkpoint" -v minimum="$target_bytes" 'BEGIN { exit !(observed >= minimum) }'; then
  echo "WAL did not reach kill candidate: observed=$wal_before_checkpoint minimum=$target_bytes" >&2
  exit 1
fi
cp "$runtime_dir/server.log" "$runtime_dir/server-before-kill.log"
cp "$runtime_dir/pipeline.log" "$runtime_dir/pipeline-before-kill.log"
scripts/ducklake-logical-fingerprint.sh \
  "$baseline_dir/stellar.ducklake" "$baseline_dir/data" \
  "$runtime_dir/baseline-logical-fingerprint.txt" "$start_ledger" "$end_ledger" >/dev/null
: >"$runtime_dir/interrupted-checkpoint-response.json"
set +e
curl -fsS -X POST -H "Authorization: Bearer $token" \
  "http://127.0.0.1:$health_port/admin/checkpoint" \
  >"$runtime_dir/interrupted-checkpoint-response.json" \
  2>"$runtime_dir/interrupted-checkpoint-curl.log" &
checkpoint_pid="$!"
set -e
checkpoint_started=false
for _ in $(seq 1 2000); do
  inflight="$(metric_value_from_url obsrvr_ducklake_checkpoint_inflight 2>/dev/null || echo 0)"
  if [[ "$inflight" == 1 ]]; then
    checkpoint_started=true
    break
  fi
  if ! kill -0 "$checkpoint_pid" 2>/dev/null; then break; fi
done
if [[ "$checkpoint_started" != true ]]; then
  wait "$checkpoint_pid" 2>/dev/null || true
  checkpoint_pid=""
  echo "did not observe checkpoint inflight before request completed" >&2
  exit 1
fi
kill -KILL "$server_pid"
for _ in $(seq 1 100); do
  kill -0 "$server_pid" 2>/dev/null || break
  sleep 0.01
done
set +e
wait "$checkpoint_pid"
checkpoint_curl_status="$?"
set -e
checkpoint_pid=""
set +e
wait "$telemetry_pid"
telemetry_status="$?"
set -e
telemetry_pid=""
server_pid=""
[[ "$telemetry_status" == 0 ]] || { echo "held telemetry gate failed during synchronized kill" >&2; exit 1; }
if [[ "$checkpoint_curl_status" == 0 ]] || grep -q '"result":"success"' "$runtime_dir/interrupted-checkpoint-response.json"; then
  echo "checkpoint completed before SIGKILL; interruption was not proven" >&2
  exit 1
fi

start_server "$runtime_dir/server-after-checkpoint-kill.log"
recovery_startup_seconds="$server_startup_seconds"
if ! awk -v observed="$recovery_startup_seconds" -v maximum="$recovery_objective_seconds" 'BEGIN { exit !(observed < maximum) }'; then
  echo "kill-during-checkpoint recovery exceeded objective: observed=${recovery_startup_seconds}s maximum=${recovery_objective_seconds}s" >&2
  exit 1
fi
wal_after_recovery="$(metric_value_from_url obsrvr_ducklake_catalog_wal_bytes)"
[[ "$(metric_value_from_url obsrvr_ducklake_checkpoint_inflight)" == 0 ]] || {
  echo "checkpoint inflight gauge did not reset after process recovery" >&2
  exit 1
}
stop_server

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
  "$runtime_dir/recovered-logical-fingerprint.txt" "$start_ledger" "$end_ledger" >/dev/null
if ! diff -u \
  "$runtime_dir/baseline-logical-fingerprint.txt" \
  "$runtime_dir/recovered-logical-fingerprint.txt" \
  >"$runtime_dir/kill-checkpoint-parity.diff"; then
  echo "logical contents differ after kill-during-checkpoint recovery" >&2
  cat "$runtime_dir/kill-checkpoint-parity.diff" >&2
  exit 1
fi

read -r watermark_count watermark_min watermark_max watermark_gaps < <(
  duckdb :memory: -csv -noheader -c "
    INSTALL ducklake; LOAD ducklake;
    ATTACH 'ducklake:$runtime_dir/stellar.ducklake' AS stellar_lake (DATA_PATH '$runtime_dir/data');
    WITH bounds AS (
      SELECT count(*) count_value, min(ledger_sequence) min_value, max(ledger_sequence) max_value
      FROM stellar_lake.ingest_watermarks WHERE network_passphrase='$network'
    ), gaps AS (
      SELECT count(*) gap_count FROM bounds,
        range(CAST(min_value AS BIGINT), CAST(max_value AS BIGINT)+1) expected(sequence)
      LEFT JOIN stellar_lake.ingest_watermarks actual
        ON actual.network_passphrase='$network' AND actual.ledger_sequence=expected.sequence
      WHERE actual.ledger_sequence IS NULL
    ) SELECT count_value,min_value,max_value,gap_count FROM bounds,gaps;" | tr ',' ' '
)
expected_count=$((ledger_count + 1))
[[ "$watermark_count" == "$expected_count" && "$watermark_min" == "$start_ledger" && "$watermark_max" == "$resume_ledger" && "$watermark_gaps" == 0 ]] || {
  echo "watermark recovery mismatch: count=$watermark_count min=$watermark_min max=$watermark_max gaps=$watermark_gaps" >&2
  exit 1
}

cat >"$runtime_dir/kill-checkpoint-summary.env" <<EOF
target_wal_mib=$target_mib
target_wal_bytes=$target_bytes
observed_wal_before_checkpoint_bytes=$wal_before_checkpoint
observed_wal_after_recovery_bytes=$wal_after_recovery
checkpoint_inflight_observed=true
checkpoint_request_interrupted=true
initial_startup_seconds=$initial_startup_seconds
recovery_startup_seconds=$recovery_startup_seconds
recovery_objective_seconds=$recovery_objective_seconds
logical_parity_differences=0
partial_ledger_commits=0
resume_ledger=$resume_ledger
resume_startup_seconds=$resume_startup_seconds
watermark_count=$watermark_count
watermark_min=$watermark_min
watermark_max=$watermark_max
watermark_gaps=$watermark_gaps
EOF
cat "$runtime_dir/kill-checkpoint-summary.env"
echo "kill-during-checkpoint gate passed; evidence retained in $runtime_dir"
