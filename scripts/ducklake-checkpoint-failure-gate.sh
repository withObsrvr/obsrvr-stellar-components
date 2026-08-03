#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

start_ledger="${CHECKPOINT_FAILURE_GATE_START_LEDGER:-62080000}"
end_ledger="${CHECKPOINT_FAILURE_GATE_END_LEDGER:-62080005}"
runtime_dir="${CHECKPOINT_FAILURE_GATE_RUNTIME_DIR:-/tmp/obsrvr-ducklake-checkpoint-failure-gate}"
for value in "$start_ledger" "$end_ledger"; do
  [[ "$value" =~ ^[0-9]+$ ]] || { echo "failure gate ledgers must be non-negative integers" >&2; exit 2; }
done
(( end_ledger >= start_ledger )) || { echo "end ledger must be >= start ledger" >&2; exit 2; }
[[ "$runtime_dir" == /tmp/* ]] || { echo "CHECKPOINT_FAILURE_GATE_RUNTIME_DIR must be beneath /tmp" >&2; exit 2; }

token="checkpoint_failure_gate_secret"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

port_range_is_free() {
  local base="$1" port
  for port in $(seq $((base + 1)) $((base + 3))); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&- 3<&-
      return 1
    fi
  done
  return 0
}
port_base="${CHECKPOINT_FAILURE_GATE_PORT_BASE:-}"
if [[ -z "$port_base" ]]; then
  for _ in $(seq 1 100); do
    candidate=$((30000 + RANDOM % 20000))
    if port_range_is_free "$candidate"; then port_base="$candidate"; break; fi
  done
fi
[[ "$port_base" =~ ^[0-9]+$ ]] && (( port_base >= 1024 && port_base <= 65532 )) || {
  echo "could not select a valid local port range" >&2
  exit 2
}
quack_port=$((port_base + 1))
health_port=$((port_base + 2))
ingest_port=$((port_base + 3))

metric_value() {
  local metric="$1"
  curl -fsS "http://127.0.0.1:$health_port/metrics" | awk -v metric="$metric" '
    $1 == metric { value += $2; found=1 }
    END { if (!found) exit 1; printf "%.12g\n", value }
  '
}
metric_labeled_sum() {
  local metric="$1" label="$2"
  curl -fsS "http://127.0.0.1:$health_port/metrics" | awk -v metric="$metric" -v label="$label" '
    index($1, metric "{") == 1 && index($1, label) > 0 { value += $2; found=1 }
    END { if (!found) exit 1; printf "%.12g\n", value }
  '
}

start_server() {
  local metadata_attach="$1" log_file="$2"
  QUACK_TOKEN="$token" \
  QUACK_URI="quack:127.0.0.1:$quack_port" \
  QUACK_HEALTH_ADDR="127.0.0.1:$health_port" \
  QUACK_INSECURE=true \
  QUACK_DISABLE_SSL=true \
  QUACK_ENABLE_EXTERNAL_ACCESS=true \
  QUACK_DISABLED_FILESYSTEMS=none \
  DUCKDB_CHECKPOINT_THRESHOLD=1GB \
  CHECKPOINT_ENABLED=true \
  CHECKPOINT_TIMEOUT=5s \
  CHECKPOINT_ADMIN_TOKEN="$token" \
  DUCKLAKE_METADATA_ATTACH_NAME="$metadata_attach" \
  INGEST_PORT="$ingest_port" \
  DUCKLAKE_CATALOG_PATH="$runtime_dir/stellar.ducklake" \
  DUCKLAKE_DATA_PATH="$runtime_dir/data" \
  bin/quack-ducklake-server >"$log_file" 2>&1 &
  server_pid="$!"
  for _ in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:$health_port/healthz" >/dev/null 2>&1; then return 0; fi
    if ! kill -0 "$server_pid" 2>/dev/null; then cat "$log_file" >&2; return 1; fi
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

TELEMETRY_GATE_RUNTIME_DIR="$runtime_dir" \
TELEMETRY_GATE_START_LEDGER="$start_ledger" \
TELEMETRY_GATE_END_LEDGER="$end_ledger" \
TELEMETRY_GATE_MANUAL_CHECKPOINT=false \
scripts/ducklake-telemetry-gate.sh
cp "$runtime_dir/telemetry-summary.env" "$runtime_dir/telemetry-summary-before-failure.env"

start_server "missing_metadata_database" "$runtime_dir/server-injected-failure.log"
wal_before="$(metric_value obsrvr_ducklake_catalog_wal_bytes)"
failure_code="$(curl -sS -o "$runtime_dir/checkpoint-failure-response.txt" -w '%{http_code}' \
  -X POST -H "Authorization: Bearer $token" \
  "http://127.0.0.1:$health_port/admin/checkpoint")"
[[ "$failure_code" == 500 ]] || { echo "injected checkpoint status=$failure_code, want 500" >&2; exit 1; }
health_after_failure="$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$health_port/healthz")"
sleep 0.2
health_persisted="$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$health_port/healthz")"
[[ "$health_after_failure" == 503 && "$health_persisted" == 503 ]] || {
  echo "checkpoint failure health did not persist: first=$health_after_failure second=$health_persisted" >&2
  exit 1
}
error_attempts="$(metric_labeled_sum obsrvr_ducklake_checkpoint_total 'result="error",trigger="manual"')"
retry_backoffs="$(metric_labeled_sum obsrvr_ducklake_checkpoint_deferred_total 'reason="retry_backoff"')"
[[ "$error_attempts" == 3 ]] || { echo "checkpoint error attempts=$error_attempts, want 3" >&2; exit 1; }
[[ "$retry_backoffs" == 2 ]] || { echo "checkpoint retry backoffs=$retry_backoffs, want 2" >&2; exit 1; }
[[ "$(metric_value obsrvr_ducklake_checkpoint_inflight)" == 0 ]] || { echo "checkpoint inflight did not clear after failure" >&2; exit 1; }
stop_server

start_server "__ducklake_metadata_stellar_lake" "$runtime_dir/server-failure-recovery.log"
recovery_code="$(curl -sS -o "$runtime_dir/checkpoint-recovery-response.json" -w '%{http_code}' \
  -X POST -H "Authorization: Bearer $token" \
  "http://127.0.0.1:$health_port/admin/checkpoint")"
[[ "$recovery_code" == 200 ]] || { echo "recovery checkpoint status=$recovery_code, want 200" >&2; exit 1; }
health_after_recovery="$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$health_port/healthz")"
[[ "$health_after_recovery" == 200 ]] || { echo "health after recovery=$health_after_recovery, want 200" >&2; exit 1; }
successes="$(metric_labeled_sum obsrvr_ducklake_checkpoint_total 'result="success",trigger="manual"')"
[[ "$successes" == 1 ]] || { echo "recovery checkpoint successes=$successes, want 1" >&2; exit 1; }
wal_after="$(metric_value obsrvr_ducklake_catalog_wal_bytes)"
if ! awk -v before="$wal_before" -v after="$wal_after" 'BEGIN { exit !(before > 0 && after < before) }'; then
  echo "recovery checkpoint did not reduce WAL: before=$wal_before after=$wal_after" >&2
  exit 1
fi
stop_server

cat >"$runtime_dir/checkpoint-failure-summary.env" <<EOF
failure_http_status=$failure_code
failure_health_status=$health_after_failure
failure_health_persisted_status=$health_persisted
bounded_attempts=$error_attempts
retry_backoffs=$retry_backoffs
inflight_after_failure=0
recovery_http_status=$recovery_code
recovery_health_status=$health_after_recovery
recovery_successes=$successes
wal_before_recovery_bytes=$wal_before
wal_after_recovery_bytes=$wal_after
EOF
cat "$runtime_dir/checkpoint-failure-summary.env"
echo "checkpoint failure/retry gate passed; evidence retained in $runtime_dir"
