#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

runtime_dir="$(realpath -m "${TELEMETRY_GATE_RUNTIME_DIR:-/tmp/obsrvr-ducklake-telemetry-gate}")"
if [[ "$runtime_dir" != /tmp/* ]]; then
  echo "TELEMETRY_GATE_RUNTIME_DIR must resolve beneath /tmp" >&2
  exit 2
fi
start_ledger="${TELEMETRY_GATE_START_LEDGER:-62080000}"
end_ledger="${TELEMETRY_GATE_END_LEDGER:-62080029}"
timeout_seconds="${TELEMETRY_GATE_TIMEOUT:-180}"
manual_checkpoint="${TELEMETRY_GATE_MANUAL_CHECKPOINT:-false}"
checkpoint_enabled="${TELEMETRY_GATE_CHECKPOINT_ENABLED:-$manual_checkpoint}"
minimum_wal_bytes="${TELEMETRY_GATE_MIN_WAL_BYTES:-0}"
reset_runtime="${TELEMETRY_GATE_RESET_RUNTIME:-true}"
server_shutdown="${TELEMETRY_GATE_SERVER_SHUTDOWN:-graceful}"
hold_after_ingest="${TELEMETRY_GATE_HOLD_AFTER_INGEST:-false}"
network="${TELEMETRY_GATE_NETWORK:-Public Global Stellar Network ; September 2015}"
# This is an isolated local fixture token, not an operator credential. Keeping
# it fixed prevents an ambient QUACK_TOKEN from being written into evidence.
token="telemetry_gate_secret"
port_base="${TELEMETRY_GATE_PORT_BASE:-}"

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

if [[ -n "$port_base" ]]; then
  if [[ ! "$port_base" =~ ^[0-9]+$ ]] || (( port_base < 1024 || port_base > 65525 )); then
    echo "TELEMETRY_GATE_PORT_BASE must be an integer from 1024 through 65525" >&2
    exit 2
  fi
  if ! port_range_is_free "$port_base"; then
    echo "TELEMETRY_GATE_PORT_BASE range $((port_base + 1))-$((port_base + 10)) is not free" >&2
    exit 2
  fi
else
  for _ in $(seq 1 100); do
    candidate=$((20000 + RANDOM % 25000))
    if port_range_is_free "$candidate"; then
      port_base="$candidate"
      break
    fi
  done
  [[ -n "$port_base" ]] || { echo "could not find a free local port range" >&2; exit 2; }
fi

quack_port=$((port_base + 1))
server_health_port=$((port_base + 2))
ingest_port=$((port_base + 3))
control_port=$((port_base + 4))
raw_grpc_port=$((port_base + 5))
processor_grpc_port=$((port_base + 6))
sink_grpc_port=$((port_base + 7))
raw_health_port=$((port_base + 8))
processor_health_port=$((port_base + 9))
sink_health_port=$((port_base + 10))
catalog_path="$runtime_dir/stellar.ducklake"
data_path="$runtime_dir/data"
pipeline_path="$runtime_dir/pipeline.yaml"
server_log="$runtime_dir/server.log"
pipeline_log="$runtime_dir/pipeline.log"
server_metrics="$runtime_dir/server-metrics.prom"
sink_metrics="$runtime_dir/sink-metrics.prom"
summary="$runtime_dir/telemetry-summary.env"
expected=$((end_ledger - start_ledger + 1))

if [[ ! "$minimum_wal_bytes" =~ ^[0-9]+$ ]]; then
  echo "TELEMETRY_GATE_MIN_WAL_BYTES must be a non-negative integer" >&2
  exit 2
fi
if [[ "$manual_checkpoint" != "true" && "$manual_checkpoint" != "false" ]]; then
  echo "TELEMETRY_GATE_MANUAL_CHECKPOINT must be true or false" >&2
  exit 2
fi
if [[ "$checkpoint_enabled" != "true" && "$checkpoint_enabled" != "false" ]]; then
  echo "TELEMETRY_GATE_CHECKPOINT_ENABLED must be true or false" >&2
  exit 2
fi
if [[ "$reset_runtime" != "true" && "$reset_runtime" != "false" ]]; then
  echo "TELEMETRY_GATE_RESET_RUNTIME must be true or false" >&2
  exit 2
fi
if [[ "$hold_after_ingest" != "true" && "$hold_after_ingest" != "false" ]]; then
  echo "TELEMETRY_GATE_HOLD_AFTER_INGEST must be true or false" >&2
  exit 2
fi
if [[ "$server_shutdown" != "graceful" && "$server_shutdown" != "kill" ]]; then
  echo "TELEMETRY_GATE_SERVER_SHUTDOWN must be graceful or kill" >&2
  exit 2
fi
if (( expected <= 0 )); then
  echo "TELEMETRY_GATE_END_LEDGER must be >= TELEMETRY_GATE_START_LEDGER" >&2
  exit 2
fi
if [[ "$catalog_path" != "$runtime_dir"/* || "$data_path" != "$runtime_dir"/* ]]; then
  echo "refusing to clean paths outside TELEMETRY_GATE_RUNTIME_DIR" >&2
  exit 2
fi
for command in go curl awk grep stat flowctl; do
  command -v "$command" >/dev/null 2>&1 || { echo "$command is required" >&2; exit 2; }
done
[[ -x bin/raw-ledger-source ]] || { echo "bin/raw-ledger-source is required" >&2; exit 2; }

if [[ "$reset_runtime" == "true" ]]; then
  rm -rf "$runtime_dir"
fi
mkdir -p "$runtime_dir" "$data_path" bin

cat >"$pipeline_path" <<YAML
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: ducklake-telemetry-gate
  description: Real mainnet ledger sample for ingest telemetry reconciliation.

spec:
  driver: process

  sources:
    - id: raw-ledger-source
      type: source
      command: ["./bin/raw-ledger-source"]
      env:
        FLOWCTL_COMPONENT_ID: "raw-ledger-source"
        BACKEND_TYPE: "ARCHIVE"
        ARCHIVE_STORAGE_TYPE: "S3"
        ARCHIVE_BUCKET_NAME: "aws-public-blockchain"
        ARCHIVE_PATH: "v1.1/stellar/ledgers/pubnet"
        AWS_REGION: "us-east-2"
        LEDGERS_PER_FILE: "1"
        FILES_PER_PARTITION: "64000"
        NETWORK_PASSPHRASE: "$network"
        START_LEDGER: "$start_ledger"
        END_LEDGER: "$end_ledger"
        GRPC_PORT: "$raw_grpc_port"
        HEALTH_PORT: "$raw_health_port"
        FLOWCTL_ENDPOINT: "127.0.0.1:$control_port"

  processors:
    - id: stellar-ledger-processor
      type: processor
      command: ["./bin/stellar-ledger-processor"]
      inputs: ["raw-ledger-source"]
      env:
        COMPONENT_ID: "stellar-ledger-processor"
        NETWORK_PASSPHRASE: "$network"
        PORT: ":$processor_grpc_port"
        HEALTH_PORT: "$processor_health_port"
        ENABLE_FLOWCTL: "true"
        FLOWCTL_ENDPOINT: "127.0.0.1:$control_port"

  sinks:
    - id: ducklake-sink
      type: sink
      command: ["./bin/ducklake-sink"]
      inputs: ["stellar-ledger-processor"]
      env:
        COMPONENT_ID: "ducklake-sink"
        DUCKLAKE_MODE: "ingest-rpc"
        INGEST_ENDPOINT: "127.0.0.1:$ingest_port"
        QUACK_TOKEN: "$token"
        PORT: ":$sink_grpc_port"
        HEALTH_PORT: "$sink_health_port"
        ENABLE_FLOWCTL: "true"
        FLOWCTL_ENDPOINT: "127.0.0.1:$control_port"
YAML

server_pid=""
pipeline_pid=""
cleanup() {
  if [[ -n "$pipeline_pid" ]] && kill -0 "$pipeline_pid" 2>/dev/null; then
    kill -TERM -- "-$pipeline_pid" 2>/dev/null || kill "$pipeline_pid" 2>/dev/null || true
    for _ in $(seq 1 10); do
      kill -0 "$pipeline_pid" 2>/dev/null || break
      sleep 1
    done
    kill -KILL -- "-$pipeline_pid" 2>/dev/null || true
    wait "$pipeline_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    if [[ "$server_shutdown" == "kill" ]]; then
      kill -KILL "$server_pid" 2>/dev/null || true
    else
      kill "$server_pid" 2>/dev/null || true
    fi
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

metric_value() {
  local file="$1" metric="$2"
  awk -v metric="$metric" '
    $1 == metric { value += $2; found = 1 }
    END { if (!found) exit 1; printf "%.12g\n", value }
  ' "$file"
}

metric_labeled_sum() {
  local file="$1" metric="$2" label="$3"
  awk -v metric="$metric" -v label="$label" '
    index($1, metric "{") == 1 && index($1, label) > 0 { value += $2; found = 1 }
    END { if (!found) exit 1; printf "%.12g\n", value }
  ' "$file"
}

file_size_or_zero() {
  if [[ -f "$1" ]]; then
    stat -c %s "$1"
  else
    echo 0
  fi
}

assert_equal() {
  local name="$1" got="$2" want="$3"
  if [[ "$got" != "$want" ]]; then
    echo "$name=$got, want $want" >&2
    exit 1
  fi
}

echo "building telemetry-gate components"
CGO_ENABLED="${CGO_ENABLED:-1}" go build -o bin/quack-ducklake-server ./components/quack-ducklake-server/cmd/component
CGO_ENABLED="${CGO_ENABLED:-1}" go build -o bin/ducklake-sink ./components/ducklake-sink/cmd/component
CGO_ENABLED="${CGO_ENABLED:-1}" go build -o bin/stellar-ledger-processor ./components/stellar-ledger-processor/cmd/component

server_start_ns="$(date +%s%N)"
QUACK_TOKEN="$token" \
QUACK_URI="quack:127.0.0.1:$quack_port" \
QUACK_HEALTH_ADDR="127.0.0.1:$server_health_port" \
QUACK_INSECURE=true \
QUACK_DISABLE_SSL=true \
QUACK_ENABLE_EXTERNAL_ACCESS=true \
QUACK_DISABLED_FILESYSTEMS=none \
DUCKDB_CHECKPOINT_THRESHOLD=1GB \
DUCKLAKE_INLINE_ROW_LIMIT=256 \
CHECKPOINT_ENABLED="$checkpoint_enabled" \
CHECKPOINT_TIMEOUT=30s \
CHECKPOINT_ADMIN_TOKEN="$token" \
INGEST_PORT="$ingest_port" \
DUCKLAKE_CATALOG_PATH="$catalog_path" \
DUCKLAKE_DATA_PATH="$data_path" \
bin/quack-ducklake-server >"$server_log" 2>&1 &
server_pid="$!"

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:$server_health_port/healthz" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$server_log" >&2
    exit 1
  fi
  sleep 1
done
curl -fsS "http://127.0.0.1:$server_health_port/healthz" >/dev/null
server_healthy_ns="$(date +%s%N)"
server_startup_seconds="$(awk -v start="$server_start_ns" -v healthy="$server_healthy_ns" 'BEGIN { printf "%.6f", (healthy - start) / 1000000000 }')"

setsid flowctl run \
  --no-persistence \
  --control-plane-port "$control_port" \
  --log-dir "$runtime_dir/flowctl-logs" \
  "$pipeline_path" >"$pipeline_log" 2>&1 &
pipeline_pid="$!"

completed=false
for _ in $(seq 1 "$timeout_seconds"); do
  if ! kill -0 "$pipeline_pid" 2>/dev/null; then
    echo "flowctl exited before telemetry gate completed" >&2
    tail -n 160 "$pipeline_log" >&2 || true
    exit 1
  fi
  if curl -fsS "http://127.0.0.1:$server_health_port/metrics" >"$server_metrics.tmp" 2>/dev/null; then
    last_ledger="$(metric_value "$server_metrics.tmp" obsrvr_ducklake_ingest_last_ledger || echo 0)"
    if [[ "$last_ledger" == "$end_ledger" ]] && curl -fsS "http://127.0.0.1:$sink_health_port/metrics" >"$sink_metrics.tmp" 2>/dev/null; then
      sink_count="$(metric_value "$sink_metrics.tmp" obsrvr_ducklake_ingest_rpc_round_trip_seconds_count || echo 0)"
      if [[ "$sink_count" == "$expected" ]]; then
        mv "$server_metrics.tmp" "$server_metrics"
        mv "$sink_metrics.tmp" "$sink_metrics"
        completed=true
        break
      fi
    fi
  fi
  sleep 1
done
if [[ "$completed" != true ]]; then
  echo "telemetry gate did not acknowledge $expected ledgers within ${timeout_seconds}s" >&2
  tail -n 160 "$pipeline_log" >&2 || true
  tail -n 160 "$server_log" >&2 || true
  exit 1
fi

if [[ "$hold_after_ingest" == true ]]; then
  cat >"$runtime_dir/after-ingest-ready.env" <<EOF
server_pid=$server_pid
server_startup_seconds=$server_startup_seconds
health_port=$server_health_port
EOF
  while [[ ! -e "$runtime_dir/after-ingest-continue" ]]; do
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "held telemetry server exited; ending synchronized gate"
      exit 0
    fi
    sleep 0.01
  done
fi

checkpoint_triggered=false
checkpoint_wal_before=0
checkpoint_wal_after=0
checkpoint_success=0
checkpoint_errors=0
checkpoint_duration_seconds=0
checkpoint_last_success=0
if [[ "$manual_checkpoint" == "true" ]]; then
  mv "$server_metrics" "$runtime_dir/server-metrics-before-checkpoint.prom"
  checkpoint_wal_before="$(metric_value "$runtime_dir/server-metrics-before-checkpoint.prom" obsrvr_ducklake_catalog_wal_bytes)"
  if ! awk -v observed="$checkpoint_wal_before" -v minimum="$minimum_wal_bytes" 'BEGIN { exit !(observed >= minimum) }'; then
    echo "WAL did not reach required size: observed=$checkpoint_wal_before minimum=$minimum_wal_bytes" >&2
    exit 1
  fi
  curl -fsS \
    -X POST \
    -H "Authorization: Bearer $token" \
    "http://127.0.0.1:$server_health_port/admin/checkpoint" \
    >"$runtime_dir/manual-checkpoint-response.json"
  for _ in $(seq 1 10); do
    curl -fsS "http://127.0.0.1:$server_health_port/metrics" >"$server_metrics"
    checkpoint_wal_after="$(metric_value "$server_metrics" obsrvr_ducklake_catalog_wal_bytes)"
    if awk -v before="$checkpoint_wal_before" -v after="$checkpoint_wal_after" 'BEGIN { exit !(after < before) }'; then
      break
    fi
    sleep 1
  done
  checkpoint_success="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_checkpoint_total 'result="success",trigger="manual"')"
  checkpoint_errors="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_checkpoint_total 'result="error",trigger="manual"')"
  checkpoint_duration_count="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_checkpoint_duration_seconds_count 'result="success",trigger="manual"')"
  checkpoint_duration_seconds="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_checkpoint_duration_seconds_sum 'result="success",trigger="manual"')"
  checkpoint_last_success="$(metric_value "$server_metrics" obsrvr_ducklake_checkpoint_last_success_timestamp_seconds)"
  assert_equal manual_checkpoint_success "$checkpoint_success" 1
  assert_equal manual_checkpoint_errors "$checkpoint_errors" 0
  assert_equal manual_checkpoint_duration_count "$checkpoint_duration_count" 1
  if ! awk -v before="$checkpoint_wal_before" -v after="$checkpoint_wal_after" 'BEGIN { exit !(before > 0 && after < before) }'; then
    echo "manual checkpoint did not reduce WAL: before=$checkpoint_wal_before after=$checkpoint_wal_after" >&2
    exit 1
  fi
  if ! awk -v timestamp="$checkpoint_last_success" 'BEGIN { exit !(timestamp > 0) }'; then
    echo "manual checkpoint did not set last-success timestamp" >&2
    exit 1
  fi
  checkpoint_triggered=true
fi

# Metrics can become visible just before flowctl flushes the component log line.
# Bound the reconciliation wait rather than racing a successful acknowledgement.
for _ in $(seq 1 50); do
  server_log_acks="$(grep -c 'ingest committed ledger' "$server_log" || true)"
  sink_log_acks="$(grep -c 'ingest-rpc committed ledger' "$pipeline_log" || true)"
  [[ "$server_log_acks" == "$expected" && "$sink_log_acks" == "$expected" ]] && break
  sleep 0.1
done
server_success="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_ingest_batches_total 'result="success"')"
server_errors="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_ingest_batches_total 'result="error"')"
server_retries="$(metric_value "$server_metrics" obsrvr_ducklake_ingest_retries_total)"
sink_retries="$(metric_value "$sink_metrics" obsrvr_ducklake_ingest_retries_total)"
sink_round_trips="$(metric_value "$sink_metrics" obsrvr_ducklake_ingest_rpc_round_trip_seconds_count)"
sink_round_trip_sum="$(metric_value "$sink_metrics" obsrvr_ducklake_ingest_rpc_round_trip_seconds_sum)"
last_ledger="$(metric_value "$server_metrics" obsrvr_ducklake_ingest_last_ledger)"
over_budget="$(metric_value "$server_metrics" obsrvr_ducklake_ingest_over_budget_total)"

assert_equal server_log_acknowledgements "$server_log_acks" "$expected"
assert_equal sink_log_acknowledgements "$sink_log_acks" "$expected"
assert_equal server_success_batches "$server_success" "$expected"
assert_equal server_error_batches "$server_errors" 0
assert_equal server_retries "$server_retries" 0
assert_equal sink_retries "$sink_retries" 0
assert_equal sink_round_trips "$sink_round_trips" "$expected"
assert_equal last_ledger "$last_ledger" "$end_ledger"

phase_sum=0
for phase in decode staging preface transfer commit cleanup; do
  count="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_ingest_phase_seconds_count "phase=\"$phase\"")"
  assert_equal "${phase}_histogram_count" "$count" "$expected"
  value="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_ingest_phase_seconds_sum "phase=\"$phase\"")"
  phase_sum="$(awk -v a="$phase_sum" -v b="$value" 'BEGIN { printf "%.12f", a + b }')"
done
total_count="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_ingest_phase_seconds_count 'phase="total"')"
total_sum="$(metric_labeled_sum "$server_metrics" obsrvr_ducklake_ingest_phase_seconds_sum 'phase="total"')"
assert_equal total_histogram_count "$total_count" "$expected"
server_total_average="$(awk -v total="$total_sum" -v count="$expected" 'BEGIN { printf "%.6f", total / count }')"
sink_round_trip_average="$(awk -v total="$sink_round_trip_sum" -v count="$expected" 'BEGIN { printf "%.6f", total / count }')"

read -r unaccounted_seconds unaccounted_ratio reconciliation_ok < <(
  awk -v total="$total_sum" -v phases="$phase_sum" -v count="$expected" 'BEGIN {
    delta = total - phases
    ratio = total > 0 ? delta / total : 0
    # Queueing and request/ack bookkeeping are intentionally outside the six
    # engine phases. Permit 15% plus 10ms per acknowledged ledger.
    tolerance = total * 0.15 + count * 0.010
    ok = delta >= -0.001 && delta <= tolerance
    printf "%.6f %.6f %s\n", delta, ratio, ok ? "true" : "false"
  }'
)
if [[ "$reconciliation_ok" != true ]]; then
  echo "phase sums do not approximately reconcile: total=$total_sum phases=$phase_sum delta=$unaccounted_seconds ratio=$unaccounted_ratio" >&2
  exit 1
fi

catalog_metric="$(metric_value "$server_metrics" obsrvr_ducklake_catalog_file_bytes)"
wal_metric="$(metric_value "$server_metrics" obsrvr_ducklake_catalog_wal_bytes)"
catalog_stat="$(file_size_or_zero "$catalog_path")"
wal_stat="$(file_size_or_zero "$catalog_path.wal")"
assert_equal catalog_file_gauge "$catalog_metric" "$catalog_stat"
assert_equal catalog_wal_gauge "$wal_metric" "$wal_stat"

cat >"$summary" <<EOF
start_ledger=$start_ledger
end_ledger=$end_ledger
server_startup_seconds=$server_startup_seconds
expected_acknowledgements=$expected
server_log_acknowledgements=$server_log_acks
sink_log_acknowledgements=$sink_log_acks
server_success_batches=$server_success
server_error_batches=$server_errors
server_retries=$server_retries
sink_retries=$sink_retries
sink_round_trips=$sink_round_trips
over_budget=$over_budget
total_histogram_count=$total_count
total_histogram_sum_seconds=$total_sum
server_total_average_seconds=$server_total_average
sink_round_trip_sum_seconds=$sink_round_trip_sum
sink_round_trip_average_seconds=$sink_round_trip_average
phase_histogram_sum_seconds=$phase_sum
unaccounted_seconds=$unaccounted_seconds
unaccounted_ratio=$unaccounted_ratio
catalog_file_metric_bytes=$catalog_metric
catalog_file_stat_bytes=$catalog_stat
catalog_wal_metric_bytes=$wal_metric
catalog_wal_stat_bytes=$wal_stat
manual_checkpoint_triggered=$checkpoint_triggered
manual_checkpoint_wal_before_bytes=$checkpoint_wal_before
manual_checkpoint_wal_after_bytes=$checkpoint_wal_after
manual_checkpoint_success=$checkpoint_success
manual_checkpoint_errors=$checkpoint_errors
manual_checkpoint_duration_seconds=$checkpoint_duration_seconds
manual_checkpoint_last_success_timestamp_seconds=$checkpoint_last_success
EOF

cat "$summary"
echo "telemetry gate passed; evidence retained in $runtime_dir"
