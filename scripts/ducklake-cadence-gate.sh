#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fixtures="${CADENCE_GATE_FIXTURES:-}"
[[ -n "$fixtures" ]] || { echo "CADENCE_GATE_FIXTURES is required" >&2; exit 2; }
fixtures="$(realpath "$fixtures")"
[[ -f "$fixtures" ]] || { echo "fixture manifest does not exist: $fixtures" >&2; exit 2; }
runtime_dir="$(realpath -m "${CADENCE_GATE_RUNTIME_DIR:-/tmp/obsrvr-ducklake-cadence-gate}")"
[[ "$runtime_dir" == /tmp/* ]] || { echo "CADENCE_GATE_RUNTIME_DIR must resolve beneath /tmp" >&2; exit 2; }

ledger_count="${CADENCE_GATE_LEDGER_COUNT:-720}"
cadence="${CADENCE_GATE_CADENCE:-5s}"
jitter="${CADENCE_GATE_JITTER:-250ms}"
max_latency="${CADENCE_GATE_MAX_LATENCY:-400ms}"
required_checkpoints="${CADENCE_GATE_REQUIRED_CHECKPOINTS:-3}"
checkpoint_soft_wal_bytes="${CADENCE_GATE_CHECKPOINT_SOFT_WAL_BYTES:-67108864}"
checkpoint_hard_wal_bytes="${CADENCE_GATE_CHECKPOINT_HARD_WAL_BYTES:-536870912}"
maintenance_interval="${CADENCE_GATE_MAINTENANCE_INTERVAL:-2m}"
maintenance_wait_seconds="${CADENCE_GATE_MAINTENANCE_WAIT_SECONDS:-30}"
compare_baseline="${CADENCE_GATE_COMPARE_BASELINE:-true}"
port_base="${CADENCE_GATE_PORT_BASE:-}"

for value in "$ledger_count" "$required_checkpoints" "$checkpoint_soft_wal_bytes" "$checkpoint_hard_wal_bytes" "$maintenance_wait_seconds"; do
  [[ "$value" =~ ^[0-9]+$ ]] || { echo "ledger/checkpoint counts must be non-negative integers" >&2; exit 2; }
done
(( ledger_count > 0 )) || { echo "CADENCE_GATE_LEDGER_COUNT must be positive" >&2; exit 2; }
(( checkpoint_soft_wal_bytes > 0 && checkpoint_hard_wal_bytes >= checkpoint_soft_wal_bytes )) || {
  echo "checkpoint limits must be positive and hard must be >= soft" >&2
  exit 2
}
[[ "$compare_baseline" == true || "$compare_baseline" == false ]] || {
  echo "CADENCE_GATE_COMPARE_BASELINE must be true or false" >&2
  exit 2
}
for command in go curl jq duckdb diff awk stat; do
  command -v "$command" >/dev/null 2>&1 || { echo "$command is required" >&2; exit 2; }
done

fixture_count="$(jq -er '.batch_count' "$fixtures")"
fixture_start="$(jq -er '.ledger_start' "$fixtures")"
network="$(jq -er '.network_passphrase' "$fixtures")"
[[ "$fixture_count" =~ ^[0-9]+$ && "$fixture_start" =~ ^[0-9]+$ ]] || {
  echo "fixture manifest contains invalid count/range values" >&2
  exit 2
}
(( ledger_count + 1 <= fixture_count )) || {
  echo "fixture has $fixture_count batches; cadence gate requires $((ledger_count + 1)) for restart/resume" >&2
  exit 2
}
[[ "$runtime_dir" != *"'"* && "$network" != *"'"* ]] || {
  echo "runtime path and network passphrase must not contain single quotes" >&2
  exit 2
}

port_range_is_free() {
  local base="$1" port
  for port in $(seq $((base + 1)) $((base + 4))); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&- 3<&-
      return 1
    fi
  done
  return 0
}

if [[ -n "$port_base" ]]; then
  [[ "$port_base" =~ ^[0-9]+$ ]] && (( port_base >= 1024 && port_base <= 65531 )) || {
    echo "CADENCE_GATE_PORT_BASE must be an integer from 1024 through 65531" >&2
    exit 2
  }
  port_range_is_free "$port_base" || { echo "requested cadence-gate ports are busy" >&2; exit 2; }
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
health_port=$((port_base + 2))
ingest_port=$((port_base + 3))
maintenance_health_port=$((port_base + 4))
token="cadence_gate_secret"
catalog_path="$runtime_dir/stellar.ducklake"
data_path="$runtime_dir/data"
baseline_catalog_path="$runtime_dir/baseline/stellar.ducklake"
baseline_data_path="$runtime_dir/baseline/data"
server_pid=""
maintenance_pid=""
last_startup_seconds=""

stop_process() {
  local pid="$1"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 150); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null || true
  fi
}

cleanup() {
  stop_process "$maintenance_pid"
  stop_process "$server_pid"
}
trap cleanup EXIT

start_server() {
  local catalog="$1" data="$2" log_path="$3" controller_enabled="$4"
  mkdir -p "$(dirname "$catalog")" "$data"
  local started_ns healthy_ns
  started_ns="$(date +%s%N)"
  QUACK_TOKEN="$token" \
  QUACK_URI="quack:127.0.0.1:$quack_port" \
  QUACK_HEALTH_ADDR="127.0.0.1:$health_port" \
  QUACK_INSECURE=true \
  QUACK_DISABLE_SSL=true \
  QUACK_ENABLE_EXTERNAL_ACCESS=true \
  QUACK_DISABLED_FILESYSTEMS=none \
  DUCKDB_CHECKPOINT_THRESHOLD=1GB \
  DUCKLAKE_INLINE_ROW_LIMIT=256 \
  CHECKPOINT_ENABLED="$controller_enabled" \
  CHECKPOINT_CONTROLLER_ENABLED="$controller_enabled" \
  CHECKPOINT_TIMEOUT=30s \
  CHECKPOINT_SOFT_WAL_BYTES="$checkpoint_soft_wal_bytes" \
  CHECKPOINT_HARD_WAL_BYTES="$checkpoint_hard_wal_bytes" \
  CHECKPOINT_POLL_INTERVAL=100ms \
  CHECKPOINT_IDLE_DURATION=750ms \
  CHECKPOINT_ADMIN_TOKEN="$token" \
  INGEST_PORT="$ingest_port" \
  DUCKLAKE_CATALOG_PATH="$catalog" \
  DUCKLAKE_DATA_PATH="$data" \
  bin/quack-ducklake-server >"$log_path" 2>&1 &
  server_pid="$!"
  for _ in $(seq 1 600); do
    if curl -fsS "http://127.0.0.1:$health_port/healthz" >/dev/null 2>&1; then
      healthy_ns="$(date +%s%N)"
      last_startup_seconds="$(awk -v start="$started_ns" -v healthy="$healthy_ns" 'BEGIN { printf "%.6f", (healthy - start) / 1000000000 }')"
      return 0
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      tail -n 160 "$log_path" >&2 || true
      return 1
    fi
    sleep 0.1
  done
  echo "server did not become healthy" >&2
  tail -n 160 "$log_path" >&2 || true
  return 1
}

echo "building cadence-gate binaries"
mkdir -p bin
CGO_ENABLED="${CGO_ENABLED:-1}" go build -o bin/quack-ducklake-server ./components/quack-ducklake-server/cmd/component
CGO_ENABLED="${CGO_ENABLED:-1}" go build -o bin/ducklake-maintenance ./components/ducklake-maintenance/cmd/component
CGO_ENABLED="${CGO_ENABLED:-1}" go build -o bin/ingest-replay ./cmd/ingest-replay

rm -rf "$runtime_dir"
mkdir -p "$runtime_dir" "$data_path"
start_server "$catalog_path" "$data_path" "$runtime_dir/server.log" true
initial_startup_seconds="$last_startup_seconds"

QUACK_TOKEN="$token" \
QUACK_URI="quack:127.0.0.1:$quack_port" \
QUACK_DISABLE_SSL=true \
MAINTENANCE_INTERVAL="$maintenance_interval" \
SNAPSHOT_RETENTION=48h \
HEALTH_PORT="$maintenance_health_port" \
bin/ducklake-maintenance >"$runtime_dir/maintenance.log" 2>&1 &
maintenance_pid="$!"

QUACK_TOKEN="$token" bin/ingest-replay \
  --fixtures "$fixtures" \
  --endpoint "127.0.0.1:$ingest_port" \
  --metrics-url "http://127.0.0.1:$health_port/metrics" \
  --profile checkpoint \
  --cadence "$cadence" \
  --jitter "$jitter" \
  --count "$ledger_count" \
  --max-latency "$max_latency" \
  --require-checkpoints "$required_checkpoints" \
  --summary "$runtime_dir/cadence-summary.json" \
  --results "$runtime_dir/cadence-results.jsonl"
jq -e '.success == true and .acknowledged == .requested_count' "$runtime_dir/cadence-summary.json" >/dev/null
curl -fsS "http://127.0.0.1:$health_port/metrics" >"$runtime_dir/server-metrics.prom"
metric_sum() {
  local metric="$1" label_one="${2:-}" label_two="${3:-}"
  awk -v metric="$metric" -v label_one="$label_one" -v label_two="$label_two" '
    ($1 == metric || index($1, metric "{") == 1) &&
    (label_one == "" || index($1, label_one) > 0) &&
    (label_two == "" || index($1, label_two) > 0) { value += $2; found = 1 }
    END { if (!found) exit 1; printf "%.12g\n", value }
  ' "$runtime_dir/server-metrics.prom"
}
ingest_errors="$(metric_sum obsrvr_ducklake_ingest_batches_total 'result="error"')"
ingest_retries="$(metric_sum obsrvr_ducklake_ingest_retries_total)"
checkpoint_errors="$(metric_sum obsrvr_ducklake_checkpoint_total 'result="error"')"
idle_checkpoints="$(metric_sum obsrvr_ducklake_checkpoint_total 'result="success"' 'trigger="idle"')"
[[ "$ingest_errors" == 0 ]] || { echo "ingest errors=$ingest_errors, want 0" >&2; exit 1; }
[[ "$ingest_retries" == 0 ]] || { echo "ingest retries=$ingest_retries, want 0" >&2; exit 1; }
[[ "$checkpoint_errors" == 0 ]] || { echo "checkpoint errors=$checkpoint_errors, want 0" >&2; exit 1; }
(( idle_checkpoints >= required_checkpoints )) || {
  echo "successful idle checkpoints=$idle_checkpoints, want at least $required_checkpoints" >&2
  exit 1
}
if ! kill -0 "$maintenance_pid" 2>/dev/null; then
  echo "maintenance process exited during cadence replay" >&2
  tail -n 160 "$runtime_dir/maintenance.log" >&2 || true
  exit 1
fi
maintenance_complete=false
for _ in $(seq 1 $((maintenance_wait_seconds * 10 + 1))); do
  if grep -q 'maintenance ok.*ducklake_flush_inlined_data' "$runtime_dir/maintenance.log" &&
     grep -q 'maintenance ok.*ducklake_merge_adjacent_files' "$runtime_dir/maintenance.log" &&
     grep -q 'maintenance ok.*ducklake_expire_snapshots' "$runtime_dir/maintenance.log"; then
    maintenance_complete=true
    break
  fi
  if ! kill -0 "$maintenance_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if [[ "$maintenance_complete" != true ]]; then
  for statement in ducklake_flush_inlined_data ducklake_merge_adjacent_files ducklake_expire_snapshots; do
    grep -q "maintenance ok.*$statement" "$runtime_dir/maintenance.log" || {
    echo "maintenance did not successfully run $statement during cadence replay" >&2
    tail -n 160 "$runtime_dir/maintenance.log" >&2 || true
    exit 1
    }
  done
fi

stop_process "$maintenance_pid"
maintenance_pid=""
stop_process "$server_pid"
server_pid=""
start_server "$catalog_path" "$data_path" "$runtime_dir/server-restart.log" true
restart_startup_seconds="$last_startup_seconds"

QUACK_TOKEN="$token" bin/ingest-replay \
  --fixtures "$fixtures" \
  --endpoint "127.0.0.1:$ingest_port" \
  --profile custom \
  --cadence 0 \
  --jitter 0 \
  --offset "$ledger_count" \
  --count 1 \
  --max-latency "$max_latency" \
  --summary "$runtime_dir/resume-summary.json" \
  --results "$runtime_dir/resume-results.jsonl"
jq -e '.success == true and .acknowledged == 1' "$runtime_dir/resume-summary.json" >/dev/null
stop_process "$server_pid"
server_pid=""

expected_count=$((ledger_count + 1))
expected_end=$((fixture_start + ledger_count))
read -r watermark_count watermark_min watermark_max watermark_gaps ledger_batch_count < <(
  duckdb :memory: -csv -noheader -c "
    INSTALL ducklake;
    LOAD ducklake;
    ATTACH 'ducklake:$catalog_path' AS stellar_lake (DATA_PATH '$data_path');
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
    ), batches AS (
      SELECT count(*) AS batch_count
      FROM stellar_lake.ledger_batches
      WHERE network_passphrase = '$network'
    )
    SELECT count_value, min_value, max_value, gap_count, batch_count FROM bounds, gaps, batches;" \
  | tr ',' ' '
)
[[ "$watermark_count" == "$expected_count" ]] || { echo "watermark count=$watermark_count, want $expected_count" >&2; exit 1; }
[[ "$watermark_min" == "$fixture_start" ]] || { echo "watermark min=$watermark_min, want $fixture_start" >&2; exit 1; }
[[ "$watermark_max" == "$expected_end" ]] || { echo "watermark max=$watermark_max, want $expected_end" >&2; exit 1; }
[[ "$watermark_gaps" == 0 ]] || { echo "watermark gaps=$watermark_gaps, want 0" >&2; exit 1; }
[[ "$ledger_batch_count" == "$expected_count" ]] || { echo "ledger batch count=$ledger_batch_count, want $expected_count" >&2; exit 1; }

parity_differences="not_run"
if [[ "$compare_baseline" == true ]]; then
  start_server "$baseline_catalog_path" "$baseline_data_path" "$runtime_dir/baseline-server.log" false
  baseline_startup_seconds="$last_startup_seconds"
  QUACK_TOKEN="$token" bin/ingest-replay \
    --fixtures "$fixtures" \
    --endpoint "127.0.0.1:$ingest_port" \
    --profile custom \
    --cadence 0 \
    --jitter 0 \
    --count "$expected_count" \
    --max-latency 0 \
    --summary "$runtime_dir/baseline-summary.json"
  stop_process "$server_pid"
  server_pid=""
  scripts/ducklake-logical-fingerprint.sh \
    "$catalog_path" "$data_path" "$runtime_dir/cadence-fingerprint.txt" \
    "$fixture_start" "$expected_end" >/dev/null
  scripts/ducklake-logical-fingerprint.sh \
    "$baseline_catalog_path" "$baseline_data_path" "$runtime_dir/baseline-fingerprint.txt" \
    "$fixture_start" "$expected_end" >/dev/null
  if ! diff -u "$runtime_dir/baseline-fingerprint.txt" "$runtime_dir/cadence-fingerprint.txt" >"$runtime_dir/parity.diff"; then
    echo "cadence/checkpoint catalog differs from saturated baseline" >&2
    cat "$runtime_dir/parity.diff" >&2
    exit 1
  fi
  parity_differences=0
else
  baseline_startup_seconds="not_run"
fi

cat >"$runtime_dir/gate-summary.env" <<EOF
fixture_manifest=$fixtures
ledger_count=$ledger_count
resume_ledger=$expected_end
initial_startup_seconds=$initial_startup_seconds
restart_startup_seconds=$restart_startup_seconds
baseline_startup_seconds=$baseline_startup_seconds
required_idle_checkpoints=$required_checkpoints
observed_idle_checkpoints=$(jq -r '.observed_idle_checkpoints' "$runtime_dir/cadence-summary.json")
over_budget=$(jq -r '.over_budget' "$runtime_dir/cadence-summary.json")
ingest_errors=$ingest_errors
ingest_retries=$ingest_retries
checkpoint_errors=$checkpoint_errors
watermark_count=$watermark_count
watermark_min=$watermark_min
watermark_max=$watermark_max
watermark_gaps=$watermark_gaps
ledger_batch_count=$ledger_batch_count
logical_parity_differences=$parity_differences
EOF
cat "$runtime_dir/gate-summary.env"
echo "cadence gate passed; evidence retained in $runtime_dir"
