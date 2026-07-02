#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

runtime_dir="${QUACK_CHAOS_RUNTIME_DIR:-/tmp/obsrvr-quack-chaos}"
catalog_path="${QUACK_CHAOS_CATALOG_PATH:-$runtime_dir/stellar.ducklake}"
data_path="${QUACK_CHAOS_DATA_PATH:-$runtime_dir/data}"
baseline_catalog_path="${QUACK_CHAOS_BASELINE_CATALOG_PATH:-$runtime_dir/baseline.ducklake}"
baseline_data_path="${QUACK_CHAOS_BASELINE_DATA_PATH:-$runtime_dir/baseline-data}"
port_base="${QUACK_CHAOS_PORT_BASE:-}"
if [[ -z "$port_base" ]]; then
  port_base=$((20000 + RANDOM % 20000))
fi
quack_port="${QUACK_CHAOS_QUACK_PORT:-$((port_base + 49))}"
quack_health_port="${QUACK_CHAOS_QUACK_HEALTH_PORT:-$((port_base + 50))}"
control_port="${QUACK_CHAOS_CONTROL_PORT:-$((port_base + 51))}"
raw_grpc_port="${QUACK_CHAOS_RAW_GRPC_PORT:-$((port_base + 52))}"
processor_grpc_port="${QUACK_CHAOS_PROCESSOR_GRPC_PORT:-$((port_base + 53))}"
sink_grpc_port="${QUACK_CHAOS_SINK_GRPC_PORT:-$((port_base + 54))}"
raw_health_port="${QUACK_CHAOS_RAW_HEALTH_PORT:-$((port_base + 55))}"
processor_health_port="${QUACK_CHAOS_PROCESSOR_HEALTH_PORT:-$((port_base + 56))}"
sink_health_port="${QUACK_CHAOS_SINK_HEALTH_PORT:-$((port_base + 57))}"
uri="${QUACK_URI:-quack:127.0.0.1:$quack_port}"
token="${QUACK_TOKEN:-chaos_secret}"
health_addr="${QUACK_HEALTH_ADDR:-127.0.0.1:$quack_health_port}"
network="${QUACK_CHAOS_NETWORK:-Public Global Stellar Network ; September 2015}"
start_ledger="${QUACK_CHAOS_START_LEDGER:-62080000}"
end_ledger="${QUACK_CHAOS_END_LEDGER:-62080002}"
kill_after="${QUACK_CHAOS_KILL_AFTER:-5}"
ingest_timeout="${QUACK_CHAOS_INGEST_TIMEOUT:-90}"
replay_timeout="${QUACK_CHAOS_REPLAY_TIMEOUT:-180}"
baseline_timeout="${QUACK_CHAOS_BASELINE_TIMEOUT:-180}"
min_script_mib="${QUACK_CHAOS_MIN_SCRIPT_MIB:-15}"
shutdown_grace="${QUACK_CHAOS_SHUTDOWN_GRACE:-10}"
pipeline_path="${QUACK_CHAOS_PIPELINE_PATH:-$runtime_dir/local-archive-quack-ducklake-flowctl.yaml}"
ingest_cmd="${QUACK_CHAOS_INGEST_CMD:-}"
replay_cmd="${QUACK_CHAOS_REPLAY_CMD:-$ingest_cmd}"
baseline_cmd="${QUACK_CHAOS_BASELINE_CMD:-$replay_cmd}"

if [[ -z "$ingest_cmd" ]]; then
  if command -v flowctl >/dev/null 2>&1; then
    ingest_cmd="flowctl run --no-persistence --control-plane-port $control_port --log-dir $runtime_dir/flowctl-chaos-logs $pipeline_path"
    replay_cmd="flowctl run --no-persistence --control-plane-port $control_port --log-dir $runtime_dir/flowctl-replay-logs $pipeline_path"
    baseline_cmd="flowctl run --no-persistence --control-plane-port $control_port --log-dir $runtime_dir/flowctl-baseline-logs $pipeline_path"
  else
  cat >&2 <<'MSG'
QUACK_CHAOS_INGEST_CMD is required when flowctl is not installed.

Set it to the command that runs the quack-mode ingest under test. The command
must point ducklake-sink at this harness server, for example:

  QUACK_CHAOS_INGEST_CMD='DUCKLAKE_MODE=quack QUACK_URI=quack:127.0.0.1:9494 QUACK_TOKEN=chaos_secret QUACK_DISABLE_SSL=true flowctl process ...'

The harness will:
  1. start quack-ducklake-server
  2. run QUACK_CHAOS_INGEST_CMD
  3. kill the server after QUACK_CHAOS_KILL_AFTER seconds
  4. require the ingest command to fail
  5. restart the server
  6. run QUACK_CHAOS_REPLAY_CMD, defaulting to the ingest command
  7. run QUACK_CHAOS_BASELINE_CMD against a never-failed baseline catalog
  8. compare final tables with EXCEPT ALL when the duckdb CLI is available
  9. run the watermark gap query when duckdb and QUACK_CHAOS_NETWORK are set
MSG
    exit 2
  fi
fi

mkdir -p "$runtime_dir"
if [[ "$catalog_path" == "$runtime_dir"/* && "$data_path" == "$runtime_dir"/* && "$baseline_catalog_path" == "$runtime_dir"/* && "$baseline_data_path" == "$runtime_dir"/* ]]; then
  rm -rf "$catalog_path" "$data_path" "$baseline_catalog_path" "$baseline_data_path"
elif [[ "${QUACK_CHAOS_ALLOW_DELETE:-false}" == "true" ]]; then
  rm -rf "$catalog_path" "$data_path" "$baseline_catalog_path" "$baseline_data_path"
else
  echo "refusing to delete catalog/data paths outside QUACK_CHAOS_RUNTIME_DIR" >&2
  echo "set QUACK_CHAOS_ALLOW_DELETE=true for an intentional destructive harness run" >&2
  exit 2
fi
mkdir -p "$data_path" "$baseline_data_path" bin

generate_default_pipeline() {
  cat >"$pipeline_path" <<YAML
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: local-archive-quack-ducklake-chaos
  description: Generated per-run pipeline for the Quack chaos harness.

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
        DUCKLAKE_MODE: "quack"
        QUACK_URI: "$uri"
        QUACK_TOKEN: "$token"
        QUACK_DISABLE_SSL: "true"
        PORT: ":$sink_grpc_port"
        HEALTH_PORT: "$sink_health_port"
        ENABLE_FLOWCTL: "true"
        FLOWCTL_ENDPOINT: "127.0.0.1:$control_port"
YAML
}

echo "building quack-ducklake-server, ducklake-sink, and stellar-ledger-processor"
CGO_ENABLED="${CGO_ENABLED:-1}" go build -o bin/quack-ducklake-server ./components/quack-ducklake-server/cmd/component
CGO_ENABLED="${CGO_ENABLED:-1}" go build -o bin/ducklake-sink ./components/ducklake-sink/cmd/component
CGO_ENABLED="${CGO_ENABLED:-1}" go build -o bin/stellar-ledger-processor ./components/stellar-ledger-processor/cmd/component
if [[ ! -x bin/raw-ledger-source ]]; then
  echo "bin/raw-ledger-source is required; build or install raw-ledger-source@0.2.2 before running this harness" >&2
  exit 2
fi

if [[ "$pipeline_path" == "$runtime_dir"/* ]]; then
  generate_default_pipeline
fi

server_pid=""
command_pid=""
command_uses_setsid="false"

command_is_done() {
  local pid="$1"
  local stat

  if ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  stat="$(ps -p "$pid" -o stat= 2>/dev/null || true)"
  [[ -z "$stat" || "$stat" == Z* ]]
}

terminate_command() {
  local pid="$1"
  local uses_setsid="$2"

  if command_is_done "$pid"; then
    wait "$pid" 2>/dev/null || true
    return 0
  fi

  if [[ "$uses_setsid" == "true" ]]; then
    kill -TERM -- "-$pid" 2>/dev/null || true
  else
    kill "$pid" 2>/dev/null || true
  fi

  for _ in $(seq 1 "$shutdown_grace"); do
    if command_is_done "$pid"; then
      wait "$pid" 2>/dev/null || true
      return 0
    fi
    sleep 1
  done

  if [[ "$uses_setsid" == "true" ]]; then
    kill -KILL -- "-$pid" 2>/dev/null || true
  else
    kill -KILL "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

start_command() {
  local cmd="$1"
  local log_file="$2"

  if command -v setsid >/dev/null 2>&1; then
    setsid bash -lc "$cmd" >"$log_file" 2>&1 &
    command_uses_setsid="true"
  else
    bash -lc "$cmd" >"$log_file" 2>&1 &
    command_uses_setsid="false"
  fi
  command_pid="$!"
}

wait_command() {
  local timeout_seconds="$1"
  local elapsed=0

  while ! command_is_done "$command_pid"; do
    if [[ "$elapsed" -ge "$timeout_seconds" ]]; then
      terminate_command "$command_pid" "$command_uses_setsid"
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  wait "$command_pid"
}

cleanup() {
  if [[ -n "$command_pid" ]] && ! command_is_done "$command_pid"; then
    terminate_command "$command_pid" "$command_uses_setsid"
  fi
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

start_server() {
  local active_catalog_path="$1"
  local active_data_path="$2"
  local log_file="$3"

  QUACK_TOKEN="$token" \
  QUACK_URI="$uri" \
  QUACK_HEALTH_ADDR="$health_addr" \
  QUACK_INSECURE=true \
  QUACK_DISABLE_SSL=true \
  QUACK_ENABLE_EXTERNAL_ACCESS=true \
  QUACK_DISABLED_FILESYSTEMS=none \
  DUCKLAKE_CATALOG_PATH="$active_catalog_path" \
  DUCKLAKE_DATA_PATH="$active_data_path" \
  bin/quack-ducklake-server >"$log_file" 2>&1 &
  server_pid="$!"

  for _ in $(seq 1 60); do
    if curl -fsS "http://$health_addr/healthz" >/dev/null 2>&1; then
      echo "quack server healthy on $health_addr"
      return 0
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "quack server exited during startup" >&2
      cat "$log_file" >&2 || true
      return 1
    fi
    sleep 1
  done
  echo "quack server did not become healthy" >&2
  cat "$log_file" >&2 || true
  return 1
}

check_script_sizes() {
  local log_file="$runtime_dir/script-sizes.log"
  local summary_file="$runtime_dir/script-size-summary.env"

  if [[ "$min_script_mib" == "0" ]]; then
    echo "skipping remote script size threshold because QUACK_CHAOS_MIN_SCRIPT_MIB=0"
    return 0
  fi
  if [[ ! -s "$log_file" ]]; then
    echo "no remote DuckLake script size measurements were captured" >&2
    return 1
  fi

  if ! awk -v min="$min_script_mib" -v summary="$summary_file" '
    /remote DuckLake write script/ {
      for (i = 1; i <= NF; i++) {
        if ($i == "is") {
          size = $(i + 1) + 0
          count++
          if (size > max) {
            max = size
          }
        }
      }
    }
    END {
      if (count == 0) {
        print "no script size measurements found" > "/dev/stderr"
        exit 2
      }
      printf "max_remote_script_mib=%.2f\n", max > summary
      printf "min_required_script_mib=%.2f\n", min >> summary
      if (max < min) {
        exit 1
      }
    }
  ' "$log_file"; then
    echo "remote script size gate failed; largest script was below ${min_script_mib} MiB" >&2
    cat "$summary_file" >&2 || true
    return 1
  fi

  cat "$summary_file"
  echo "remote script size gate passed"
}

echo "starting initial quack server"
start_server "$catalog_path" "$data_path" "$runtime_dir/quack-server-chaos.log"

echo "running ingest command; will kill server after ${kill_after}s"
set +e
start_command "$ingest_cmd" "$runtime_dir/ingest-before-kill.log"
sleep "$kill_after"
kill "$server_pid" 2>/dev/null || true
wait "$server_pid" 2>/dev/null || true
server_pid=""
wait_command "$ingest_timeout"
ingest_status="$?"
set -e

if [[ "$ingest_status" -eq 0 ]]; then
  echo "ingest command succeeded after server kill; expected non-zero crash-and-replay signal" >&2
  exit 1
fi
if ! grep -Eq "fatal ledger batch handling error|Process failed.*ducklake-sink|remote DuckLake write batch" "$runtime_dir/ingest-before-kill.log"; then
  echo "ingest command failed, but the log did not prove a ducklake-sink write failure" >&2
  tail -n 120 "$runtime_dir/ingest-before-kill.log" >&2 || true
  exit 1
fi
echo "ingest failed as expected after server kill"

echo "restarting quack server"
start_server "$catalog_path" "$data_path" "$runtime_dir/quack-server-replay.log"

echo "running replay command"
set +e
start_command "$replay_cmd" "$runtime_dir/replay.log"
wait_command "$replay_timeout"
replay_status="$?"
set -e
if [[ "$replay_status" -ne 0 && "$replay_status" -ne 124 ]]; then
  echo "replay command failed with status $replay_status" >&2
  tail -n 160 "$runtime_dir/replay.log" >&2 || true
  exit "$replay_status"
fi
if [[ "$replay_status" -eq 124 ]]; then
  echo "replay command reached ${replay_timeout}s; continuing to catalog checks after stopping the pipeline"
fi

if grep -ah "remote DuckLake write script" "$runtime_dir/ingest-before-kill.log" "$runtime_dir/replay.log" >"$runtime_dir/script-sizes.log" 2>/dev/null; then
  echo "captured remote script size measurements in $runtime_dir/script-sizes.log"
  check_script_sizes
elif [[ "$min_script_mib" != "0" ]]; then
  echo "failed to capture remote script size measurements" >&2
  exit 1
fi

if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  server_pid=""
fi

echo "starting baseline quack server"
start_server "$baseline_catalog_path" "$baseline_data_path" "$runtime_dir/quack-server-baseline.log"

echo "running never-failed baseline command"
set +e
start_command "$baseline_cmd" "$runtime_dir/baseline.log"
wait_command "$baseline_timeout"
baseline_status="$?"
set -e
if [[ "$baseline_status" -ne 0 && "$baseline_status" -ne 124 ]]; then
  echo "baseline command failed with status $baseline_status" >&2
  tail -n 160 "$runtime_dir/baseline.log" >&2 || true
  exit "$baseline_status"
fi
if [[ "$baseline_status" -eq 124 ]]; then
  echo "baseline command reached ${baseline_timeout}s; continuing to catalog checks after stopping the pipeline"
fi

if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  server_pid=""
fi

run_duckdb_checks() {
  local compare_sql="$runtime_dir/compare.sql"
  cat >"$compare_sql" <<SQL
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:$catalog_path' AS chaos (DATA_PATH '$data_path');
ATTACH 'ducklake:$baseline_catalog_path' AS baseline (DATA_PATH '$baseline_data_path');

CREATE TEMP TABLE parity_diffs(table_name VARCHAR, diff_count BIGINT);
INSERT INTO parity_diffs
SELECT 'ledger_batches', count(*) FROM (
  (SELECT network_passphrase, ledger_sequence, closed_at_unix, schema_version, extraction_version, transaction_count, operation_count, bronze_row_count FROM chaos.ledger_batches
   EXCEPT ALL
   SELECT network_passphrase, ledger_sequence, closed_at_unix, schema_version, extraction_version, transaction_count, operation_count, bronze_row_count FROM baseline.ledger_batches)
  UNION ALL
  (SELECT network_passphrase, ledger_sequence, closed_at_unix, schema_version, extraction_version, transaction_count, operation_count, bronze_row_count FROM baseline.ledger_batches
   EXCEPT ALL
   SELECT network_passphrase, ledger_sequence, closed_at_unix, schema_version, extraction_version, transaction_count, operation_count, bronze_row_count FROM chaos.ledger_batches)
);
INSERT INTO parity_diffs
SELECT 'bronze_rows', count(*) FROM (
  (SELECT network_passphrase, ledger_sequence, table_name, count(*) AS row_count FROM chaos.bronze_rows GROUP BY 1, 2, 3
   EXCEPT ALL
   SELECT network_passphrase, ledger_sequence, table_name, count(*) AS row_count FROM baseline.bronze_rows GROUP BY 1, 2, 3)
  UNION ALL
  (SELECT network_passphrase, ledger_sequence, table_name, count(*) AS row_count FROM baseline.bronze_rows GROUP BY 1, 2, 3
   EXCEPT ALL
   SELECT network_passphrase, ledger_sequence, table_name, count(*) AS row_count FROM chaos.bronze_rows GROUP BY 1, 2, 3)
);
INSERT INTO parity_diffs
SELECT 'ingest_watermarks', count(*) FROM (
  (SELECT network_passphrase, ledger_sequence FROM chaos.ingest_watermarks EXCEPT ALL SELECT network_passphrase, ledger_sequence FROM baseline.ingest_watermarks)
  UNION ALL
  (SELECT network_passphrase, ledger_sequence FROM baseline.ingest_watermarks EXCEPT ALL SELECT network_passphrase, ledger_sequence FROM chaos.ingest_watermarks)
);
SQL

  cat >>"$compare_sql" <<SQL
INSERT INTO parity_diffs
SELECT 'bronze.ledgers_row_v2', count(*) FROM (
  (SELECT * EXCLUDE (ingestion_timestamp) FROM chaos.bronze.ledgers_row_v2
   EXCEPT ALL
   SELECT * EXCLUDE (ingestion_timestamp) FROM baseline.bronze.ledgers_row_v2)
  UNION ALL
  (SELECT * EXCLUDE (ingestion_timestamp) FROM baseline.bronze.ledgers_row_v2
   EXCEPT ALL
   SELECT * EXCLUDE (ingestion_timestamp) FROM chaos.bronze.ledgers_row_v2)
);
SQL

  local table
  cat >>"$compare_sql" <<SQL
INSERT INTO parity_diffs
SELECT 'bronze.accounts_snapshot_v1', count(*) FROM (
  (SELECT * EXCLUDE (created_at, updated_at) FROM chaos.bronze.accounts_snapshot_v1
   EXCEPT ALL
   SELECT * EXCLUDE (created_at, updated_at) FROM baseline.bronze.accounts_snapshot_v1)
  UNION ALL
  (SELECT * EXCLUDE (created_at, updated_at) FROM baseline.bronze.accounts_snapshot_v1
   EXCEPT ALL
   SELECT * EXCLUDE (created_at, updated_at) FROM chaos.bronze.accounts_snapshot_v1)
);
SQL

  for table in \
    transactions_row_v2 operations_row_v2 effects_row_v1 \
    trades_row_v1 trustlines_snapshot_v1 account_signers_snapshot_v1 \
    offers_snapshot_v1 \
    liquidity_pools_snapshot_v1 claimable_balances_snapshot_v1 \
    contract_events_stream_v1 contract_data_snapshot_v1 contract_code_snapshot_v1 \
    config_settings_snapshot_v1 ttl_snapshot_v1 evicted_keys_state_v1 \
    restored_keys_state_v1 contract_creations_v1 token_transfers_stream_v1
  do
    cat >>"$compare_sql" <<SQL
INSERT INTO parity_diffs
SELECT 'bronze.$table', count(*) FROM (
  (SELECT * EXCLUDE (created_at) FROM chaos.bronze.$table EXCEPT ALL SELECT * EXCLUDE (created_at) FROM baseline.bronze.$table)
  UNION ALL
  (SELECT * EXCLUDE (created_at) FROM baseline.bronze.$table EXCEPT ALL SELECT * EXCLUDE (created_at) FROM chaos.bronze.$table)
);
SQL
  done

  for table in native_balances_snapshot_v1
  do
    cat >>"$compare_sql" <<SQL
INSERT INTO parity_diffs
SELECT 'bronze.$table', count(*) FROM (
  (SELECT * FROM chaos.bronze.$table EXCEPT ALL SELECT * FROM baseline.bronze.$table)
  UNION ALL
  (SELECT * FROM baseline.bronze.$table EXCEPT ALL SELECT * FROM chaos.bronze.$table)
);
SQL
  done

  cat >>"$compare_sql" <<SQL
.mode csv
.headers on
.once $runtime_dir/parity-diffs.csv
SELECT * FROM parity_diffs WHERE diff_count <> 0 ORDER BY table_name;

CREATE TEMP TABLE gate_failures(check_name VARCHAR, observed BIGINT, details VARCHAR);
INSERT INTO gate_failures
SELECT 'transactions_row_v2_rows', 0, 'no transaction rows were written'
WHERE NOT EXISTS (SELECT 1 FROM chaos.bronze.transactions_row_v2);
INSERT INTO gate_failures
SELECT 'transactions_row_v2_xdr_not_null', count(*), 'tx_envelope, tx_result, or tx_meta had NULL values'
FROM chaos.bronze.transactions_row_v2
WHERE tx_envelope IS NULL OR tx_result IS NULL OR tx_meta IS NULL
HAVING count(*) > 0;
INSERT INTO gate_failures
SELECT 'transactions_row_v2_soroban_fields', 0, 'no transaction rows populated soroban_resources_instructions'
WHERE NOT EXISTS (
  SELECT 1
  FROM chaos.bronze.transactions_row_v2
  WHERE soroban_resources_instructions IS NOT NULL
);
INSERT INTO gate_failures
SELECT 'operations_row_v2_soroban_fields', 0, 'no operation rows populated soroban_operation and soroban_arguments_json'
WHERE NOT EXISTS (
  SELECT 1
  FROM chaos.bronze.operations_row_v2
  WHERE soroban_operation IS NOT NULL
    AND soroban_arguments_json IS NOT NULL
);
.once $runtime_dir/gate-failures.csv
SELECT * FROM gate_failures ORDER BY check_name;
SQL

  echo "comparing chaos and never-failed baseline tables"
  duckdb :memory: <"$compare_sql" >/dev/null
  if [[ -s "$runtime_dir/parity-diffs.csv" ]] && [[ "$(wc -l <"$runtime_dir/parity-diffs.csv")" -gt 1 ]]; then
    echo "parity differences found:" >&2
    cat "$runtime_dir/parity-diffs.csv" >&2
    return 1
  fi
  echo "chaos and baseline tables match"
  if [[ -s "$runtime_dir/gate-failures.csv" ]] && [[ "$(wc -l <"$runtime_dir/gate-failures.csv")" -gt 1 ]]; then
    echo "catalog gate failures found:" >&2
    cat "$runtime_dir/gate-failures.csv" >&2
    return 1
  fi
  echo "typed/XDR/Soroban catalog gates passed"
}

if command -v duckdb >/dev/null 2>&1; then
  run_duckdb_checks
else
  echo "skipping parity compare; install duckdb CLI to enable it"
fi

if [[ -n "$network" ]] && command -v duckdb >/dev/null 2>&1; then
  echo "running watermark gap query"
  duckdb :memory: <<SQL
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:$catalog_path' AS stellar_lake (DATA_PATH '$data_path');
USE stellar_lake;
WITH bounds AS (
  SELECT
    min(ledger_sequence) AS min_seq,
    max(ledger_sequence) AS max_seq
  FROM ingest_watermarks
  WHERE network_passphrase = '$network'
),
expected AS (
  SELECT range AS ledger_sequence
  FROM bounds, range(CAST(min_seq AS BIGINT), CAST(max_seq AS BIGINT) + 1)
)
SELECT expected.ledger_sequence
FROM expected
LEFT JOIN ingest_watermarks USING (ledger_sequence)
WHERE ingest_watermarks.ledger_sequence IS NULL
ORDER BY expected.ledger_sequence;
SQL
else
  echo "skipping watermark gap query; set QUACK_CHAOS_NETWORK and install duckdb CLI to enable it"
fi

echo "quack chaos harness completed; logs are in $runtime_dir"
