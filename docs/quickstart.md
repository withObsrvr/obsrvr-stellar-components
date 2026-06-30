# Quickstart

This guide gets you from a fresh checkout to a local Stellar ledger pipeline:

```text
raw-ledger-source@0.2.2 -> stellar-ledger-processor -> jsonl-sink
```

Use this repository when you want reusable flowctl components for production-shaped pipelines. Use Nebu when you want ad hoc Unix-pipe exploration. A common path is:

```text
Nebu prototype -> flowctl component pipeline -> Postgres/DuckLake materialization -> Obsrvr Lake APIs
```

## What You Will Build

The first run writes one normalized `stellar.ledger.batch.v1` event to JSONL. That batch contains:

- compatibility rows: `ledgers`, `transactions`, `operations`
- full bronze extractor rows in `bronze_rows`
- deterministic row IDs for replay and idempotent sink writes

After JSONL works, the same processor output can feed `postgres-sink` for hot operational tables and `ducklake-sink` for analytical files.

## Prerequisites

- Nix with flakes enabled
- Go, provided by `nix develop`
- `flowctl` available on `PATH` for local pipeline runs
- a checkout of `stellar-raw-ledger-origin` for `raw-ledger-source@0.2.2`

Clone both repositories side by side, or set `RAW_LEDGER_SOURCE_REPO` to the source checkout:

```bash
git clone https://github.com/withObsrvr/obsrvr-stellar-components.git
git clone https://github.com/withObsrvr/stellar-raw-ledger-origin.git
```

If you already have `stellar-raw-ledger-origin` somewhere else:

```bash
export RAW_LEDGER_SOURCE_REPO="/path/to/stellar-raw-ledger-origin"
```

## 1. Enter the Reproducible Shell

```bash
cd obsrvr-stellar-components
nix develop
```

The flake pins the Go and protobuf toolchain used by the repo.

Install `flowctl` inside the shell if it is not already available:

```bash
go install github.com/withobsrvr/flowctl@latest
export PATH="$HOME/go/bin:$PATH"
```

## 2. Verify and Build Components

```bash
make proto
make lint
make test
make build
```

Expected local binaries:

```text
bin/stellar-ledger-processor
bin/jsonl-sink
bin/postgres-sink
bin/ducklake-sink
```

## 3. Smoke Test One Mainnet Ledger

Before running flowctl, verify archive access, XDR decode, and normalization directly. This example uses the public AWS Stellar archive, which does not require Obsrvr credentials:

```bash
go run ./cmd/ledger-smoke \
  -backend ARCHIVE \
  -archive-storage-type S3 \
  -archive-bucket-name aws-public-blockchain \
  -archive-path v1.1/stellar/ledgers/pubnet \
  -aws-region us-east-2 \
  -ledgers-per-file 1 \
  -files-per-partition 64000 \
  -ledger 62080000 \
  -timeout 60s
```

A successful run prints a `stellar.ledger.batch.v1` summary with non-zero transaction, operation, and bronze row counts.

## 4. Run a Local Flowctl JSONL Pipeline

Build the external source binary:

```bash
export COMPONENTS_REPO="$(pwd)"
export RAW_LEDGER_SOURCE_REPO="${RAW_LEDGER_SOURCE_REPO:-../stellar-raw-ledger-origin}"
export QUICKSTART_DIR="$(mktemp -d "${TMPDIR:-/tmp}/obsrvr-stellar-quickstart.XXXXXX")"

(
  cd "$RAW_LEDGER_SOURCE_REPO/source"
  go build -o "$QUICKSTART_DIR/raw-ledger-source" ./cmd/raw-ledger-source
)
```

Create a one-ledger archive pipeline:

```bash
cat >"$QUICKSTART_DIR/archive-jsonl.yaml" <<YAML
apiVersion: flowctl/v1
kind: Pipeline
metadata:
  name: archive-single-ledger-jsonl
spec:
  driver: process
  sources:
    - id: raw-ledger-source
      type: source
      command: ["$QUICKSTART_DIR/raw-ledger-source"]
      env:
        FLOWCTL_COMPONENT_ID: "raw-ledger-source"
        BACKEND_TYPE: "ARCHIVE"
        ARCHIVE_STORAGE_TYPE: "S3"
        ARCHIVE_BUCKET_NAME: "aws-public-blockchain"
        ARCHIVE_PATH: "v1.1/stellar/ledgers/pubnet"
        AWS_REGION: "us-east-2"
        LEDGERS_PER_FILE: "1"
        FILES_PER_PARTITION: "64000"
        NETWORK_PASSPHRASE: "Public Global Stellar Network ; September 2015"
        START_LEDGER: "62080000"
        END_LEDGER: "62080000"
        GRPC_PORT: "55171"
        HEALTH_PORT: "19181"
        FLOWCTL_ENDPOINT: "127.0.0.1:19180"

  processors:
    - id: stellar-ledger-processor
      type: processor
      command: ["$COMPONENTS_REPO/bin/stellar-ledger-processor"]
      inputs: ["raw-ledger-source"]
      env:
        COMPONENT_ID: "stellar-ledger-processor"
        NETWORK_PASSPHRASE: "Public Global Stellar Network ; September 2015"
        PORT: ":55172"
        HEALTH_PORT: "19182"
        ENABLE_FLOWCTL: "true"
        FLOWCTL_ENDPOINT: "127.0.0.1:19180"

  sinks:
    - id: jsonl-sink
      type: sink
      command: ["$COMPONENTS_REPO/bin/jsonl-sink"]
      inputs: ["stellar-ledger-processor"]
      env:
        COMPONENT_ID: "jsonl-sink"
        JSONL_PATH: "$QUICKSTART_DIR/ledger_batches.jsonl"
        PORT: ":55173"
        HEALTH_PORT: "19183"
        ENABLE_FLOWCTL: "true"
        FLOWCTL_ENDPOINT: "127.0.0.1:19180"
YAML
```

Run it:

```bash
timeout 75s flowctl run \
  --control-plane-port 19180 \
  --db-path "$QUICKSTART_DIR/flowctl.db" \
  --log-dir "$QUICKSTART_DIR/logs" \
  "$QUICKSTART_DIR/archive-jsonl.yaml"
```

`flowctl run` is a supervisor, so `timeout` ending the process is expected for this bounded smoke test. The pass condition is one JSONL row:

```bash
wc -l "$QUICKSTART_DIR/ledger_batches.jsonl"
```

Expected:

```text
1 <quickstart-dir>/ledger_batches.jsonl
```

Inspect the payload:

```bash
jq '{
  ledgerSequence,
  txs: (.transactions | length),
  ops: (.operations | length),
  bronzeRows: (.bronzeRows | length)
}' "$QUICKSTART_DIR/ledger_batches.jsonl"
```

### Using an Obsrvr GCS Archive

If your team has access to an Obsrvr GCS archive, replace the source archive environment with:

```yaml
BACKEND_TYPE: "ARCHIVE"
ARCHIVE_STORAGE_TYPE: "GCS"
ARCHIVE_BUCKET_NAME: "obsrvr-stellar-ledger-data-pubnet-data"
ARCHIVE_PATH: "landing/ledgers/pubnet"
LEDGERS_PER_FILE: "1"
FILES_PER_PARTITION: "64000"
```

GCS uses Application Default Credentials:

```bash
gcloud auth application-default login
```

## 5. Materialize to Postgres

Use `postgres-sink` when you want hot, idempotent relational tables:

```text
stellar_ledgers
stellar_transactions
stellar_operations
stellar_bronze_rows
```

Set:

```bash
export POSTGRES_DSN="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
```

Then swap `jsonl-sink` for `postgres-sink` in the pipeline. The sink writes canonical ledger, transaction, and operation tables, plus the full bronze extractor surface in `stellar_bronze_rows` as JSONB.

## 6. Materialize to DuckDB/DuckLake Files

Use `ducklake-sink` when you want local analytical files that DuckDB can scan:

```bash
export DUCKDB_EXPORT_DIR="./duckdb-ledger-batches"
```

Output layout:

```text
duckdb-ledger-batches/
  schema.sql
  network=<network>/
    ledger_range=<range>/
      ledger_<sequence>.jsonl
```

Load or query with DuckDB using the generated `schema.sql` helper and JSONL files.

## Choosing the Right Tool

| Need | Use |
| --- | --- |
| Quick one-off analysis with pipes | Nebu |
| Building or discovering community processors | `nebu-processor-registry` |
| Production-shaped source -> processor -> sink pipelines | `obsrvr-stellar-components` with flowctl |
| API-level analytics for applications and analysts | Obsrvr Lake Query API |

`obsrvr-stellar-components` is the flowctl path. It packages the shared Stellar normalization logic as services with protobuf event contracts, health checks, component metadata, and sink boundaries.

## Troubleshooting

If archive smoke works but flowctl receives zero JSONL rows, make sure this repo is using `github.com/withObsrvr/flowctl-sdk v0.1.2` or newer. Earlier SDK versions could drop the final event for bounded source ranges.

This warning is currently non-blocking:

```text
Health server error: listen tcp :<port>: bind: address already in use
```

It comes from the SDK starting a health server twice in some component paths. Components can still register healthy and deliver events.

If GCS archive access fails, verify Application Default Credentials:

```bash
gcloud auth application-default login
```

If the selected ports are busy, change `GRPC_PORT`, `PORT`, `HEALTH_PORT`, and `--control-plane-port` together in the temporary pipeline.

## Next Reads

- [Architecture](./architecture.md)
- [Event Contracts](./event-contracts.md)
- [Nomad Migration](./nomad-migration.md)
- [Rebuild Plan](./rebuild-plan.md)
