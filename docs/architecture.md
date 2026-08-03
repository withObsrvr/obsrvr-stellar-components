# Architecture

`obsrvr-stellar-components` separates acquisition, Stellar interpretation,
storage materialization, catalog ownership, and serving replication.

The canonical current status and open production gates live in
[`current-state.md`](current-state.md).

## Production topology

```text
raw-ledger-source
  -> stellar-ledger-processor
  -> ducklake-sink (DUCKLAKE_MODE=ingest-rpc)
       -> BronzeIngestService
          quack-ducklake-server (primary DuckLake owner)
            ├─ ducklake-maintenance
            ├─ index-materializer (server-side SQL through Quack)
            └─ ducklake-replica-sync
                 -> quack-ducklake-server (serving replica owner)
                      -> stellar-query-api / obsrvr-gateway
```

`ingest-rpc` is the intended production write path. It sends each normalized
ledger batch over an ordered gRPC stream to the catalog-owning server, which
stages typed rows in native DuckDB memory tables and commits one DuckLake
transaction per ledger. The server acknowledges a ledger only after its data
and ingest watermark commit.

Quack remains the shared server-side SQL surface for maintenance,
materialization, replication, and query access. The sink's staged-Parquet
`quack` mode remains a fallback; `embedded` mode remains for development and
tests.

## Boundaries

- Sources acquire raw ledger data and emit `stellar.ledger.v1`.
- Processors own Stellar semantics, XDR decoding, and normalized row extraction.
- Sinks transport or materialize normalized batches; they do not parse Stellar
  XDR.
- `quack-ducklake-server` exclusively owns each production DuckLake attachment.
- Maintenance, materialization, and replica synchronization execute bounded SQL
  against the owning server rather than attaching the same catalog independently.
- Query services read from serving replicas, keeping user load off the primary.

## Batch and commit contract

The primary processor emits one `stellar.ledger.batch.v1` event per ledger. The
protobuf payload is `stellar.components.v1.LedgerBatch`.

A ledger batch is the storage commit boundary. The ingest service processes one
ledger at a time and writes typed bronze rows plus `ingest_watermarks` in the
same DuckLake transaction. On a failed or uncertain commit, retry uses
ledger-scoped delete-then-insert replacement. Replaying a ledger therefore
converges instead of appending duplicates.

Ingest and explicit catalog checkpoints share one server-side writer
coordinator. Manual checkpoints use a dedicated DuckDB connection and only run
when `TryLock` proves ingest is idle. Catalog-WAL checkpoints target DuckLake's
hidden `__ducklake_metadata_<attach-name>` DuckDB database; checkpointing the
logical DuckLake attachment invokes data-file maintenance instead.

DuckLake snapshots are the durable catalog commit boundary and the change-feed
source for replicas. `ducklake-replica-sync` checkpoints by snapshot, discovers
changed ledgers, and rebuilds their current rows in a serving catalog. Derived
indexes and serving replicas are rebuildable; primary bronze remains
authoritative.

## Operational contract

- Use one Stellar network per catalog.
- Keep `SNAPSHOT_RETENTION` longer than worst-case replica checkpoint lag.
- Pair `DUCKLAKE_INLINE_ROW_LIMIT` with maintenance cadence. The sub-400ms
  profile uses limit `256` and maintenance every 1–5 minutes.
- Snapshot expiry does not reclaim physical storage; file cleanup is a separate,
  deliberately deferred operation.
- Production file-backed Quack deployments currently require explicit insecure
  filesystem access and therefore need network isolation or external TLS
  termination.

## Nix flake

The flake provides Go, gopls, protobuf tooling, and DuckDB dependencies. Use
`nix develop` before running repository commands.
