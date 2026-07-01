# index-materializer

Runs server-side SQL materializations through Quack.

The component converts the older row-moving transformer pattern into
`INSERT ... SELECT` statements that execute inside the DuckLake-owning Quack
server. This keeps reads and writes close to the DuckLake catalog and avoids
each transformer attaching the catalog independently.

## Supported Indexes

- `tx_hash_index`
  - source: `bronze.transactions_row_v2`
  - target: `index.tx_hash_index`
  - logical grain: `tx_hash`
  - rebuild key: ledger range `(START_LEDGER, END_LEDGER]`

- `contract_events_index`
  - source: `bronze.contract_events_stream_v1`
  - target: `index.contract_events_index`
  - logical grain: `(contract_id, ledger_sequence)`
  - rebuild key: ledger range `(START_LEDGER, END_LEDGER]`

## Configuration

- `QUACK_URI`, default `quack:127.0.0.1:9494`
- `QUACK_TOKEN`, required
- `QUACK_REMOTE_DB`, default `remote_lake`
- `QUACK_DISABLE_SSL`, default `true`
- `DUCKLAKE_ATTACH_NAME`, default `stellar_lake`
- `INDEX_NAME`, default `tx_hash_index`
- `START_LEDGER`, default `0`
- `END_LEDGER`, default max int64

`START_LEDGER` and `END_LEDGER` must be unsigned integers with
`START_LEDGER <= END_LEDGER`. Values are parsed as `uint64`, but the derived
tables store `ledger_sequence` as signed `BIGINT`, so the effective usable range
is `0 .. 9223372036854775807` (int64 max). Each run rebuilds the requested
ledger range by deleting existing derived rows for `(START_LEDGER, END_LEDGER]`
and inserting them again from bronze tables in the same remote transaction.

Reruns are idempotent at the row grain: replaying the same range yields the same
rows. Data-bearing columns are derived from the source, including
`contract_events_index.first_seen_at`, which is `min(closed_at)` of the ledger
(the ledger close time) and is therefore stable across rebuilds. The `created_at`
column is a `now()` materialization-audit timestamp and is refreshed on each
rebuild. This delete-then-insert shape also handles corrected or replayed source
ledgers, where a bronze row's contents change.

## Local Examples

```bash
QUACK_TOKEN=dev_secret \
INDEX_NAME=tx_hash_index \
START_LEDGER=62079999 \
END_LEDGER=62080000 \
bin/index-materializer
```

```bash
QUACK_TOKEN=dev_secret \
INDEX_NAME=contract_events_index \
START_LEDGER=62079999 \
END_LEDGER=62080000 \
bin/index-materializer
```

This component is the first step toward replacing bespoke index transformer
services with declarative SQL materialization jobs.
