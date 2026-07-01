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
  - idempotency key: `tx_hash`

- `contract_events_index`
  - source: `bronze.contract_events_stream_v1`
  - target: `index.contract_events_index`
  - idempotency key: `(contract_id, ledger_sequence)`

## Configuration

- `QUACK_URI`, default `quack:127.0.0.1:9494`
- `QUACK_TOKEN`, required
- `QUACK_REMOTE_DB`, default `remote_lake`
- `DUCKLAKE_ATTACH_NAME`, default `stellar_lake`
- `INDEX_NAME`, default `tx_hash_index`
- `START_LEDGER`, default `0`
- `END_LEDGER`, default max int64

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
