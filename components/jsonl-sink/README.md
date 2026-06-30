# jsonl-sink

Consumes `stellar.ledger.batch.v1` events and appends protobuf JSON to a JSONL file for local inspection and golden fixtures.

Environment:

- `JSONL_PATH`, default `ledger_batches.jsonl`
- `PORT`, default `:50052`
- `HEALTH_PORT`, default `8089`
