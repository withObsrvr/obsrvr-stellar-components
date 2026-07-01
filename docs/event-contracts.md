# Event Contracts

## Raw Ledger Input

```text
Event.Type: stellar.ledger.v1
Payload:    stellar.v1.RawLedger
Producer:   raw-ledger-source@0.2.2
```

`stellar.v1.RawLedger` is defined by `flow-proto`.

## Normalized Ledger Batch

```text
Event.Type: stellar.ledger.batch.v1
Payload:    stellar.components.v1.LedgerBatch
Producer:   stellar-ledger-processor
```

`LedgerBatch` is defined in `proto/stellar/components/v1/ledger_batch.proto`.

The compatibility fields populate:

- `ledgers`
- `transactions`
- `operations`

The full bronze extractor surface is carried in:

- `bronze_rows`

Each `BronzeRow` has the destination table name and a JSON serialization of the matching `stellar-extract` row type. This covers the row families used by `stellar-history-loader` and `stellar-postgres-ingester`, including effects, trades, accounts, offers, trustlines, account signers, claimable balances, liquidity pools, config settings, TTL entries, native balances, contract events, contract data, contract code, contract creations, token transfers, evicted keys, and restored keys.

`ducklake-sink` preserves that envelope in `bronze_rows` and also materializes the same rows into typed `bronze.*` tables compatible with the `stellar-history-loader` bronze schema.

## Idempotency

Rows use deterministic IDs:

- ledger: network + ledger sequence
- transaction: network + ledger sequence + transaction index + transaction hash
- operation: network + ledger sequence + transaction index + operation index
- bronze row: network + ledger sequence + table name + row ordinal + row JSON hash

Sinks should treat these IDs as replay keys.
