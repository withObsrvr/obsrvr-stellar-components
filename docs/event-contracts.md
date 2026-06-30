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

The first implementation populates:

- `ledgers`
- `transactions`
- `operations`

The contract reserves typed repeated fields for:

- `contract_events`
- `contract_invocations`
- `token_transfers`
- `account_effects`

Those fields remain part of the protobuf contract even while extraction coverage is hardened.

## Idempotency

Rows use deterministic IDs:

- ledger: network + ledger sequence
- transaction: network + ledger sequence + transaction index + transaction hash
- operation: network + ledger sequence + transaction index + operation index

Sinks should treat these IDs as replay keys.
