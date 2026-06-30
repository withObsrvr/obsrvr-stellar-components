# Architecture

`obsrvr-stellar-components` separates acquisition, Stellar interpretation, and storage materialization.

```text
raw-ledger-source@0.2.2
  -> stellar-ledger-processor
    -> jsonl-sink
    -> postgres-sink
    -> ducklake-sink
```

## Boundaries

- Sources acquire raw ledger data and emit `stellar.ledger.v1`.
- Processors own Stellar semantics, XDR decoding, and normalized row extraction.
- Sinks materialize typed batch payloads and avoid parsing Stellar XDR.

## Batch Contract

The primary processor emits one `stellar.ledger.batch.v1` event per ledger. The protobuf payload is `stellar.components.v1.LedgerBatch`.

Batch events are the commit boundary for replay and idempotency. Sinks should upsert or overwrite by stable row IDs derived from network, ledger sequence, transaction index, operation index, and row kind.

## Nix Flake

The flake provides:

- Go
- gopls
- protobuf compiler
- protoc-gen-go
- make

Use `nix develop` before running repository commands.
