# LedgerBatch cadence fixtures

Production-gate fixtures use length-delimited
`stellar.components.v1.LedgerBatch` protobuf messages. A manifest records the
network, contiguous ledger range, schema/extraction versions, chunk sizes,
SHA-256 hashes, and optional object-store base URL. Fixture chunks (`*.pb`) are
ignored by Git because the real mainnet corpus is large; retain the manifest in
Git and publish the chunks at the manifest's `object_store_url`.

## Record the mainnet corpus

First run the archive -> `stellar-ledger-processor` -> `jsonl-sink` pipeline for
ledgers `62080000` through `62080999`. The pipeline in
`pipelines/local-jsonl.yaml` is the one-ledger template; set its start/end
ledger and point `JSONL_PATH` at a temporary filesystem with enough space.

Then convert the JSONL output without changing message contents:

```bash
make build

bin/ledger-fixture-recorder \
  --input /tmp/pubnet-62080000-62080999.jsonl \
  --manifest testdata/ledger-batches/pubnet-62080000-62080999.manifest.json \
  --batches-per-file 100 \
  --object-url s3://REPLACE_WITH_BUCKET/ledger-batches/pubnet-62080000-62080999/
```

The recorder refuses to overwrite existing chunks or manifests and rejects
gaps, duplicate/out-of-order ledgers, or changing network/schema/extraction
versions. Before replay, `ingest-replay` verifies each local chunk's declared
size and SHA-256 hash, then validates every decoded batch against the manifest.

Download all manifest-listed chunks beside the manifest before running a gate.
The repository intentionally does not prescribe credentials or an object-store
client; use the deployment's approved artifact mechanism and preserve the
manifest filenames exactly.
