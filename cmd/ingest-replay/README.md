# ingest-replay

`ingest-replay` removes archive download and extraction CPU from the ingest
latency experiment. It verifies a hashed fixture manifest, streams the recorded
`LedgerBatch` protobufs directly to `BronzeIngestService`, and waits for the
matching acknowledgement before scheduling the next ledger.

The latency gate uses scheduled arrival-to-ack time. The JSON output also
reports RPC send-to-ack latency and schedule lag separately. This distinction
matters when one slow acknowledgement carries into the next ledger's arrival
window.

## Profiles

| Profile | Cadence | Jitter | Default duration | Special behavior |
|---|---:|---:|---:|---|
| `live` | 5s | +/-250ms | 1h | 400ms live SLO |
| `future` | 2s | +/-100ms | 1h | non-blocking future-cadence experiment |
| `catch-up` | 5s after burst | +/-250ms | 1h | first 100 ledgers saturated and SLO-exempt |
| `checkpoint` | 5s | +/-250ms | 1h | requires three new successful idle checkpoints |
| `maintenance` | 5s | +/-250ms | 1h | intended to run alongside maintenance |
| `backfill` | saturated | 0 | full requested corpus | one bounded contiguous range in flight |
| `custom` | required | 0 | full requested corpus | all scheduling values are explicit |

An explicit `--count` takes precedence over profile duration. Jitter is
deterministic for a given `--seed` (default `1`). A catch-up burst is still
fully measured, but its saturated ledgers are excluded from the live latency
gate; the normal-cadence tail is gated.

## Run

The ingest service currently uses plaintext transport plus the same
`x-ingest-token` shared token as `ducklake-sink`. Prefer `QUACK_TOKEN` over the
`--token` flag so the credential does not appear in a process listing.

```bash
QUACK_TOKEN="$QUACK_TOKEN" bin/ingest-replay \
  --fixtures testdata/ledger-batches/pubnet-62080000-62080999.manifest.json \
  --endpoint 127.0.0.1:9495 \
  --metrics-url http://127.0.0.1:8088/metrics \
  --profile checkpoint \
  --count 720 \
  --summary /tmp/ingest-replay-summary.json \
  --results /tmp/ingest-replay-results.jsonl
```

The process exits nonzero for an RPC error, acknowledgement mismatch, missing
requested fixture, configured latency breach, or insufficient idle checkpoint
count. Its summary includes median, p95, p99, maximum, mean, and over-budget
ledger sequences for both RPC and scheduled-arrival latency.

The backfill profile uses the additive micro-batch RPC. It flushes on the first
reached bound: `--microbatch-ledgers` (default 25),
`--microbatch-max-encoded-bytes` (default 256 MiB), or
`--microbatch-max-bronze-rows` (default 500,000). The JSON summary records the
effective minimum and maximum ledgers per transaction, throughput, encoded
bytes, Bronze rows, and range RPC latency.

```bash
QUACK_TOKEN="$QUACK_TOKEN" bin/ingest-replay \
  --fixtures testdata/ledger-batches/pubnet-62080000-62080999.manifest.json \
  --endpoint 127.0.0.1:9495 \
  --profile backfill \
  --microbatch-ledgers 25 \
  --microbatch-max-encoded-bytes 268435456 \
  --microbatch-max-bronze-rows 500000 \
  --count 1000 \
  --summary /tmp/backfill-summary.json \
  --results /tmp/backfill-results.jsonl
```

This direct replay excludes archive fetch and extraction CPU. Treat its rate as
a sink benchmark, not an end-to-end backfill forecast.

`--offset` selects the next fixture after a controlled restart:

```bash
QUACK_TOKEN="$QUACK_TOKEN" bin/ingest-replay \
  --fixtures testdata/ledger-batches/pubnet-62080000-62080999.manifest.json \
  --endpoint 127.0.0.1:9495 \
  --profile custom \
  --cadence 0 \
  --offset 720 \
  --count 1
```

## Gate tiers

The deterministic 30-ledger scheduling/protocol smoke test runs with:

```bash
make test-ingest-replay-smoke
```

The local release gate owns a Quack server, runs maintenance during real
cadence, requires idle checkpoints, restarts the same catalog, resumes with the
next fixture, checks watermark range/count/gaps and ledger-batch parity, then
compares the logical catalog with a no-controller baseline:

```bash
CADENCE_GATE_FIXTURES=testdata/ledger-batches/pubnet-62080000-62080999.manifest.json \
make test-cadence-gate
```

The default is 720 ledgers at real five-second cadence. Use
`CADENCE_GATE_LEDGER_COUNT=30`, a shortened cadence, and
`CADENCE_GATE_COMPARE_BASELINE=false` only for local smoke iteration; that is
not release evidence.
