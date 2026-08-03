# Ingest-RPC Latency Diagnosis — 2026-08-03

## Symptom

The 1,000-ledger correctness gate passed, but the recorded sink latency did not
meet a hard 400ms upper bound:

```text
replay:   median 296ms, p95 414ms, p99 864ms, max 1.923s
baseline: median 297ms, p95 421ms, p99 1.088s, max 6.598s
```

## Feedback loop

The captured replay and baseline logs were joined by ledger sequence across:

- processor row-count logs
- sink send-to-ack timing
- server receive-to-ack timing
- server staging, preface, transfer, and commit phases

A fresh A/B harness then ingested the same 650-ledger range into new catalogs,
changing only DuckDB's `checkpoint_threshold`. A 100-ledger inverse test used a
smaller threshold to verify directionality.

## Findings

### 1. DuckDB catalog auto-checkpoints are the primary tail source

DuckDB's default `checkpoint_threshold` and `wal_autocheckpoint` are `16 MiB`.
The DuckLake catalog grew to approximately 199 MiB over the 1,000-ledger run.
Automatic catalog checkpoints execute synchronously inside `tx.Commit()`, so the
ingest ledger that crosses the threshold pays the checkpoint latency.

Evidence from the original baseline:

```text
ledger 62080629
sink:          6.598s
server total:  6.588s
staging:          38ms
preface:          15ms
transfer:         34ms
commit:         6.460s
```

The extreme time is entirely inside DuckLake commit, not gRPC or the sink.

The controlled 650-ledger A/B result:

| Setting | Median | p95 | p99 | Max | >400ms |
|---|---:|---:|---:|---:|---:|
| DuckDB default (`16 MiB`) | 296ms | 400ms | 592ms | 2.504s | 33 |
| `checkpoint_threshold=1GB` | 288ms | 369ms | 418ms | 918ms | 13 |

After excluding the first 150 warm-up ledgers:

| Setting | Median | p95 | p99 | Max | >400ms |
|---|---:|---:|---:|---:|---:|
| Default | 285ms | 362ms | 705ms | 2.504s | 17/500 |
| `1GB` | 277ms | 327ms | 366ms | 417ms | 2/500 |

The inverse 100-ledger test with `checkpoint_threshold=1MB` made the result much
worse: median `381ms`, p95 `835ms`, p99 `1.365s`, max `1.372s`, with 39/100
commits over 400ms. Commit alone reached p95 `599ms`.

The bidirectional threshold experiment makes auto-checkpointing causal, not
merely correlated.

### 2. gRPC/client overhead is stable and not responsible for tails

Across both original 1,000-ledger runs, sink time outside the server was:

```text
median 16–17ms
p95    24–26ms
p99    29–30ms
max    35–37ms
```

Sink latency correlated almost perfectly with server latency (`r ≈ 1.0`). The
6.598s maximum had only 10ms outside the server.

### 3. Ledger transaction/operation counts do not explain the spikes

Sink latency had effectively no correlation with transaction or operation count
(`r` between approximately `-0.08` and `+0.03`). Cross-run sink-latency
correlation for the same ledgers was only `0.23`, and only one ledger exceeded
one second in both original runs. Large spikes are therefore mostly runtime
checkpoint events rather than deterministic heavy-ledger behavior.

### 4. Concurrent DuckLake maintenance was not the cause

Replay and never-maintained baseline distributions were nearly identical. The
baseline had the larger maximum. Maintenance completed near the beginning of
the replay and produced no ingest retry/conflict logs.

### 5. The ordinary budget remains narrow

Typical server work is approximately:

```text
unlogged decode + cleanup: 64–69ms
native staging:            51–53ms
preface:                    13–14ms
staged transfer:            36–38ms
commit:                     93–102ms median
client/gRPC:                16–17ms
```

That leaves limited headroom beneath 400ms. Deferring automatic checkpoints
makes steady backfill fit comfortably at p95/p99, but it does not prove a hard
maximum, especially during startup and eventual checkpoint work.

## Implemented mitigation

`quack-ducklake-server` now accepts `DUCKDB_CHECKPOINT_THRESHOLD`. The production
Nomad template sets it to `1GB` as an interim control. The setting is applied
before the ingest connection starts and before configuration is locked.

A kill/restart/replay chaos run with `1GB` passed catalog parity, typed gates,
watermark gap checks, and concurrent maintenance.

Increasing the threshold does not weaken per-commit WAL durability, but it
allows a larger WAL and moves checkpoint cost later. Recovery time, disk use,
and eventual checkpoint latency still need operational bounds.

The implementation project for the remaining work is specified in
`docs/checkpoint-latency-production-gate-plan.md`.

## Remaining work before a hard latency SLO

1. Design an explicit checkpoint policy that keeps checkpoint work off the hot
   ingest commit path, or move the DuckLake catalog to a backend without local
   DuckDB file checkpoint stalls.
2. Measure crash recovery and checkpoint duration at the chosen maximum WAL
   size.
3. Run a cadence-shaped test rather than only saturated backfill; production
   ledger cadence may reduce filesystem contention but cannot eliminate a
   synchronous auto-checkpoint.
4. Add percentile/histogram telemetry for decode, staging, transfer, commit,
   send-to-ack, checkpoint duration, and WAL/catalog size.
5. Treat sub-400ms as a typical/steady target until a test demonstrates the hard
   upper bound across checkpoint and recovery cycles.

Raw A/B evidence:

```text
/tmp/obsrvr-ingest-latency-default
/tmp/obsrvr-ingest-latency-checkpoint1g
/tmp/obsrvr-ingest-latency-checkpoint1m
/tmp/obsrvr-ingest-rpc-checkpoint1g-chaos
```
