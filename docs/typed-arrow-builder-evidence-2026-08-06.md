# Generated Typed Arrow Builder Evidence

**Date:** 2026-08-06

**Status:** Passed; generated builders replace reflected projection for four hot tables

## Purpose

The bounded parallel writer still projected most extracted rows through a
generic bridge before Arrow could consume them:

```text
stellar-extract struct -> reflection -> []any -> generic Arrow append
```

The preceding 1,000-ledger profile attributed 23.23 aggregate worker-seconds
to that projection and 8.43 foreground build-seconds to transactions,
operations, effects, and token transfers. This change generates typed Arrow
append code for those four tables directly from the authoritative Bronze table
specifications.

```text
stellar-extract struct -> generated typed Arrow append
```

Contract events retain their existing direct builder. The other typed tables
continue through the generic path, which remains the parity oracle and rollback
path.

## Generation and schema contract

`go generate ./pkg/bronze/columnar` reads `bronze.TypedTableSpecs` and the Arrow
layout derived from each table's DDL. It emits direct appenders for:

- `transactions_row_v2`
- `operations_row_v2`
- `effects_row_v1`
- `token_transfers_stream_v1`

The generated code preserves column order, nullability, integer narrowing,
microsecond timestamps, JSON string-slice encoding, extraction-version
defaults, and raw transaction XDR overrides. A direct builder is admitted only
when its Arrow schema is logically equal to the current Bronze DDL. Generic and
direct rows cannot be mixed in one table writer.

Unit tests compare generated Arrow records with generic reflected projection
using Arrow record equality. An artifact-level test then writes reversed rows
through both paths and requires identical Parquet hashes, sizes, row counts,
ranges, and schema fingerprints.

## 1,000-ledger result

Both runs used mainnet range `62,080,000-62,080,999`, the Obsrvr GCS archive,
Arrow/Snappy, one process, four extraction workers, eight ledgers in flight,
two projection workers, two Parquet encoders, four pending row groups, and
16,384-row groups.

| Measurement | Reflected hot tables | Generated hot tables | Change |
|---|---:|---:|---:|
| Wall time | 28.391s | 26.795s | -5.6% |
| Ledgers/s | 35.223 | 37.321 | +6.0% |
| Output rows/s | 285,673 | 302,688 | +6.0% |
| Reflected projection | 23.230 worker-s | 5.053 worker-s | -78.3% |
| Foreground append | 25.261s | 21.847s | -13.5% |
| Parquet encode | 22.487 worker-s | 21.390 worker-s | -4.9% |
| Peak RSS | 1,354,864 KiB | 1,317,248 KiB | -2.8% |

Each attempt read 1,733,356,100 source bytes and produced 8,110,413 output
rows in 22 files totaling 1,115,553,147 bytes. The normalized file-identity
digest was identical on both paths:

```text
0872bc0159df8320b4ec979b0f3c959fdc1aed0a3ea419dfff23dc179160ae5d
```

That digest covers every table name, Parquet SHA-256, byte size, row count,
ledger range, and schema fingerprint after excluding only attempt-local URIs.

## Per-table effect

| Table | Rows | Old build | Generated build | Sort | Encode |
|---|---:|---:|---:|---:|---:|
| transactions | 310,805 | 3.915s | 3.487s | 0.197s | 4.753s |
| operations | 704,848 | 2.332s | 1.181s | 0.614s | 2.151s |
| effects | 524,134 | 0.882s | 0.538s | 0.140s | 0.940s |
| token transfers | 879,574 | 1.305s | 0.725s | 0.981s | 1.624s |
| contract events | 4,152,056 | 5.070s | 5.248s | 4.567s | 9.841s |

The four newly generated builders reduce their combined foreground build time
from 8.43s to 5.93s. Transactions improve less than the other three because
their 636 MB of raw envelope, result, and meta XDR still must be copied into
Arrow buffers and encoded.

## Rejected placement: sorting in extraction workers

An intermediate implementation sorted direct rows inside concurrent extraction
workers. It reduced foreground append to 17.04s, but forced canonical sorting
to compete with extraction and Parquet work. Wall time regressed to 44.94s,
pipeline wait rose to 14.67s, and throughput fell to 22.25 ledgers/s.

Canonical sorts therefore remain on the ordered writer path. This keeps worker
CPU bounded and preserves exact artifact order without turning extraction into
another contended scheduling domain.

## Host-scaling observation

Two 500-ledger processes did not improve this result. With two extraction
workers each, the host reached 33.11 aggregate ledgers/s while the writers
waited 13.79-17.61s for ordered input. Increasing each process to four
extraction workers reduced append to 8.37-8.77s per worker, but concurrent GCS
source time rose to 35.29-36.88s and aggregate throughput fell to 26.58
ledgers/s.

The direct builders are therefore not the limiting factor in that probe.
Multi-process scaling now requires a source-side experiment—bounded concurrent
object fetch with local range caching or independently provisioned workers—so
source acquisition can feed each file writer without host-level contention.
The recommended local measurement remains the single-process profile above.

## Remaining path to the fleet target

At 37.32 ledgers/s, one equivalent worker would project a fixed 63,804,680
ledger history in approximately 19.8 days. Reaching 1,000 ledgers/s still
requires horizontal shard workers and about 27 ideal equivalents before
source, coordination, upload, registration, skew, and historical-density
losses.

The next worker optimization should target the largest measured work rather
than add generic concurrency:

1. remove or replace the 4.57s contract-event canonical sort only after proving
   extractor order is deterministic across retries;
2. reduce contract-event record construction and Parquet encode cost;
3. evaluate transaction XDR buffer ownership and row-group sizing; and
4. test source prefetch/caching separately from encoding so network variance
   does not obscure CPU improvements.

Item 4 is complete. Archive prefetch, not encoding, was the dominant limit at
the SDK defaults, and the two-process regression above is a shared source
bandwidth ceiling rather than CPU contention. Measured under tuned prefetch,
this range reaches 39.45 ledgers/s cold and 42.17 ledgers/s warm, and the
four-worker, two-encoder knee re-validated network-free. Items 1 through 3
remain open and should now be measured with `--stage` and a warm source cache
so network variance is excluded. See
[`bounded-source-prefetch-evidence-2026-08-06.md`](bounded-source-prefetch-evidence-2026-08-06.md).
