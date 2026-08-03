# From 25 seconds to 270 milliseconds: five walls on the way to a fast DuckLake write path

*Draft for external publication — review before publishing. Every number in
this post comes from instrumented runs on 2026-07-23 against Stellar pubnet
ledgers 62080000–62080005 (DuckDB 1.5.4, stable ducklake extension, Quack
from core_nightly, single Linux host).*

---

I run a Stellar data platform on what I think of as the boring-sharp stack:
DuckDB, DuckLake, and HashiCorp Nomad. One process owns the DuckLake catalog
attachment and serves it over the new [Quack protocol](https://duckdb.org/2026/05/12/quack-protocol);
everything else — ingestion, maintenance, replication — is a client. Stellar
closes a ledger roughly every 5 seconds (the network is aiming for 2), and a
"ledger" here is real work: on the mainnet range I test with, each one carries
~350 transactions, ~700 operations, and ~5,000 contract events, landing across
roughly twenty bronze tables.

My requirement was blunt: **at most 400ms from ledger arrival to queryable in
the lake.** When I started measuring, I was at ~25 seconds per ledger — worse
than useless for live tracking, since the queue grows faster than it drains.

One day of measurement-driven work later:

| | ledger → queryable |
|---|---|
| Start | ~25s (falling behind the chain) |
| Wall 1 down | ~13s |
| Wall 2–3 down | ~2.7s |
| Wall 4 down | ~1.1s |
| **Wall 5 down** | **232–318ms** |

None of it required exotic infrastructure. Every step was: instrument, find
the actual cost, delete it. This post is the map of the five walls, because I
suspect a lot of people will hit them once DuckDB 2.0 ships the Quack–DuckLake
catalog integration this fall and streaming writes into DuckLake become a
mainstream idea.

## Wall 1: my data was traveling as source code

The original write path rendered every row into SQL text — a giant
`INSERT INTO … VALUES (…), (…), …` script, ~31 MiB per ledger, shipped over
Quack's `query()` function and parsed by the server.

Two micro-benchmarks told me everything:

- Inserting 15,144 rows into a DuckLake table via `INSERT … SELECT`
  (columnar path): **25ms**, commit included.
- A transaction touching 10 tables: **46ms**.
- The same data as a 31 MiB SQL script: **~24 seconds**.

The engine was doing ~100ms of real work; the other ~99% was the DuckDB
parser chewing through nine thousand inlined VALUES tuples. A SQL parser
builds an AST node per literal — it is the worst possible way to hand data to
a columnar engine. Postgres users never pay this tax because the client
protocol ships rows as *data*; I had accidentally built a pipeline that ships
rows as *programs*.

**Lesson 1: data should never travel as source code.**

## Wall 2: I was writing the same data three times

Measuring the script's composition: 13.8 MiB was the full ledger serialized
as one JSON blob (an audit envelope), 8.3 MiB was every row serialized
*again* as per-row JSON, and only ~9 MiB was the actual typed rows anyone
queried. Nothing read the first two — the upstream archive is the replay
source of truth. Deleting the envelope writes cut every script by ~70% and
took the path from 25s to ~13s without touching architecture.

Worth auditing in any pipeline: how many times does the same byte get
persisted, and who actually reads each copy?

## Wall 3: Parquet as the wire format — and the per-row Exec surprise

The structural fix for Wall 1 was staging each ledger's rows as per-table
Parquet files and shrinking the script to ~20 KiB of orchestration:
`BEGIN; DELETE …; INSERT INTO bronze.x SELECT … FROM read_parquet('…'); COMMIT`.
This is the same trick a Postgres→DuckLake flusher uses with `postgres_scan`:
the SQL is a delivery note; the data rides a bulk reader.

Commit time collapsed from 13s to ~1.6s. But the *staging* step — decoding
rows and inserting them into a local DuckDB before `COPY TO parquet` — cost
~4s, and here's the instructive part: I assumed JSON decoding dominated, so I
parallelized it across cores. It saved 0.4s out of 4. The real cost was
**per-row `Exec` calls through database/sql at ~0.4ms each** — 9,000 round
trips. Batching inserts into 128-row chunks (same binder, identical value
semantics) collapsed staging from 3.7s to 0.9s.

**Lesson 2: measure before believing your own story.** My parallel-decode fix
was architecturally elegant and empirically irrelevant.

## Wall 4: DuckLake tables charge per statement

At ~2.7s per ledger I moved the commit in-process: a small gRPC ingest
service on the catalog-owning server (protobuf in, one transaction per
ledger). First attempt: run the same 128-row chunked inserts directly against
the DuckLake tables. Result: **2.5 seconds** — slower than the Parquet path
it replaced.

Chunked inserts into *native* DuckDB tables are fast; the same statements
against *DuckLake* tables pay per-statement catalog overhead, ~70 times per
ledger. The fix is the same shape at a different layer: stage into native
in-memory tables first, then move each table with a single engine-native
`INSERT … SELECT` (~35ms total). One subtlety worth knowing: a DuckDB
transaction may write to only one attached database, so staging must fully
commit (memory-only) before the DuckLake transaction begins.

That left a phase profile of: staging ~0.9s, transfer ~35ms, preface ~15ms —
and a stubborn **commit of ~1.6s** that had been hiding under the transport
noise all along.

## Wall 5: inlining is a hot buffer, not a bulk path

DuckLake's *data inlining* stores small inserts directly in the catalog
database instead of writing Parquet — instantly durable, instantly queryable,
no small-file problem, flushed to Parquet later by a maintenance job. It is a
wonderful feature and I had configured it catastrophically.

Chasing "no Parquet on the hot path," I had raised the inline row limit to
20,000 so every per-ledger insert inlined. Then I measured the commit:

| `data_inlining_row_limit` | commit per ledger | parquet files per ledger |
|---:|---:|---:|
| 20,000 (everything inlined) | ~1.7s | 0 |
| 1,024 | ~0.55s | ~1 |
| 256 | **~85ms** | ~7 |

Inlined commits cost **~0.18ms per row** — the extension writes inlined rows
into the catalog database row-wise. That's why DuckDB's default limit is 10:
inlining is engineered as a hot buffer for *small* writes, not a bulk path.
The limit is really a **tiering knob**: small tables (watermarks, metadata)
inline; big tables (events, operations) take the fast columnar Parquet path —
both inside the same atomic commit, both queryable the instant it lands, and
both visible to `table_changes` CDC immediately (I verified inlined,
not-yet-flushed rows appear in the change feed — downstream consumers don't
wait for the flush).

The cost of limit 256 is ~7 small Parquet files per ledger, which is why the
knob is *paired* with the maintenance cadence: a `ducklake_merge_adjacent_files`
pass every couple of minutes keeps the file count flat. Widen the limit and
the interval together, never independently.

**Lesson 3: read the defaults as design documentation.** A default of 10 was
the extension's author telling me what inlining is for.

## The final path

```
processor ──protobuf──▶ sink ──gRPC (one ledger in flight)──▶ ingest service
                                                                    │
    decode rows → typed values (parallel, JSON parsed once)   ~60ms │
    Appender → native staging tables (typed, zero SQL)        ~45ms │
    one DuckLake txn:                                               │
      watermark check → fresh ledger? skip replay-DELETEs           │
      INSERT..SELECT per table from staging                   ~35ms │
      tiered COMMIT (inline limit 256)                        ~95ms │
                                                                    ▼
                                   queryable + CDC-visible: 232–318ms
```

The last staging win came from `duckdb.NewAppenderWithColumns` — rows enter
the engine as typed columnar data with no SQL anywhere (staging went from
0.9s to ~45ms), and from noticing my decoder parsed each row's JSON twice.

Crash semantics stayed boring on purpose: the watermark row commits in the
same transaction as the data; a fresh ledger skips replay deletes; any failed
or *uncertain* commit forces delete-then-insert on retry, so a half-known
outcome can't double-write. The chaos harness kills the server mid-stream and
requires the replayed catalog to be byte-identical to one that never crashed.
It passes.

## What I'd tell you to take away

1. **Data must never travel as SQL text.** Parquet, Arrow, protobuf, a bulk
   reader — anything but VALUES literals.
2. **The per-row and per-statement taxes are where streaming lakehouse
   latency actually lives** — not in the columnar engine, which is absurdly
   fast once you hand it data properly.
3. **Inlining is DuckLake's built-in hot buffer.** Tier with the limit, pair
   it with maintenance cadence, and you get Postgres-hot-buffer semantics
   with one system and one source of truth.
4. **A legible stack makes the optimization loop fast.** Every wall fell to
   one instrumented rerun on a single box — no cluster, no profiler safari.
   Five walls in one day is a property of the stack, not heroics.

DuckDB 2.0's announced Quack–DuckLake integration (a remote DuckDB server as
the DuckLake catalog, "especially with inlining") will likely replace my
custom ingest RPC with something upstream — and everything above about
inlining economics, tiering, and per-statement costs will matter *more* once
streaming writes are one `ATTACH` away for everyone.

*The chaos harness, components, and Nomad jobs referenced here live in the
obsrvr-stellar-components repo.*
