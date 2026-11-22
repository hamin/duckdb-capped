# How capped is designed

`capped` gives DuckDB tables that forget: rows leave when the table gets too
old, too big or too full. This document explains why it is built the way it
is, what DuckDB itself does and doesn't offer, and the trade-offs that follow.
The README covers how to use it; [open segments](open-segments.md) covers how
the newest data is written.

## 1. The idea, and where the MongoDB analogy breaks

MongoDB's capped collections are the model: inserts append, and the oldest
data is evicted first-in-first-out when the collection hits its cap (`size`
in bytes, or `max` documents). Age-based expiry is a separate MongoDB feature
(TTL indexes) that capped collections don't support.

capped combines both, aimed at telemetry (logs, metrics, traces, events),
where data ages out and nobody queries it after N days:

| Cap | Option | Eviction |
|---|---|---|
| Time | `ttl = '7 days'` | drop data older than `now() - ttl` |
| Size | `max_bytes = '1GB'` | drop the oldest data until under budget |
| Count | `max_rows = 10000000` | drop the oldest data until under the cap |
| Any combination | several options at once | evict when any limit is breached, oldest first |

DuckDB brings things MongoDB didn't have: columnar compression (telemetry
often compresses about 10×, so a byte cap means something very different),
per-row-group min/max statistics, and vectorized scans. It does not bring
physical insertion order, per-table byte accounting, triggers, insert hooks,
or any eviction primitive. capped supplies those by owning its storage.

## 2. Prior art

| System | Mechanism | How it enforces |
|---|---|---|
| ClickHouse TTL | `TTL ts + INTERVAL 7 DAY DELETE`, column TTL, move to cold disk, rollup before delete | during background merges (about every 4 h by default); best practice is partition by day with `ttl_only_drop_parts=1`, so expiry drops whole parts |
| QuestDB | `TTL` table property | a background job drops whole partitions |
| Databricks Auto-TTL | table property on a timestamp column | an async job runs `DELETE` + `VACUUM`; timing is documented as not guaranteed |
| TimescaleDB | `add_retention_policy()` | a scheduler job per chunk; the chunk is the unit of eviction |
| MongoDB | capped collections; TTL indexes separately | synchronous FIFO at insert (capped); background job (TTL) |

What capped takes from them:

1. **Retention can be eventual.** Nobody promises synchronous expiry, and
   users accept a background cadence.
2. **Evict whole units.** ClickHouse, QuestDB and TimescaleDB all expire by
   dropping whole parts, partitions or chunks: a metadata operation that
   never rewrites live data. Row-level `DELETE` is the fallback nobody likes.
3. **Rollup before delete** and **column TTL** (both ClickHouse) are valuable
   for telemetry. They are not built yet (§9).

## 3. DuckDB ground truth

These constraints, checked against the DuckDB 2.0 documentation, shape the
design:

- **`DELETE` does not reclaim space.** Space comes back only at `CHECKPOINT`,
  and only partially: DuckDB merges row groups once about 25% of the rows in
  adjacent row groups are deleted. `VACUUM` updates statistics and reclaims
  nothing.
- **One file per database.** A `.duckdb` file holds every table, so there is
  no per-table file to measure or drop, and DuckDB exposes no exact per-table
  byte size (`duckdb_tables()` estimates rows; `database_size()` is
  database-wide).
- **No triggers and no insert hook.** An extension can add functions, parser
  extensions, optimizer rules and whole attached catalogs, but it cannot
  intercept an insert into a native table.
- **Long transactions block reclamation.** MVCC keeps deleted row groups alive
  for open readers.
- **Deletes grow the WAL** until a checkpoint.
- **Background threads in extensions have precedent** (the MySQL storage
  extension runs one), so a retention thread is reasonable.

### Why not a retention governor over native tables

The smaller alternative is an extension that stores policies and runs
scheduled `DELETE`s plus `CHECKPOINT` against ordinary tables. It would work
on any existing table, but every constraint above works against it: byte caps
can only be estimated, reclaiming space depends on checkpoint timing and the
25% rule, delete churn grows the WAL, and with no insert hook there is no way
to reject a write at the cap. capped takes the other road and owns its files.

## 4. The design: a store of immutable segments

`ATTACH 'dir' AS t (TYPE capped)` opens a directory as a DuckDB catalog. Each
table in it is an ordered list of **segments**, recorded in the table's
`manifest.json`:

- **Sealed segments** are immutable Parquet files. Nothing ever rewrites them.
- **One open segment** takes new rows. It is a small set of fragment files,
  one per commit, merged in the background; see [open segments](open-segments.md).
- **Sealing** freezes the open segment into one Parquet file and starts a new
  one. It happens automatically when the segment passes about 128 MB, 1 M rows,
  a 1 h event-time span or 32 fragment files, or on demand with `capped_seal`.
- **Retention drops whole sealed segments.** A segment is age-droppable only
  when its entire time span is past `ttl + grace`, which keeps late data safe.
  Size and count caps drop the oldest segments until the table fits. Executing
  a plan is one manifest rewrite plus one file delete per segment, with no
  tombstones, no MVCC garbage and no compaction of sealed data.
- **Byte caps are exact**, because capped owns the files and knows their sizes.
- **Hard caps.** With `on_violation = 'block'`, a commit that would leave the
  table over budget even after eviction is rejected before anything is
  published.
- **Scans** hand the segment list to DuckDB's own Parquet reader, so filter
  and limit pushdown and row-group pruning work exactly as with `read_parquet`.

The price is granularity. One row still inside `ttl` keeps its whole segment,
a size cap that needs a few bytes of relief drops a whole segment, and the
open segment can't be evicted at all. The seal limits are therefore also the
retention granularity.

Retention runs at every commit, seal and `capped_add_segment`, again at
`ATTACH`, on a background thread every `enforce_interval` (default 60 s), and
on demand with `capped_enforce`.

## 5. The policy model

```
RETENTION POLICY (per table, set in CREATE TABLE ... WITH (...))
  time_col       : column | none     -- none: age by ingest time (hidden _ingest_ts)
  ttl            : duration | none   -- age limit
  max_bytes      : bytes | none      -- footprint budget
  max_rows       : count | none      -- row cap (Mongo's max)
  grace          : duration          -- extra slack for late, out-of-order data
  evict_order    : time | insert     -- what "oldest" means
  on_violation   : evict | block     -- evict (default) or reject the commit
  search_columns : VARCHAR columns   -- opt-in BM25 over sealed segments
  vector_columns : FLOAT[N] columns  -- opt-in HNSW, vector_metric l2sq|cosine|ip
```

At least one cap is required. The Rust core validates every option at
`CREATE TABLE`, where a typo can still be fixed.

Deliberate choices:

- **Event time or ingest time.** `time_col` ages rows by a real column,
  ClickHouse's model, with `grace` protecting late arrivals. When producers'
  clocks can't be trusted, leave `time_col` out: the store keeps a hidden
  `_ingest_ts` column, stamps each commit's rows with the commit time, and ages
  by arrival. The column lives in the manifest and the Parquet files, but not
  in the DuckDB-facing table: `DESCRIBE`, `INSERT` arity and `SELECT *` behave
  as if it didn't exist, and the name is reserved. It is stamped in the flush
  `COPY` rather than with a column `DEFAULT`, because DuckDB evaluates defaults
  inside its own insert operator, which capped's write path bypasses.
- **A missing time is never an age.** A segment with no time bounds (an adopted
  file without `_ingest_ts`, or an all-NULL time column) stores no bounds at
  all. A stored `0` would read as 1970 and the next pass would delete live
  data. Such segments can only be evicted by size or count, and they sort last
  under `evict_order = 'time'`. A `time_col` whose values never read as
  timestamps is rejected at the write that would produce such a segment.
- **One eviction routine.** "Too old", "too big" and "too full" all evict
  oldest first, through the same planner.
- **Search is derived data.** `search_columns` and `vector_columns` never
  change what is stored, scanned or evicted (§7).

## 6. Rust core, thin C++ shell

DuckDB's loadable-extension C API, which pure-Rust extensions build on, does
not expose storage extensions. Registering `ATTACH ... (TYPE capped)` still
needs the C++ `StorageExtension` interface, and DuckDB's C API discussions list
storage and catalog extensions as out of scope for now.

capped follows the `lance-duckdb` precedent:

- **`crates/capped-core` (Rust)** owns policy parsing, the manifest, seal and
  eviction decisions, fragment compaction and search planning. It has no DuckDB
  dependency and is unit tested with `cargo test`.
- **The C++ shell (`src/`)** registers the storage type and catalog, owns files
  and transactions, and executes the core's plans: write fragments, seal,
  delete dropped files, persist the manifest.
- **The boundary** is a C ABI (`src/include/capped.h`) that passes only JSON
  strings and primitives. The manifest's JSON is also its on-disk format, so
  the shell persists it without reparsing.

## 7. Search on sealed segments

When a table sets `search_columns` or `vector_columns`, each sealed segment
gets a sibling `<id>.seg.idx.duckdb` file holding the searchable columns, a
DuckDB `fts` BM25 index and one HNSW index (from `vss`) per vector column.
`capped_fts` and `capped_vector_search` ask each segment's index for its own
top k and merge the lists across the table.

The index follows its segment. It is built at seal, deleted when the segment
is evicted or the table dropped, and rebuildable with
`capped_index(..., rebuild := true)`. A failed build never fails a write: the
segment stays scannable, `capped_index` reports the error, and the store stops
retrying until the next `ATTACH` or an explicit call. Indexes don't count
against `max_bytes`, since capping derived data would let index churn evict
real data; `capped_status` and `capped_tables` report their size instead. The
open segment is never indexed because it changes on every commit.

**Why sidecars, not Lance.** Lance segments would fold vector and full-text
search into the storage format. Measured on the same telemetry, Lance was 4 to
20× slower on the analytics queries and took 4.7× the bytes, and its scan reads
exactly one dataset, so segments can't be composed into one table. One Lance
dataset per table with `DELETE`/`VACUUM` would give up whole-file eviction,
the property the store exists for. DuckDB-native sidecars mean capped ships no
index format of its own, and everything version-sensitive sits behind
`src/capped_index.cpp`. On 125k-row segments an fts build takes about 0.9 s,
HNSW about 1.5 s, and either query about 20 ms.

Sharp edges found while building it, all still true:

- `fts` stems terms and drops a default English stopword list, so a query made
  only of stopwords matches nothing.
- The searching connection must `LOAD fts` too, not just the building one,
  because the index's query plan calls `stem()`.
- `fts` and `CREATE INDEX` resolve only against the current database, so each
  per-segment query runs after a `USE` of that sidecar.
- HNSW needs DuckDB's standard 256 KB block size and fails with an internal
  error that invalidates the database otherwise. capped uses 16 KB blocks
  (about 190 KB per sidecar instead of 1.8 MB) only when a table has no vector
  columns; with HNSW the floor is about 2 MB per sidecar.
- In an out-of-tree build, sqllogictest's `require fts` can never be
  satisfied, so the file reports "skipped" while testing nothing. The search
  suite `LOAD`s the extensions itself and either runs or fails loudly.

## 8. Scope

- File-backed stores only; capped has no in-memory mode.
- One writer process per store. The write lock is in-process, so other
  processes should attach `READ_ONLY`.
- No `UPDATE` or `DELETE`. Tables forget through retention.
- Linux and macOS.

## 9. Not built yet

- **Rollup before delete:** keep a downsample of old metrics after the raw
  segments go.
- **Column TTL:** drop a heavy column (a log body) earlier than the row.
- **Per-table seal limits:** the limits in §4 are fixed for now.
- **A cross-process write lock**, so two processes can't both write a store.
- **Lance, revisited**, if its analytics speed and footprint close the gap.
- **`CREATE CAPPED TABLE` sugar** as a parser extension; today the options go
  in `CREATE TABLE ... WITH (...)`.
