# capped: bounded storage for DuckDB

Tables that forget. `capped` is a DuckDB storage extension for telemetry
(logs, metrics, traces, events). Rows append in, and the oldest data goes
away when a table gets too **old**, too **big**, or too **full**.

```sql
-- Attaching applies retention, then a background thread keeps applying it
-- (enforce_interval, default 60s).
ATTACH 'telemetry-store' AS tlog (TYPE capped);

CREATE TABLE tlog.main.events (
    ts TIMESTAMPTZ, severity VARCHAR, service VARCHAR, body VARCHAR
) WITH (time_col = 'ts', ttl = '7 days', max_bytes = '10GB');

-- With no time_col, rows age from when they arrived (a hidden column the store keeps).
CREATE TABLE tlog.main.ingest_log (severity VARCHAR, body VARCHAR) WITH (ttl = '7 days');

INSERT INTO tlog.main.events SELECT * FROM read_parquet('ingest/*.parquet');

SELECT count(*) FROM tlog.main.events WHERE severity = 'error';  -- Parquet pushdown as usual

SELECT * FROM capped_status('tlog', 'events');  -- segments, sizes, caps
```

Available from DuckDB's [community extensions](https://duckdb.org/community_extensions/extensions/capped)
for DuckDB **v2.0.1** on Linux and macOS.

## How it works

Each table is an ordered list of immutable Parquet files (**segments**) plus
one open segment that takes new rows. When the open segment reaches about
128 MB, 1 M rows or a 1 hour span, it is sealed into one file and a new one
starts. Retention then works by **deleting whole sealed files**:

- a segment is dropped for age only when *all* of its rows are older than
  `ttl` (plus `grace`), so late-arriving data is safe;
- `max_bytes` and `max_rows` drop the oldest segments until the table fits;
- nothing that survives is read, rewritten or compacted, so the cost is one
  file delete per dropped segment, and byte caps are exact.

Compare a native DuckDB table cleaned with scheduled `DELETE`s: the delete
reclaims no space by itself, `CHECKPOINT` only gets it back once about 25% of
the rows in neighbouring row groups are gone, and `VACUUM` reclaims nothing.
capped avoids tombstones and checkpoint timing by making the file the unit of
forgetting.

The price is granularity. Retention forgets a segment at a time: one row
still inside `ttl` keeps its whole segment, and a size cap that needs a few
bytes back drops a whole segment. The open segment is never evicted, so a
table can sit over budget until the next seal; `on_violation = 'block'`
rejects such commits instead.

The full design, prior art and trade-offs are in
[docs/design.md](docs/design.md).

## Install

capped is a [DuckDB community extension](https://duckdb.org/community_extensions/extensions/capped).
In DuckDB v2.0.1 on Linux or macOS (arm64 or x86_64):

```sql
INSTALL capped FROM community;
LOAD capped;
```

To run your own build instead, [build it from source](#building-from-source)
and load it unsigned: `duckdb -unsigned -c "LOAD 'build/release/extension/capped/capped.duckdb_extension'"`.
A source build targets the DuckDB version pinned in the `duckdb` submodule
(currently v2.0.1), so load it with that version's CLI.

## SQL reference

| Call | What it does |
|---|---|
| `ATTACH 'dir' AS t (TYPE capped [, enforce_interval '60s'] [, READ_ONLY])` | Open a store. Applies retention, then keeps applying it on a background thread (`'0'` turns the thread off). A `READ_ONLY` attach writes nothing at all and refuses inserts, DDL and `capped_add_segment`; use it for every process except the one writer. |
| `CREATE TABLE t.main.x (...) WITH (...)` | Create a capped table. At least one cap is required. Options below. |
| `INSERT INTO t.main.x ...` | Append rows (`VALUES` or `SELECT`). They are written at commit. |
| `SELECT ... FROM t.main.x` | Scan sealed and open segments through DuckDB's Parquet reader. |
| `capped_status('t', 'x')` | One row per segment: rows, bytes, time range, index state, fragment count, plus the table's caps. |
| `capped_tables('t')` | One row per table: segment counts, rows, bytes, time range, caps, bytes written, pending deletes, maintenance errors. |
| `capped_seal('t', 'x')` | Seal the open segment now and start a new one. |
| `capped_enforce('t' [, 'x'])` | Apply retention now. Returns what was dropped and sealed per table. |
| `capped_add_segment('t', 'x', 'x/9.seg.parquet')` | Adopt a Parquet file in the table's directory as a sealed segment (fast bulk loading; see below). |
| `capped_fts('t', 'x', 'query' [, k := 10])` | BM25 full-text search over the sealed segments. |
| `capped_vector_search('t', 'x', vec [, k := 10] [, vector_column := 'c'])` | Nearest-neighbour search over an indexed vector column. |
| `capped_index('t', 'x' [, rebuild := true])` | Build missing search indexes (or rebuild all). One row per segment, with any `build_error`. |
| `capped_version()` | The capped version. |
| `SET capped_pending_delete_grace = '5min'` | Testing knob: how long replaced files wait before deletion (see [on-disk layout](#on-disk-layout)). |

The table functions are called as `SELECT * FROM capped_status('t', 'x')`.

`CREATE TABLE` options:

| Option | Meaning |
|---|---|
| `time_col` | The timestamp column rows age by. Omit it to age rows by arrival time. |
| `ttl` | Maximum age, e.g. `'7 days'`. |
| `max_bytes` | Size cap, e.g. `'10GB'`. |
| `max_rows` | Row-count cap. |
| `grace` | Extra time before a segment counts as expired, for late data. |
| `evict_order` | Which segments size and count caps drop first: `'time'` (oldest data, the default) or `'insert'`. |
| `on_violation` | `'evict'` (default) drops old segments; `'block'` rejects a commit that would leave the table over its cap. |
| `search_columns`, `vector_columns`, `vector_metric` | Opt-in search indexes; see [Search](#search). |

To bulk-load without going through `INSERT`, write a Parquet file into the
table's directory and adopt it. It must have the table's columns.

```sql
COPY (SELECT * FROM read_parquet('bulk/*.parquet'))
    TO 'telemetry-store/events/9.seg.parquet' (FORMAT parquet);
SELECT * FROM capped_add_segment('tlog', 'events', 'events/9.seg.parquet');
```

### How a segment's age is decided

A table ages by one of two clocks:

- **Event time** (`time_col = 'ts'`): age comes from a column in the data, as
  in ClickHouse. Use it when the producer's timestamps can be trusted.
- **Arrival time** (no `time_col`): the store adds a hidden `_ingest_ts`
  column and stamps each row with its commit time. Use it when event
  timestamps can't be trusted. The column is invisible: `DESCRIBE`, `INSERT`
  and `SELECT *` behave as if it doesn't exist, and you can't declare a
  column with that name.

Either way, **a missing time never counts as old**. A segment with no known
time (an adopted file without `_ingest_ts`, or a time column that is all
NULL) has no time range (`capped_status` shows NULL, not 1970). It is never
dropped for age, only by size or count caps, and it sorts last for
`evict_order = 'time'`. A `time_col` whose values can't be read as
timestamps is rejected when it is written, rather than quietly turning
retention off.

### Search

Retention forgets data on a schedule; search finds it while it is still
there. Indexes are opt-in:

```sql
CREATE TABLE t.main.logs (level VARCHAR, msg VARCHAR, embedding FLOAT[768])
    WITH (ttl = '30 days', search_columns = 'msg,level',
          vector_columns = 'embedding', vector_metric = 'cosine');
```

When a segment seals, the store writes a sibling `<id>.seg.idx.duckdb` file
with DuckDB's own `fts` (BM25) index over the search columns and an HNSW
index per vector column. The Parquet file stays the only thing scans read.

- **Indexes follow retention.** An index is deleted with its segment, so a
  search hit never outlives its row. `capped_index(..., rebuild := true)`
  rebuilds them.
- **Indexing is best-effort.** A failed build never fails a write; the
  segment just isn't searchable, and `capped_index` reports the error.
- **Indexes don't count toward `max_bytes`.** `capped_status` and
  `capped_tables` show `index_bytes` so the overhead stays visible.
- **The open segment isn't indexed.** The newest rows become searchable when
  their segment seals; `capped_seal` does that on demand.
- **`k` is the size of the whole answer.** Each segment returns its own top
  `k` and the lists are merged.
- **Scores follow the metric.** `l2sq` and `cosine` are distances (smallest
  first); `ip` is a similarity (largest first).

Two things to know: `fts` stems words and drops English stopwords, so a
query made only of words like "the" matches nothing; and each index file has
a fixed minimum size (about 190 KB for text only, about 2 MB with HNSW), so
indexing many tiny segments costs more than the data. Search needs the `fts`
and `vss` extensions available in the session.

## On-disk layout

```
<store>/
  events/
    manifest.json            # schema, policy, segment list
    0.seg.parquet            # a sealed segment
    0.seg.idx.duckdb         # its search index (only if the table has one)
    1.frag-000000.parquet    # the open segment: one fragment file per commit,
    1.frag-000008.parquet    #   merged 8 at a time until the segment seals
```

Segments are plain Parquet files you can read with any Parquet tool. Each
commit writes **one new fragment** of the open segment, so a commit never
rewrites earlier data. Fragments are merged as they pile up, and sealing
merges them into the final `<id>.seg.parquet`. Every change is written to a
temp file, renamed, and then published by swapping the manifest, so a crash
never leaves a half-written table.

Files replaced by a merge or seal wait 5 minutes before deletion, so queries
already reading them can finish. Crash leftovers that no manifest mentions
are cleaned up at `ATTACH`. That cleanup only deletes files named like
capped's own (`<id>.frag-<digits>.parquet`, `.flush-<digits>.tmp.parquet`,
`<id>.seg-<digits>.parquet`, and the open segment's planned
`<id>.seg.parquet`), so don't put files with those names in a table
directory. Anything else is left alone. The details are in
[docs/open-segments.md](docs/open-segments.md).

### Known limitations

- **One writer process per store.** The write lock is in-process only; other
  processes should attach `READ_ONLY`.
- **No read-your-writes.** Reading a table in the same transaction that
  inserted into it is an error; commit first. UPDATE and DELETE are not
  supported: capped tables forget through retention.
- **Size caps are slightly conservative while a segment is open.** Many small
  fragments compress worse than the merged file will.
- **Files waiting for deletion don't count toward `max_bytes`.** The
  overshoot is at most about one open segment's worth per 5 minutes.
- **Very long queries can fail.** A query that keeps a file list open for
  more than 5 minutes can hit file-not-found if a merge or seal removed those
  files meanwhile.
- **Seal limits are fixed** (about 128 MB, 1 M rows, 1 hour span, or 32
  fragments); they can't be set per table yet.
- **Maintenance error counts reset on restart.** `maintenance_failures` and
  `last_maintenance_error` in `capped_tables` are kept in memory.

## Examples

Both scripts need the release build from below and leave their store in a
temp directory for you to explore (set `WORK=` to choose the directory and
`DUCKDB=` to use another `duckdb` binary).

**Telemetry** (`examples/telemetry/demo.sh`) generates real OTLP files
(`gen_otlp.py`, standard library only), loads them through the community
[`otlp`](https://duckdb.org/community_extensions/extensions/otlp) extension,
and shows each kind of cap on three batches of data that are 5 days, 3 days
and 1 hour old:

| Table | Cap | Result |
|---|---|---|
| `logs` | `ttl = '2 days'` on `ts` | the two expired batches are gone (1200 rows → 400) |
| `logs_by_arrival` | `ttl = '2 days'`, no `time_col` | nothing is dropped (1200 → 1200): the same data all *arrived* just now |
| `traces` | `max_rows = '300'` | the oldest batch is dropped (382 → 252) |
| `metrics` | a measured `max_bytes` | the oldest batch is dropped (288 → 192) |

Each batch is its own sealed segment, so `capped_status` shows exactly which
files survived. The script ends with queries across what's left: error rate
by service, span p95, request volume.

**Event log** (`examples/event_log/demo.sh`) uses no OTLP: one table takes
seven days of SQL-generated rows, one sealed batch per day. `ttl = '3 days'`
drops the four expired days, and a measured `max_bytes` drops the oldest
survivor after that.

## Building from source

```sh
git clone --recurse-submodules https://github.com/hamin/duckdb-capped
cd duckdb-capped
cargo test                 # Rust core
GEN=ninja make release     # DuckDB + extension
make test                  # sqllogictests
scripts/smoke.sh           # end-to-end check against your local duckdb CLI
```

You need a C++ toolchain, CMake, Ninja, Rust and `python3` 3.10 or newer.
No environment variables are needed. The first `make test` downloads the
official `fts` and `vss` builds that the search tests use. `make asan` runs
the whole suite under AddressSanitizer. Bumping DuckDB is covered in
[docs/updating-duckdb.md](docs/updating-duckdb.md).

## Repo layout

| Path | What |
|---|---|
| `crates/capped-core` | Rust core: policy parsing, manifest, seal and eviction planning, the C ABI. No DuckDB dependency. |
| `src/` | C++ storage extension: catalog, tables, transactions, write path, search indexes. It calls the Rust core for every retention decision. |
| `test/sql/` | sqllogictests. |
| `examples/` | The two demos above. |
| `docs/` | [Design](docs/design.md), [open segments](docs/open-segments.md), [updating DuckDB](docs/updating-duckdb.md). |

Why two languages: DuckDB's C extension API, which pure-Rust extensions use,
doesn't support storage extensions (`ATTACH ... (TYPE x)`), so a thin C++
layer registers the storage type and does the file work, and the Rust core
makes the decisions. See [design §6](docs/design.md).

## License

Apache-2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE).

The build tree at the repo root (`Makefile`, `CMakeLists.txt`, `src/`,
`test/`, `scripts/`, the `duckdb` and `extension-ci-tools` submodules) is
built from the DuckDB extension template, which stays MIT-licensed
(Copyright 2018-2025 Stichting DuckDB Foundation) and keeps its own license
text in [LICENSE-extension-template](LICENSE-extension-template).
