# capped — bounded storage for DuckDB

Tables that forget. `capped` is a DuckDB storage extension for telemetry data
(logs, metrics, traces, events): data appends in, the oldest data flows out
when the table gets too **old**, too **big**, or too **full**.

```sql
-- Attaching a store applies retention, then a background thread keeps it up
-- (enforce_interval, default 60s)
ATTACH 'telemetry-store' AS tlog (TYPE capped);

CREATE TABLE tlog.main.events (
    ts TIMESTAMPTZ, severity VARCHAR, service VARCHAR, body VARCHAR
) WITH (time_col = 'ts', ttl = '7 days', max_bytes = '10GB');

-- no time_col: rows age by arrival instead, via a hidden store-maintained column
CREATE TABLE tlog.main.ingest_log (severity VARCHAR, body VARCHAR) WITH (ttl = '7 days');

INSERT INTO tlog.main.events
    SELECT * FROM read_parquet('ingest/*.parquet');

SELECT count(*) FROM tlog.main.events WHERE severity = 'error';  -- pushdown intact

SELECT * FROM capped_seal('tlog', 'events');    -- rotate the open segment
SELECT * FROM capped_status('tlog', 'events');  -- segments, sizes, caps
```

```sql
-- Bulk ingest stays available too: adopt parquet files as sealed segments
COPY (SELECT * FROM read_parquet('bulk/*.parquet'))
    TO 'telemetry-store/events/9.seg.parquet' (FORMAT parquet);
SELECT * FROM capped_add_segment('tlog', 'events', 'events/9.seg.parquet');
```

Design in one line: each table is an ordered sequence of immutable sealed
segment files plus one open segment, so retention is **deleting whole files**
— O(1), exact byte caps, no delete tombstones, no compaction. Full design
rationale and prior art: [`docs/capped-tables.md`](../docs/capped-tables.md).

## Repo layout

| Path | What |
|---|---|
| `crates/capped-core` | Pure Rust: policy model, segment manifest, seal/eviction planning, C ABI (`src/capi.rs`). No DuckDB dependency. `cargo test`. |
| `shell/` | DuckDB extension (extension-template based, pinned to DuckDB v1.5.5): `ATTACH (TYPE capped)`, catalog/schema/tables/transactions. `GEN=ninja make release`, then `scripts/smoke.sh`. |
| `examples/telemetry` | End-to-end demo: OTLP files → capped store → retention → analytics (`demo.sh`). |
| `docs/` (repo root) | Design rationale and prior-art survey. |

## Building

```sh
cargo test                                  # Rust core
cd shell && GEN=ninja make release          # DuckDB + extension
shell/scripts/smoke.sh                      # end-to-end against local duckdb CLI
```

Local development loads the unsigned extension: `duckdb -unsigned -c "LOAD '…/capped.duckdb_extension'; …"`.

## Try it: telemetry demo

```sh
examples/telemetry/demo.sh        # needs the release build above
```

Generates real OTLP wire-format files (protobuf, no third-party deps —
`gen_otlp.py` writes them directly), loads them through the community
[`otlp`](https://duckdb.org/docs/stable/core_extensions/otlp) extension, and
shows all three kinds of cap, on both clocks, doing their job on the same store:

| table | cap | what happens to three day-batches (5d, 3d, 1h old) |
|---|---|---|
| `logs` | `ttl='2 days'` on `ts` | the two expired segments are gone (1200 rows → 400) |
| `logs_by_arrival` | `ttl='2 days'`, no `time_col` | nothing is dropped (1200 → 1200): identical data, but it all *arrived* now, and this table ages by `_ingest_ts` |
| `traces` | `max_rows='300'` | the oldest segment is dropped (382 → 252) |
| `metrics` | `max_bytes=…` | the oldest segment is dropped (288 → 192) |

Each batch is inserted in its own transaction and sealed, so one segment is
one day and retention is a whole-file decision — `capped_status` shows exactly
which files survived, and the script finishes with cross-signal analytics
(error rate by service, span p95, request volume) over what is left. The store
is left behind in a temp directory for poking at (set `WORK=` to keep it
somewhere specific, `DUCKDB=` to point at another binary).

## Why a shell layer exists at all

DuckDB's loadable-extension C API — what pure-Rust extensions are built on
(`duckdb-rs` with `loadable-extension`, `extension-template-rs`) — does **not**
expose storage extensions: registering a custom `ATTACH ... (TYPE x)` catalog
still requires the C++ `StorageExtension` interface. Confirmed by the odbc-scanner
discussion ("ATTACH support is planned in future, but only after it is added to
the C API") and the "Everything C API v2!" discussion (Oct 2026), where
storage/catalog extensions remain explicitly out of scope.

The plan therefore follows the `lance-duckdb` precedent: all real logic lives
in Rust here; the shell is a thin C++ registration layer that
1. registers the `capped` storage type and a catalog backed by segment files,
2. calls into `capped-core` (staticlib + C ABI) for policy parsing, manifest
   updates, seal decisions, and eviction plans,
3. executes plans: seal the open segment, delete dropped segment files,
   persist the manifest.

## Status

- [x] `capped-core`: policy model + parsing (`'7 days'`, `'1GB'`), manifest, eviction planning, seal triggers — unit tested
- [x] C ABI between core and shell (`src/capi.rs` + `shell/src/include/capped.h`)
- [x] M1a shell: `ATTACH (TYPE capped)`, catalog/schema over manifests,
      `CREATE TABLE ... WITH (...)` via Rust policy validation, `DROP TABLE`, `SHOW TABLES`
- [x] M1b scan path: sealed segments are read by DuckDB's own parquet reader
      (composition over `parquet_scan`), so filter/limit pushdown and row-group
      pruning work exactly like `read_parquet`; `capped_add_segment(...)` adopts
      parquet files as sealed segments (bulk ingest / testing)
- [x] M1c write path: `INSERT INTO` (VALUES + SELECT) buffers rows per
      transaction and merges them into the table's single open segment at
      commit; open segments are scanned like any other segment; `capped_seal`
      rotates; eviction (age/size/count) runs at commit and after `capped_add_segment`;
      `capped_status(catalog, table)` lists segments + caps; UPDATE/DELETE are
      rejected by design (capped tables forget via retention, not deletes)
- [x] Segment file format: Parquet, one file per sealed segment
      (Lance still under consideration as an alternative for vector/FTS)
- [x] Hard caps (`on_violation => block`): commits that would leave the table
      over budget even after eviction are rejected before anything is published
- [x] M2a enforcement ergonomics: retention also runs at `ATTACH` (idle stores
      catch up on open) and on demand via `capped_enforce(catalog[, table])`;
      `capped_tables(catalog)` gives a store overview; reading a table inside
      the transaction that wrote to it now errors instead of silently omitting
      the buffered rows
- [x] M2b background enforcement: each attached store runs an enforcement
      thread (`enforce_interval` ATTACH option, default 60s, `0` disables);
      expired segments leave idle stores on their own, no client traffic needed
- [x] Both time sources: `time_col` for trustworthy event timestamps, and ingest
      time (no `time_col`) via a store-maintained hidden `_ingest_ts` stamped at
      commit — invisible in `DESCRIBE`/`INSERT`/`SELECT *`, reserved as a user
      column name, and the basis of age eviction when event time can't be trusted
- [x] M2c telemetry demo: `examples/telemetry/` generates OTLP wire-format
      files and loads them through duckdb-otlp into a store with four tables
      (event-time / ingest-time / count / size caps), then queries what
      retention left behind
- [x] Searchable sealed segments: `search_columns` builds a per-segment fts
      BM25 index and `vector_columns` an HNSW index, in a DuckDB sidecar next to
      each sealed parquet file; `capped_fts` / `capped_vector_search` answer from
      them and merge the per-segment top-k table-wide. Parquet stays the only
      source of truth — the index is derived, best-effort, and follows retention
- [ ] M2c (open): Lance segment variant (measured; deferred — DuckDB-native
      sidecars won for now), name revisit before publishing

## How data lives on disk

```
<store>/
  events/
    manifest.json            # Rust-owned: schema, policy, segment list
    0.seg.parquet            # sealed segment (root-relative "events/0.seg.parquet" in the manifest)
    0.seg.idx.duckdb         # its search index (only for tables that asked for one)
    1.seg.parquet            # the open segment: same format, flagged unsealed in the manifest
```

The manifest owns the schema, which is why an ingest-time table's hidden
`_ingest_ts` appears there (and in the parquet files) but nowhere in the DuckDB
catalog: `BuildCreateTableInfo` skips it when handing the table to DuckDB.
Segments are plain Parquet files; scans delegate to the parquet extension
(statically linked in the built shell; autoloaded by the CLI). `INSERT`
buffers rows in the transaction and, at commit, merges them into the table's
single open segment file (write new parquet → swap → update manifest, so a
crash never leaves a half-published table). The open segment is sealed
automatically when it grows past the seal limits (~128 MB / 1 M rows / 1 h
event-time span) or manually via `capped_seal`; the eviction planner then
drops whole sealed files to honor `ttl` / `max_bytes` / `max_rows`, oldest
first. `COPY ... TO '<store>/<table>/<id>.seg.parquet' (FORMAT parquet)` +
`SELECT capped_add_segment('<catalog>', '<table>', '<table>/<id>.seg.parquet')`
remains the zero-copy bulk-ingest path (stats read from parquet metadata).

Known v1 limitations: uncommitted rows are only visible to their own
transaction's commit, and reading a table inside the transaction that inserted
into it errors (rather than silently missing the buffered rows) — commit first.
Retention is applied at every write, seal, and `capped_add_segment`, again at
`ATTACH`, by a background thread every `enforce_interval` (default 60s; `0`
disables it), and on demand via `capped_enforce`.

## SQL surface

| Call | Meaning |
|---|---|
| `ATTACH 'dir' AS t (TYPE capped [, enforce_interval '60s'])` | open a store; applies retention, then enforces on a background thread (0 disables) |
| `CREATE TABLE t.main.x(...) WITH (time_col, ttl, max_bytes, max_rows, grace, evict_order, on_violation[, search_columns, vector_columns, vector_metric])` | create a capped table (≥ 1 cap required; no `time_col` ⇒ age by ingest time via the hidden `_ingest_ts`; options validated by capped-core) |
| `INSERT INTO t.main.x ...` | append (VALUES / SELECT); buffered, merged into the open segment on commit |
| `SELECT ... FROM t.main.x` | scan via parquet pushdown; sealed + open segments |
| `SELECT * FROM capped_fts('t','x','query'[, k := 10])` | BM25 top-k over the searchable columns of the sealed segments |
| `SELECT * FROM capped_vector_search('t','x',vec[, k := 10][, vector_column := 'c'])` | nearest neighbours over an indexed vector column of the sealed segments |
| `SELECT * FROM capped_index('t','x'[, rebuild := true])` | build (or rebuild) the missing search indexes; one row per segment with `build_error` |
| `SELECT * FROM capped_status('t','x')` | per-segment rows/bytes/event-time + the table's caps + `indexed`/`index_bytes` |
| `SELECT * FROM capped_tables('t')` | one row per table: segments, sealed/open, rows, bytes, time range, caps, indexed count |
| `SELECT * FROM capped_seal('t','x')` | rotate: seal the open segment, start a new one |
| `SELECT * FROM capped_enforce('t')` / `capped_enforce('t','x')` | apply retention now (idle stores, bulk loads); returns dropped/sealed per table |
| `SELECT * FROM capped_add_segment('t','x','x/9.seg.parquet')` | adopt a parquet file as a sealed segment (bulk ingest; the file must hold the table's columns — `_ingest_ts` optional) |
| `SELECT capped_version()` | capped-core version |

### What decides a segment's age

A table ages by one of two **time sources**:

- **`time_col = 'ts'`** — age comes from a real column (ClickHouse's model). Use
  it when the producer's timestamps are trustworthy; `grace` protects rows that
  arrive late with old timestamps.
- **ingest time** — the default when no `time_col` is given: the store maintains
  a hidden `_ingest_ts` column and stamps each row with its commit time, so data
  ages from the moment it arrives. This is what telemetry ingestion wants when
  event timestamps cannot be trusted. The column lives in the segment files and
  in every retention decision, but not in the table users see: `DESCRIBE`,
  `INSERT` arity and `SELECT *` stay exactly as written, `SELECT _ingest_ts`
  fails as an unknown column, and declaring that name yourself is refused.

Either way, **a missing time is never read as epoch**. A segment whose time is
unknown — a bulk-adopted file that carries no arrival time, or a time column
that is all NULL — stores **no** bounds (`capped_status` shows NULL, not 1970)
and is therefore never dropped by age; only the size and count caps can evict it,
and it sorts last under `evict_order = 'time'`. A `time_col` whose values never
read as timestamps is rejected at the write that would produce such a segment,
rather than silently disabling retention.

### Searching sealed segments

Retention forgets data on a schedule; search finds it again while it is still
there. Both live on the same segments, and search never becomes the source of
truth.

```sql
CREATE TABLE t.main.logs(level VARCHAR, msg VARCHAR, embedding FLOAT[768])
    WITH (ttl = '30 days', search_columns = 'msg,level',
          vector_columns = 'embedding', vector_metric = 'cosine');
```

`search_columns` (VARCHAR) and `vector_columns` (`FLOAT[N]`) are validated at
`CREATE TABLE`, where a typo can still be fixed. When a segment seals, the
store writes a sibling `<id>.seg.idx.duckdb` holding the searchable columns
plus DuckDB's own `fts` BM25 index and one HNSW index per vector column. The
parquet file remains the only thing scans read, so analytics are untouched.

What that buys, and what it costs:

- **It follows retention.** The index is built at seal, deleted the moment its
  segment is evicted (or the table is dropped), and rebuildable with
  `capped_index('t','x', rebuild := true)`. A hit can never outlive its row.
- **It is best-effort.** A failed build never fails a write: the segment stays
  searchable by scan only, `capped_index` reports the `build_error`, and the
  store stops retrying until the next `ATTACH` or an explicit call.
- **It is not capped.** `max_bytes` counts segment bytes only, since indexes are
  derived data. `capped_status`/`capped_tables` report `indexed` and
  `index_bytes` so the overhead stays visible.
- **The open segment is never indexed** — it is rewritten on every commit.
  Sealed data is searchable, the newest rows are not; `capped_seal` closes that
  gap on demand.
- **`k` is the table-wide answer size.** Each sidecar is asked for its own top-k
  (its BM25 statistics only cover its own rows), then the lists are merged.
- **Scores follow the metric**: `l2sq`/`cosine` are distances (closest first),
  `ip` is a similarity (largest first).

Two sharp edges worth knowing: `fts` stems terms and drops a default English
stopword list, so a phrase full of `the`/`again` matches nothing; and each
sidecar has a fixed floor (~190 KB text-only, ~2 MB once HNSW is involved, which
needs DuckDB's standard block size) — indexing a handful of tiny segments costs
more index than data, which is exactly why search is opt-in and uncounted.
Searching needs the `fts` and `vss` extensions available to the session.
