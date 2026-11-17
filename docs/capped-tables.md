# Capped Tables for DuckDB — Design Exploration

**Idea:** MongoDB capped-collection-style tables for DuckDB, capped by **size**, **time**, or **both** — aimed at telemetry/log workloads (logs, metrics, traces, events) where data ages out and nobody ever queries it after N days.

Status: exploration. No existing DuckDB extension does this (checked community extension list + GitHub; only per-query result caches with TTL exist).

---

## 1. What we're porting from MongoDB, and where the analogy breaks

MongoDB capped collections: fixed-size collection, inserts append, oldest data is evicted FIFO when the cap is hit, insertion order is preserved (the `$natural` scan), caps set via `size` and/or `max` (count). TTL-by-age is a *separate* mechanism (TTL indexes), which capped collections don't even support.

The user-facing ask combines both: **rows vanish when they're too old, and/or when the table gets too big** — with flexible policy:

| Cap | Policy knob | Eviction |
|---|---|---|
| Time | `ttl = '7 days'` on a timestamp column | drop rows where `ts < now() - ttl` |
| Size | `max_bytes = '1GB'` | drop oldest rows until under budget |
| Count | `max_rows = 10000000` (cheap bonus, maps to Mongo `max`) | drop oldest rows until under cap |
| Both/any | intersection — evict when *any* limit is breached | oldest-first |

What DuckDB gives us for free that Mongo didn't have: columnar compression (telemetry compresses ~10x, so caps mean very different things), zone-map/min-max metadata per row group, and vectorized scans. What DuckDB does **not** give us: physical insertion-order guarantees, per-table byte accounting, triggers, insert hooks, or any eviction primitive.

## 2. Prior art (validation that the feature is wanted)

| System | Mechanism | Enforcement model |
|---|---|---|
| **ClickHouse TTL** | `TTL ts + INTERVAL 7 DAY DELETE [WHERE ...]`, column-level TTL, move-to-cold-disk, rollup-before-delete | asynchronous, applied during background **merges** (~4h default, `merge_with_ttl_timeout`); best practice is partition-by-day + `ttl_only_drop_parts=1` so expiry = drop whole parts |
| **QuestDB** | `TTL` table property | background job drops **whole partitions**; `ALTER TABLE ... DROP PARTITION` |
| **Databricks Auto-TTL** | table property on a timestamp column | async job runs `DELETE` + `VACUUM`; docs explicitly say timing is *not guaranteed*, can lag days |
| **TimescaleDB** | `add_retention_policy()`, compression/rollup policies | scheduler job per chunk; chunks are the eviction unit |
| **MongoDB** | capped collections (`size`/`max`), TTL indexes separately | synchronous FIFO at insert (capped) / background TTL index job |

Patterns worth stealing:
1. **Everyone is eventually-consistent.** ClickHouse, Databricks, QuestDB all delete on a background cadence. Nobody promises synchronous expiry. We can be async and still be credible.
2. **Whole-unit eviction is the trick.** ClickHouse/QuestDB/Timescale all rely on deleting *whole partitions/parts/chunks* — O(1) metadata operations, no rewriting live data. Row-level `DELETE` is the fallback nobody likes.
3. **Rollup-before-delete** (ClickHouse): keep a 5-minute downsample of 90-day-old metrics after dropping raw data. Big value for telemetry; roadmap material.
4. **Column TTL** (ClickHouse): drop the fat `body`/`payload` column after 3 days but keep the row's metadata. Great fit for logs; roadmap material.

Adjacent DuckDB project worth knowing: **`smithclay/duckdb-otlp`** — an OTLP/OTAP ingest server + `read_otlp_*` table functions that streams OpenTelemetry data into DuckDB. It solves ingestion; retention is exactly the missing half. Our extension would be its natural partner (and the demo story: "DuckDB as a self-hosted OTel backend with automatic retention").

## 3. DuckDB ground truth (the constraints that shape the design)

Verified from DuckDB 2.0 docs:

- **`DELETE` does not reclaim space.** Space is only (partially) reclaimed at `CHECKPOINT`, and the implementation "merges row groups that have a significant amount of deletes together. In the current implementation this requires ~25% of rows to be deleted in adjacent row groups." `VACUUM` is a stats tool; it does not reclaim space. (Since v1.4, checkpoint also applies to in-memory DBs.)
- **One file per database.** A `.duckdb` file holds all tables — no per-table file we can size or drop. Per-table exact byte size is not exposed (`duckdb_tables()` gives row estimates; `database_size()` is DB-wide).
- **No triggers, and no extension hook for INSERT.** Extensions can add table/scalar functions, parser overloads, optimizer rules, operator plugins, and whole attached-storage catalogs — but not intercept a plain-table insert. So "enforce at insert time like Mongo" is off the table for native tables; enforcement must be a `CALL`, a background thread, or built into our own storage.
- **A long-lived open transaction blocks reclamation** — MVCC keeps deleted row groups alive for readers. Background cleanup must run short transactions.
- **WAL growth:** deletes accumulate in the WAL until checkpoint; `checkpoint_threshold` config exists to force auto-checkpoint by WAL size.
- **Background threads in extensions are precedented** (e.g., the MySQL storage extension runs a maintenance thread), so a retention daemon is buildable.

## 4. Architecture options

### Option A — Retention governor over native tables (the MVP)

The extension is a *policy manager + enforcer*, not a new format. Tables are ordinary DuckDB tables; the extension stores policies and runs cleanup.

```
INSTALL retention; LOAD retention;

CALL set_retention('main.telemetry',
    time_col    => 'ts',
    ttl         => '7 days',      -- optional
    max_bytes   => '1GB',         -- optional
    max_rows    => 50000000,      -- optional
    schedule    => '5 minutes');  -- enforcement cadence

CALL unset_retention('main.telemetry');
SELECT * FROM retention_status();      -- policy, last run, rows/bytes removed, errors
CALL retention_enforce();              -- run all due policies now
```

Mechanics:
- Policies persist in a real table inside the attached DB (survives restart), not a session map.
- **Time cap:** `DELETE FROM t WHERE ts < now() - INTERVAL 7 DAY` — with telemetry in time order, deletes hit whole row groups at the head, which is the one case DuckDB reclaims well.
- **Size/count cap:** oldest-first eviction via `DELETE ... WHERE rowid IN (SELECT rowid ORDER BY ts LIMIT n)`, sized down from a byte estimate (compressed bytes/row from recent row groups × rows to drop). *Soft* cap — DuckDB won't give exact per-table bytes; document the fuzziness.
- Periodic `CHECKPOINT` (rate-limited, and only when the WAL/footprint justifies it) so reclaimed space actually returns; respect open transactions.
- **Enforcement triggers:** (1) the `CALL` above, (2) an optional background thread started on load (mysql_scanner precedent), (3) user cron/agent. Being eventual is fine and matches the whole prior-art field.

Pros: small surface, works with *any* existing table, buildable in C++ against the stable extension API in days, demoable in an afternoon. Zero impact on the hot insert path (that's also the con).
Cons: enforcement is asynchronous and the size cap is approximate; heavy delete churn is awkward (deletion vectors, checkpoint timing, WAL); no natural-order guarantee.

### Option B — A true capped store: `ATTACH ... (TYPE capped)` (the real thing)

An attached-storage extension with its own catalog, where a "capped table" is an **ordered sequence of immutable segment files** — our own version of the ClickHouse-partition trick:

```
ATTACH 'telemetry-store' AS tlog (TYPE capped);

CREATE TABLE tlog.main.events (ts TIMESTAMPTZ, sev VARCHAR, body VARCHAR, attrs MAP(VARCHAR,VARCHAR));
INSERT INTO tlog.main.events SELECT ... ;            -- appends to the open segment
SELECT * FROM tlog.main.events WHERE sev='error';    -- union scan over segments, pushdown intact
```

- **Sealing:** an open segment gets `max_bytes` / `max_rows` / `max_time_span`; when breached it's frozen and a new one opens. Inserts are always append-only.
- **Retention = drop whole sealed segments** whose `max(ts) < now() - ttl` (safe with late data) or while `Σ bytes > budget` (oldest first). O(1) file deletion, zero MVCC garbage, zero compaction pressure — this is where *hard* caps become honest.
- **Size is exact** because we own the segment files.
- **Natural order** falls out of `(segment_id, row_number)` — our `$natural`.

Segment file format decision is the fork in the road:

| Segment format | Wins | Costs |
|---|---|---|
| (b1) Each segment = a mini `.duckdb` file | real SQL on segments for free, updates/deletes on the hot one, native reader | many attached DB instances, heavier open/close, WAL per segment |
| (b2) Segments = Parquet | trivial to drop, DuckDB's own parquet reader does pushdown, trivially inspectable | no UPDATE/DELETE, own metadata for stats |
| (b3) Segments = **Lance** | ties into the `lance` extension — sealed telemetry gets vector + FTS search for free (log embeddings, semantic log search!) | newer ecosystem, extra dependency |

Hot/cold split variant worth considering: hot segment = live attached DuckDB (recent data, updatable), sealed segments = Parquet or Lance. That's how most real telemetry backends are shaped anyway.

Pros: exact caps, O(1) eviction, append-optimized, the version that scales. Cons: real storage-extension work (catalog, transactions, scan pushdown, `CREATE TABLE`/`INSERT` paths), weeks not days.

### Option C — A on the outside, B underneath

Ship A's SQL surface, then swap the engine underneath to segment-store tables without changing user SQL. Probably the endgame; don't design for it prematurely, but don't paint into a corner either: keep policy/eviction logic independent of the "how do I remove old data" primitive so the swap is local.

## 5. The policy model (shared by A and B)

```
RETENTION POLICY (per table)
  time_col     : column name | NULL   -- TIMESTAMPTZ/DATE; NULL ⇒ ingest time (hidden `_ingest_ts`)
  ttl          : INTERVAL | NULL      -- age limit
  max_bytes    : BYTES  | NULL        -- footprint budget
  max_rows     : BIGINT | NULL        -- count cap (Mongo `max`)
  evict_order  : time_col | insert    -- which "oldest" means
  grace        : INTERVAL             -- extra slack so late/out-of-order data isn't nuked
  on_violation : evict | block | error -- Mongo is block-at-cap; we default to evict
  schedule     : INTERVAL             -- enforcement cadence
  search_columns : [VARCHAR]          -- opt-in BM25 over sealed segments
  vector_columns : [FLOAT[N]]         -- opt-in HNSW, with vector_metric l2sq|cosine|ip
```

Deliberate design calls:
- **Event time vs ingest time.** TTL on event `ts` is ClickHouse's model and what telemetry users expect, but late-arriving data with old timestamps should be protected → the `grace` knob (and in Option B, only evict segments where `max(ts) < cutoff`). When the producer's clock can't be trusted, the table instead ages by *arrival*: no `time_col` means the store maintains a hidden `_ingest_ts` itself (see §8).
- **Absence of a time is not evidence of age.** A segment with no time bounds (an adopted bulk file without `_ingest_ts`, or an all-NULL time column) stores *no* bound rather than `0`. A fabricated `0` reads as 1970, so the next enforcement pass would delete live data — the bug that made this an explicit invariant. Such segments are evictable by size/count only, and they sort last in `evict_order => time`.
- **"Too old" and "too big" evict the same way** — oldest first. One eviction routine, two triggers.
- **`on_violation => block`** is the Mongo-capped behavior (reject inserts when at cap). Trivial in B (the storage layer decides), weird in A (no insert hook) — punt on `block` in A.
- **Search is a policy option, not a source of truth.** `search_columns`/`vector_columns` say "make sealed segments searchable"; they never change what is stored, what scans read, or what eviction deletes. The index is derived data with the same lifecycle as its segment, it is excluded from `max_bytes` (capping derived data would let index churn evict real data), and a failed index build must never fail a write. The open segment is not indexed at all — it is rewritten on every commit.
- Multiple policies per table (hot 7d raw, 90d downsampled) is the ClickHouse pattern — out of MVP scope, but the schema shouldn't preclude it.

## 6. Hard problems (in rough order of pain)

1. **Byte accounting in A.** No per-table exact size on a shared file → size caps are estimates. Mitigate with sampled compressed-bytes/row + honest docs ("your 1GB cap is ±15%"). Fully solved only in B.
2. **Reclamation path in A.** DELETE → tombstones → CHECKPOINT merges row groups at the ~25%-deleted threshold → WAL/footprint dynamics. Mitigations: evict in whole row-group-aligned batches (`rowid` ranges), checkpoint on cadence rather than per-delete, keep cleanup transactions short.
3. **Clock semantics.** `now()` vs event time vs the *user's* clock in tests. Expose `at => '2026-11-07 00:00:00'` on `retention_enforce()` so policy behavior is deterministically testable (this is not optional — retention code without a fake clock is untestable).
4. **Crash safety.** Policies in a table, not memory; eviction that checkpoints must be idempotent; background thread dies with the instance cleanly.
5. **Concurrency.** cleanup transaction vs long-running reader → skipped cycle, retry next tick; never block inserts.
6. **Memory tables.** In-memory DuckDB behaves differently (checkpoint applies since 1.4). Decide v1 = file-backed only, and say so.
7. **DuckDB version churn.** Core extension APIs move fast; pin to LTS, keep the core logic in plain C++ decoupled from DuckDB headers so upgrades are cheap.

## 7. Build plan (as chosen: Option B directly)

Layout: `capped/` — Rust workspace `crates/capped-core` (policy, manifest,
eviction, C ABI) + the repo root (DuckDB extension template, pinned v2.0.0).
The C++ shell orchestrates DuckDB integration; all retention decisions go
through the Rust C ABI (JSON across the boundary; manifests are Rust-owned).

- **M1a — attachable store + DDL** ✅: `ATTACH ... (TYPE capped)` over a store
  directory, catalog that lists tables from `<table>/manifest.json`,
  `CREATE TABLE ... WITH (time_col, ttl, max_bytes, max_rows, grace,
  evict_order, on_violation)` (policy validated by Rust — no cap ⇒ rejected),
  `DROP TABLE`, `SHOW TABLES`, transactions as no-ops.
- **M1b — scan path** ✅: sealed segments are read by composing DuckDB's own
  `parquet_scan` (the manifest's segment list is injected at bind time), so
  filter/limit pushdown and row-group pruning behave exactly like
  `read_parquet`. Bulk ingest via `COPY ... TO <table dir>/<id>.seg.parquet` +
  `capped_add_segment(catalog, table, path)` (stats read from parquet
  metadata; the file's column set must match the table, since DuckDB's parquet
   reader needs one uniform schema across a table's segments — the hidden
   `_ingest_ts` is the one optional column). Prepared-statement rebind handling
   is folded into M1c.
- **M1c — write path + enforcement** ✅: `INSERT INTO` (VALUES and SELECT) sinks
  rows into a per-transaction buffer (temp table + vectorized `Appender` on a
  transaction-owned internal connection); commit merges the buffer into the
  table's single open segment under a catalog write lock — write merged
  parquet → hard-cap gate → evict → publish (rename + atomic manifest
  write) → auto-seal when the open segment exceeds the seal limits
  (`capped_should_seal`, defaults 128 MB / 1 M rows / 1 h span). Rollback
  discards the buffer (nothing on disk). Open segments are scanned like sealed
  ones. `capped_seal(catalog, table)` rotates manually; `capped_status`
  reports per-segment rows/bytes/event-time + caps; `on_violation => block`
  rejects the commit before publishing when eviction cannot get under budget.
  Prepared statements rebind correctly (binder registers a catalog read;
  verified empirically). UPDATE/DELETE/ON CONFLICT/RETURNING rejected by design.
- **M2a — enforcement ergonomics** ✅: retention now also runs at `ATTACH`
  (best-effort, never fails the attach) and on demand via
  `capped_enforce(catalog[, table])` (returns dropped/sealed per table);
  `capped_tables(catalog)` gives a store overview (segments, sealed/open,
  rows, bytes, time range, caps). A read-your-writes guard makes reading a
  table inside the transaction that inserted into it a clear error instead of
  silently omitting the still-buffered rows.
- **M2b — background enforcement** ✅: each attached catalog owns one
  enforcement thread started in `Initialize` and joined in the destructor
  (condition-variable sleep, `stop_requested` flag — DETACH is prompt).
  Cadence is the `enforce_interval` ATTACH option (Rust-parsed durations,
  default 60s, `0` disables); ticks are best-effort (a table that fails
  retention — e.g. an unsatisfiable block policy — never kills the loop).
  `CREATE`/`DROP TABLE` now take the same catalog write lock as the flush/
  enforce paths, so a tick can't recreate a dropped table's manifest.
  Verified deterministically: an expired segment sealed out-of-band is gone
  (manifest entry + file) within one tick, no client call.
- **M2c — telemetry demo** ✅ (`examples/telemetry/`): `gen_otlp.py` writes
  OTLP `ExportLogsServiceRequest` / `ExportTraceServiceRequest` /
  `ExportMetricsServiceRequest` files with a hand-rolled protobuf encoder
  (field numbers from `opentelemetry-proto` v1, stdlib only — no grpc/protobuf
  packages to install), and `demo.sh` loads them through duckdb-otlp's
  `read_otlp_logs` / `read_otlp_traces` / `read_otlp_metrics_gauge` /
  `read_otlp_metrics_sum` into one store with three tables — `ttl='2 days'`,
  `max_rows='300'`, `max_bytes=<measured>`. Three day-batches (5d, 3d, 1h old)
  are inserted one transaction per batch, each sealed, so retention is a
  whole-file decision and `capped_status` shows which files survived. The size
  cap is *measured*, not guessed: a probe table takes one batch first, and the
  real cap is set to ~2.5 × that segment.
  Things the demo surfaced: the community `otlp` extension needs
  `-unsigned` + `LOAD otlp` (it is not in the core repository index), its
  readers must be globbed per signal (`logs-*.otlp`, not `*.otlp`), the span
  duration column is `duration_time_unix_nano`, `read_otlp_metrics()` is
  deliberately unimplemented (one reader per metric shape), and
  `capped_tables`/`capped_status` were reporting every cap as NULL because
  `yyjson_is_sint()` is false for positive integers — they carry the *uint*
  subtype; those columns now use `yyjson_is_int()` and are covered by
  sqllogictest.
- **M2c — ingest-time retention** ✅: no `time_col` ⇒ `TimeSource::IngestTime`.
  `capped_new_manifest` appends the reserved `_ingest_ts TIMESTAMPTZ` to the
  manifest schema, `BuildCreateTableInfo` hides it from DuckDB, and the flush
  `COPY` stamps it with `now()` (one commit = one arrival time);
  `capped_manifest_time_col` and the policy summary report it, so stats, status
  and enforcement treat it like any other time column. Adopted files may omit it
  (⇒ unknown bounds, never age-droppable), and adoption now validates the file's
  columns against the table — which also closes a way to make an entire table
  unreadable.
- **M2c — searchable sealed segments** ✅: `search_columns` / `vector_columns` /
  `vector_metric` are policy options validated at `CREATE TABLE`, owned by the
  Rust core (`search.rs`, over the C ABI as `capped_manifest_search_json` /
  `capped_manifest_set_segment_index`). At seal the shell writes a sibling
  `<id>.seg.idx.duckdb` holding the searchable columns plus DuckDB's own `fts`
  BM25 index and one HNSW index per vector column; `capped_fts` and
  `capped_vector_search` ask each indexed segment for its own top-k and merge the
  lists table-wide. Parquet stays the only segment format and the only thing
  scans read; the sidecar is derived data — built at seal, deleted with its
  segment, rebuildable via `capped_index(rebuild => true)`, excluded from
  `max_bytes`, and a failed build never fails a write (the catalog latches
  "search degraded" and stops retrying until re-`ATTACH` or an explicit call).
  Measured on 125k-row segments: fts build 0.86 s, HNSW 1.48 s, both queries
  ~20 ms. Things this surfaced: the searching connection must `LOAD fts` as well
  as the building one, because the index's own plan calls `stem()`;
  `match_bm25(filter_col, q)` takes the *filter column* value, not the rowid;
  `fts` stems terms and drops a default English stopword list, so an all-stopword
  query silently matches nothing; `fts_main_<tbl>` and `CREATE INDEX` only resolve
  against the *current* database, hence one `USE <sidecar>` per query; HNSW
  rejects a reduced `BLOCK_SIZE` (its `FixedSizeAllocator` needs the standard
  256 KB blocks) and does so as an *internal* error that invalidates the whole
  database — so the small blocks that cut a text-only sidecar from 1.8 MB to
  190 KB are used only when no vector index is present; a cast in the sidecar's
  `CREATE TABLE ... AS SELECT` renames the column to `CAST(...)`, so the
  `FLOAT[N]` cast must be aliased back; `column` is reserved, so the named
  parameter is `vector_column :=`; and `require fts` cannot be satisfied at all
  in an out-of-tree build (1.5 ships them as remote extensions and the runner only
  answers `require` for statically linked code — the file reported "skipped" while
  testing nothing), so the suite `LOAD`s them and either runs or fails loudly.
- **M2c — polish (open)**: Lance revisit (one dataset per table replacing
  parquet+sidecars — it lost this round on measurement: 4–20× slower on analytics,
  4.7× the bytes for the same telemetry, and `__lance_scan` reads one dataset so
  segments cannot compose); index footprint for small segments (a sidecar per
  segment has a ~190 KB floor, but batching segments into one sidecar would break
  O(1) whole-file eviction); rollup-before-delete + column TTL (roadmap); name
  decision before publishing.

## 8. Decisions

- **Architecture: Option B** — attached capped store (`ATTACH ... (TYPE capped)`), segment-file storage with whole-file eviction. Chosen for the interesting build, not the fastest one.
- **Name: `capped`** (for now). Noted: evokes Mongo capped collections; revisit before any community-extension submission.
- **Scope: file-backed databases only.** In-memory DuckDB stays out of v1.
- **Language: Rust** via `duckdb-rs`, with a thin C++ storage shell — `ATTACH`/`StorageExtension` is not exposed by the loadable-extension C API (confirmed by odbc-scanner discussion and the "Everything C API v2!" discussion, Oct 2026; it's planned for the v2 line, no date). Precedent: `lance-duckdb` itself (C++ glue around the Rust `lance` crate). Implementation lives in `capped/` at the repo root.
- **Search: DuckDB-native sidecars, not a new segment format.** Each sealed segment keeps its parquet file and gains a sibling `.duckdb` file holding the searchable columns with `fts` BM25 + HNSW indexes over them, built at seal by DuckDB itself. Rejected: all-Lance segments (measured 4–20× slower on the analytics queries and 4.7× the bytes for the same telemetry, and `__lance_scan` reads exactly one dataset so segments cannot compose), and one Lance dataset per table with DELETE/VACUUM (that destroys whole-file O(1) eviction, the property the store exists for). Keeping both indexes DuckDB's own means the store ships no index format of its own, and everything version-dependent sits behind one seam (`src/capped_index.cpp`).
- **Two time sources, and an absent time is never an age.** `time_col` gives ClickHouse-style event-time TTL; omitting it means **ingest time** — the store maintains a hidden `_ingest_ts` (`TIMESTAMPTZ`) and stamps each row with its commit time, so rows age from arrival, which is what telemetry ingestion wants when event timestamps are untrustworthy. The column is part of the manifest schema and of the parquet files, but the shell drops it when building the DuckDB-facing table, so `DESCRIBE`, `INSERT` arity and `SELECT *` behave as if it didn't exist, `SELECT _ingest_ts` is an unknown-column error, and users cannot declare that name. Stamping happens in the flush `COPY` (`now()`, i.e. the transaction start, so one commit is one arrival time) rather than via a column `DEFAULT`: DuckDB evaluates defaults inside its insert operator, not in the chunk our path appends. An adopted bulk file may legitimately lack the column — its rows get *no* bounds, which per §5 means "never age-droppable", not "ancient".

## 9. Open questions

- Exact name revisit before publishing (`capped` vs `decay`/`wane`/`logstore`).
- Parser extension for real `CREATE CAPPED TABLE ... WITH (ttl='7d', max_bytes='1GB')` sugar, or `CREATE TABLE ... WITH (...)` option capture in the storage layer? (The latter needs no parser work — `WITH` options land in `BindInfo`.)
- Segment file format: settled for v1 — Parquet segments (boring, works, own the metadata) with search in per-segment DuckDB sidecars (§7, M2c). Lance stays an explicit *revisit*: one dataset per table would fold vector + FTS search into the storage format itself, at the cost of the analytics speed, the footprint, and the whole-file eviction the store is built on.
- Which enforcement mode is the *default*: daemon on, or explicit `CALL capped_enforce()` only?
- Ingest-time tables currently always carry `_ingest_ts`, even with only size/count caps (a few bytes per row, and it keeps retention sane if a `ttl` is added later). Worth revisiting only if it shows up in a footprint measurement.
- Publish as a community extension (review process, must track DuckDB releases) or personal-registry/standalone build to start?
