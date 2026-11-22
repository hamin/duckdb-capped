# Open segments: fragments, compaction and seal

A capped table is a list of immutable sealed segments plus **one open
segment** that takes new rows. This document covers how the open segment is
written, kept small, sealed and cleaned up. The Rust side is
`crates/capped-core/src/{manifest,compaction}.rs`, reached through the C ABI
in `capi.rs`; the shell side is `src/capped_write.cpp`.

## 1. The problem: rewrite-per-commit

The obvious design keeps the open segment as one Parquet file and, at every
commit, rewrites it with the new rows appended. The cost grows with the
square of the commits per segment.

A telemetry ingester typically commits every 5 seconds, and a segment seals at
~128 MB, 1 M rows or a 1 h event-time span. A time-sealed segment therefore
sees ~720 commits, each rewriting a file that grows linearly. With `r` ingest
bytes per second:

```
bytes written per segment ≈ 5r · (720·721/2) ≈ 1.3M · r
bytes stored              ≈ 3600 · r
amplification             ≈ 360×
```

At 1 KB/s that is ~1.3 GB written per hour to store 3.6 MB, and busier tables
are worse in CPU as well as disk. Replacing a file in place also lets a scan
that already planned its file list read a different file than it planned for.

capped instead writes each commit as a new immutable **fragment** file,
merges fragments in a size-tiered way, and rewrites everything once more at
seal. Every row is written a bounded number of times.

## 2. On-disk layout

```
<store>/<table>/
  manifest.json
  0.seg.parquet                 # sealed segment
  0.seg.idx.duckdb              # its search index (tables with search options only)
  1.frag-000004.parquet         # open segment 1, fragment seq 4 (tier 1, merged)
  1.frag-000005.parquet         # open segment 1, fragment seq 5 (tier 0)
  1.frag-000006.parquet         # open segment 1, fragment seq 6 (tier 0)
```

- A **fragment** is `<segment_id>.frag-<seq:06>.parquet`. `seq` is per
  segment, monotonic and never reused; a merge output takes a fresh seq.
- A **sealed segment** is one file, `<segment_id>.seg.parquet`. If that name
  is already taken on disk or by the manifest (for example a user file placed
  there for `capped_add_segment`), the seal writes
  `<segment_id>.seg-<micros>.parquet` instead. No path is ever overwritten.
- Every file is written under a temporary name `.flush-<micros>.tmp.parquet`
  and renamed into place, so a crash mid-`COPY` never leaves a half-written
  file under a name the manifest could reference.

## 3. Manifest model

The manifest (`manifest.rs`) carries, beyond the sealed segment list:

- `SegmentMeta.fragments: Vec<FragmentMeta>`: the open segment's data files
  in insertion order, each with `seq`, `path`, `rows`, `bytes`,
  `min_ts`/`max_ts` and `tier` (0 = written by a commit, n = merged from
  tier n−1). For an open segment, `path` is the *planned* sealed path and
  does not exist on disk yet.
- `SegmentMeta.next_fragment_seq`.
- `pending_deletes: Vec<PendingDelete { path, not_before }>`: files waiting
  out their grace period (§7).
- `write_stats { bytes_written, files_written, bytes_sealed }`: cumulative
  accounting of every Parquet byte capped wrote.

Invariants, checked on every mutation:

- at most one open segment;
- the open segment's stored `rows`/`bytes`/`min_ts`/`max_ts` equal the
  aggregate of its fragments (stored, so seal and eviction planning stay
  unchanged); an unknown fragment time bound makes the segment's bound
  unknown, which means never age-droppable;
- a sealed segment has no fragments;
- every fragment seq is below `next_fragment_seq`. List order is data order,
  not seq order: a merge output has a higher seq but sits where its inputs
  were;
- no path appears twice across segments, fragments and pending deletes.

All new fields have serde defaults, so manifests written before fragments
existed still load (`crates/capped-core/testdata/legacy_manifest.json`).

## 4. Write path

`CappedWrite::FlushBuffer`, under the catalog write lock:

1. If the open segment is a legacy one (§8), seal it in place first.
2. `COPY` only the commit's buffered rows (stamped with `_ingest_ts` on
   ingest-time tables) to a temp file. Existing data is never re-read.
3. Compute the fragment's stats; build a candidate manifest with a new open
   segment if needed, plus the fragment (`capped_manifest_add_fragment`).
4. Run the eviction plan. Under `on_violation => block`, a commit that would
   leave the table over budget is rejected here, and the temp file is removed.
5. Rename the fragment to its final name, then write the manifest (temp file
   + rename). **The manifest write is the commit point.** Evicted sealed
   files are deleted after it.
6. Maintenance, each step its own write → manifest swap: the compaction loop
   (§5), the file-count seal, the size/row/span seal (§6), and the
   pending-delete sweep (§7). The commit is already durable, so a failure here
   is recorded in `capped_tables.maintenance_failures` and retried by the
   next commit or enforcement pass; it never fails the commit.
7. Build missing search indexes for anything that just sealed (best effort).

## 5. Size-tiered compaction

`compaction::plan_compaction` merges runs of **K = 8 contiguous fragments of
the same tier** into one fragment of the next tier, lowest tier first, up to
**tier T = 2**; tier-2 files are never merged again. The shell executes a plan
as `COPY (SELECT * FROM read_parquet([inputs in order])) TO <output>` and
applies it with `capped_manifest_apply_compaction`, which puts the output at
the run's position and queues the inputs in `pending_deletes`. It loops until
no plan is left, since a tier-1 merge can cascade into a tier-2 merge.

- Only adjacent runs merge, so list order stays insertion order.
- File count: at most 7 tier-0 + 7 tier-1 files plus one tier-2 file per ~64
  commits. An hour of 5-second commits peaks around 26 files.
- Write cost: each row is written once as tier 0, at most once per merge tier,
  and once at seal, so at most 4×.
- If the next seq's file name is occupied by a foreign file, the merge takes
  the next free seq and records a maintenance failure rather than retrying
  forever.

K and T are constants in core; they are not table options.

## 6. Seal is the final compaction

`SealOpenUnlocked` is shared by `FlushBuffer`, the enforcement pass and
`capped_seal`:

1. No open segment, or one with nothing written yet: nothing to seal.
2. Exactly one fragment: the fragment **becomes** the sealed file (its path
   is adopted, no rewrite). Low-traffic tables seal for free this way.
3. Otherwise merge all fragments in order into the planned path, or the
   `.seg-<micros>` fallback (§2).
4. Recompute stats from the output, so sealed stats are exact.
5. `capped_manifest_seal_open` marks the segment sealed, clears its fragment
   list and queues the merged-away fragments for deletion; then the manifest
   swap commits.

A seal fires when `should_seal` trips (~128 MB / 1 M rows / 1 h event-time
span) or when the open segment reaches **32 fragment files**
(`MAX_OPEN_FRAGMENTS`). The second trigger matters because nothing else bounds
commits per segment: an ingest-time table has no event-time span, and tiny
commits can take tens of thousands of commits to reach the row or byte limit.

After a seal the segment is evictable and, if the table asked for search,
gets its index sidecar.

## 7. Scans, deferred deletes and orphan GC

A scan reads the manifest at bind time. `capped_manifest_list_segments` emits
a `files` list per segment: `[path]` for a sealed or legacy open segment, the
fragment paths in order for an open one. `SegmentPaths()` in
`src/capped_table.cpp` flattens these into the parquet scan's file list.

DuckDB opens those files lazily, so deleting a merged-away fragment at once
would fail an in-flight scan. Instead:

- **Pending deletes.** Merge inputs and sealed-away fragments go into
  `pending_deletes` with `not_before = now + 5 minutes`. The sweep removes due
  entries from the manifest first, then deletes the files; a crash in between
  leaves unreferenced files for the orphan GC. The sweep runs after every
  commit, in every enforcement pass and at `ATTACH`. The grace is adjustable
  for tests with `SET capped_pending_delete_grace`.
- **Orphan GC, at `ATTACH` only.** It looks at the top level of each table
  directory and deletes a file only if all of these hold: its name matches
  one of capped's own leftover patterns exactly — `<id>.frag-<digits>.parquet`,
  `.flush-<digits>.tmp.parquet`, `<id>.seg-<digits>.parquet`, or
  `<id>.seg.parquet` for the *current open* segment's id; the manifest does
  not reference it (segments, fragments, index sidecars, pending deletes); and
  it is older than the grace period (a peer process may be mid-write). Any
  other file, including user Parquet copied in for `capped_add_segment`, is
  left alone.

A `READ_ONLY` attach skips the sweep, the GC and every other write.

## 8. Legacy open segments

A store written before fragments existed may have an open segment with rows
but no fragments, its data in one complete file at `path`. At `ATTACH` (and
lazily, if a write or seal meets one) it is sealed in place: the flag flips,
the path and stats stay, nothing is rewritten. The next insert starts a fresh
fragment-based open segment.

## 9. Observability

- `capped_status`: `fragments` (the open segment's file count; 0 for sealed
  segments).
- `capped_tables`: `fragments`, `bytes_written` (cumulative Parquet bytes
  capped wrote), `pending_delete_files`, `maintenance_failures` and
  `last_maintenance_error`.

New columns are appended at the end, never reordered, because downstream
readers select them with `SELECT *`.

## 10. Crash safety

The commit point is always the manifest write (temp file + rename). After a
crash at each point:

| Crash after… | On disk | Manifest | Recovery |
|---|---|---|---|
| fragment temp written, before rename | `.flush-*.tmp.parquet` | old | ATTACH GC deletes it; the commit was never acknowledged |
| fragment renamed, before manifest | orphan `.frag-` | old | ATTACH GC deletes it |
| manifest with new fragment | fragment referenced | new | none needed |
| merge output written, before manifest | orphan `.frag-` | old, inputs still referenced | ATTACH GC deletes the output |
| merge manifest swapped | output + inputs | output referenced, inputs pending | sweep deletes the inputs after the grace |
| seal output written, before manifest | orphan `<id>.seg.parquet` or `.seg-<micros>` | old, segment still open | ATTACH GC deletes it (the planned name only while that segment is open) |
| seal manifest swapped | sealed file + fragments | sealed, fragments pending | sweep |
| sweep manifest swapped, before deletes | unreferenced files | entries gone | ATTACH GC deletes them |

Files are not fsynced before the manifest rename, so this covers process
crashes, not power loss.

## 11. Limits

- An open segment's `bytes` is a sum of small fragments, which compress worse
  than the sealed file, so `max_bytes` eviction is slightly conservative while
  a segment is open.
- Files in `pending_deletes` don't count against `max_bytes`; the overshoot is
  bounded by roughly one open segment's worth per grace window.
- A query that keeps a file list open for longer than the grace (5 minutes)
  can fail with file-not-found if a merge or seal sweeps those files meanwhile.
- Evicted *sealed* segments are deleted immediately, not deferred.
- Merges and seals run under the write lock, so a large seal delays the next
  commit while it rewrites.

## 12. Measuring it

`scripts/amplification.sh` (after `GEN=ninja make release`) runs 720
separately committed ~2 KB INSERT batches, an hour at 5-second commits, then
`capped_seal`, and reads `bytes_written` and `bytes` from `capped_tables`. It
gates on **row amplification ≤ 4.0** (rows across every file written ÷ sealed
rows) and **byte ratio ≤ 6.0**. The byte gate is looser because a 28-row
fragment compresses about twice as badly as the sealed segment, a Parquet
cost rather than an extra rewrite. Measured: 3.98× rows and 5.03× bytes,
against ~360× for rewrite-per-commit. The simulation tests in
`compaction.rs` check the same bounds (≤ 4× bytes, ≤ 26 open files) for equal,
skewed and byte-capped commit streams, and that the 32-file trigger holds
for 10k tiny commits.
