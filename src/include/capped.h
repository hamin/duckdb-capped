/* C ABI exported by the Rust crate `capped-core` (crates/capped-core/src/capi.rs).
 * The Rust side is authoritative; keep this header in sync with it.
 *
 * Boundary rules:
 * - Only JSON strings cross the FFI. Output strings are allocated by Rust and
 *   must be released with capped_free_string().
 * - Return CAPPED_OK (0) on success. On failure return CAPPED_ERR and, when
 *   the out_err parameter is non-null, a malloc-by-Rust message in *out_err.
 */
#ifndef CAPPED_CORE_H
#define CAPPED_CORE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CAPPED_OK 0
#define CAPPED_ERR 1

/* policy_json: {"time_col":"ts","ttl":"7 days","max_bytes":"1GB",
 *               "max_rows":100000,"grace":"1 hour","evict_order":"time",
 *               "on_violation":"evict"}  (all fields optional except at least
 * one cap; time_col omitted or "__ingest__" means hidden ingest time)        */
int32_t capped_parse_policy(const char *policy_json, char **out_policy_json, char **out_err);

/* parse a human duration ('30s', '5 minutes', '1 hour'; bare number = seconds)
 * into microseconds. */
int32_t capped_parse_duration(const char *input, int64_t *out_micros, char **out_err);

/* columns_json: [{"name":"ts","type":"TIMESTAMPTZ"}, ...] */
int32_t capped_new_manifest(const char *table, const char *columns_json, const char *policy_json,
                            char **out_manifest_json, char **out_err);

int32_t capped_manifest_table_name(const char *manifest_json, char **out, char **out_err);
int32_t capped_manifest_columns(const char *manifest_json, char **out, char **out_err);
int32_t capped_manifest_column_count(const char *manifest_json, int64_t *out_count, char **out_err);
int32_t capped_manifest_column(const char *manifest_json, int64_t index, char **out_name,
                               char **out_type, char **out_err);
/* next free segment id (the shell assigns ids when adding segments) */
int32_t capped_manifest_next_segment_id(const char *manifest_json, int64_t *out_id, char **out_err);
/* event-time column of the policy; empty string for ingest time */
int32_t capped_manifest_time_col(const char *manifest_json, char **out, char **out_err);
/* [{"id":0,"path":"t/segments/0.seg.parquet","rows":3,"bytes":123,"sealed":true,
 *   "files":["t/segments/0.seg.parquet"],"fragments":0}]
 * paths are relative to the store root; "files" is what a scan reads: [path]
 * sealed / legacy-open, the fragment paths in insertion order for a
 * fragment-based open segment; "fragments" is the open segment's file count. */
int32_t capped_manifest_list_segments(const char *manifest_json, char **out, char **out_err);

/* segment_json is a SegmentMeta: {"id":7,"path":"t/7.seg.parquet","rows":1,
 * "bytes":2,"min_ts":0,"max_ts":0,"sealed":true,"sealed_at":null}           */
int32_t capped_manifest_put_segment(const char *manifest_json, const char *segment_json,
                                    char **out_manifest_json, char **out_err);
/* ids_json: "[3,4]" */
int32_t capped_manifest_remove_segments(const char *manifest_json, const char *ids_json,
                                        char **out_manifest_json, char **out_err);

/* ---- open-segment fragments (fragments plan §3.3–§3.8). The manifest swap
 * (persisting out_manifest_json) is the single commit point of every step. */

/* fragment_json: {"seq":4,"path":"t/1.frag-000004.parquet","rows":12,
 * "bytes":512,"min_ts":123,"max_ts":456,"tier":0}; the file is already
 * written — this names it in the manifest and updates the open segment's
 * aggregates and write_stats. Rejects legacy open segments. */
int32_t capped_manifest_add_fragment(const char *manifest_json, int64_t segment_id,
                                     const char *fragment_json, char **out_manifest_json,
                                     char **out_err);
/* next_fragment_seq of the open segment: the shell assigns fragment seqs from
 * this (per segment, monotonic, never reused; merged outputs take a fresh
 * seq). Errors when the manifest has no open segment. */
int32_t capped_manifest_next_fragment_seq(const char *manifest_json, int64_t *out_seq,
                                          char **out_err);
/* Next fragment merge for the open segment under the built-in size-tiered
 * limits (fan-in K=8, max tier T=2), or JSON "null" when nothing qualifies:
 * {"segment":1,"inputs":[4,5,6,7,8,9,10,11],"output_tier":1,
 *  "input_paths":["t/1.frag-000004.parquet",...]}. Call in a loop until
 * "null". inputs are seqs of a contiguous same-tier run; input_paths are the
 * same run's files in order (for the merge's read_parquet; ignored by
 * apply_compaction). */
int32_t capped_plan_compaction(const char *manifest_json, char **out_plan_json, char **out_err);
/* Commit a merge written to disk: replaces the plan's input run with the
 * already-written output fragment (fresh seq, tier = inputs' tier + 1) at the
 * run's list position; inputs go to pending_deletes (now + 5 min grace). */
int32_t capped_manifest_apply_compaction(const char *manifest_json, const char *plan_json,
                                         const char *output_fragment_json, int64_t now_micros,
                                         char **out_manifest_json, char **out_err);
/* output_segment_json: SegmentMeta with the final path and EXACT stats from
 * the written file; rows must equal the fragments'. Seals the open segment,
 * queues its fragments (except one adopted by reusing its path) for deferred
 * deletion. Rejects legacy open segments. */
int32_t capped_manifest_seal_open(const char *manifest_json, const char *output_segment_json,
                                  int64_t now_micros, char **out_manifest_json, char **out_err);
/* Removes pending deletes whose grace elapsed: out_manifest_json is the
 * swept manifest, out_paths_json the paths (JSON array, store-relative) the
 * shell deletes after persisting it. */
int32_t capped_manifest_sweep_due(const char *manifest_json, int64_t now_micros,
                                  char **out_manifest_json, char **out_paths_json,
                                  char **out_err);
/* 1 when the open segment is a legacy (pre-fragment) one: unsealed, no
 * fragments, data at path. Migrate it before using add_fragment/seal_open. */
int32_t capped_manifest_is_legacy_open(const char *manifest_json, int32_t *out_legacy,
                                       char **out_err);
/* Migrates a legacy open segment in place (flips sealed, sealed_at =
 * now_micros; no file is written). Errors unless the open segment has
 * exactly the legacy shape. */
int32_t capped_manifest_seal_legacy_open(const char *manifest_json, int64_t now_micros,
                                         char **out_manifest_json, char **out_err);
/* JSON array of every store-relative path the manifest references (sealed
 * data, fragments, legacy open data, index sidecars, pending deletes). The
 * ATTACH orphan GC must never delete a file named here. The open segment's
 * PLANNED sealed path is deliberately excluded (it names no file). */
int32_t capped_manifest_owned_paths(const char *manifest_json, char **out_paths_json,
                                    char **out_err);

/* Writes DELETE_GRACE (pending-delete grace period, fragments plan §3.2) in
 * microseconds: the minimum age for orphan GC candidates, and the constant
 * the shell uses to translate the test-only grace override into the
 * now_micros passed to the queueing calls. */
int32_t capped_delete_grace_micros(int64_t *out_micros, char **out_err);

/* out_plan_json: {"drops":[{"id":7,"reason":"age"}],"still_over_budget":false} */
int32_t capped_plan_eviction(const char *manifest_json, int64_t now_micros,
                             char **out_plan_json, char **out_err);

/* limits_json: {"max_bytes":128000000,"max_rows":1000000,"max_span":"1 hour"} */
int32_t capped_should_seal(const char *manifest_json, const char *limits_json,
                           int32_t *out_seal, char **out_err);

/* same, under the built-in default telemetry seal limits */
int32_t capped_should_seal_default(const char *manifest_json, int32_t *out_seal, char **out_err);

/* 1 when the open segment holds at least MAX_OPEN_FRAGMENTS (32) fragment
 * files: the file-count seal trigger (fragments plan §5 T6), checked after
 * the compaction loop alongside capped_should_seal_default. 0 when the
 * manifest has no open segment. */
int32_t capped_manifest_needs_fragment_seal(const char *manifest_json, int32_t *out_seal, char **out_err);

/* Table-level accounting for capped_tables (fragments plan §3.9): every
 * Parquet byte capped wrote for this table (write_stats.bytes_written; an
 * adopted seal output is not counted twice), and the number of files waiting
 * in pending_deletes. */
int32_t capped_manifest_stats(const char *manifest_json, int64_t *out_bytes_written,
                              int64_t *out_pending_delete_files, char **out_err);

/* {"time_col":"ts","ttl":604800000000,"max_bytes":null,"max_rows":null,
 *  "grace":0,"evict_order":"time","on_violation":"evict"} (micros; null=unset) */
int32_t capped_manifest_policy_json(const char *manifest_json, char **out, char **out_err);

/* Search configuration of the table, as written in CREATE TABLE:
 * {"columns":["body"],"vector_columns":["embedding"],"metric":"l2sq"}
 * Empty arrays mean that kind of index is off. */
int32_t capped_manifest_search_json(const char *manifest_json, char **out, char **out_err);
/* Record (or clear, with index_path == NULL) the search index of a segment.
 * index_path is store-relative, as returned by capped_index_path_for_segment. */
int32_t capped_manifest_set_segment_index(const char *manifest_json, int64_t segment_id,
                                          const char *index_path, char **out_manifest_json,
                                          char **out_err);
/* The index file belonging to a segment file, store-relative and in the same
 * directory: "logs/2.seg.parquet" -> "logs/2.seg.idx.duckdb" */
int32_t capped_index_path_for_segment(const char *segment_path, char **out, char **out_err);

/* Name of the store-maintained ingest timestamp; mirrors
 * capped-core's policy::INGEST_TS_COLUMN. capped_new_manifest adds it to the
 * manifest schema of an ingest-time table, capped_manifest_time_col reports it
 * as that table's time column, and segment files carry it. The shell keeps it
 * out of the DuckDB-facing table schema so INSERT/SELECT * keep their natural
 * arity, and stamps it with the commit time on flush. */
#define CAPPED_INGEST_TS_COLUMN "_ingest_ts"

void capped_free_string(char *s);
const char *capped_version(void);

#ifdef __cplusplus
}
#endif

#endif /* CAPPED_CORE_H */
