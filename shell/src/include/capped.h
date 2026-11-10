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
/* [{"id":0,"path":"t/segments/0.seg.parquet","rows":3,"bytes":123,"sealed":true}]
 * paths are relative to the store root */
int32_t capped_manifest_list_segments(const char *manifest_json, char **out, char **out_err);

/* segment_json is a SegmentMeta: {"id":7,"path":"t/7.seg.parquet","rows":1,
 * "bytes":2,"min_ts":0,"max_ts":0,"sealed":true,"sealed_at":null}           */
int32_t capped_manifest_put_segment(const char *manifest_json, const char *segment_json,
                                    char **out_manifest_json, char **out_err);
/* ids_json: "[3,4]" */
int32_t capped_manifest_remove_segments(const char *manifest_json, const char *ids_json,
                                        char **out_manifest_json, char **out_err);

/* out_plan_json: {"drops":[{"id":7,"reason":"age"}],"still_over_budget":false} */
int32_t capped_plan_eviction(const char *manifest_json, int64_t now_micros,
                             char **out_plan_json, char **out_err);

/* limits_json: {"max_bytes":128000000,"max_rows":1000000,"max_span":"1 hour"} */
int32_t capped_should_seal(const char *manifest_json, const char *limits_json,
                           int32_t *out_seal, char **out_err);

/* same, under the built-in default telemetry seal limits */
int32_t capped_should_seal_default(const char *manifest_json, int32_t *out_seal, char **out_err);

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
