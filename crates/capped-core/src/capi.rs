//! C ABI boundary between the C++ storage shell and the Rust brain.
//!
//! Boundary rules:
//! - Only JSON strings and primitives cross the FFI. Both sides already have
//!   JSON tooling (serde / serde_json on this side; the shell uses DuckDB's
//!   vendored JSON or plain string passing), and the format is the manifest's
//!   on-disk format, so the shell can persist manifests without reparsing.
//! - The shell owns files and DuckDB integration; this crate owns policy
//!   parsing, manifest semantics, seal and eviction decisions.
//! - All output strings are allocated by Rust and must be released with
//!   `capped_free_string`. Return code 0 == success; on failure `*out_err`
//!   (when non-null) receives a human-readable message, also freed with
//!   `capped_free_string`.
//! - Every entry point catches panics: a Rust panic must never unwind into
//!   C++ frames.

use crate::eviction::{plan_eviction, should_seal, SealLimits};
use crate::manifest::{Column, Manifest, SegmentId, SegmentMeta};
use crate::search::{index_path_for, SearchSpec, VectorMetric};
use crate::policy::{
    parse_duration, parse_size, EvictOrder, OnViolation, RetentionPolicy, TimeSource, INGEST_TS_COLUMN,
};
use serde::Deserialize;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};

const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");

/// Success / failure return codes shared with the C header.
pub const CAPPED_OK: c_int = 0;
pub const CAPPED_ERR: c_int = 1;

// ---------------------------------------------------------------------------
// input specs (wire format, hand-drawn; canonical types live in policy.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PolicySpec {
    /// Column holding event time. Omit, or use `"__ingest__"`, for ingest time.
    #[serde(default)]
    time_col: Option<String>,
    #[serde(default)]
    ttl: Option<String>,
    #[serde(default)]
    max_bytes: Option<String>,
    #[serde(default, deserialize_with = "de_opt_u64_or_string")]
    max_rows: Option<u64>,
    #[serde(default)]
    grace: Option<String>,
    #[serde(default)]
    evict_order: Option<String>,
    #[serde(default)]
    on_violation: Option<String>,
    /// Comma-separated text columns to index for BM25 search on sealed segments.
    #[serde(default)]
    search_columns: Option<String>,
    /// Comma-separated fixed-size float vector columns to index with HNSW.
    #[serde(default)]
    vector_columns: Option<String>,
    /// Distance metric for the vector indexes: `l2sq` | `cosine` | `ip`.
    #[serde(default)]
    vector_metric: Option<String>,
}

/// Split a comma-separated SQL option value into trimmed, non-empty names.
fn split_columns(value: Option<&str>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Accepts `1000` or `"1000"` — SQL `WITH (max_rows=1000)` options arrive as
/// stringified constant values from the C++ shell.
fn de_opt_u64_or_string<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    use serde::de::Error;
    let v = serde_json::Value::deserialize(d)?;
    match v {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Number(n) => n
            .as_u64()
            .map(Some)
            .ok_or_else(|| D::Error::custom(format!("max_rows: expected integer, got {n}"))),
        serde_json::Value::String(s) => s
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| D::Error::custom(format!("max_rows: expected integer, got '{s}'"))),
        other => Err(D::Error::custom(format!(
            "max_rows: expected integer or numeric string, got {other}"
        ))),
    }
}

#[derive(Debug, Deserialize)]
struct LimitsSpec {
    #[serde(default)]
    max_bytes: Option<u64>,
    #[serde(default)]
    max_rows: Option<u64>,
    #[serde(default)]
    max_span: Option<String>,
}

fn spec_to_policy(spec: PolicySpec) -> Result<RetentionPolicy, String> {
    let time_source = match spec.time_col.as_deref() {
        None | Some("") | Some("__ingest__") => TimeSource::IngestTime,
        Some(col) => TimeSource::Column(col.to_string()),
    };
    let mut b = RetentionPolicy::builder(time_source);
    if let Some(ttl) = &spec.ttl {
        b = b.ttl(parse_duration(ttl).map_err(|e| e.to_string())?);
    }
    if let Some(max_bytes) = &spec.max_bytes {
        b = b.max_bytes(parse_size(max_bytes).map_err(|e| e.to_string())?);
    }
    if let Some(max_rows) = spec.max_rows {
        b = b.max_rows(max_rows);
    }
    if let Some(grace) = &spec.grace {
        b = b.grace(parse_duration(grace).map_err(|e| e.to_string())?);
    }
    if let Some(order) = &spec.evict_order {
        b = b.evict_order(match order.as_str() {
            "time" => EvictOrder::Time,
            "insert" => EvictOrder::Insert,
            other => return Err(format!("evict_order must be 'time' or 'insert', got '{other}'")),
        });
    }
    if let Some(on_violation) = &spec.on_violation {
        b = b.on_violation(match on_violation.as_str() {
            "evict" => OnViolation::Evict,
            "block" => OnViolation::Block,
            "error" => OnViolation::Error,
            other => {
                return Err(format!(
                    "on_violation must be 'evict', 'block', or 'error', got '{other}'"
                ))
            }
        });
    }
    b.build().map_err(|e| e.to_string())
}

fn spec_to_limits(spec: LimitsSpec) -> Result<SealLimits, String> {
    Ok(SealLimits {
        max_bytes: spec.max_bytes,
        max_rows: spec.max_rows,
        max_span: match &spec.max_span {
            Some(s) => Some(parse_duration(s).map_err(|e| e.to_string())?),
            None => None,
        },
    })
}

// ---------------------------------------------------------------------------
// FFI plumbing
// ---------------------------------------------------------------------------

unsafe fn input<'a>(p: *const c_char, what: &str) -> Result<&'a str, String> {
    if p.is_null() {
        return Err(format!("{what}: null pointer"));
    }
    CStr::from_ptr(p)
        .to_str()
        .map_err(|e| format!("{what}: invalid utf-8: {e}"))
}

unsafe fn write_string(out: *mut *mut c_char, s: String) -> Result<(), String> {
    if out.is_null() {
        return Err("output pointer is null".to_string());
    }
    // serde_json escapes control characters, so the JSON has no interior NUL;
    // new_string() can therefore only fail on an OOM-like condition.
    let cs = CString::new(s).map_err(|_| "output contains NUL byte".to_string())?;
    *out = cs.into_raw();
    Ok(())
}

/// Run one FFI call body: catch panics, translate Err into *out_err.
unsafe fn guard<F: FnOnce() -> Result<(), String>>(f: F, out_err: *mut *mut c_char) -> c_int {
    let result = catch_unwind(AssertUnwindSafe(f));
    match result {
        Ok(Ok(())) => CAPPED_OK,
        Ok(Err(msg)) => {
            if !out_err.is_null() {
                *out_err = CString::new(msg).map(|c| c.into_raw()).unwrap_or(std::ptr::null_mut());
            }
            CAPPED_ERR
        }
        Err(_) => {
            if !out_err.is_null() {
                *out_err = CString::new("internal panic in capped-core")
                    .map(|c| c.into_raw())
                    .unwrap_or(std::ptr::null_mut());
            }
            CAPPED_ERR
        }
    }
}

// ---------------------------------------------------------------------------
// exported functions — see include/capped.h for the C declarations
// ---------------------------------------------------------------------------

/// Parse a policy spec JSON into canonical policy JSON.
///
/// Input:  {"time_col":"ts","ttl":"7 days","max_bytes":"1GB","grace":"1 hour",
///          "max_rows":100000,"evict_order":"time","on_violation":"evict"}
/// Output: canonical `RetentionPolicy` JSON.
///
/// # Safety
/// All pointers must be null or valid NUL-terminated UTF-8 strings; out
/// pointers must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_parse_policy(
    policy_json: *const c_char,
    out_policy_json: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let raw = input(policy_json, "policy_json")?;
            let spec: PolicySpec =
                serde_json::from_str(raw).map_err(|e| format!("policy: {e}"))?;
            let policy = spec_to_policy(spec)?;
            write_string(out_policy_json, serde_json::to_string(&policy).unwrap())
        },
        out_err,
    )
}

/// Parse a human duration (`'30s'`, `'5 minutes'`, `'1 hour'`; a bare number
/// means seconds) and write the value in microseconds to `out_micros`.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_parse_duration(
    input_str: *const c_char,
    out_micros: *mut i64,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            if out_micros.is_null() {
                return Err("out_micros is null".to_string());
            }
            let d = parse_duration(input(input_str, "input")?).map_err(|e| e.to_string())?;
            *out_micros = d.as_micros() as i64;
            Ok(())
        },
        out_err,
    )
}

/// The manifest owns its column list: an ingest-time table always carries the
/// hidden [`INGEST_TS_COLUMN`] (the shell keeps it out of the DuckDB-facing
/// schema so `INSERT`/`SELECT *` keep their natural arity, and stamps it with
/// the commit time on flush), and user columns may never claim that name.
fn prepare_columns(mut columns: Vec<Column>, policy: &RetentionPolicy) -> Result<Vec<Column>, String> {
    let user_named_ingest = columns.iter().any(|c| c.name == INGEST_TS_COLUMN);
    match policy.time_source {
        TimeSource::IngestTime => {
            if user_named_ingest {
                return Err(format!(
                    "'{INGEST_TS_COLUMN}' is maintained by the store on ingest-time tables"
                ));
            }
            columns.push(Column {
                name: INGEST_TS_COLUMN.to_string(),
                ty: "TIMESTAMPTZ".to_string(),
            });
        }
        TimeSource::Column(_) => {
            if user_named_ingest {
                return Err(format!("'{INGEST_TS_COLUMN}' is a reserved column name"));
            }
        }
    }
    Ok(columns)
}

/// Create a fresh manifest JSON.
///
/// `columns_json`: `[{"name":"ts","type":"TIMESTAMPTZ"}, ...]`
/// `policy_json`:  policy spec JSON (parsed like `capped_parse_policy`).
///
/// # Safety
/// Pointers must be null or valid NUL-terminated UTF-8; out pointers writable.
#[no_mangle]
pub unsafe extern "C" fn capped_new_manifest(
    table: *const c_char,
    columns_json: *const c_char,
    policy_json: *const c_char,
    out_manifest_json: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let table = input(table, "table")?;
            let columns: Vec<Column> = serde_json::from_str(input(columns_json, "columns_json")?)
                .map_err(|e| format!("columns: {e}"))?;
            if columns.is_empty() {
                return Err("a capped table needs at least one column".to_string());
            }
            if columns.iter().any(|c| c.ty.trim().is_empty()) {
                return Err("every column needs a type".to_string());
            }
            let spec: PolicySpec =
                serde_json::from_str(input(policy_json, "policy_json")?).map_err(|e| e.to_string())?;
            let search = SearchSpec {
                columns: split_columns(spec.search_columns.as_deref()),
                vector_columns: split_columns(spec.vector_columns.as_deref()),
                metric: match spec.vector_metric.as_deref() {
                    Some(m) => VectorMetric::parse(m)?,
                    None => VectorMetric::default(),
                },
            };
            let policy = spec_to_policy(spec)?;
            let columns = prepare_columns(columns, &policy)?;
            // names are checked against the table's own schema (including the
            // hidden ingest column), so a typo fails at CREATE TABLE, not at seal
            let search = search.validate(table, &columns)?;
            let mut manifest = Manifest::new(table, columns, policy);
            manifest.search = search;
            write_string(out_manifest_json, serde_json::to_string(&manifest).unwrap())
        },
        out_err,
    )
}

/// Read the table name from a manifest.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_table_name(
    manifest_json: *const c_char,
    out: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            write_string(out, m.table)
        },
        out_err,
    )
}

/// Read the columns array from a manifest.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_columns(
    manifest_json: *const c_char,
    out: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            write_string(out, serde_json::to_string(&m.columns).unwrap())
        },
        out_err,
    )
}

/// Number of columns in the manifest.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_column_count(
    manifest_json: *const c_char,
    out_count: *mut i64,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            if out_count.is_null() {
                return Err("out_count is null".to_string());
            }
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            *out_count = m.columns.len() as i64;
            Ok(())
        },
        out_err,
    )
}

/// Column `index` as separate name/type strings.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_column(
    manifest_json: *const c_char,
    index: i64,
    out_name: *mut *mut c_char,
    out_type: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            let idx = usize::try_from(index)
                .map_err(|_| format!("column index out of range: {index}"))?;
            let col = m
                .columns
                .get(idx)
                .ok_or_else(|| format!("column index out of range: {index}"))?;
            write_string(out_name, col.name.clone())?;
            match write_string(out_type, col.ty.clone()) {
                Ok(()) => Ok(()),
                Err(e) => {
                    // Release the already-written name to avoid leaking it.
                    if !out_name.is_null() && !(*out_name).is_null() {
                        capped_free_string(*out_name);
                        *out_name = std::ptr::null_mut();
                    }
                    Err(e)
                }
            }
        },
        out_err,
    )
}

/// next_segment_id of the manifest (the shell uses this to assign segment IDs).
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_next_segment_id(
    manifest_json: *const c_char,
    out_id: *mut i64,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            if out_id.is_null() {
                return Err("out_id is null".to_string());
            }
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            *out_id = m.next_segment_id as i64;
            Ok(())
        },
        out_err,
    )
}

/// The event-time column of the manifest's policy. Ingest-time policies report
/// the canonical hidden column name ([`INGEST_TS_COLUMN`]), so callers can read
/// it from the segment files like any other time column.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_time_col(
    manifest_json: *const c_char,
    out: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            let col = match &m.policy.time_source {
                TimeSource::Column(c) => c.clone(),
                TimeSource::IngestTime => INGEST_TS_COLUMN.to_string(),
            };
            write_string(out, col)
        },
        out_err,
    )
}

/// All segments as `[{"id":0,"path":"t/segments/0.seg.parquet","rows":3,
/// "bytes":123,"sealed":true}]`. Paths are relative to the store root; the
/// shell resolves them.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_list_segments(
    manifest_json: *const c_char,
    out: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            let rows: Vec<serde_json::Value> = m
                .segments
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "id": s.id.0,
                        "path": s.path,
                        "rows": s.rows,
                        "bytes": s.bytes,
                        "min_ts": s.min_ts,
                        "max_ts": s.max_ts,
                        "sealed": s.sealed,
                        "index_file": s.index_file,
                    })
                })
                .collect();
            write_string(out, serde_json::to_string(&rows).map_err(|e| e.to_string())?)
        },
        out_err,
    )
}

/// Insert or replace one segment (JSON = `SegmentMeta`) in the manifest.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_put_segment(
    manifest_json: *const c_char,
    segment_json: *const c_char,
    out_manifest_json: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let mut m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            let seg: SegmentMeta = serde_json::from_str(input(segment_json, "segment_json")?)
                .map_err(|e| format!("segment: {e}"))?;
            m.put_segment(seg)?;
            write_string(out_manifest_json, serde_json::to_string(&m).unwrap())
        },
        out_err,
    )
}

/// Drop segments by id. `ids_json` = `[3,4]`.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_remove_segments(
    manifest_json: *const c_char,
    ids_json: *const c_char,
    out_manifest_json: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let mut m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            let ids: Vec<u64> = serde_json::from_str(input(ids_json, "ids_json")?)
                .map_err(|e| format!("ids: {e}"))?;
            m.remove_segments(&ids.into_iter().map(SegmentId).collect::<Vec<_>>());
            write_string(out_manifest_json, serde_json::to_string(&m).unwrap())
        },
        out_err,
    )
}

/// Eviction planning. Output:
/// `{"drops":[{"id":7,"reason":"age"}, ...], "still_over_budget":false}`
/// with reason one of `"age" | "size" | "count"`.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_plan_eviction(
    manifest_json: *const c_char,
    now_micros: i64,
    out_plan_json: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            let plan = plan_eviction(&m, now_micros);
            let json = serde_json::json!({
                "drops": plan.drops.iter().map(|(id, reason)| serde_json::json!({
                    "id": id.0,
                    "reason": match reason {
                        crate::eviction::EvictionReason::Age => "age",
                        crate::eviction::EvictionReason::Size => "size",
                        crate::eviction::EvictionReason::Count => "count",
                    },
                })).collect::<Vec<_>>(),
                "still_over_budget": plan.still_over_budget,
            });
            write_string(out_plan_json, json.to_string())
        },
        out_err,
    )
}

/// Should the open segment of `manifest_json` be sealed? Writes 0/1 to
/// `out_seal`. `limits_json` = `{"max_bytes":128000000,"max_rows":1000000,
/// "max_span":"1 hour"}` (fields optional). No open segment => 0.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_should_seal(
    manifest_json: *const c_char,
    limits_json: *const c_char,
    out_seal: *mut c_int,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            if out_seal.is_null() {
                return Err("out_seal is null".to_string());
            }
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            let limits = spec_to_limits(
                serde_json::from_str(input(limits_json, "limits_json")?)
                    .map_err(|e| format!("limits: {e}"))?,
            )?;
            *out_seal = match m.open_segment() {
                Some(open) => i32::from(should_seal(open, &limits)),
                None => 0,
            };
            Ok(())
        },
        out_err,
    )
}

/// Should the open segment be sealed under the default telemetry limits
/// (`SealLimits::default_telemetry()`)? Writes 0/1 to `out_seal`. No open
/// segment => 0.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_should_seal_default(
    manifest_json: *const c_char,
    out_seal: *mut c_int,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            if out_seal.is_null() {
                return Err("out_seal is null".to_string());
            }
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            *out_seal = match m.open_segment() {
                Some(open) => i32::from(should_seal(open, &SealLimits::default_telemetry())),
                None => 0,
            };
            Ok(())
        },
        out_err,
    )
}

/// Policy summary for status/tooling:
/// `{"time_col":"ts","ttl":604800000000,"max_bytes":1000000000,"max_rows":null,
///   "grace":0,"evict_order":"time","on_violation":"evict"}`.
/// Durations are microseconds; `time_col` is [`INGEST_TS_COLUMN`] for ingest
/// time; caps are `null` when unset.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_policy_json(
    manifest_json: *const c_char,
    out: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            let p = &m.policy;
            let json = serde_json::json!({
                "time_col": match &p.time_source {
                    TimeSource::Column(c) => c.clone(),
                    TimeSource::IngestTime => INGEST_TS_COLUMN.to_string(),
                },
                "ttl": p.ttl.map(|d| d.as_micros() as i64),
                "max_bytes": p.max_bytes.map(|b| b as i64),
                "max_rows": p.max_rows.map(|r| r as i64),
                "grace": p.grace.as_micros() as i64,
                "evict_order": match p.evict_order {
                    EvictOrder::Time => "time",
                    EvictOrder::Insert => "insert",
                },
                "on_violation": match p.on_violation {
                    OnViolation::Evict => "evict",
                    OnViolation::Block => "block",
                    OnViolation::Error => "error",
                },
            });
            write_string(out, json.to_string())
        },
        out_err,
    )
}

/// Search configuration of the manifest:
/// `{"columns":["body"],"vector_columns":["embedding"],"metric":"l2sq"}`.
/// Empty arrays mean that kind of index is off; `metric` is the HNSW distance
/// metric the shell passes to `vss`.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_search_json(
    manifest_json: *const c_char,
    out: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            let json = serde_json::json!({
                "columns": m.search.columns,
                "vector_columns": m.search.vector_columns,
                "metric": m.search.metric.as_str(),
            });
            write_string(out, json.to_string())
        },
        out_err,
    )
}

/// Record the search index built for a segment, or clear it (`index_path` null).
/// The path is store-relative, as returned by [`capped_index_path_for_segment`].
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_manifest_set_segment_index(
    manifest_json: *const c_char,
    segment_id: i64,
    index_path: *const c_char,
    out_manifest_json: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let mut m: Manifest = serde_json::from_str(input(manifest_json, "manifest_json")?)
                .map_err(|e| format!("manifest: {e}"))?;
            if segment_id < 0 {
                return Err(format!("segment_id must be >= 0, got {segment_id}"));
            }
            let index = if index_path.is_null() {
                None
            } else {
                let p = input(index_path, "index_path")?;
                if p.trim().is_empty() {
                    return Err("index_path must not be empty (pass null to clear)".to_string());
                }
                Some(p.to_string())
            };
            m.set_segment_index(SegmentId(segment_id as u64), index)?;
            write_string(out_manifest_json, serde_json::to_string(&m).unwrap())
        },
        out_err,
    )
}

/// The index file that belongs to a segment file, store-relative and in the
/// same directory: `logs/2.seg.parquet` -> `logs/2.seg.idx.duckdb`. Keeping the
/// convention here means the shell never has to guess where an index lives.
///
/// # Safety
///
/// Pointers must be null or valid NUL-terminated UTF-8 strings; out pointers
/// must be writable.
#[no_mangle]
pub unsafe extern "C" fn capped_index_path_for_segment(
    segment_path: *const c_char,
    out: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> c_int {
    guard(
        || {
            let path = input(segment_path, "segment_path")?;
            if path.trim().is_empty() {
                return Err("segment_path must not be empty".to_string());
            }
            write_string(out, index_path_for(path))
        },
        out_err,
    )
}

/// Release any string returned by this crate.
///
/// # Safety
///
/// The pointer must come from a capped_* call (or be null).
#[no_mangle]
pub unsafe extern "C" fn capped_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}

/// Version of capped-core. Static storage; do not free.
#[no_mangle]
pub extern "C" fn capped_version() -> *const c_char {
    VERSION.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Call an FFI fn that produces a string, return the Rust-owned copy.
    unsafe fn call_str(
        f: impl FnOnce(*mut *mut c_char, *mut *mut c_char) -> c_int,
    ) -> Result<String, String> {
        let mut out: *mut c_char = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();
        let rc = f(&mut out, &mut err);
        if rc == CAPPED_OK {
            assert!(!out.is_null());
            let s = CStr::from_ptr(out).to_str().unwrap().to_string();
            capped_free_string(out);
            Ok(s)
        } else {
            let msg = if err.is_null() {
                "unknown error".to_string()
            } else {
                let s = CStr::from_ptr(err).to_str().unwrap().to_string();
                capped_free_string(err);
                s
            };
            Err(msg)
        }
    }

    #[test]
    fn parse_policy_ok_and_err() {
        let spec = CString::new(
            r#"{"time_col":"ts","ttl":"7 days","max_bytes":"1GB","grace":"1 hour"}"#,
        )
        .unwrap();
        let canonical =
            unsafe { call_str(|o, e| capped_parse_policy(spec.as_ptr(), o, e)) }.unwrap();
        let v: serde_json::Value = serde_json::from_str(&canonical).unwrap();
        assert_eq!(v["ttl"], 7 * 86_400_000_000u64); // micros
        assert_eq!(v["max_bytes"], 1_000_000_000u64);

        let nocap = CString::new(r#"{"time_col":"ts"}"#).unwrap();
        let err = unsafe { call_str(|o, e| capped_parse_policy(nocap.as_ptr(), o, e)) }.unwrap_err();
        assert!(err.contains("cap"), "got: {err}");
    }

    #[test]
    fn ingest_time_tables_carry_the_hidden_column() {
        let new_manifest = |cols: &str, policy: &str| -> Result<String, String> {
            let table = CString::new("t").unwrap();
            let cols = CString::new(cols).unwrap();
            let policy = CString::new(policy).unwrap();
            unsafe {
                let mut out: *mut c_char = std::ptr::null_mut();
                let mut err: *mut c_char = std::ptr::null_mut();
                if capped_new_manifest(table.as_ptr(), cols.as_ptr(), policy.as_ptr(), &mut out, &mut err)
                    == CAPPED_OK
                {
                    let s = CStr::from_ptr(out).to_str().unwrap().to_string();
                    capped_free_string(out);
                    Ok(s)
                } else {
                    let s = CStr::from_ptr(err).to_str().unwrap().to_string();
                    capped_free_string(err);
                    Err(s)
                }
            }
        };

        // no time_col => ingest time: the store adds the hidden column and
        // reports it as the time source (so stats/status/enforce treat it like
        // any other time column)
        let m = new_manifest(r#"[{"name":"body","type":"VARCHAR"}]"#, r#"{"ttl":"1 hour"}"#).unwrap();
        let v: serde_json::Value = serde_json::from_str(&m).unwrap();
        assert_eq!(v["policy"]["time_source"], "IngestTime");
        assert_eq!(v["columns"][1]["name"], INGEST_TS_COLUMN);
        assert_eq!(v["columns"][1]["type"], "TIMESTAMPTZ");
        let mc = CString::new(m).unwrap();
        assert_eq!(
            unsafe { call_str(|o, e| capped_manifest_time_col(mc.as_ptr(), o, e)) }.unwrap(),
            INGEST_TS_COLUMN
        );
        let summary = unsafe { call_str(|o, e| capped_manifest_policy_json(mc.as_ptr(), o, e)) }.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&summary).unwrap()["time_col"],
            INGEST_TS_COLUMN
        );

        // an explicit time_col leaves the user's columns untouched
        let m = new_manifest(
            r#"[{"name":"ts","type":"TIMESTAMPTZ"}]"#,
            r#"{"time_col":"ts","ttl":"1 hour"}"#,
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&m).unwrap();
        assert_eq!(v["columns"].as_array().unwrap().len(), 1);

        // the name is reserved in both modes
        let err =
            new_manifest(r#"[{"name":"_ingest_ts","type":"TIMESTAMPTZ"}]"#, r#"{"ttl":"1 hour"}"#).unwrap_err();
        assert!(err.contains("maintained by the store"), "got: {err}");
        let err = new_manifest(
            r#"[{"name":"_ingest_ts","type":"TIMESTAMPTZ"}]"#,
            r#"{"time_col":"ts","max_rows":"10"}"#,
        )
        .unwrap_err();
        assert!(err.contains("reserved"), "got: {err}");
    }

    #[test]
    fn manifest_roundtrip_via_ffi() {
        let table = CString::new("events").unwrap();
        let cols =
            CString::new(r#"[{"name":"ts","type":"TIMESTAMPTZ"},{"name":"body","type":"VARCHAR"}]"#)
                .unwrap();
        let policy = CString::new(r#"{"time_col":"ts","ttl":"1 day"}"#).unwrap();
        let manifest = unsafe {
            let mut out: *mut c_char = std::ptr::null_mut();
            let mut err: *mut c_char = std::ptr::null_mut();
            let rc =
                capped_new_manifest(table.as_ptr(), cols.as_ptr(), policy.as_ptr(), &mut out, &mut err);
            assert_eq!(rc, CAPPED_OK);
            let s = CStr::from_ptr(out).to_str().unwrap().to_string();
            capped_free_string(out);
            s
        };

        let m = CString::new(manifest.clone()).unwrap();
        let name =
            unsafe { call_str(|o, e| capped_manifest_table_name(m.as_ptr(), o, e)) }.unwrap();
        assert_eq!(name, "events");
        let back_cols =
            unsafe { call_str(|o, e| capped_manifest_columns(m.as_ptr(), o, e)) }.unwrap();
        assert_eq!(back_cols, r#"[{"name":"ts","type":"TIMESTAMPTZ"},{"name":"body","type":"VARCHAR"}]"#);

        // indexed column accessors
        let mut count: i64 = -1;
        let mut err: *mut c_char = std::ptr::null_mut();
        assert_eq!(
            unsafe { capped_manifest_column_count(m.as_ptr(), &mut count, &mut err) },
            CAPPED_OK
        );
        assert_eq!(count, 2);
        let mut name: *mut c_char = std::ptr::null_mut();
        let mut ty: *mut c_char = std::ptr::null_mut();
        unsafe {
            assert_eq!(
                capped_manifest_column(m.as_ptr(), 1, &mut name, &mut ty, &mut err),
                CAPPED_OK
            );
            assert_eq!(CStr::from_ptr(name).to_str().unwrap(), "body");
            assert_eq!(CStr::from_ptr(ty).to_str().unwrap(), "VARCHAR");
            capped_free_string(name);
            capped_free_string(ty);
        }
        name = std::ptr::null_mut();
        ty = std::ptr::null_mut();
        assert_ne!(
            unsafe { capped_manifest_column(m.as_ptr(), 2, &mut name, &mut ty, &mut err) },
            CAPPED_OK
        );
        assert!(name.is_null() && ty.is_null());

        // WITH-style stringified numbers are accepted for max_rows
        let spec = CString::new(r#"{"time_col":"ts","max_rows":"100000"}"#).unwrap();
        let canonical =
            unsafe { call_str(|o, e| capped_parse_policy(spec.as_ptr(), o, e)) }.unwrap();
        let v: serde_json::Value = serde_json::from_str(&canonical).unwrap();
        assert_eq!(v["max_rows"], 100000u64);
    }

    #[test]
    fn segment_listing_helpers_via_ffi() {
        let table = CString::new("logs").unwrap();
        let cols = CString::new(
            r#"[{"name":"ts","type":"TIMESTAMPTZ"},{"name":"body","type":"VARCHAR"}]"#,
        )
        .unwrap();
        let policy = CString::new(r#"{"time_col":"ts","ttl":"1 day"}"#).unwrap();
        let manifest = unsafe {
            let mut out: *mut c_char = std::ptr::null_mut();
            let mut err: *mut c_char = std::ptr::null_mut();
            assert_eq!(
                capped_new_manifest(table.as_ptr(), cols.as_ptr(), policy.as_ptr(), &mut out, &mut err),
                CAPPED_OK
            );
            let s = CStr::from_ptr(out).to_str().unwrap().to_string();
            capped_free_string(out);
            s
        };
        let m = CString::new(manifest).unwrap();

        // time column + next segment id + empty listing
        let col = unsafe { call_str(|o, e| capped_manifest_time_col(m.as_ptr(), o, e)) }.unwrap();
        assert_eq!(col, "ts");
        let mut id: i64 = -1;
        let mut err: *mut c_char = std::ptr::null_mut();
        assert_eq!(
            unsafe { capped_manifest_next_segment_id(m.as_ptr(), &mut id, &mut err) },
            CAPPED_OK
        );
        assert_eq!(id, 0);
        assert_eq!(
            unsafe { call_str(|o, e| capped_manifest_list_segments(m.as_ptr(), o, e)) }.unwrap(),
            "[]"
        );

        // add a sealed segment, list again
        let seg = CString::new(
            r#"{"id":0,"path":"logs/segments/0.seg.parquet","rows":3,"bytes":90,"min_ts":1,"max_ts":2,"sealed":true,"sealed_at":null}"#,
        )
        .unwrap();
        let m1 =
            unsafe { call_str(|o, e| capped_manifest_put_segment(m.as_ptr(), seg.as_ptr(), o, e)) }
                .unwrap();
        let m1 = CString::new(m1).unwrap();
        let segs =
            unsafe { call_str(|o, e| capped_manifest_list_segments(m1.as_ptr(), o, e)) }.unwrap();
        let v: serde_json::Value = serde_json::from_str(&segs).unwrap();
        assert_eq!(v[0]["id"], 0);
        assert_eq!(v[0]["path"], "logs/segments/0.seg.parquet");
        assert_eq!(v[0]["sealed"], true);
        assert_eq!(v[0]["min_ts"], 1);
        assert_eq!(v[0]["max_ts"], 2);
        assert_eq!(
            unsafe { capped_manifest_next_segment_id(m1.as_ptr(), &mut id, &mut err) },
            CAPPED_OK
        );
        assert_eq!(id, 1);
    }

    #[test]
    fn policy_json_and_default_seal_via_ffi() {
        let table = CString::new("logs").unwrap();
        let cols = CString::new(
            r#"[{"name":"ts","type":"TIMESTAMPTZ"},{"name":"msg","type":"VARCHAR"}]"#,
        )
        .unwrap();
        let policy = CString::new(r#"{"time_col":"ts","ttl":"1 day","max_bytes":"1GB"}"#).unwrap();
        let manifest = unsafe {
            let mut out: *mut c_char = std::ptr::null_mut();
            let mut err: *mut c_char = std::ptr::null_mut();
            assert_eq!(
                capped_new_manifest(table.as_ptr(), cols.as_ptr(), policy.as_ptr(), &mut out, &mut err),
                CAPPED_OK
            );
            let s = CStr::from_ptr(out).to_str().unwrap().to_string();
            capped_free_string(out);
            s
        };
        let m = CString::new(manifest).unwrap();

        let summary =
            unsafe { call_str(|o, e| capped_manifest_policy_json(m.as_ptr(), o, e)) }.unwrap();
        let v: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(v["time_col"], "ts");
        assert_eq!(v["ttl"], 86_400_000_000i64);
        assert_eq!(v["max_bytes"], 1_000_000_000i64);
        assert_eq!(v["max_rows"], serde_json::Value::Null);
        assert_eq!(v["on_violation"], "evict");
        assert_eq!(v["evict_order"], "time");

        // no open segment => never seal
        let mut seal: c_int = -1;
        let mut err: *mut c_char = std::ptr::null_mut();
        assert_eq!(
            unsafe { capped_should_seal_default(m.as_ptr(), &mut seal, &mut err) },
            CAPPED_OK
        );
        assert_eq!(seal, 0);

        // open segment big enough for the default telemetry limits => seal
        let big = CString::new(
            r#"{"id":0,"path":"logs/0.seg.parquet","rows":2000000,"bytes":200000000,"min_ts":1,"max_ts":2,"sealed":false,"sealed_at":null}"#,
        )
        .unwrap();
        let m2 =
            unsafe { call_str(|o, e| capped_manifest_put_segment(m.as_ptr(), big.as_ptr(), o, e)) }
                .unwrap();
        let m2 = CString::new(m2).unwrap();
        assert_eq!(
            unsafe { capped_should_seal_default(m2.as_ptr(), &mut seal, &mut err) },
            CAPPED_OK
        );
        assert_eq!(seal, 1);

        // a tiny open segment => no seal
        let small = CString::new(
            r#"{"id":0,"path":"logs/0.seg.parquet","rows":5,"bytes":500,"min_ts":1,"max_ts":2,"sealed":false,"sealed_at":null}"#,
        )
        .unwrap();
        let m3 =
            unsafe { call_str(|o, e| capped_manifest_put_segment(m.as_ptr(), small.as_ptr(), o, e)) }
                .unwrap();
        let m3 = CString::new(m3).unwrap();
        assert_eq!(
            unsafe { capped_should_seal_default(m3.as_ptr(), &mut seal, &mut err) },
            CAPPED_OK
        );
        assert_eq!(seal, 0);
    }

    #[test]
    fn parse_duration_via_ffi() {
        let cases: &[(&str, i64)] = &[
            ("30s", 30_000_000),
            ("5 minutes", 300_000_000),
            ("1 hour", 3_600_000_000),
            ("1 second", 1_000_000),
            ("10", 10_000_000), // bare number = seconds
            ("0", 0),           // disabling the background loop
        ];
        for (input, want) in cases {
            let c = CString::new(*input).unwrap();
            let mut out: i64 = -1;
            let mut err: *mut c_char = std::ptr::null_mut();
            assert_eq!(
                unsafe { capped_parse_duration(c.as_ptr(), &mut out, &mut err) },
                CAPPED_OK,
                "input: {input}"
            );
            assert_eq!(out, *want, "input: {input}");
        }
        for bad in ["banana", "7 fortnights", ""] {
            let c = CString::new(bad).unwrap();
            let mut out: i64 = 0;
            let mut err: *mut c_char = std::ptr::null_mut();
            unsafe {
                assert_eq!(
                    capped_parse_duration(c.as_ptr(), &mut out, &mut err),
                    CAPPED_ERR,
                    "input: {bad}"
                );
                if !err.is_null() {
                    capped_free_string(err);
                }
            }
        }
    }

    #[test]
    fn put_and_remove_segments() {
        let m0 = Manifest::new("t", vec![Column { name: "x".into(), ty: "INTEGER".into() }],
            RetentionPolicy::builder(TimeSource::IngestTime).max_rows(500).build().unwrap());
        let m0 = CString::new(serde_json::to_string(&m0).unwrap()).unwrap();

        let seg = |id: u64, sealed: bool| {
            CString::new(format!(
                r#"{{"id":{id},"path":"t/{id}.seg","rows":10,"bytes":100,"min_ts":1,"max_ts":2,"sealed":{sealed},"sealed_at":null}}"#
            )).unwrap()
        };

        // open segment 0
        let m1 = unsafe {
            let mut out: *mut c_char = std::ptr::null_mut();
            let mut err: *mut c_char = std::ptr::null_mut();
            assert_eq!(
                capped_manifest_put_segment(m0.as_ptr(), seg(0, false).as_ptr(), &mut out, &mut err),
                CAPPED_OK
            );
            let s = CStr::from_ptr(out).to_str().unwrap().to_string();
            capped_free_string(out);
            s
        };
        // second open segment rejected
        let m1c = CString::new(m1).unwrap();
        let reject_msg = unsafe {
            call_str(|o, e| capped_manifest_put_segment(m1c.as_ptr(), seg(1, false).as_ptr(), o, e))
        }
        .unwrap_err();
        assert!(reject_msg.contains("still open"), "got: {reject_msg}");

        // put sealed segment, then remove
        let m2c = m1c.clone();
        let m2 =
            unsafe { call_str(|o, e| capped_manifest_put_segment(m2c.as_ptr(), seg(0, true).as_ptr(), o, e)) }
                .unwrap();
        let m2 = CString::new(m2).unwrap();
        let ids = CString::new("[0]").unwrap();
        let m3 =
            unsafe { call_str(|o, e| capped_manifest_remove_segments(m2.as_ptr(), ids.as_ptr(), o, e)) }
                .unwrap();
        let v: serde_json::Value = serde_json::from_str(&m3).unwrap();
        assert_eq!(v["segments"].as_array().unwrap().len(), 0);
        assert_eq!(v["next_segment_id"], 1);
    }

    #[test]
    fn segments_without_time_bounds_roundtrip_and_survive_ttl() {
        // The contract the shell relies on for tables with no time_col: null
        // bounds must survive put/list and must never make a segment age out.
        let m0 = Manifest::new(
            "t",
            vec![Column { name: "x".into(), ty: "INTEGER".into() }],
            RetentionPolicy::builder(TimeSource::IngestTime)
                .ttl(Duration::from_secs(1))
                .max_rows(500)
                .build()
                .unwrap(),
        );
        let m0 = CString::new(serde_json::to_string(&m0).unwrap()).unwrap();
        let seg = CString::new(
            r#"{"id":0,"path":"t/0.seg","rows":10,"bytes":100,"min_ts":null,"max_ts":null,"sealed":true,"sealed_at":null}"#,
        )
        .unwrap();

        let m1 =
            unsafe { call_str(|o, e| capped_manifest_put_segment(m0.as_ptr(), seg.as_ptr(), o, e)) }.unwrap();
        let v: serde_json::Value = serde_json::from_str(&m1).unwrap();
        assert_eq!(v["segments"][0]["min_ts"], serde_json::Value::Null);
        assert_eq!(v["segments"][0]["max_ts"], serde_json::Value::Null);

        let m1c = CString::new(m1).unwrap();
        let listed = unsafe { call_str(|o, e| capped_manifest_list_segments(m1c.as_ptr(), o, e)) }.unwrap();
        let v: serde_json::Value = serde_json::from_str(&listed).unwrap();
        assert_eq!(v[0]["min_ts"], serde_json::Value::Null);

        // now = year ~275760: a 1s ttl would have deleted this segment if an
        // unknown bound were treated as "epoch"
        let plan =
            unsafe { call_str(|o, e| capped_plan_eviction(m1c.as_ptr(), i64::MAX / 2, o, e)) }.unwrap();
        let v: serde_json::Value = serde_json::from_str(&plan).unwrap();
        assert_eq!(v["drops"].as_array().unwrap().len(), 0, "got: {plan}");
    }

    #[test]
    fn plan_and_seal_via_ffi() {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .ttl(Duration::from_secs(86_400))
            .build()
            .unwrap();
        let mut m = Manifest::new(
            "t",
            vec![Column { name: "x".into(), ty: "INTEGER".into() }],
            policy,
        );
        m.put_segment(SegmentMeta {
            id: SegmentId(0),
            path: "t/0.seg".into(),
            rows: 5,
            bytes: 50,
            min_ts: Some(0),
            max_ts: Some(1),
            sealed: true,
            sealed_at: Some(1),
            index_file: None,
        })
        .unwrap();
        let mc = CString::new(serde_json::to_string(&m).unwrap()).unwrap();

        let plan = unsafe {
            call_str(|o, e| capped_plan_eviction(mc.as_ptr(), 10 * 86_400_000_000, o, e))
        }
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(&plan).unwrap();
        assert_eq!(v["drops"][0]["id"], 0);
        assert_eq!(v["drops"][0]["reason"], "age");

        let limits = CString::new(r#"{"max_rows":1000}"#).unwrap();
        let mut seal: c_int = -1;
        let mut err: *mut c_char = std::ptr::null_mut();
        assert_eq!(
            unsafe { capped_should_seal(mc.as_ptr(), limits.as_ptr(), &mut seal, &mut err) },
            CAPPED_OK
        );
        assert_eq!(seal, 0); // no open segment
    }

    /// Helper: build a manifest over a telemetry-shaped schema.
    fn search_manifest(policy: &str) -> Result<String, String> {
        let table = CString::new("logs").unwrap();
        let cols = CString::new(
            r#"[{"name":"ts","type":"TIMESTAMPTZ"},{"name":"body","type":"VARCHAR"},
                {"name":"embedding","type":"FLOAT[8]"}]"#,
        )
        .unwrap();
        let pol = CString::new(policy).unwrap();
        unsafe {
            let mut out: *mut c_char = std::ptr::null_mut();
            let mut err: *mut c_char = std::ptr::null_mut();
            if capped_new_manifest(table.as_ptr(), cols.as_ptr(), pol.as_ptr(), &mut out, &mut err)
                == CAPPED_OK
            {
                let s = CStr::from_ptr(out).to_str().unwrap().to_string();
                capped_free_string(out);
                Ok(s)
            } else {
                let s = CStr::from_ptr(err).to_str().unwrap().to_string();
                capped_free_string(err);
                Err(s)
            }
        }
    }

    #[test]
    fn search_options_are_validated_when_the_table_is_created() {
        let m = search_manifest(
            r#"{"max_rows":"1000","search_columns":" body ","vector_columns":"EMBEDDING","vector_metric":"cosine"}"#,
        )
        .unwrap();
        let mc = CString::new(m).unwrap();
        let search = unsafe { call_str(|o, e| capped_manifest_search_json(mc.as_ptr(), o, e)) }.unwrap();
        let v: serde_json::Value = serde_json::from_str(&search).unwrap();
        // names are stored as the table spells them, whitespace is trimmed
        assert_eq!(v["columns"], serde_json::json!(["body"]));
        assert_eq!(v["vector_columns"], serde_json::json!(["embedding"]));
        assert_eq!(v["metric"], "cosine");

        // a plain table wants no indexes at all
        let plain = CString::new(search_manifest(r#"{"max_rows":"10"}"#).unwrap()).unwrap();
        let plain = unsafe { call_str(|o, e| capped_manifest_search_json(plain.as_ptr(), o, e)) }.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&plain).unwrap()["columns"],
            serde_json::json!([])
        );

        // mistakes surface at CREATE TABLE, not when a segment tries to seal
        assert!(search_manifest(r#"{"max_rows":"10","search_columns":"nope"}"#)
            .unwrap_err()
            .contains("no column 'nope'"));
        assert!(search_manifest(r#"{"max_rows":"10","vector_columns":"body"}"#)
            .unwrap_err()
            .contains("fixed-size float array"));
        assert!(search_manifest(
            r#"{"max_rows":"10","search_columns":"body","vector_metric":"manhattan"}"#
        )
        .unwrap_err()
        .contains("vector_metric"));
        assert!(search_manifest(r#"{"max_rows":"10","search_columns":"body,body"}"#)
            .unwrap_err()
            .contains("indexed more than once"));
    }

    #[test]
    fn segment_index_is_recorded_in_the_manifest() {
        let m = search_manifest(r#"{"max_rows":"1000","search_columns":"body"}"#).unwrap();
        let seg = CString::new(
            r#"{"id":0,"path":"logs/0.seg.parquet","rows":3,"bytes":30,
                "min_ts":null,"max_ts":null,"sealed":true,"sealed_at":null,"index_file":null}"#,
        )
        .unwrap();
        let m = unsafe {
            let mut out: *mut c_char = std::ptr::null_mut();
            let mut err: *mut c_char = std::ptr::null_mut();
            let mc = CString::new(m).unwrap();
            assert_eq!(
                capped_manifest_put_segment(mc.as_ptr(), seg.as_ptr(), &mut out, &mut err),
                CAPPED_OK
            );
            let s = CStr::from_ptr(out).to_str().unwrap().to_string();
            capped_free_string(out);
            s
        };

        // no index yet
        let mc = CString::new(m.clone()).unwrap();
        let segs = unsafe { call_str(|o, e| capped_manifest_list_segments(mc.as_ptr(), o, e)) }.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&segs).unwrap()[0]["index_file"],
            serde_json::Value::Null
        );

        // the path convention lives in core, and the manifest remembers it
        let sp = CString::new("logs/0.seg.parquet").unwrap();
        let idx =
            unsafe { call_str(|o, e| capped_index_path_for_segment(sp.as_ptr(), o, e)) }.unwrap();
        assert_eq!(idx, "logs/0.seg.idx.duckdb");

        let m = unsafe {
            let mut out: *mut c_char = std::ptr::null_mut();
            let mut err: *mut c_char = std::ptr::null_mut();
            let mc = CString::new(m).unwrap();
            let ic = CString::new(idx.clone()).unwrap();
            assert_eq!(
                capped_manifest_set_segment_index(mc.as_ptr(), 0, ic.as_ptr(), &mut out, &mut err),
                CAPPED_OK
            );
            let s = CStr::from_ptr(out).to_str().unwrap().to_string();
            capped_free_string(out);
            s
        };
        let mc = CString::new(m.clone()).unwrap();
        let segs = unsafe { call_str(|o, e| capped_manifest_list_segments(mc.as_ptr(), o, e)) }.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&segs).unwrap()[0]["index_file"],
            idx
        );

        // unknown segment ids are refused rather than silently ignored
        let mc = CString::new(m).unwrap();
        let ic = CString::new(idx).unwrap();
        let err = unsafe {
            let mut out: *mut c_char = std::ptr::null_mut();
            let mut err: *mut c_char = std::ptr::null_mut();
            assert_eq!(
                capped_manifest_set_segment_index(mc.as_ptr(), 77, ic.as_ptr(), &mut out, &mut err),
                CAPPED_ERR
            );
            let s = CStr::from_ptr(err).to_str().unwrap().to_string();
            capped_free_string(err);
            s
        };
        assert!(err.contains("no segment 77"), "got: {err}");
    }

    #[test]
    fn manifests_without_the_search_fields_still_load() {
        // a store written before search existed must remain readable
        let json = r#"{"table":"t","columns":[{"name":"x","type":"INTEGER"}],
                       "policy":{"time_source":"IngestTime","ttl":null,"max_bytes":null,
                                 "max_rows":null,"grace":0,"evict_order":"Time","on_violation":"Evict"},
                       "segments":[{"id":0,"path":"t/0.seg.parquet","rows":1,"bytes":10,
                                    "min_ts":null,"max_ts":null,"sealed":true,"sealed_at":null}],
                       "next_segment_id":1}"#;
        let mc = CString::new(json).unwrap();
        let search = unsafe { call_str(|o, e| capped_manifest_search_json(mc.as_ptr(), o, e)) }.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&search).unwrap()["columns"],
            serde_json::json!([])
        );
        let segs = unsafe { call_str(|o, e| capped_manifest_list_segments(mc.as_ptr(), o, e)) }.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&segs).unwrap()[0]["index_file"],
            serde_json::Value::Null
        );
    }
}
