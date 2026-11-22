//! # capped-core
//!
//! The pure-Rust brain of the `capped` DuckDB storage extension.
//!
//! `capped` gives DuckDB bounded, append-optimized tables for telemetry:
//! data flows in, the oldest flows out when the table gets too old, too big,
//! or too full. Eviction is O(1) — whole sealed segments are deleted as files.
//!
//! This crate knows nothing about DuckDB. It owns:
//! - [`policy`] — the retention policy model (ttl / max_bytes / max_rows,
//!   grace, evict order, on-violation behavior) and parsing of human strings
//!   like `'7 days'` / `'1GB'`.
//! - [`manifest`] — per-table bookkeeping: sealed segments + one open segment
//!   (as fragments), persisted as JSON.
//! - [`eviction`] — pure planning: given a manifest and a clock reading, which
//!   segments should be dropped, and when the open segment should be sealed.
//! - [`compaction`] — pure planning: which adjacent fragment run to merge next
//!   in an open segment (size-tiered, see docs/open-segments.md).
//!
//! The DuckDB-facing layer (`ATTACH ... (TYPE capped)`, catalog, insert/scan
//! paths, and running plans against real files) lives outside this crate; see
//! the repo README for the architecture decision.

pub mod capi;
pub mod compaction;
pub mod eviction;
pub mod manifest;
pub mod policy;
pub mod search;

pub use compaction::{plan_compaction, CompactionLimits, CompactionPlan};
pub use eviction::{plan_eviction, should_seal, EvictionPlan, EvictionReason, SealLimits};
pub use manifest::{Column, Manifest, Micros, SegmentId, SegmentMeta};
pub use policy::{
    parse_duration, parse_size, EvictOrder, OnViolation, PolicyError, RetentionPolicy, TimeSource,
    INGEST_TS_COLUMN,
};
pub use search::{index_path_for, SearchSpec, VectorMetric, INDEX_FILE_SUFFIX};

/// Columns the store owns. Users cannot declare them and cannot index them.
pub fn is_reserved_column(name: &str) -> bool {
    name.eq_ignore_ascii_case(INGEST_TS_COLUMN)
}
