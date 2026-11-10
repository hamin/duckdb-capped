//! Segment manifest: the bookkeeping a capped table keeps about its own data.
//!
//! A capped table is an ordered sequence of immutable, sealed segments plus at
//! most one open (appendable) segment. All retention decisions are made
//! against this manifest, never against live data.

use crate::policy::RetentionPolicy;
use serde::{Deserialize, Serialize};

/// Timestamps in core are microseconds since the Unix epoch. The shell layer
/// maps DuckDB TIMESTAMP / TIMESTAMPTZ onto these.
pub type Micros = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub u64);

/// Statistics for one segment. These drive every eviction decision, so a
/// sealed segment's stats must be exact — they are final.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub id: SegmentId,
    /// Store-relative path, e.g. `events/segments/000007.seg.parquet`.
    pub path: String,
    pub rows: u64,
    /// On-disk (compressed) bytes.
    pub bytes: u64,
    /// Bounds of the policy time source over this segment.
    ///
    /// `None` means the segment has no known event time — the table has no
    /// `time_col`, or its values were not temporal. Such a segment is **never
    /// age-droppable** (a made-up bound would silently delete live data); only
    /// the size/count caps can evict it.
    #[serde(default)]
    pub min_ts: Option<Micros>,
    #[serde(default)]
    pub max_ts: Option<Micros>,
    pub sealed: bool,
    /// When the segment was sealed, if it has been.
    pub sealed_at: Option<Micros>,
    /// Store-relative path of this segment's search index, when one was built
    /// (see [`crate::search`]). `None` means the segment is not searchable —
    /// either the table asked for no indexes, or the build failed.
    #[serde(default)]
    pub index_file: Option<String>,
}

/// One column of a capped table. Types are stored as DuckDB type strings
/// ("TIMESTAMPTZ", "VARCHAR", "INTEGER[]"); the shell converts to LogicalType.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

/// Per-table store state, persisted as JSON next to the segment files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Table name this manifest belongs to.
    pub table: String,
    /// Table schema. The manifest is the source of truth for schema — there
    /// is no DuckDB catalog file.
    pub columns: Vec<Column>,
    pub policy: RetentionPolicy,
    /// Search indexes to build per sealed segment. Empty by default: retention
    /// and analytics do not depend on it.
    #[serde(default)]
    pub search: crate::search::SearchSpec,
    /// All segments, open one last (insertion order == id order).
    pub segments: Vec<SegmentMeta>,
    pub next_segment_id: u64,
}

impl Manifest {
    pub fn new(table: impl Into<String>, columns: Vec<Column>, policy: RetentionPolicy) -> Self {
        Self {
            table: table.into(),
            columns,
            policy,
            search: crate::search::SearchSpec::default(),
            segments: Vec::new(),
            next_segment_id: 0,
        }
    }

    /// Record (or clear) the search index built for a segment.
    pub fn set_segment_index(&mut self, id: SegmentId, index: Option<String>) -> Result<(), String> {
        let seg = self
            .segments
            .iter_mut()
            .find(|s| s.id == id)
            .ok_or_else(|| format!("no segment {} in table '{}'", id.0, self.table))?;
        seg.index_file = index;
        Ok(())
    }

    /// Segments that currently have a usable search index.
    pub fn indexed_segments(&self) -> impl Iterator<Item = &SegmentMeta> {
        self.segments
            .iter()
            .filter(|s| s.sealed && s.index_file.is_some())
    }

    pub fn open_segment(&self) -> Option<&SegmentMeta> {
        self.segments.iter().find(|s| !s.sealed)
    }

    pub fn sealed_segments(&self) -> impl Iterator<Item = &SegmentMeta> {
        self.segments.iter().filter(|s| s.sealed)
    }

    pub fn total_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.bytes).sum()
    }

    pub fn total_rows(&self) -> u64 {
        self.segments.iter().map(|s| s.rows).sum()
    }

    /// Insert or replace a segment by id; keeps `next_segment_id` ahead of all ids.
    /// Rejects states with more than one open segment.
    pub fn put_segment(&mut self, seg: SegmentMeta) -> Result<(), String> {
        if !seg.sealed {
            if let Some(open) = self.open_segment() {
                if open.id != seg.id {
                    return Err(format!(
                        "cannot open segment {} while segment {} is still open",
                        seg.id.0, open.id.0
                    ));
                }
            }
        }
        let existing = self.segments.iter().position(|s| s.id == seg.id);
        match existing {
            Some(i) => self.segments[i] = seg,
            None => {
                self.segments.push(seg);
                self.segments.sort_by_key(|s| s.id);
            }
        }
        if let Some(max_id) = self.segments.iter().map(|s| s.id.0).max() {
            self.next_segment_id = self.next_segment_id.max(max_id + 1);
        }
        Ok(())
    }

    pub fn remove_segments(&mut self, ids: &[SegmentId]) {
        self.segments.retain(|s| !ids.contains(&s.id));
    }

    pub fn apply(&mut self, plan: &crate::eviction::EvictionPlan) {
        self.remove_segments(&plan.dropped_ids().collect::<Vec<_>>());
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::policy::{parse_size, TimeSource};
    use std::time::Duration;

    pub(crate) fn seg(id: u64, min_ts: Micros, max_ts: Micros, rows: u64, bytes: u64) -> SegmentMeta {
        SegmentMeta {
            id: SegmentId(id),
            path: format!("t/{id}.seg"),
            rows,
            bytes,
            min_ts: Some(min_ts),
            max_ts: Some(max_ts),
            sealed: true,
            sealed_at: Some(max_ts),
            index_file: None,
        }
    }

    /// A segment with no known event time (no `time_col`, or non-temporal values).
    pub(crate) fn seg_untimed(id: u64, rows: u64, bytes: u64) -> SegmentMeta {
        SegmentMeta {
            id: SegmentId(id),
            path: format!("t/{id}.seg"),
            rows,
            bytes,
            min_ts: None,
            max_ts: None,
            sealed: true,
            sealed_at: None,
            index_file: None,
        }
    }

    pub(crate) fn open_seg(id: u64) -> SegmentMeta {
        SegmentMeta {
            id: SegmentId(id),
            path: format!("t/{id}.seg"),
            rows: 10,
            bytes: 1_000,
            min_ts: None,
            max_ts: None,
            sealed: false,
            sealed_at: None,
            index_file: None,
        }
    }

    fn manifest(segments: Vec<SegmentMeta>) -> Manifest {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .ttl(Duration::from_secs(1))
            .max_bytes(parse_size("1GB").unwrap())
            .build()
            .unwrap();
        Manifest {
            table: "t".into(),
            columns: vec![],
            policy,
            search: Default::default(),
            segments,
            next_segment_id: 100,
        }
    }

    #[test]
    fn totals_and_open_segment() {
        let m = manifest(vec![seg(0, 0, 10, 100, 5_000), seg(1, 10, 20, 200, 5_000), open_seg(2)]);
        assert_eq!(m.total_bytes(), 11_000);
        assert_eq!(m.total_rows(), 310);
        assert_eq!(m.open_segment().map(|s| s.id), Some(SegmentId(2)));
        assert_eq!(m.sealed_segments().count(), 2);
    }

    #[test]
    fn manifest_json_roundtrip() {
        let m = manifest(vec![seg(0, 0, 10, 100, 5_000), open_seg(1)]);
        let json = m.to_json().unwrap();
        let back = Manifest::from_json(&json).unwrap();
        assert_eq!(m, back);
    }
}
