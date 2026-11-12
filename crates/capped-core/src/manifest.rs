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

/// How long a merged or sealed-away fragment lingers in `pending_deletes`
/// before it may be removed, so in-flight scans (which open files lazily)
/// can finish reading it (plan §3.7).
pub const DELETE_GRACE: Micros = 5 * 60 * 1_000_000;

/// One immutable Parquet file belonging to the open segment (plan §3.1).
/// `seq` is per segment, monotonic, and never reused: merged outputs take a
/// fresh seq. Every field defaults so fragment-era JSON still loads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentMeta {
    #[serde(default)]
    pub seq: u64,
    /// Store-relative path, e.g. `events/1.frag-000004.parquet`.
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub rows: u64,
    /// On-disk (compressed) bytes.
    #[serde(default)]
    pub bytes: u64,
    /// Bounds of the policy time source over this fragment; `None` means
    /// unknown, which keeps the whole open segment's bound unknown (§3.2).
    #[serde(default)]
    pub min_ts: Option<Micros>,
    #[serde(default)]
    pub max_ts: Option<Micros>,
    /// 0 = written by a commit; n = merged from tier n-1 (plan §3.4).
    #[serde(default)]
    pub tier: u8,
}

/// A file whose deletion is deferred so scans bound to an older manifest can
/// finish (plan §3.7). Removed once `not_before` has passed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingDelete {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub not_before: Micros,
}

/// Cumulative per-table write accounting: observability and the T10
/// amplification gate (plan §3.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteStats {
    /// Every Parquet byte capped wrote: fragments, compaction merges, and
    /// seal rewrites. Adopted fragments (seal fast path) are not rewritten
    /// and so are not counted twice.
    #[serde(default)]
    pub bytes_written: u64,
    #[serde(default)]
    pub files_written: u64,
    /// Bytes of sealed outputs capped produced (not `capped_add_segment`).
    #[serde(default)]
    pub bytes_sealed: u64,
}

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
    /// Open segment only: its data files, in insertion order. Empty for sealed
    /// segments (their data is `path`). `path` of an open segment is the
    /// *planned* sealed path and does not exist on disk (plan §3.1).
    ///
    /// A compaction output takes the position of the run it replaced (§3.4),
    /// so seqs are unique and never reused but are not sorted in list order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fragments: Vec<FragmentMeta>,
    /// The seq the next fragment of this segment should use; seqs are never
    /// reused (plan §3.1).
    #[serde(default)]
    pub next_fragment_seq: u64,
}

impl SegmentMeta {
    /// A pre-fragment open segment (plan §3.8): unsealed, no fragments, and
    /// still holding its data at `path`. Migrated by [`Manifest::seal_legacy_open`].
    pub fn is_legacy_open(&self) -> bool {
        !self.sealed && self.fragments.is_empty() && self.rows > 0
    }
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
    /// Merged/sealed-away fragment files awaiting deletion once the grace
    /// period passes (plan §3.7). Their bytes are not counted against
    /// `max_bytes`; they are bounded by roughly one open segment per grace
    /// window.
    #[serde(default)]
    pub pending_deletes: Vec<PendingDelete>,
    /// Cumulative write accounting (plan §3.2), for observability and the
    /// amplification gate.
    #[serde(default)]
    pub write_stats: WriteStats,
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
            pending_deletes: Vec::new(),
            write_stats: WriteStats::default(),
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
    /// Rejects states that violate the §3.2 invariants: more than one open
    /// segment, sealed segments with fragments, open-segment stats that do not
    /// match their fragments, fragment seqs reused or ahead of
    /// `next_fragment_seq`, and a path appearing twice across segments,
    /// fragments, and pending deletes.
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
        if seg.path.is_empty() {
            return Err(format!("segment {} has an empty path", seg.id.0));
        }
        check_fragment_list(&seg)?;
        check_unique_within(&seg)?;
        if self.path_taken(&seg.path, Some(seg.id)) {
            return Err(format!(
                "path '{}' is already used by another segment or pending delete in table '{}'",
                seg.path, self.table
            ));
        }
        for f in &seg.fragments {
            if self.path_taken(&f.path, Some(seg.id)) {
                return Err(format!(
                    "fragment path '{}' is already used in table '{}'",
                    f.path, self.table
                ));
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

    // ---- fragment mutators (open-segment fragments plan §3.2–§3.5) --------

    /// Record a freshly written fragment file on the open segment (plan §3.3
    /// step 4). The caller has already written the file at `frag.path`; this
    /// only makes the manifest name it, updates the open segment's stored
    /// aggregates to match its fragments, and bumps `write_stats`.
    ///
    /// `frag.seq` must be at least the segment's `next_fragment_seq` so seqs
    /// are never reused; the fragment is appended (commits are list order).
    pub fn add_fragment(&mut self, segment: SegmentId, frag: FragmentMeta) -> Result<(), String> {
        let idx = self.open_index(segment)?;
        {
            let seg = &self.segments[idx];
            if seg.is_legacy_open() {
                return Err(format!(
                    "open segment {} is a legacy open segment; migrate it (seal_legacy_open) before adding fragments",
                    segment.0
                ));
            }
            check_fragment_list(seg)?;
            if frag.path.is_empty() {
                return Err(format!("fragment for segment {} has an empty path", segment.0));
            }
            if frag.seq < seg.next_fragment_seq {
                return Err(format!(
                    "fragment seq {} was already used in segment {} (next seq is {})",
                    frag.seq, segment.0, seg.next_fragment_seq
                ));
            }
            if seg.path == frag.path || seg.fragments.iter().any(|f| f.path == frag.path) {
                return Err(format!(
                    "path '{}' is already used by segment {}",
                    frag.path, segment.0
                ));
            }
            if self.path_taken(&frag.path, Some(segment)) {
                return Err(format!(
                    "fragment path '{}' is already used in table '{}'",
                    frag.path, self.table
                ));
            }
        }
        let seg = &mut self.segments[idx];
        seg.next_fragment_seq = frag.seq + 1;
        let frag_bytes = frag.bytes;
        seg.fragments.push(frag);
        let (rows, bytes, min_ts, max_ts) = fragment_aggregates(&seg.fragments);
        seg.rows = rows;
        seg.bytes = bytes;
        seg.min_ts = min_ts;
        seg.max_ts = max_ts;
        self.write_stats.bytes_written += frag_bytes;
        self.write_stats.files_written += 1;
        Ok(())
    }

    /// Apply one size-tiered merge (plan §3.4): replace the contiguous run of
    /// same-tier fragments named by `inputs` (seqs, in list order) with the
    /// already-written `output` at the run's position, so insertion order is
    /// preserved. The inputs move to `pending_deletes` with
    /// `not_before = now + DELETE_GRACE`; `write_stats` is bumped for the
    /// output. The open segment's aggregates are recomputed from its fragments.
    pub fn apply_compaction(
        &mut self,
        segment: SegmentId,
        inputs: &[u64],
        output: FragmentMeta,
        now: Micros,
    ) -> Result<(), String> {
        if inputs.len() < 2 {
            return Err(format!(
                "compaction of segment {} needs at least two inputs, got {}",
                segment.0,
                inputs.len()
            ));
        }
        let idx = self.open_index(segment)?;
        let (start, input_paths) = {
            let seg = &self.segments[idx];
            if seg.is_legacy_open() {
                return Err(format!(
                    "open segment {} is a legacy open segment; migrate it before compacting",
                    segment.0
                ));
            }
            check_fragment_list(seg)?;
            let start = seg
                .fragments
                .windows(inputs.len())
                .position(|w| w.iter().map(|f| f.seq).eq(inputs.iter().copied()))
                .ok_or_else(|| {
                    format!("inputs {inputs:?} are not a contiguous run of open segment {}", segment.0)
                })?;
            let run = &seg.fragments[start..start + inputs.len()];
            let tier = run[0].tier;
            if !run.iter().all(|f| f.tier == tier) {
                return Err(format!(
                    "compaction inputs for segment {} mix tiers; only a same-tier run may merge",
                    segment.0
                ));
            }
            if output.tier != tier + 1 {
                return Err(format!(
                    "compaction output tier {} must be one above its inputs (tier {})",
                    output.tier, tier
                ));
            }
            if output.path.is_empty() {
                return Err(format!("compaction output for segment {} has an empty path", segment.0));
            }
            if output.seq < seg.next_fragment_seq {
                return Err(format!(
                    "compaction output seq {} was already used in segment {} (next seq is {})",
                    output.seq, segment.0, seg.next_fragment_seq
                ));
            }
            if seg.path == output.path || seg.fragments.iter().any(|f| f.path == output.path) {
                return Err(format!(
                    "compaction output path '{}' collides with a live file of segment {}",
                    output.path, segment.0
                ));
            }
            if self.path_taken(&output.path, Some(segment)) {
                return Err(format!(
                    "compaction output path '{}' is already used in table '{}'",
                    output.path, self.table
                ));
            }
            (
                start,
                run.iter().map(|f| f.path.clone()).collect::<Vec<_>>(),
            )
        };
        let seg = &mut self.segments[idx];
        seg.next_fragment_seq = output.seq + 1;
        seg.fragments.splice(start..start + inputs.len(), [output]);
        let (rows, bytes, min_ts, max_ts) = fragment_aggregates(&seg.fragments);
        seg.rows = rows;
        seg.bytes = bytes;
        seg.min_ts = min_ts;
        seg.max_ts = max_ts;
        self.write_stats.bytes_written += seg.fragments[start].bytes;
        self.write_stats.files_written += 1;
        let not_before = now + DELETE_GRACE;
        for path in input_paths {
            self.pending_deletes.push(PendingDelete { path, not_before });
        }
        Ok(())
    }

    /// Seal the open segment whose fragments the caller has just merged (or
    /// adopted) into the final file (plan §3.5). `output` carries the final
    /// path and the **exact** stats computed from that file; `rows` must match
    /// the fragments being replaced — a seal never loses or gains rows.
    ///
    /// The segment becomes sealed (`sealed = true`, `sealed_at = now`,
    /// `path = output.path`, fragments cleared). Fragment files move to
    /// `pending_deletes`, except a fragment whose path is reused as the output
    /// path — the single-fragment fast path that adopts the file without
    /// rewriting it (plan §3.5 step 3), which therefore adds nothing to
    /// `bytes_written`/`files_written`. `bytes_sealed` counts the output in
    /// both cases.
    pub fn seal_open(&mut self, segment: SegmentId, output: &SegmentMeta, now: Micros) -> Result<(), String> {
        if output.id != segment {
            return Err(format!(
                "seal target is segment {} but output names segment {}",
                segment.0, output.id.0
            ));
        }
        if !output.fragments.is_empty() {
            return Err(format!(
                "seal output for segment {} must not carry fragments",
                segment.0
            ));
        }
        if output.path.is_empty() {
            return Err(format!("seal output for segment {} has an empty path", segment.0));
        }
        let idx = self.open_index(segment)?;
        let adopted = {
            let seg = &self.segments[idx];
            if seg.is_legacy_open() {
                return Err(format!(
                    "open segment {} is a legacy open segment; migrate it (seal_legacy_open) instead of sealing with an output",
                    segment.0
                ));
            }
            if seg.fragments.is_empty() {
                return Err(format!(
                    "open segment {} has no fragments; nothing to seal",
                    segment.0
                ));
            }
            check_fragment_list(seg)?;
            if output.rows != seg.rows {
                return Err(format!(
                    "sealed segment {} must have exactly the fragments' rows ({}), got {}",
                    segment.0, seg.rows, output.rows
                ));
            }
            if seg.path != output.path && self.path_taken(&output.path, Some(segment)) {
                return Err(format!(
                    "seal output path '{}' is already used in table '{}'",
                    output.path, self.table
                ));
            }
            seg.fragments.iter().any(|f| f.path == output.path)
        };
        let seg = &mut self.segments[idx];
        let old_fragments = std::mem::take(&mut seg.fragments);
        let not_before = now + DELETE_GRACE;
        for f in &old_fragments {
            if f.path != output.path {
                self.pending_deletes.push(PendingDelete { path: f.path.clone(), not_before });
            }
        }
        seg.path = output.path.clone();
        seg.rows = output.rows;
        seg.bytes = output.bytes;
        seg.min_ts = output.min_ts;
        seg.max_ts = output.max_ts;
        seg.sealed = true;
        seg.sealed_at = Some(now);
        // A freshly sealed segment carries no search index yet; it is built
        // after the swap by the shell (set_segment_index).
        seg.index_file = None;
        self.write_stats.bytes_sealed += output.bytes;
        if !adopted {
            self.write_stats.bytes_written += output.bytes;
            self.write_stats.files_written += 1;
        }
        Ok(())
    }

    /// Queue a file for deferred deletion (plan §3.7). Rejects any path the
    /// manifest still references — this is what makes the GC rule "never
    /// delete a referenced file" hold by construction.
    pub fn queue_delete(&mut self, path: impl Into<String>, not_before: Micros) -> Result<(), String> {
        let path = path.into();
        if path.is_empty() {
            return Err("cannot queue an empty path for deletion".into());
        }
        if self
            .segments
            .iter()
            .any(|s| s.path == path || s.fragments.iter().any(|f| f.path == path))
        {
            return Err(format!("cannot queue referenced path '{path}' for deletion"));
        }
        if self.pending_deletes.iter().any(|d| d.path == path) {
            return Err(format!("'{path}' is already queued for deletion"));
        }
        self.pending_deletes.push(PendingDelete { path, not_before });
        Ok(())
    }

    /// Remove and return the paths whose grace period has elapsed; the shell
    /// deletes those files after the manifest swap (plan §3.7). Order is kept.
    pub fn take_due_deletes(&mut self, now: Micros) -> Vec<String> {
        let mut due = Vec::new();
        self.pending_deletes.retain(|d| {
            if d.not_before <= now {
                due.push(d.path.clone());
                false
            } else {
                true
            }
        });
        due
    }

    /// Whether the open segment is a pre-fragment (legacy) one, plan §3.8.
    pub fn is_legacy_open(&self) -> bool {
        self.open_segment().is_some_and(SegmentMeta::is_legacy_open)
    }

    /// Migrate a legacy open segment in place (plan §3.8): it is already a
    /// complete Parquet file at its `path`, so sealing just flips the flag —
    /// no rewrite, no new name, no bytes counted in `write_stats`. The next
    /// insert starts a fresh fragment-based open segment.
    pub fn seal_legacy_open(&mut self, now: Micros) -> Result<(), String> {
        let idx = match self.segments.iter().position(|s| !s.sealed) {
            Some(i) => i,
            None => return Err(format!("table '{}' has no open segment to migrate", self.table)),
        };
        if !self.segments[idx].is_legacy_open() {
            return Err(format!(
                "open segment {} is not a legacy open segment (fragments: {})",
                self.segments[idx].id.0,
                self.segments[idx].fragments.len()
            ));
        }
        self.segments[idx].sealed = true;
        self.segments[idx].sealed_at = Some(now);
        Ok(())
    }

    /// Index of the open segment with this id.
    fn open_index(&self, id: SegmentId) -> Result<usize, String> {
        self.segments
            .iter()
            .position(|s| s.id == id && !s.sealed)
            .ok_or_else(|| format!("segment {} is not the open segment of table '{}'", id.0, self.table))
    }

    /// Is `path` named by any segment (other than `skip_id`) or its fragments,
    /// or sitting in `pending_deletes`? (§3.2: a path appears at most once.)
    fn path_taken(&self, path: &str, skip_id: Option<SegmentId>) -> bool {
        self.pending_deletes.iter().any(|d| d.path == path)
            || self.segments.iter().any(|s| {
                Some(s.id) != skip_id
                    && (s.path == path || s.fragments.iter().any(|f| f.path == path))
            })
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

/// The aggregate stats a fragment list implies for its open segment (§3.2).
/// A bound that is unknown on any fragment makes the aggregate unknown: a
/// made-up bound would silently evict live data. An empty list is (0,0,None,None).
fn fragment_aggregates(fragments: &[FragmentMeta]) -> (u64, u64, Option<Micros>, Option<Micros>) {
    if fragments.is_empty() {
        return (0, 0, None, None);
    }
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut min_ts = Some(i64::MAX);
    let mut max_ts = Some(i64::MIN);
    for f in fragments {
        rows += f.rows;
        bytes += f.bytes;
        min_ts = match (min_ts, f.min_ts) {
            (Some(a), Some(b)) => Some(a.min(b)),
            _ => None,
        };
        max_ts = match (max_ts, f.max_ts) {
            (Some(a), Some(b)) => Some(a.max(b)),
            _ => None,
        };
    }
    (rows, bytes, min_ts, max_ts)
}

/// The per-segment half of the §3.2 invariants: sealed segments have no
/// fragments; fragment seqs are unique and below `next_fragment_seq` (they
/// are never reused); with fragments present, the stored open-segment stats
/// equal their aggregate.
fn check_fragment_list(seg: &SegmentMeta) -> Result<(), String> {
    if seg.sealed && !seg.fragments.is_empty() {
        return Err(format!(
            "sealed segment {} cannot have fragments",
            seg.id.0
        ));
    }
    let mut seen: Vec<u64> = Vec::with_capacity(seg.fragments.len());
    for f in &seg.fragments {
        if f.path.is_empty() {
            return Err(format!("segment {} has a fragment with an empty path", seg.id.0));
        }
        if seen.contains(&f.seq) {
            return Err(format!(
                "fragment seq {} is used twice in segment {}",
                f.seq, seg.id.0
            ));
        }
        seen.push(f.seq);
        if f.seq >= seg.next_fragment_seq {
            return Err(format!(
                "fragment seq {} of segment {} is not below next_fragment_seq {}",
                f.seq, seg.id.0, seg.next_fragment_seq
            ));
        }
    }
    if !seg.fragments.is_empty() {
        let (rows, bytes, min_ts, max_ts) = fragment_aggregates(&seg.fragments);
        if seg.rows != rows || seg.bytes != bytes || seg.min_ts != min_ts || seg.max_ts != max_ts {
            return Err(format!(
                "open segment {} stats ({},{:?},{:?}, {} bytes) do not match its fragments ({rows},{min_ts:?},{max_ts:?}, {bytes} bytes)",
                seg.id.0, seg.rows, seg.min_ts, seg.max_ts, seg.bytes
            ));
        }
    }
    Ok(())
}

/// No path may appear twice inside one segment (its path plus fragments).
fn check_unique_within(seg: &SegmentMeta) -> Result<(), String> {
    let mut names: Vec<&str> = vec![seg.path.as_str()];
    for f in &seg.fragments {
        if names.contains(&f.path.as_str()) {
            return Err(format!(
                "path '{}' appears twice in segment {}",
                f.path, seg.id.0
            ));
        }
        names.push(&f.path);
    }
    Ok(())
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
            fragments: Vec::new(),
            next_fragment_seq: 0,
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
            fragments: Vec::new(),
            next_fragment_seq: 0,
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
            fragments: Vec::new(),
            next_fragment_seq: 0,
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
            pending_deletes: Vec::new(),
            write_stats: WriteStats::default(),
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

    // ---- open-segment fragments (plan T1) ---------------------------------

    /// A fragment as the shell would name it: `<seg>.frag-<seq:06>.parquet`.
    pub(crate) fn frag(
        seg_id: u64,
        seq: u64,
        tier: u8,
        rows: u64,
        bytes: u64,
        ts: Option<(Micros, Micros)>,
    ) -> FragmentMeta {
        FragmentMeta {
            seq,
            path: format!("t/{seg_id}.frag-{seq:06}.parquet"),
            rows,
            bytes,
            min_ts: ts.map(|(a, _)| a),
            max_ts: ts.map(|(_, b)| b),
            tier,
        }
    }

    /// Open segment with `frags` and stored aggregates that match them, as
    /// `add_fragment`/`apply_compaction` leave it.
    fn open_with_frags(id: u64, frags: Vec<FragmentMeta>, next_fragment_seq: u64) -> SegmentMeta {
        let (rows, bytes, min_ts, max_ts) = fragment_aggregates(&frags);
        SegmentMeta {
            id: SegmentId(id),
            path: format!("t/{id}.seg.parquet"),
            rows,
            bytes,
            min_ts,
            max_ts,
            sealed: false,
            sealed_at: None,
            index_file: None,
            fragments: frags,
            next_fragment_seq,
        }
    }

    /// A freshly allocated open segment: no data yet.
    fn fresh_open(id: u64) -> SegmentMeta {
        open_with_frags(id, Vec::new(), 0)
    }

    /// A full SegmentMeta as the shell would hand to `seal_open`.
    fn sealed_output(id: u64, path: &str, rows: u64, bytes: u64, min_ts: Micros, max_ts: Micros) -> SegmentMeta {
        SegmentMeta {
            id: SegmentId(id),
            path: path.into(),
            rows,
            bytes,
            min_ts: Some(min_ts),
            max_ts: Some(max_ts),
            sealed: true,
            sealed_at: None,
            index_file: None,
            fragments: Vec::new(),
            next_fragment_seq: 0,
        }
    }

    #[test]
    fn old_json_fixture_loads_with_defaults() {
        // Real pre-fragment manifest, produced by examples/telemetry/demo.sh
        // against the release build (T1 fixture requirement).
        const OLD: &str = include_str!("../testdata/legacy_manifest.json");
        for absent in ["fragments", "next_fragment_seq", "pending_deletes", "write_stats"] {
            assert!(!OLD.contains(absent), "fixture unexpectedly contains '{absent}'");
        }
        let m = Manifest::from_json(OLD).unwrap();
        assert_eq!(m.table, "logs");
        assert!(m.pending_deletes.is_empty());
        assert_eq!(m.write_stats, WriteStats::default());
        assert_eq!(m.segments.len(), 1);
        let s = &m.segments[0];
        // The values demo.sh actually wrote for the surviving "recent" batch.
        assert_eq!(s.path, "logs/2.seg.parquet");
        assert_eq!((s.rows, s.bytes), (400, 10_022));
        assert!(s.sealed && s.fragments.is_empty() && s.next_fragment_seq == 0);
        assert!(m.open_segment().is_none()); // the demo seals every batch
        // Round-trip through the new format is stable.
        assert_eq!(Manifest::from_json(&m.to_json().unwrap()).unwrap(), m);
    }

    #[test]
    fn old_json_with_open_segment_loads_migrates_and_grows() {
        // Hand-written from the pre-change struct definitions: an old store's
        // manifest.json, with no fragments/write_stats keys anywhere and an
        // open (legacy) segment holding data at `path`.
        const OLD: &str = r#"{
            "table": "t",
            "columns": [{"name": "ts", "type": "TIMESTAMPTZ"}],
            "policy": {"time_source": "IngestTime", "grace": 0, "evict_order": "Time", "on_violation": "Evict"},
            "segments": [{"id": 7, "path": "t/7.seg.parquet", "rows": 10, "bytes": 900,
                          "min_ts": 100, "max_ts": 200, "sealed": false, "sealed_at": null, "index_file": null}],
            "next_segment_id": 8
        }"#;
        let mut m = Manifest::from_json(OLD).unwrap();
        let open = m.open_segment().unwrap();
        assert!(open.fragments.is_empty() && open.next_fragment_seq == 0);
        assert!(open.is_legacy_open() && m.is_legacy_open());
        // Fragments cannot be added to a legacy open segment...
        assert!(m.add_fragment(SegmentId(7), frag(7, 0, 0, 1, 10, Some((1, 2)))).is_err());
        // ...which migrates in place: flag flips, path/stats stay, no write counted.
        m.seal_legacy_open(555).unwrap();
        let s = m.segments.iter().find(|s| s.id == SegmentId(7)).unwrap();
        assert!(s.sealed && s.sealed_at == Some(555) && s.path == "t/7.seg.parquet");
        assert_eq!((s.rows, s.bytes, s.min_ts, s.max_ts), (10, 900, Some(100), Some(200)));
        assert_eq!(m.write_stats, WriteStats::default());
        // A fresh fragment-based open segment takes over and round-trips.
        m.put_segment(fresh_open(8)).unwrap();
        m.add_fragment(SegmentId(8), frag(8, 0, 0, 4, 80, Some((300, 350)))).unwrap();
        assert_eq!(Manifest::from_json(&m.to_json().unwrap()).unwrap(), m);
    }

    #[test]
    fn add_fragment_updates_aggregates_and_stats() {
        let mut m = manifest(vec![]);
        m.put_segment(fresh_open(3)).unwrap();
        m.add_fragment(SegmentId(3), frag(3, 0, 0, 5, 100, Some((10, 20)))).unwrap();
        let s = m.open_segment().unwrap();
        assert_eq!((s.rows, s.bytes, s.min_ts, s.max_ts), (5, 100, Some(10), Some(20)));
        assert_eq!(s.next_fragment_seq, 1);
        m.add_fragment(SegmentId(3), frag(3, 1, 0, 3, 50, Some((5, 30)))).unwrap();
        let s = m.open_segment().unwrap();
        assert_eq!((s.rows, s.bytes, s.min_ts, s.max_ts), (8, 150, Some(5), Some(30)));
        // An unknown bound on any fragment makes the open segment's bound
        // unknown: never age-droppable rather than guessed (§3.2).
        m.add_fragment(SegmentId(3), frag(3, 2, 0, 1, 60, None)).unwrap();
        let s = m.open_segment().unwrap();
        assert_eq!((s.rows, s.bytes, s.min_ts, s.max_ts), (9, 210, None, None));
        assert_eq!((s.next_fragment_seq, s.fragments.len()), (3, 3));
        assert_eq!(m.write_stats.bytes_written, 210);
        assert_eq!(m.write_stats.files_written, 3);
        // An empty fragment list serializes like the old format.
        let mut m2 = manifest(vec![]);
        m2.put_segment(fresh_open(0)).unwrap();
        assert!(!m2.to_json().unwrap().contains("\"fragments\""));
    }

    #[test]
    fn add_fragment_rejects_bad_states() {
        let mut m = manifest(vec![seg(0, 0, 10, 100, 5_000)]);
        // No open segment at all.
        assert!(m.add_fragment(SegmentId(1), frag(1, 0, 0, 1, 10, None)).is_err());
        m.put_segment(fresh_open(1)).unwrap();
        // Target must be the open segment, not a sealed one.
        assert!(m.add_fragment(SegmentId(0), frag(0, 0, 0, 1, 10, None)).is_err());
        // A legacy open segment must be migrated first.
        let mut legacy = manifest(vec![]);
        legacy.put_segment(open_seg(9)).unwrap(); // rows > 0, no fragments
        assert!(legacy.is_legacy_open());
        assert!(legacy.add_fragment(SegmentId(9), frag(9, 0, 0, 1, 10, None)).is_err());
        // Seqs are never reused or rewound.
        m.add_fragment(SegmentId(1), frag(1, 0, 0, 1, 10, None)).unwrap();
        assert!(m.add_fragment(SegmentId(1), frag(1, 0, 0, 1, 10, None)).is_err());
        assert!(m.add_fragment(SegmentId(1), frag(1, 0, 0, 1, 11, Some((1, 2)))).is_err());
        // A new seq pointing at a live file.
        let dup = FragmentMeta { seq: 7, path: "t/1.frag-000000.parquet".into(), ..Default::default() };
        assert!(m.add_fragment(SegmentId(1), dup).is_err());
        // A path owned by another segment.
        let steals = FragmentMeta { seq: 8, path: "t/0.seg".into(), ..Default::default() };
        assert!(m.add_fragment(SegmentId(1), steals).is_err());
        // A path in pending deletes, and the empty path.
        m.queue_delete("t/ghost.parquet", 1_000).unwrap();
        let ghost = FragmentMeta { seq: 9, path: "t/ghost.parquet".into(), ..Default::default() };
        assert!(m.add_fragment(SegmentId(1), ghost).is_err());
        let empty = FragmentMeta { seq: 10, path: String::new(), ..Default::default() };
        assert!(m.add_fragment(SegmentId(1), empty).is_err());
        // The planned sealed path is not up for grabs as a fragment file.
        let planned = FragmentMeta { seq: 11, path: "t/1.seg.parquet".into(), ..Default::default() };
        assert!(m.add_fragment(SegmentId(1), planned).is_err());
    }

    #[test]
    fn apply_compaction_replaces_run_in_place() {
        let mut m = manifest(vec![]);
        m.put_segment(fresh_open(4)).unwrap();
        for i in 0..9u64 {
            m.add_fragment(SegmentId(4), frag(4, i, 0, 10, 100, Some((100 + i as i64, 200 + i as i64)))).unwrap();
        }
        // Merge the oldest 8 tier-0 fragments into one tier-1 fragment.
        let inputs: Vec<u64> = (0..8).collect();
        let out = FragmentMeta {
            seq: 9,
            path: "t/4.frag-000009.parquet".into(),
            rows: 80,
            bytes: 700,
            min_ts: Some(100),
            max_ts: Some(207),
            tier: 1,
        };
        m.apply_compaction(SegmentId(4), &inputs, out, 1_000).unwrap();
        let s = m.open_segment().unwrap();
        // The output takes the run's position; insertion order survives.
        assert_eq!(
            s.fragments.iter().map(|f| (f.seq, f.tier)).collect::<Vec<_>>(),
            vec![(9, 1), (8, 0)]
        );
        assert_eq!((s.rows, s.bytes, s.min_ts, s.max_ts), (90, 800, Some(100), Some(208)));
        assert_eq!(s.next_fragment_seq, 10);
        // Inputs wait out the grace period, in order.
        assert_eq!(m.pending_deletes.len(), 8);
        assert!(m.pending_deletes.iter().all(|d| d.not_before == 1_000 + DELETE_GRACE));
        assert_eq!(m.pending_deletes[0].path, "t/4.frag-000000.parquet");
        assert_eq!(m.pending_deletes[7].path, "t/4.frag-000007.parquet");
        // 9 commit writes + 1 merge output.
        assert_eq!(m.write_stats.files_written, 10);
        assert_eq!(m.write_stats.bytes_written, 9 * 100 + 700);
        // Still round-trips after a merge.
        assert_eq!(Manifest::from_json(&m.to_json().unwrap()).unwrap(), m);
    }

    #[test]
    fn apply_compaction_rejects_bad_plans() {
        let mut m = manifest(vec![]);
        m.put_segment(seg(0, 0, 10, 100, 5_000)).unwrap();
        m.put_segment(fresh_open(4)).unwrap();
        for i in 0..9u64 {
            m.add_fragment(SegmentId(4), frag(4, i, 0, 10, 100, Some((100, 200)))).unwrap();
        }
        let out = || FragmentMeta {
            seq: 9,
            path: "t/4.frag-000009.parquet".into(),
            rows: 20,
            bytes: 180,
            min_ts: Some(100),
            max_ts: Some(200),
            tier: 1,
        };
        // Fewer than two inputs.
        assert!(m.apply_compaction(SegmentId(4), &[0], out(), 0).is_err());
        // Not a contiguous run, and unknown seqs.
        assert!(m.apply_compaction(SegmentId(4), &[0, 2], out(), 0).is_err());
        assert!(m.apply_compaction(SegmentId(4), &[99, 100], out(), 0).is_err());
        // Output tier must be one above the inputs'.
        let mut same = out();
        same.tier = 0;
        assert!(m.apply_compaction(SegmentId(4), &[0, 1], same, 0).is_err());
        let mut two_up = out();
        two_up.tier = 2;
        assert!(m.apply_compaction(SegmentId(4), &[0, 1], two_up, 0).is_err());
        // Output seq already used.
        let mut reused = out();
        reused.seq = 3;
        assert!(m.apply_compaction(SegmentId(4), &[0, 1], reused, 0).is_err());
        // Output path collides with a live fragment...
        let mut dup = out();
        dup.path = "t/4.frag-000005.parquet".into();
        assert!(m.apply_compaction(SegmentId(4), &[0, 1], dup, 0).is_err());
        // ...another segment...
        let mut on_sealed = out();
        on_sealed.path = "t/0.seg".into();
        assert!(m.apply_compaction(SegmentId(4), &[0, 1], on_sealed, 0).is_err());
        // ...or a pending delete.
        m.queue_delete("t/ghost", 1).unwrap();
        let mut on_ghost = out();
        on_ghost.path = "t/ghost".into();
        assert!(m.apply_compaction(SegmentId(4), &[0, 1], on_ghost, 0).is_err());
        // A mixed-tier run never merges.
        let mut mix = manifest(vec![]);
        mix.put_segment(open_with_frags(
            2,
            vec![frag(2, 0, 0, 5, 50, Some((1, 2))), frag(2, 1, 1, 5, 50, Some((1, 2)))],
            2,
        ))
        .unwrap();
        let mixed_out = FragmentMeta { seq: 2, path: "t/2.frag-000002.parquet".into(), rows: 10, bytes: 90, ..Default::default() };
        assert!(mix.apply_compaction(SegmentId(2), &[0, 1], mixed_out, 0).is_err());
        // Legacy open segments must be migrated first.
        let mut legacy = manifest(vec![]);
        legacy.put_segment(open_seg(1)).unwrap();
        assert!(legacy.apply_compaction(SegmentId(1), &[0, 1], out(), 0).is_err());
    }

    #[test]
    fn seal_open_rewrite_seals_and_queues_fragments() {
        let mut m = manifest(vec![]);
        m.put_segment(fresh_open(5)).unwrap();
        for i in 0..3u64 {
            m.add_fragment(SegmentId(5), frag(5, i, 0, 7, 300, Some((10 + i as i64, 50 + i as i64)))).unwrap();
        }
        // Sealed stats come from the output file: bytes are exact after the
        // rewrite (may differ from the fragment sum; here compression wins).
        let output = sealed_output(5, "t/5.seg.parquet", 21, 800, 10, 52);
        m.seal_open(SegmentId(5), &output, 4_242).unwrap();
        let s = &m.segments[0];
        assert!(s.sealed && s.sealed_at == Some(4_242));
        assert_eq!(s.path, "t/5.seg.parquet");
        assert!(s.fragments.is_empty());
        assert_eq!((s.rows, s.bytes, s.min_ts, s.max_ts), (21, 800, Some(10), Some(52)));
        assert_eq!(m.pending_deletes.len(), 3); // fragments wait out the grace
        assert!(m.pending_deletes.iter().all(|d| d.not_before == 4_242 + DELETE_GRACE));
        // 3 fragment writes + 1 seal rewrite; bytes_sealed is the exact output.
        assert_eq!(m.write_stats, WriteStats { bytes_written: 900 + 800, files_written: 4, bytes_sealed: 800 });
        assert!(m.open_segment().is_none());
        assert_eq!(Manifest::from_json(&m.to_json().unwrap()).unwrap(), m);
    }

    #[test]
    fn seal_open_single_fragment_adoption_is_free() {
        let mut m = manifest(vec![]);
        m.put_segment(fresh_open(6)).unwrap();
        m.add_fragment(SegmentId(6), frag(6, 0, 0, 4, 111, Some((7, 9)))).unwrap();
        // Fast path (§3.5 step 3): the fragment file *becomes* the sealed file.
        let output = sealed_output(6, "t/6.frag-000000.parquet", 4, 111, 7, 9);
        m.seal_open(SegmentId(6), &output, 77).unwrap();
        let s = &m.segments[0];
        assert!(s.sealed && s.path == "t/6.frag-000000.parquet");
        assert!(m.pending_deletes.is_empty(), "the adopted file must not be queued for deletion");
        assert_eq!(m.write_stats, WriteStats { bytes_written: 111, files_written: 1, bytes_sealed: 111 });
        assert_eq!(s.next_fragment_seq, 1, "seqs are never reused, even across seal");
    }

    #[test]
    fn seal_open_rejects_bad_outputs() {
        let mut m = manifest(vec![]);
        m.put_segment(seg(1, 0, 10, 5, 500)).unwrap();
        m.put_segment(fresh_open(5)).unwrap();
        m.add_fragment(SegmentId(5), frag(5, 0, 0, 7, 300, Some((10, 50)))).unwrap();
        let base = || sealed_output(5, "t/5.seg.parquet", 7, 280, 10, 50);
        // A seal never loses rows: output stats must match the fragments.
        let mut lost = base();
        lost.rows = 6;
        assert!(m.seal_open(SegmentId(5), &lost, 0).is_err());
        // Id mismatch.
        let mut wrong = base();
        wrong.id = SegmentId(4);
        assert!(m.seal_open(SegmentId(5), &wrong, 0).is_err());
        // Target must be open.
        assert!(m.seal_open(SegmentId(1), &base(), 0).is_err());
        // Output must not carry fragments.
        let mut with_frags = base();
        with_frags.fragments = vec![frag(5, 9, 0, 1, 1, None)];
        assert!(m.seal_open(SegmentId(5), &with_frags, 0).is_err());
        // Output path owned by another segment.
        let mut collide = base();
        collide.path = "t/1.seg".into();
        assert!(m.seal_open(SegmentId(5), &collide, 0).is_err());
        // The planned sealed path is manifest-owned: it cannot even be queued.
        assert!(m.queue_delete("t/5.seg.parquet", 5).is_err());
        // A pending delete's path blocks the seal output too (e.g. the
        // fallback name an earlier crashed seal had already claimed).
        m.queue_delete("t/5.seg-999.parquet", 5).unwrap();
        let mut fallback = base();
        fallback.path = "t/5.seg-999.parquet".into();
        assert!(m.seal_open(SegmentId(5), &fallback, 0).is_err());
        // Zero-fragment and legacy open segments are not this seal's business.
        let mut empty = manifest(vec![]);
        empty.put_segment(fresh_open(2)).unwrap();
        let out2 = SegmentMeta {
            id: SegmentId(2),
            path: "t/2.seg.parquet".into(),
            rows: 0,
            bytes: 0,
            min_ts: None,
            max_ts: None,
            sealed: true,
            sealed_at: None,
            index_file: None,
            fragments: Vec::new(),
            next_fragment_seq: 0,
        };
        assert!(empty.seal_open(SegmentId(2), &out2, 0).is_err());
        let mut legacy = manifest(vec![]);
        legacy.put_segment(open_seg(3)).unwrap();
        let out3 = SegmentMeta { id: SegmentId(3), path: "t/3.seg.parquet".into(), rows: 10, ..out2.clone() };
        assert!(legacy.seal_open(SegmentId(3), &out3, 0).is_err());
    }

    #[test]
    fn pending_deletes_queue_and_sweep() {
        let mut m = manifest(vec![seg(0, 0, 10, 100, 5_000)]);
        m.queue_delete("a.parquet", 100).unwrap();
        m.queue_delete("b.parquet", 500).unwrap();
        assert!(m.queue_delete("t/0.seg", 600).is_err(), "a referenced path is not deletable");
        assert!(m.queue_delete("a.parquet", 600).is_err(), "no double queue");
        assert!(m.queue_delete("", 1).is_err());
        assert_eq!(m.take_due_deletes(99), Vec::<String>::new());
        assert_eq!(m.take_due_deletes(100), vec!["a.parquet".to_string()]);
        assert_eq!(m.take_due_deletes(1_000), vec!["b.parquet".to_string()]);
        assert!(m.take_due_deletes(1_000).is_empty());
        // Live fragment paths are referenced too.
        let mut m2 = manifest(vec![]);
        m2.put_segment(fresh_open(1)).unwrap();
        m2.add_fragment(SegmentId(1), frag(1, 0, 0, 1, 10, None)).unwrap();
        assert!(m2.queue_delete("t/1.frag-000000.parquet", 1).is_err());
        // take_due_deletes keeps list order.
        let mut m3 = manifest(vec![]);
        for (p, t) in [("x", 1), ("y", 1), ("z", 2)] {
            m3.queue_delete(p, t).unwrap();
        }
        assert_eq!(m3.take_due_deletes(1), vec!["x".to_string(), "y".to_string()]);
        assert_eq!(m3.pending_deletes.len(), 1);
    }

    #[test]
    fn put_segment_enforces_invariants() {
        let mut m = manifest(vec![seg(0, 0, 10, 100, 5_000)]);
        // Sealed segments have no fragments.
        let mut sealed_with_frags = seg(1, 0, 10, 10, 100);
        sealed_with_frags.fragments = vec![frag(1, 0, 0, 10, 100, Some((0, 10)))];
        sealed_with_frags.next_fragment_seq = 1;
        assert!(m.put_segment(sealed_with_frags).is_err());
        // Open-segment stats must equal the fragment aggregate.
        let mut bad_stats = open_with_frags(2, vec![frag(2, 0, 0, 10, 100, Some((0, 10)))], 1);
        bad_stats.bytes = 999;
        assert!(m.put_segment(bad_stats).is_err());
        // Fragment seqs unique and below next_fragment_seq.
        let dup_seq = open_with_frags(
            3,
            vec![
                frag(3, 0, 0, 5, 50, None),
                FragmentMeta { seq: 0, path: "t/3.frag-000099.parquet".into(), rows: 5, bytes: 50, ..Default::default() },
            ],
            1,
        );
        assert!(m.put_segment(dup_seq).is_err());
        let ahead = open_with_frags(4, vec![frag(4, 5, 0, 5, 50, None)], 5);
        assert!(m.put_segment(ahead).is_err());
        // Path uniqueness within a segment: fragment == own path, and twice.
        let own_path = SegmentMeta {
            id: SegmentId(6),
            path: "t/x.parquet".into(),
            rows: 5,
            bytes: 50,
            min_ts: None,
            max_ts: None,
            sealed: false,
            sealed_at: None,
            index_file: None,
            fragments: vec![FragmentMeta { seq: 0, path: "t/x.parquet".into(), rows: 5, bytes: 50, ..Default::default() }],
            next_fragment_seq: 1,
        };
        assert!(m.put_segment(own_path).is_err());
        let dup_frag = open_with_frags(
            7,
            vec![
                FragmentMeta { seq: 0, path: "t/dup.parquet".into(), rows: 5, bytes: 50, ..Default::default() },
                FragmentMeta { seq: 1, path: "t/dup.parquet".into(), rows: 5, bytes: 50, ..Default::default() },
            ],
            2,
        );
        assert!(m.put_segment(dup_frag).is_err());
        // Path uniqueness across segments and against pending deletes.
        let steals = open_with_frags(
            8,
            vec![FragmentMeta { seq: 0, path: "t/0.seg".into(), rows: 3, bytes: 30, ..Default::default() }],
            1,
        );
        assert!(m.put_segment(steals).is_err());
        let planned_on_used = SegmentMeta { path: "t/0.seg".into(), ..fresh_open(9) };
        assert!(m.put_segment(planned_on_used).is_err());
        m.queue_delete("t/pending.parquet", 1).unwrap();
        let planned_on_pending = SegmentMeta { path: "t/pending.parquet".into(), ..fresh_open(10) };
        assert!(m.put_segment(planned_on_pending).is_err());
        // Empty path, and only one open segment (existing rule, still holds).
        let empty_path = SegmentMeta { path: String::new(), ..fresh_open(11) };
        assert!(m.put_segment(empty_path).is_err());
        m.put_segment(fresh_open(12)).unwrap();
        assert!(m.put_segment(fresh_open(13)).is_err());
        // The pre-change flow still works: replace the open segment by id
        // (legacy FlushBuffer style: stats set, no fragments).
        let mut m3 = manifest(vec![]);
        m3.put_segment(open_seg(1)).unwrap();
        let mut again = open_seg(1);
        again.rows = 20;
        again.bytes = 2_000;
        m3.put_segment(again).unwrap();
        assert_eq!(m3.open_segment().unwrap().rows, 20);
    }

    #[test]
    fn seal_legacy_open_only_touches_the_legacy_shape() {
        let mut m = manifest(vec![seg(0, 0, 10, 1, 10)]);
        assert!(m.seal_legacy_open(1).is_err(), "no open segment");
        m.put_segment(fresh_open(1)).unwrap();
        assert!(m.seal_legacy_open(1).is_err(), "an empty open segment is not legacy");
        m.add_fragment(SegmentId(1), frag(1, 0, 0, 5, 50, None)).unwrap();
        assert!(m.seal_legacy_open(1).is_err(), "fragment-based open segments seal via seal_open");
        let mut legacy = manifest(vec![]);
        legacy.put_segment(open_seg(4)).unwrap();
        legacy.seal_legacy_open(999).unwrap();
        assert!(legacy.open_segment().is_none());
        assert_eq!(legacy.segments[0].sealed_at, Some(999));
        assert_eq!(legacy.write_stats, WriteStats::default(), "in-place migration writes nothing");
    }
}
