//! Eviction planning: pure functions that decide *what to drop*, given a
//! manifest and a clock reading. The storage shell executes the plan; core
//! never touches files.
//!
//! Invariants:
//! - Only **sealed** segments are evictable. The open segment is never in a plan.
//! - A segment is age-droppable only when its entire span is past `ttl + grace`
//!   (so late-arriving rows sharing a segment's time span are protected).
//! - A segment with an **unknown** time bound is never age-droppable: absence of
//!   an event time is not evidence of age. Size/count caps still apply to it.
//! - Caps are enforced oldest-first, in `evict_order`; among time-ordered
//!   candidates, segments of unknown age sort last (least information goes last).
//! - Planning is deterministic and idempotent: planning twice drops nothing the
//!   first plan didn't already drop.

use crate::manifest::{Manifest, Micros, SegmentId, SegmentMeta};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionReason {
    /// Whole segment aged out: `max_ts < now - ttl - grace`.
    Age,
    /// Dropped to get under `max_bytes`, oldest first.
    Size,
    /// Dropped to get under `max_rows`, oldest first.
    Count,
}

/// What the storage layer should delete. Execution is the shell's business;
/// applying the same plan twice must be a no-op.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EvictionPlan {
    pub drops: Vec<(SegmentId, EvictionReason)>,
    /// True if, after dropping every evictable segment, caps are still
    /// exceeded (only the open segment remains). Callers may consult
    /// `OnViolation` to decide between warn, error, or block.
    pub still_over_budget: bool,
}

impl EvictionPlan {
    pub fn is_dropped(&self, id: SegmentId) -> bool {
        self.drops.iter().any(|(d, _)| *d == id)
    }

    pub fn is_empty(&self) -> bool {
        self.drops.is_empty()
    }

    pub fn dropped_ids(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.drops.iter().map(|(id, _)| *id)
    }
}

/// Decide what to drop from `manifest` at wall time `now`.
pub fn plan_eviction(manifest: &Manifest, now: Micros) -> EvictionPlan {
    let policy = &manifest.policy;
    let mut plan = EvictionPlan::default();
    let mut remaining: Vec<&SegmentMeta> = manifest.sealed_segments().collect();

    // 1. Age. Expired whole segments go first, regardless of size caps.
    //    A segment without a known `max_ts` is not evidence of age: it stays.
    if let Some(ttl) = policy.ttl {
        let cutoff = now - micros(ttl + policy.grace);
        let (expired, alive): (Vec<_>, Vec<_>) =
            remaining.into_iter().partition(|s| s.max_ts.is_some_and(|mx| mx < cutoff));
        plan.drops.extend(expired.iter().map(|s| (s.id, EvictionReason::Age)));
        remaining = alive;
    }

    // Caps constrain the *whole table*, but only sealed segments are evictable.
    // The open segment always counts toward totals (it is part of the table's
    // footprint) and can therefore push the table permanently over budget.
    let open = manifest.open_segment();

    // 2. Size, oldest first.
    if let Some(max_bytes) = policy.max_bytes {
        let order = order_cmp(policy.evict_order);
        let mut total: u64 =
            remaining.iter().map(|s| s.bytes).sum::<u64>() + open.map_or(0, |s| s.bytes);
        remaining.sort_by(order);
        while total > max_bytes {
            let Some(oldest) = remaining.first().copied() else { break };
            total -= oldest.bytes;
            remaining.remove(0);
            plan.drops.push((oldest.id, EvictionReason::Size));
        }
    }

    // 3. Count, oldest first.
    if let Some(max_rows) = policy.max_rows {
        let order = order_cmp(policy.evict_order);
        let mut total: u64 =
            remaining.iter().map(|s| s.rows).sum::<u64>() + open.map_or(0, |s| s.rows);
        remaining.sort_by(order);
        while total > max_rows {
            let Some(oldest) = remaining.first().copied() else { break };
            total -= oldest.rows;
            remaining.remove(0);
            plan.drops.push((oldest.id, EvictionReason::Count));
        }
    }

    plan.still_over_budget = still_over_budget(&remaining, open, policy);
    plan
}

/// When the open segment can no longer accept appends and should be sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SealLimits {
    pub max_bytes: Option<u64>,
    pub max_rows: Option<u64>,
    /// Maximum wall-clock span (`max_ts - min_ts`) a single segment may cover.
    pub max_span: Option<Duration>,
}

impl SealLimits {
    /// Defaults tuned for telemetry: ~1 row group of compressed data per segment.
    pub fn default_telemetry() -> Self {
        Self {
            max_bytes: Some(128 * 1_000_000),
            max_rows: Some(1_000_000),
            max_span: Some(Duration::from_secs(3_600)),
        }
    }
}

pub fn should_seal(open: &SegmentMeta, limits: &SealLimits) -> bool {
    if let Some(max_bytes) = limits.max_bytes {
        if open.bytes >= max_bytes {
            return true;
        }
    }
    if let Some(max_rows) = limits.max_rows {
        if open.rows >= max_rows {
            return true;
        }
    }
    if let Some(span) = limits.max_span {
        // unknown bounds => no span information => let the size/row triggers decide
        if let (Some(min), Some(max)) = (open.min_ts, open.max_ts) {
            if (max - min) >= micros(span) {
                return true;
            }
        }
    }
    false
}

fn order_cmp(
    order: crate::policy::EvictOrder,
) -> impl Fn(&&SegmentMeta, &&SegmentMeta) -> std::cmp::Ordering {
    move |a, b| match order {
        // Oldest-first: a segment that ends earlier is older; id breaks ties.
        // Unknown age sorts last — evict what we can actually date first.
        crate::policy::EvictOrder::Time => (a.max_ts.unwrap_or(i64::MAX), a.id)
            .cmp(&(b.max_ts.unwrap_or(i64::MAX), b.id)),
        // Insertion order == segment id order.
        crate::policy::EvictOrder::Insert => a.id.cmp(&b.id),
    }
}

fn still_over_budget(
    remaining: &[&SegmentMeta],
    open: Option<&SegmentMeta>,
    policy: &crate::policy::RetentionPolicy,
) -> bool {
    let open_bytes = open.map_or(0, |s| s.bytes);
    let open_rows = open.map_or(0, |s| s.rows);
    if let Some(max_bytes) = policy.max_bytes {
        if remaining.iter().map(|s| s.bytes).sum::<u64>() + open_bytes > max_bytes {
            return true;
        }
    }
    if let Some(max_rows) = policy.max_rows {
        if remaining.iter().map(|s| s.rows).sum::<u64>() + open_rows > max_rows {
            return true;
        }
    }
    false
}

fn micros(d: Duration) -> Micros {
    d.as_micros().min(i64::MAX as u128) as Micros
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::tests::{open_seg, seg, seg_untimed};
    use crate::policy::{parse_size, EvictOrder, OnViolation, RetentionPolicy, TimeSource};

    const DAY: Micros = 86_400 * 1_000_000;

    fn manifest_with(policy: RetentionPolicy, segments: Vec<SegmentMeta>) -> Manifest {
        Manifest {
            table: "t".into(),
            columns: vec![],
            policy,
            search: Default::default(),
            segments,
            next_segment_id: 100,
            pending_deletes: Vec::new(),
            write_stats: Default::default(),
        }
    }

    fn ttl(ttl_secs: u64) -> RetentionPolicy {
        RetentionPolicy::builder(TimeSource::IngestTime)
            .ttl(Duration::from_secs(ttl_secs))
            .build()
            .unwrap()
    }

    #[test]
    fn age_evicts_whole_expired_segments_only() {
        let now = 10 * DAY;
        let m = manifest_with(
            ttl(7 * 86_400),
            vec![
                seg(0, 0, DAY, 100, 1_000),              // fully expired
                seg(1, 2 * DAY, 3 * DAY - 1, 100, 1_000), // fully expired (just past cutoff)
                seg(2, 8 * DAY, 9 * DAY, 100, 1_000), // alive
                open_seg(3),
            ],
        );
        let plan = plan_eviction(&m, now);
        assert_eq!(plan.dropped_ids().collect::<Vec<_>>(), vec![SegmentId(0), SegmentId(1)]);
        assert!(plan.drops.iter().all(|(_, r)| *r == EvictionReason::Age));
        assert!(!plan.is_dropped(SegmentId(2))); // alive
        assert!(!plan.is_dropped(SegmentId(3))); // open never dropped
    }

    #[test]
    fn grace_protects_borderline_segments() {
        let now = 10 * DAY;
        let ttl_us = Duration::from_secs(2 * 86_400);
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .ttl(ttl_us)
            .grace(Duration::from_secs(86_400))
            .build()
            .unwrap();
        // max_ts exactly at ttl cutoff (without grace) -> grace must save it.
        let borderline = seg(0, 0, now - ttl_us.as_micros() as i64, 10, 10);
        let m = manifest_with(policy, vec![borderline]);
        assert!(plan_eviction(&m, now).is_empty());
    }

    #[test]
    fn size_cap_evicts_oldest_first_until_under() {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .max_bytes(10_000)
            .build()
            .unwrap();
        let m = manifest_with(
            policy,
            vec![
                seg(0, 0, DAY, 100, 4_000),
                seg(1, DAY, 2 * DAY, 100, 4_000),
                seg(2, 2 * DAY, 3 * DAY, 100, 4_000),
                open_seg(3),
            ],
        );
        // 12k total, cap 10k -> drop oldest (id 0) => 8k, done.
        let plan = plan_eviction(&m, 5 * DAY);
        assert_eq!(plan.dropped_ids().collect::<Vec<_>>(), vec![SegmentId(0)]);
        assert_eq!(plan.drops[0].1, EvictionReason::Size);
        assert!(!plan.still_over_budget);
    }

    #[test]
    fn count_cap_evicts_until_under() {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .max_rows(250)
            .build()
            .unwrap();
        let m = manifest_with(
            policy,
            vec![seg(0, 0, DAY, 100, 1), seg(1, DAY, 2 * DAY, 100, 1), seg(2, 2 * DAY, 3 * DAY, 100, 1)],
        );
        let plan = plan_eviction(&m, 3 * DAY);
        // 300 rows, cap 250 -> one segment (100) goes => 200 <= 250.
        assert_eq!(plan.dropped_ids().collect::<Vec<_>>(), vec![SegmentId(0)]);
        assert_eq!(plan.drops[0].1, EvictionReason::Count);
    }

    #[test]
    fn insert_order_evicts_by_id_even_if_time_is_weird() {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .max_rows(100)
            .evict_order(EvictOrder::Insert)
            .build()
            .unwrap();
        // id 0 has the *newest* timestamps but was inserted first.
        let m = manifest_with(
            policy,
            vec![seg(0, 9 * DAY, 10 * DAY, 100, 1), seg(1, 0, DAY, 100, 1)],
        );
        let plan = plan_eviction(&m, 10 * DAY);
        assert_eq!(plan.dropped_ids().collect::<Vec<_>>(), vec![SegmentId(0)]);
    }

    #[test]
    fn both_caps_age_first_then_size() {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .ttl(Duration::from_secs(86_400))
            .max_bytes(3_000)
            .build()
            .unwrap();
        let m = manifest_with(
            policy,
            vec![
                seg(0, 0, DAY, 10, 4_000),               // expired by age
                seg(1, 9 * DAY, 10 * DAY, 10, 4_000),    // alive, size forces drop
                seg(2, 9 * DAY, 10 * DAY + 1, 10, 500),  // newest, survives
            ],
        );
        let plan = plan_eviction(&m, 10 * DAY);
        assert!(plan.is_dropped(SegmentId(0)));
        assert_eq!(plan.drops.iter().find(|(id, _)| *id == SegmentId(0)).unwrap().1, EvictionReason::Age);
        assert!(plan.is_dropped(SegmentId(1)));
        assert_eq!(plan.drops.iter().find(|(id, _)| *id == SegmentId(1)).unwrap().1, EvictionReason::Size);
        assert!(!plan.is_dropped(SegmentId(2)));
    }

    #[test]
    fn idempotent() {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .ttl(Duration::from_secs(86_400))
            .max_bytes(parse_size("1MB").unwrap())
            .build()
            .unwrap();
        let mut m = manifest_with(
            policy,
            vec![seg(0, 0, DAY, 10, 2_000_000), seg(1, 9 * DAY, 10 * DAY, 10, 500)],
        );
        let now = 10 * DAY;
        let plan1 = plan_eviction(&m, now);
        assert!(!plan1.is_empty());
        m.apply(&plan1);
        let plan2 = plan_eviction(&m, now);
        assert!(plan2.is_empty(), "second plan must drop nothing: {plan2:?}");
    }

    #[test]
    fn open_segment_only_can_be_reported_over_budget() {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .max_rows(10)
            .on_violation(OnViolation::Block)
            .build()
            .unwrap();
        let mut open = open_seg(0);
        open.rows = 500;
        open.bytes = 500;
        let m = manifest_with(policy, vec![open]);
        let plan = plan_eviction(&m, DAY);
        assert!(plan.is_empty()); // open segment is never evicted
        assert!(plan.still_over_budget);
    }

    #[test]
    fn seal_triggers() {
        let limits = SealLimits {
            max_bytes: Some(1_000),
            max_rows: Some(1_000),
            max_span: Some(Duration::from_secs(3_600)),
        };
        let mut s = seg(0, 0, 0, 0, 0);
        assert!(!should_seal(&s, &limits));
        s.bytes = 1_000;
        assert!(should_seal(&s, &limits));
        s.bytes = 999;
        s.rows = 1_000;
        assert!(should_seal(&s, &limits));
        s.rows = 999;
        s.max_ts = Some(3_600 * 1_000_000);
        assert!(should_seal(&s, &limits));
        s.max_ts = Some(3_599 * 1_000_000);
        assert!(!should_seal(&s, &limits));
        // no time bounds at all => the span trigger cannot fire
        s.min_ts = None;
        s.max_ts = Some(i64::MAX / 2);
        assert!(!should_seal(&s, &limits));
    }

    #[test]
    fn unknown_time_range_is_never_age_evicted() {
        // ttl of 1s against segments that simply have no event time: they must
        // survive (this is what an ingest-time / non-temporal table looks like).
        let m = manifest_with(ttl(1), vec![seg_untimed(0, 10, 100), seg_untimed(1, 10, 100)]);
        assert!(plan_eviction(&m, 100 * DAY).is_empty());
    }

    #[test]
    fn unknown_time_range_still_yields_to_size_and_count_caps() {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .ttl(Duration::from_secs(1))
            .max_bytes(1_000)
            .build()
            .unwrap();
        let m = manifest_with(policy, vec![seg_untimed(0, 10, 600), seg_untimed(1, 10, 600)]);
        let plan = plan_eviction(&m, 100 * DAY);
        assert_eq!(plan.dropped_ids().collect::<Vec<_>>(), vec![SegmentId(0)]);
        assert_eq!(plan.drops[0].1, EvictionReason::Size);
    }

    #[test]
    fn segments_of_unknown_age_evict_last_in_time_order() {
        let policy = RetentionPolicy::builder(TimeSource::IngestTime)
            .max_bytes(1_000)
            .build()
            .unwrap();
        // both are 600 bytes over a 1000 cap; the dated one goes, the undated stays
        let m = manifest_with(
            policy,
            vec![seg_untimed(0, 10, 600), seg(1, 0, DAY, 10, 600)],
        );
        let plan = plan_eviction(&m, 100 * DAY);
        assert_eq!(plan.dropped_ids().collect::<Vec<_>>(), vec![SegmentId(1)]);
    }
}
