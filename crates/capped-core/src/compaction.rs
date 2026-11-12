//! Size-tiered fragment compaction policy (open-segment fragments plan §3.4).
//!
//! Every commit appends a tier-0 fragment to the open segment; left alone,
//! those files grow the segment's scan into thousands of tiny Parquet readers.
//! The policy merges **runs of `K` adjacent same-tier fragments** into one
//! fragment of the next tier, up to tier `T`. With K=8, T=2 an open segment
//! holds at most K−1 tier-0 + K−1 tier-1 fragments plus accumulated tier-2
//! files (about one per K^K commits), and every byte is rewritten at most
//! `T` times by merging plus once by the final seal: ≤ 4× write cost.
//!
//! Merging is **contiguous-only**: only an adjacent run may merge, and the
//! output takes the run's position in the list, so list order stays the data
//! insertion order (scans simply read the fragment list in order). The shell
//! executes a plan as a `COPY (SELECT * FROM read_parquet([inputs])) TO
//! (output)` and swaps the manifest via `capped_manifest_apply_compaction`.

use crate::manifest::{SegmentId, SegmentMeta};
use serde::{Deserialize, Serialize};

/// Merge fan-in `K`: how many adjacent same-tier fragments trigger a merge.
pub const DEFAULT_FAN_IN: usize = 8;
/// Highest tier a fragment may reach; tier `T` files are never merged again.
pub const DEFAULT_MAX_TIER: u8 = 2;
/// Upper bound on the number of files one open segment may have (§5 T6).
/// Compaction alone does not bound it: `max_span` measures data timestamps
/// (unset without a time column), the size/row seals can take ~36k tiny
/// commits to fire, so the shell seals once the open segment reaches this
/// many fragments — checked after the compaction loop, in addition to
/// `should_seal`.
pub const MAX_OPEN_FRAGMENTS: usize = 32;

/// Tunables for [`plan_compaction`]. Defaults: `fan_in = 8`, `max_tier = 2`
/// (plan §3.4). Table options are deliberately not exposed yet (F4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionLimits {
    pub fan_in: usize,
    pub max_tier: u8,
}

impl Default for CompactionLimits {
    fn default() -> Self {
        Self {
            fan_in: DEFAULT_FAN_IN,
            max_tier: DEFAULT_MAX_TIER,
        }
    }
}

/// One merge step chosen by [`plan_compaction`]: merge `inputs` (fragment
/// seqs, in list order — always a contiguous same-tier run) into a single
/// fragment of `output_tier`. The caller assigns the output a fresh seq
/// (the open segment's `next_fragment_seq`), writes the file, and applies
/// the plan with [`Manifest::apply_compaction`].
///
/// [`Manifest::apply_compaction`]: crate::manifest::Manifest::apply_compaction
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionPlan {
    pub segment: SegmentId,
    pub inputs: Vec<u64>,
    pub output_tier: u8,
}

/// Lowest tier first: if the open segment has `K` **contiguous** fragments at
/// tier `t < max_tier`, merge the oldest `K` of them into one tier-`t+1`
/// fragment (plan §3.4). Returns `None` when nothing qualifies.
///
/// "Oldest" is list position, which is insertion order: the first qualifying
/// run at the lowest tier that has one wins, and the plan takes that run's
/// first `K` fragments. The shell calls this in a loop until it returns
/// `None` (a tier-1 merge can cascade into a tier-2 merge).
///
/// Degenerate limits are inert rather than infinite-loop fuel: `fan_in < 2`
/// or `max_tier == 0` always return `None`.
pub fn plan_compaction(open: &SegmentMeta, limits: &CompactionLimits) -> Option<CompactionPlan> {
    if limits.fan_in < 2 || open.fragments.is_empty() {
        return None;
    }
    let frags = &open.fragments;
    // Lowest tier first, so tier-0 pressure drains before tier-1 runs build up.
    for tier in 0..limits.max_tier {
        let mut start = 0usize;
        while start < frags.len() {
            if frags[start].tier != tier {
                start += 1;
                continue;
            }
            let mut end = start;
            while end < frags.len() && frags[end].tier == tier {
                end += 1;
            }
            if end - start >= limits.fan_in {
                return Some(CompactionPlan {
                    segment: open.id,
                    inputs: frags[start..start + limits.fan_in]
                        .iter()
                        .map(|f| f.seq)
                        .collect(),
                    output_tier: tier + 1,
                });
            }
            start = end;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eviction::{should_seal, SealLimits};
    use crate::manifest::{FragmentMeta, Micros, WriteStats};
    use crate::policy::{parse_size, RetentionPolicy, TimeSource};

    const K: usize = DEFAULT_FAN_IN;

    fn limits() -> CompactionLimits {
        CompactionLimits::default()
    }

    fn frag(seg: u64, seq: u64, tier: u8, bytes: u64) -> FragmentMeta {
        FragmentMeta {
            seq,
            path: format!("t/{seg}.frag-{seq:06}.parquet"),
            rows: 1,
            bytes,
            min_ts: Some(seq as Micros),
            max_ts: Some(seq as Micros),
            tier,
        }
    }

    /// Open segment with the given fragments (aggregates computed to match,
    /// as the T1 mutators keep them).
    fn open_with(frags: Vec<FragmentMeta>) -> SegmentMeta {
        let rows = frags.iter().map(|f| f.rows).sum();
        let bytes = frags.iter().map(|f| f.bytes).sum();
        let next = frags.iter().map(|f| f.seq).max().map_or(0, |m| m + 1);
        SegmentMeta {
            id: SegmentId(1),
            path: "t/1.seg.parquet".into(),
            rows,
            bytes,
            min_ts: Some(0),
            max_ts: Some(0),
            sealed: false,
            sealed_at: None,
            index_file: None,
            fragments: frags,
            next_fragment_seq: next,
        }
    }

    fn tier_run(start: u64, count: u64, tier: u8, seg: u64) -> Vec<FragmentMeta> {
        (0..count).map(|i| frag(seg, start + i, tier, 100)).collect()
    }

    // ---- plan_compaction unit tests ----------------------------------------

    #[test]
    fn no_plan_while_every_run_is_short() {
        // A typical steady-state open segment: K-1 tier-0, K-1 tier-1, some tier-2.
        let mut frags = tier_run(0, (K - 1) as u64, 2, 1);
        frags.extend(tier_run(100, (K - 1) as u64, 1, 1));
        frags.extend(tier_run(200, (K - 1) as u64, 0, 1));
        assert_eq!(plan_compaction(&open_with(frags), &limits()), None);
    }

    #[test]
    fn merges_oldest_k_contiguous_tier0() {
        let frags = tier_run(0, K as u64, 0, 1);
        let plan = plan_compaction(&open_with(frags), &limits()).unwrap();
        assert_eq!(plan.segment, SegmentId(1));
        assert_eq!(plan.inputs, (0..K as u64).collect::<Vec<_>>());
        assert_eq!(plan.output_tier, 1);
    }

    #[test]
    fn lowest_tier_wins_when_both_qualify() {
        // tier-0 run at the tail, tier-1 run at the head: tier 0 is planned first.
        let mut frags = tier_run(0, K as u64, 1, 1);
        frags.extend(tier_run(100, K as u64, 0, 1));
        let plan = plan_compaction(&open_with(frags), &limits()).unwrap();
        assert_eq!(plan.output_tier, 1);
        assert_eq!(plan.inputs, (100..100 + K as u64).collect::<Vec<_>>());
    }

    #[test]
    fn merges_tier1_when_tier0_run_is_short() {
        let mut frags = tier_run(0, 3, 0, 1);
        frags.extend(tier_run(10, K as u64, 1, 1));
        let plan = plan_compaction(&open_with(frags), &limits()).unwrap();
        assert_eq!(plan.output_tier, 2);
        assert_eq!(plan.inputs, (10..10 + K as u64).collect::<Vec<_>>());
    }

    #[test]
    fn tier_at_max_tier_never_merges() {
        // 3K tier-2 files: below-max tiers are empty, so nothing is planned.
        let frags = tier_run(0, 3 * K as u64, 2, 1);
        assert_eq!(plan_compaction(&open_with(frags), &limits()), None);
    }

    #[test]
    fn long_run_yields_oldest_k() {
        // A 2K run merges its oldest K first; the next pass takes the rest.
        let frags = tier_run(0, 2 * K as u64, 0, 1);
        let plan = plan_compaction(&open_with(frags), &limits()).unwrap();
        assert_eq!(plan.inputs, (0..K as u64).collect::<Vec<_>>());
    }

    #[test]
    fn a_later_qualifying_run_merges_when_the_first_is_short() {
        // [t0 x3][t1][t0 x8]: the first tier-0 run is too short; the second qualifies.
        let mut frags = tier_run(0, 3, 0, 1);
        frags.extend(tier_run(3, 1, 1, 1));
        frags.extend(tier_run(4, K as u64, 0, 1));
        let plan = plan_compaction(&open_with(frags), &limits()).unwrap();
        assert_eq!(plan.inputs, (4..4 + K as u64).collect::<Vec<_>>());
        assert_eq!(plan.output_tier, 1);
    }

    #[test]
    fn custom_limits_respected() {
        let l = CompactionLimits { fan_in: 2, max_tier: 1 };
        let plan = plan_compaction(&open_with(tier_run(0, 2, 0, 1)), &l).unwrap();
        assert_eq!(plan.inputs, vec![0, 1]);
        assert_eq!(plan.output_tier, 1);
        // tier-1 is the max now: two adjacent tier-1 fragments do not merge.
        assert_eq!(plan_compaction(&open_with(tier_run(0, 2, 1, 1)), &l), None);
    }

    #[test]
    fn degenerate_limits_are_inert() {
        let frags = tier_run(0, 3 * K as u64, 0, 1);
        let open = open_with(frags);
        assert_eq!(plan_compaction(&open, &CompactionLimits { fan_in: 0, max_tier: 2 }), None);
        assert_eq!(plan_compaction(&open, &CompactionLimits { fan_in: 1, max_tier: 2 }), None);
        assert_eq!(plan_compaction(&open, &CompactionLimits { fan_in: 8, max_tier: 0 }), None);
        assert_eq!(plan_compaction(&open_with(Vec::new()), &limits()), None);
    }

    // ---- simulation: the T2 gate --------------------------------------------
    //
    // Models one table under continuous commits: each commit appends a tier-0
    // fragment, plans are applied to a fixed point, and segments are sealed by
    // `should_seal` (or at the end of the run). Bytes are additive — a merge or
    // seal output costs exactly the sum of its inputs — matching the plan's
    // "model bytes as additive" for the ≤ 4× gate. The gate asserts, per the
    // plan: bytes written / bytes sealed <= 4.0, max open-segment fragments
    // <= 26, and insertion order preserved (checked after every single step).

    struct Sim {
        m: crate::manifest::Manifest,
        /// live fragment seq -> the commit ids it carries, in order.
        ids: std::collections::BTreeMap<u64, Vec<u64>>,
        commits: u64,
        /// first commit id still living in the open segment
        open_start: u64,
        max_fragments: usize,
    }

    impl Sim {
        fn new() -> Self {
            let policy = RetentionPolicy::builder(TimeSource::IngestTime)
                .max_bytes(parse_size("1TB").unwrap())
                .build()
                .unwrap();
            Sim {
                m: crate::manifest::Manifest::new("t", vec![], policy),
                ids: std::collections::BTreeMap::new(),
                commits: 0,
                open_start: 0,
                max_fragments: 0,
            }
        }

        fn ensure_open(&mut self) -> SegmentId {
            if let Some(s) = self.m.open_segment() {
                return s.id;
            }
            let id = SegmentId(self.m.next_segment_id);
            self.m
                .put_segment(SegmentMeta {
                    id,
                    path: format!("t/{}.seg.parquet", id.0),
                    rows: 0,
                    bytes: 0,
                    min_ts: None,
                    max_ts: None,
                    sealed: false,
                    sealed_at: None,
                    index_file: None,
                    fragments: Vec::new(),
                    next_fragment_seq: 0,
                })
                .unwrap();
            id
        }

        /// One committed INSERT: append a tier-0 fragment, then compact to a
        /// fixed point, verifying every invariant after every merge.
        fn commit(&mut self, bytes: u64, now: Micros) {
            let id = self.ensure_open();
            let seq = self.m.open_segment().unwrap().next_fragment_seq;
            self.m
                .add_fragment(
                    id,
                    FragmentMeta {
                        seq,
                        path: format!("t/{}.frag-{seq:06}.parquet", id.0),
                        rows: 1,
                        bytes,
                        min_ts: Some(now),
                        max_ts: Some(now),
                        tier: 0,
                    },
                )
                .unwrap();
            self.ids.insert(seq, vec![self.commits]);
            self.commits += 1;
            // The manifest is now swapped with this fragment in: this is a
            // state a scan could bind to, so it counts toward the peak.
            self.record_peak();
            self.compact_to_fixed_point(now);
            self.check_order();
        }

        /// Each published manifest state (after a commit, after each merge)
        /// is what a scan could observe; track the largest open-segment file
        /// count over all of them.
        fn record_peak(&mut self) {
            let n = self.m.open_segment().map_or(0, |s| s.fragments.len());
            self.max_fragments = self.max_fragments.max(n);
        }

        fn compact_to_fixed_point(&mut self, now: Micros) {
            while let Some(plan) = plan_compaction(self.m.open_segment().unwrap(), &limits()) {
                let open = self.m.open_segment().unwrap();
                // The plan must name the oldest qualifying run at the lowest
                // qualifying tier — an independent re-derivation, not a replay
                // of the planner's code path.
                let k = limits().fan_in;
                let tier = plan.output_tier - 1;
                for lower in 0..tier {
                    assert!(
                        Self::oldest_run(open, lower, k).is_none(),
                        "planner skipped a qualifying run at lower tier {lower}"
                    );
                }
                let expect_start = Self::oldest_run(open, tier, k)
                    .unwrap_or_else(|| panic!("planned run at tier {tier} is not qualifying"));
                let positions: Vec<usize> = open
                    .fragments
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| plan.inputs.contains(&f.seq))
                    .map(|(i, _)| i)
                    .collect();
                assert_eq!(positions.len(), plan.inputs.len(), "planned fragments exist");
                assert_eq!(
                    positions,
                    (expect_start..expect_start + positions.len()).collect::<Vec<_>>(),
                    "inputs are the oldest qualifying run, contiguously"
                );
                assert!(
                    open.fragments[expect_start..expect_start + positions.len()]
                        .iter()
                        .all(|f| f.tier == tier),
                    "inputs share the tier"
                );
                assert_eq!(
                    plan.inputs,
                    open.fragments[expect_start..expect_start + k]
                        .iter()
                        .map(|f| f.seq)
                        .collect::<Vec<_>>(),
                    "inputs are the run's oldest K, in list order"
                );

                let inputs = plan.inputs.clone();
                let out_seq = open.next_fragment_seq;
                let seg_id = open.id;
                let in_frags: Vec<&FragmentMeta> = open
                    .fragments
                    .iter()
                    .filter(|f| inputs.contains(&f.seq))
                    .collect();
                // additive bytes model: output = exact sum of inputs
                let output = FragmentMeta {
                    seq: out_seq,
                    path: format!("t/{}.frag-{out_seq:06}.parquet", seg_id.0),
                    rows: in_frags.iter().map(|f| f.rows).sum(),
                    bytes: in_frags.iter().map(|f| f.bytes).sum(),
                    min_ts: in_frags.iter().filter_map(|f| f.min_ts).min(),
                    max_ts: in_frags.iter().filter_map(|f| f.max_ts).max(),
                    tier: plan.output_tier,
                };
                let merged: Vec<u64> =
                    inputs.iter().flat_map(|s| self.ids[s].clone()).collect();
                self.m.apply_compaction(seg_id, &inputs, output, now).unwrap();
                self.record_peak();
                for s in &inputs {
                    self.ids.remove(s);
                }
                self.ids.insert(out_seq, merged);
                // §3.4: the output takes the run's position in the list.
                assert_eq!(
                    self.m.open_segment().unwrap().fragments[expect_start].seq,
                    out_seq,
                    "output takes the run's position"
                );
            }
        }

        /// Start index of the first run of >= k adjacent tier-`t` fragments.
        fn oldest_run(open: &SegmentMeta, t: u8, k: usize) -> Option<usize> {
            let frags = &open.fragments;
            let mut i = 0;
            while i < frags.len() {
                if frags[i].tier != t {
                    i += 1;
                    continue;
                }
                let start = i;
                while i < frags.len() && frags[i].tier == t {
                    i += 1;
                }
                if i - start >= k {
                    return Some(start);
                }
            }
            None
        }

        /// List order is data order: flattening the live fragments must
        /// reproduce the commits appended since this segment opened, in order.
        fn check_order(&self) {
            let open = self.m.open_segment().unwrap();
            let flat: Vec<u64> =
                open.fragments.iter().flat_map(|f| self.ids[&f.seq].iter().copied()).collect();
            assert_eq!(
                flat,
                (self.open_start..self.commits).collect::<Vec<_>>(),
                "insertion order preserved"
            );
        }

        /// Seal = final compaction into one file (additive model: the rewrite
        /// costs the fragments' bytes; the single-fragment fast path adopts).
        fn seal(&mut self, now: Micros) {
            let open = self.m.open_segment().unwrap();
            if open.fragments.is_empty() {
                return;
            }
            let seg_id = open.id;
            let paths: Vec<String> = open.fragments.iter().map(|f| f.path.clone()).collect();
            let adopt = paths.len() == 1;
            let output = SegmentMeta {
                id: seg_id,
                path: if adopt { paths[0].clone() } else { open.path.clone() },
                rows: open.rows,
                bytes: open.bytes, // additive: sealed file = sum of fragments
                min_ts: open.min_ts,
                max_ts: open.max_ts,
                sealed: true,
                sealed_at: None,
                index_file: None,
                fragments: Vec::new(),
                next_fragment_seq: 0,
            };
            self.m.seal_open(seg_id, &output, now).unwrap();
            // ids only ever tracks the open segment's live fragments; they are
            // all gone now (sealed away, pending-deleted by grace).
            self.ids.clear();
            self.open_start = self.commits;
        }

        /// amplification = every parquet byte capped wrote / bytes stored sealed.
        fn ratio(&self) -> f64 {
            let w: u64 = self.m.write_stats.bytes_written;
            let s: u64 = self.m.write_stats.bytes_sealed;
            assert!(s > 0, "nothing sealed");
            w as f64 / s as f64
        }
    }

    /// T2 gate: 720 equal commits (the ~1h-at-5s-span seal of §1), then seal.
    #[test]
    fn sim_720_equal_commits_then_seal() {
        let mut sim = Sim::new();
        let mut now: Micros = 0;
        for _ in 0..720 {
            sim.commit(2 * 1024, now);
            now += 5_000_000; // one 5 s group commit
        }
        sim.seal(now);
        let ratio = sim.ratio();
        println!(
            "720 equal commits (K={K}, T={}): bytes_written={} bytes_sealed={} amplification={ratio:.3}x max_fragments={}",
            limits().max_tier,
            sim.m.write_stats.bytes_written,
            sim.m.write_stats.bytes_sealed,
            sim.max_fragments
        );
        assert!(ratio <= 4.0, "write amplification {ratio:.3}x exceeds the 4.0x gate");
        assert!(sim.max_fragments <= 26, "open segment peaked at {} fragments", sim.max_fragments);
        assert_eq!(sim.commits, 720);
        assert!(sim.m.open_segment().is_none(), "everything sealed");
    }

    /// T2 gate: 10k commits into byte-cap-sealed segments (a busy table
    /// heading toward the size seal; the row cap models the 1 h span seal's
    /// bounded commit count — the plan's 26-file bound assumes that bound).
    #[test]
    fn sim_10k_commits_byte_cap_seal() {
        let mut sim = Sim::new();
        // 1 KB commits; seal at 512 KB => ~512 commits per segment.
        let seal_limits = SealLimits {
            max_bytes: Some(512 * 1024),
            max_rows: Some(720),
            max_span: None,
        };
        let mut now: Micros = 0;
        for _ in 0..10_000 {
            sim.commit(1024, now);
            now += 5_000_000;
            if sim
                .m
                .open_segment()
                .is_some_and(|o| should_seal(o, &seal_limits))
            {
                sim.seal(now);
            }
        }
        sim.seal(now);
        let ratio = sim.ratio();
        println!(
            "10k commits, byte-cap seal: bytes_written={} bytes_sealed={} amplification={ratio:.3}x max_fragments={} segments={}",
            sim.m.write_stats.bytes_written,
            sim.m.write_stats.bytes_sealed,
            sim.max_fragments,
            sim.m.segments.len()
        );
        assert!(ratio <= 4.0, "write amplification {ratio:.3}x exceeds the 4.0x gate");
        assert!(sim.max_fragments <= 26, "open segment peaked at {} fragments", sim.max_fragments);
        assert!(sim.m.segments.len() > 1, "the byte cap must actually split segments");
    }

    /// T2 gate: same as above with skewed commit sizes (a size-tiered policy
    /// merges by tier and count, not size, so heavy bytes must still stay
    /// under 4× and the file bound under 26).
    #[test]
    fn sim_skewed_commit_sizes() {
        let mut sim = Sim::new();
        let seal_limits = SealLimits {
            max_bytes: Some(4 * 1024 * 1024),
            max_rows: Some(720),
            max_span: None,
        };
        let mut now: Micros = 0;
        for i in 0..10_000u64 {
            // mostly 1..9 KB, every 50th commit a 200 KB burst
            let bytes = if i % 50 == 49 {
                200 * 1024
            } else {
                (1 + (i % 9)) * 1024
            };
            sim.commit(bytes, now);
            now += 5_000_000;
            if sim
                .m
                .open_segment()
                .is_some_and(|o| should_seal(o, &seal_limits))
            {
                sim.seal(now);
            }
        }
        sim.seal(now);
        let ratio = sim.ratio();
        println!(
            "skewed commit sizes: bytes_written={} bytes_sealed={} amplification={ratio:.3}x max_fragments={} segments={}",
            sim.m.write_stats.bytes_written,
            sim.m.write_stats.bytes_sealed,
            sim.max_fragments,
            sim.m.segments.len()
        );
        assert!(ratio <= 4.0, "write amplification {ratio:.3}x exceeds the 4.0x gate");
        assert!(sim.max_fragments <= 26, "open segment peaked at {} fragments", sim.max_fragments);
        assert!(sim.m.segments.len() > 1, "the byte cap must actually split segments");
    }

    /// §5 T6 gate: nothing bounds commits per segment except the file-count
    /// seal — the span trigger is gone (no time column), the size/row caps are
    /// set never to trip. 10k tiny commits must keep every published manifest
    /// state at <= MAX_OPEN_FRAGMENTS open files, and the 4.0x amplification
    /// gate must still hold.
    #[test]
    fn sim_10k_tiny_commits_fragment_cap() {
        let mut sim = Sim::new();
        let never = SealLimits {
            max_bytes: Some(u64::MAX),
            max_rows: Some(u64::MAX),
            max_span: None,
        };
        let mut now: Micros = 0;
        for _ in 0..10_000u64 {
            sim.commit(64, now); // tiny: only the file-count seal can ever fire
            now += 10_000;
            if sim
                .m
                .open_segment()
                .is_some_and(|o| o.fragments.len() >= MAX_OPEN_FRAGMENTS)
            {
                sim.seal(now);
            }
        }
        sim.seal(now);
        let ratio = sim.ratio();
        println!(
            "10k tiny commits, {}-file seal: bytes_written={} bytes_sealed={} amplification={ratio:.3}x max_fragments={} segments={}",
            MAX_OPEN_FRAGMENTS,
            sim.m.write_stats.bytes_written,
            sim.m.write_stats.bytes_sealed,
            sim.max_fragments,
            sim.m.segments.len()
        );
        assert!(ratio <= 4.0, "write amplification {ratio:.3}x exceeds the 4.0x gate");
        assert!(
            sim.max_fragments <= MAX_OPEN_FRAGMENTS,
            "open segment peaked at {} fragment files in a published manifest",
            sim.max_fragments
        );
        assert!(!should_seal(&sim.m.segments[0], &never), "the other seal limits never trip");
        // The trigger did the work: compaction alone lets tier-2 files
        // accumulate without bound in one segment, so the peak touching the
        // cap and the segment rotation both prove the file-count seal fired.
        assert!(
            sim.m.segments.len() > 1,
            "the file-count seal must rotate segments, got {}",
            sim.m.segments.len()
        );
        assert_eq!(
            sim.max_fragments, MAX_OPEN_FRAGMENTS,
            "the open segment should peak exactly at the cap that seals it"
        );
    }

    #[test]
    fn sim_seals_an_adoption_and_counts_no_write_for_it() {
        // A single-commit segment: seal adopts the fragment (T6's fast path),
        // so the sealed bytes were written exactly once.
        let mut sim = Sim::new();
        sim.commit(1024, 0);
        sim.seal(1);
        assert_eq!(
            sim.m.write_stats,
            WriteStats { bytes_written: 1024, files_written: 1, bytes_sealed: 1024 }
        );
        assert_eq!(sim.ratio(), 1.0);
    }
}
