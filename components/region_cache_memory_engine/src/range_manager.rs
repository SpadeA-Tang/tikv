// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, BTreeSet};

use engine_traits::CacheRange;

use crate::engine::SnapshotList;

#[derive(Debug, Default)]
pub struct RangeMeta {
    range_snapshot_list: BTreeMap<CacheRange, SnapshotList>,
    ranges_evcited: BTreeSet<CacheRange>,
    ranges_unreadable: BTreeSet<CacheRange>,
    safe_ts: u64,
}

impl RangeMeta {
    pub(crate) fn range_snapshot_list(&self) -> &BTreeMap<CacheRange, SnapshotList> {
        &self.range_snapshot_list
    }

    pub(crate) fn merge_meta(&mut self, mut other: RangeMeta) {
        self.range_snapshot_list
            .append(&mut other.range_snapshot_list);
        self.ranges_evcited.append(&mut other.ranges_evcited);
        self.ranges_unreadable.append(&mut other.ranges_unreadable);

        // merge safe_ts to the min of them if not 0
        if self.safe_ts != 0 && other.safe_ts != 0 {
            self.safe_ts = u64::min(self.safe_ts, other.safe_ts);
        } else if other.safe_ts != 0 {
            self.safe_ts = other.safe_ts;
        }
    }

    pub(crate) fn set_range_readable(&mut self, range: &CacheRange, set_readable: bool) {
        if set_readable {
            assert!(self.ranges_unreadable.remove(range));
        } else {
            self.ranges_unreadable.insert(range.clone());
        }
    }
}

#[derive(Default)]
pub struct RangeManager {
    // the range reflects the range of data that is accessable.
    ranges: BTreeMap<CacheRange, RangeMeta>,
}

impl RangeManager {
    pub(crate) fn ranges(&self) -> &BTreeMap<CacheRange, RangeMeta> {
        &self.ranges
    }

    pub(crate) fn new_range(&mut self, range: CacheRange) {
        if let Some(sibling) = self.ranges.keys().find(|r| r.is_sibling(&range)).cloned() {
            let left_sib = sibling < range;
            let mut sib_meta = self.ranges.remove(&sibling).unwrap();
            sib_meta.ranges_unreadable.insert(range.clone());
            let mut range = CacheRange::merge(sibling, range);

            // if sibling found above is the left sibling of the range, there could be a
            // right sibling
            if left_sib {
                if let Some(right_sibling) =
                    self.ranges.keys().find(|r| r.is_sibling(&range)).cloned()
                {
                    let right_sib_meta = self.ranges.remove(&right_sibling).unwrap();
                    sib_meta.merge_meta(right_sib_meta);
                    range = CacheRange::merge(range, right_sibling);
                }
            }
            self.ranges.insert(range, sib_meta);
        } else {
            let mut range_meta = RangeMeta::default();
            range_meta.ranges_unreadable.insert(range.clone());
            self.ranges.insert(range, range_meta);
        }
    }

    pub fn set_range_readable(&mut self, range: &CacheRange, set_readable: bool) {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains(range))
            .unwrap()
            .clone();
        let meta = self.ranges.get_mut(&range_key).unwrap();
        meta.set_range_readable(range, set_readable);
    }

    pub fn set_safe_ts(&mut self, range: &CacheRange, safe_ts: u64) -> bool {
        if let Some(meta) = self.ranges.get_mut(range) {
            if meta.safe_ts > safe_ts {
                return false;
            }
            meta.safe_ts = safe_ts;
            true
        } else {
            false
        }
    }

    pub(crate) fn overlap_with_range(&self, range: &CacheRange) -> bool {
        self.ranges.keys().any(|r| r.overlaps(range))
    }

    pub(crate) fn range_snapshot(&mut self, range: &CacheRange, read_ts: u64) -> bool {
        let Some(range_key) = self.ranges.keys().find(|&r| r.contains(range)) else {
            return false;
        };
        let meta = self.ranges.get(range_key).unwrap();

        if read_ts <= meta.safe_ts {
            // todo(SpadeA): add metrics for it
            return false;
        }

        if meta.ranges_evcited.iter().any(|r| r.overlaps(range)) {
            return false;
        }

        if meta.ranges_unreadable.iter().any(|r| r.overlaps(range)) {
            return false;
        }

        let meta = self.ranges.get_mut(&range_key.clone()).unwrap();
        meta.range_snapshot_list
            .entry(range.clone())
            .or_default()
            .new_snapshot(read_ts);
        true
    }

    pub(crate) fn remove_range_snapshot(&mut self, range: &CacheRange, read_ts: u64) {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains(range))
            .unwrap()
            .clone();
        let meta = self.ranges.get_mut(&range_key).unwrap();
        let snap_list = meta.range_snapshot_list.get_mut(range).unwrap();
        snap_list.remove_snapshot(read_ts);
        if snap_list.is_empty() {
            meta.range_snapshot_list.remove(range).unwrap();
        }
    }

    // return true means we can evict it directly
    pub(crate) fn mark_range_evicted(&mut self, range: &CacheRange) -> bool {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains(range))
            .unwrap()
            .clone();
        let meta = self.ranges.get_mut(&range_key).unwrap();
        assert!(meta.ranges_evcited.insert(range.clone()));
        !meta.range_snapshot_list.keys().any(|r| r.overlaps(range))
    }

    // A range is evictable if:
    // 1. it is marked as evicted (so it's in the ranges_evicted)
    // 2. there's no snapshot whose range overlap with it
    //
    // `range` denotes the range of the snapshot dropped and after the drop, there
    // may be some ranges marked evicated can be evicted now.
    pub(crate) fn ranges_evictable_after_snapshot_drop(
        &mut self,
        range: &CacheRange,
    ) -> Vec<CacheRange> {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains(range))
            .unwrap()
            .clone();
        let meta = self.ranges.get_mut(&range_key).unwrap();
        meta.ranges_evcited
            .iter()
            .filter(|r| {
                r.overlaps(range) && !meta.range_snapshot_list.keys().any(|r2| r2.overlaps(r))
            })
            .map(|r| r.clone())
            .collect()
    }

    // Evict a range which results in a split of the range containing it. Meta
    // should also be splitted.
    // Note: this should only be called when there's no snapshots whose range is
    // overlapped with `range`
    pub(crate) fn evict_range(&mut self, range: &CacheRange) {
        let range_key = self
            .ranges
            .keys()
            .find(|&r| r.contains(range))
            .unwrap()
            .clone();
        let mut meta = self.ranges.remove(&range_key).unwrap();

        assert!(meta.ranges_evcited.remove(range));
        let range_snapshot_list2 = meta.range_snapshot_list.split_off(&range_key);
        let evicted2 = meta.ranges_evcited.split_off(&range_key);
        let unreadable2 = meta.ranges_unreadable.split_off(&range_key);

        // range overlap assertion check
        if let Some(r) = range_snapshot_list2.keys().next() {
            assert!(!r.overlaps(range));
        }
        if let Some(r) = meta.range_snapshot_list.keys().last() {
            assert!(!r.overlaps(range));
        }
        if let Some(r) = evicted2.iter().next() {
            assert!(!r.overlaps(range));
        }
        if let Some(r) = meta.ranges_evcited.iter().last() {
            assert!(!r.overlaps(range));
        }
        if let Some(r) = unreadable2.iter().next() {
            assert!(!r.overlaps(range));
        }
        if let Some(r) = meta.ranges_unreadable.iter().last() {
            assert!(!r.overlaps(range));
        }

        let (r1, r2) = range_key.split_off(range);
        let safe_ts = meta.safe_ts;
        self.ranges.insert(r1, meta);
        self.ranges.insert(
            r2,
            RangeMeta {
                range_snapshot_list: range_snapshot_list2,
                ranges_evcited: evicted2,
                ranges_unreadable: unreadable2,
                safe_ts,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::CacheRange;

    use super::RangeManager;

    #[test]
    fn test_range_manager() {
        let mut range_mgr = RangeManager::default();
        let r1 = CacheRange::new(b"k00".to_vec(), b"k03".to_vec());
        let r1_1 = CacheRange::new(b"k00".to_vec(), b"k01".to_vec());
        let r1_2 = CacheRange::new(b"k01".to_vec(), b"k02".to_vec());

        range_mgr.new_range(r1.clone());
        range_mgr.set_range_readable(&r1, true);
        range_mgr.set_safe_ts(&r1, 5);
        assert!(!range_mgr.range_snapshot(&r1, 5));
        assert!(range_mgr.range_snapshot(&r1, 8));
        let tmp_r = CacheRange::new(b"k00".to_vec(), b"k04".to_vec());
        assert!(!range_mgr.range_snapshot(&tmp_r, 8));
        assert!(range_mgr.range_snapshot(&r1, 10));
        range_mgr.set_range_readable(&r1_1, false);
        assert!(!range_mgr.range_snapshot(&r1_1, 10));
        range_mgr.set_range_readable(&r1_2, false);
        assert!(!range_mgr.range_snapshot(&r1_2, 10));

        let r2 = CacheRange::new(b"k05".to_vec(), b"k10".to_vec());
        let r2_1 = CacheRange::new(b"k05".to_vec(), b"k07".to_vec());
        let r2_2 = CacheRange::new(b"k07".to_vec(), b"k08".to_vec());
        range_mgr.new_range(r2.clone());
        range_mgr.set_range_readable(&r2, true);
        range_mgr.set_safe_ts(&r2, 3);
        assert!(range_mgr.range_snapshot(&r2, 10));
        range_mgr.set_range_readable(&r2_1, false);
        range_mgr.set_range_readable(&r2_2, false);

        let r3 = CacheRange::new(b"k03".to_vec(), b"k05".to_vec());
        // it makes all ranges merged
        range_mgr.new_range(r3.clone());
        range_mgr.set_range_readable(&r3, true);
        let r = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        assert_eq!(range_mgr.ranges.len(), 1);
        let meta = range_mgr.ranges.get(&r).unwrap();
        assert_eq!(meta.safe_ts, 3);
        assert_eq!(meta.ranges_unreadable.len(), 4);
        assert_eq!(meta.range_snapshot_list.len(), 2);
        assert_eq!(meta.range_snapshot_list.get(&r1).unwrap().len(), 2);
        assert_eq!(meta.range_snapshot_list.get(&r2).unwrap().len(), 1);
    }
}
