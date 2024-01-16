// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, fmt::Debug};

use keys::{enc_end_key, enc_start_key};
use kvproto::metapb;

use crate::{Iterable, Snapshot, WriteBatchExt};

/// RangeCaheEngine works as a region cache caching some regions (in Memory or
/// NVME for instance) to improve the read performance.
pub trait RangeCacheEngine:
    WriteBatchExt + Iterable + Debug + Clone + Unpin + Send + Sync + 'static
{
    type Snapshot: Snapshot;

    // If None is returned, the RangeCaheEngine is currently not readable for this
    // region or read_ts.
    // Sequence number is shared between RangeCaheEngine and disk KvEnigne to
    // provide atomic write
    fn snapshot(&self, range: CacheRange, read_ts: u64, seq_num: u64) -> Option<Self::Snapshot>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl CacheRange {
    pub fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        Self { start, end }
    }

    pub fn from_region(region: &metapb::Region) -> Self {
        Self {
            start: enc_start_key(region),
            end: enc_end_key(region),
        }
    }
}

impl PartialOrd for CacheRange {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if self.end <= other.start {
            return Some(cmp::Ordering::Less);
        }

        if other.end <= self.start {
            return Some(cmp::Ordering::Greater);
        }

        if self == other {
            return Some(cmp::Ordering::Equal);
        }

        None
    }
}

impl Ord for CacheRange {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let c = self.start.cmp(&other.start);
        if !c.is_eq() {
            return c;
        }
        self.end.cmp(&other.end)
    }
}

impl CacheRange {
    // todo: need to consider ""?
    pub fn contains(&self, other: &CacheRange) -> bool {
        self.start <= other.start && self.end >= other.end
    }

    pub fn overlaps(&self, other: &CacheRange) -> bool {
        self.start < other.end && other.start < self.end
    }

    pub fn is_sibling(&self, other: &CacheRange) -> bool {
        self.start == other.end || self.end == other.start
    }

    pub fn split_off(&self, key: &CacheRange) -> (CacheRange, CacheRange) {
        (
            CacheRange {
                start: self.start.clone(),
                end: key.start.clone(),
            },
            CacheRange {
                start: key.end.clone(),
                end: self.end.clone(),
            },
        )
    }

    // r1 and r2 should be unoverlap
    pub fn merge(r1: CacheRange, r2: CacheRange) -> CacheRange {
        assert!(!r1.overlaps(&r2));
        assert!(r1.is_sibling(&r2));
        let CacheRange { start: s1, end: e1 } = r1;
        let CacheRange { start: s2, end: e2 } = r2;
        if s1 < s2 {
            CacheRange { start: s1, end: e2 }
        } else {
            CacheRange { start: s2, end: e1 }
        }
    }
}
