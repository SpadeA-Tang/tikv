use bytes::Bytes;
use engine_traits::{
    CacheRange, Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT,
};
use tikv_util::box_err;

use crate::{
    engine::{cf_to_id, SkiplistEngine},
    keys::{encode_key, ValueType},
    range_manager::RangeManager,
    RangeCacheMemoryEngine,
};

pub struct RangeCacheWriteBatch {
    range_in_cache: bool,
    buffer: Vec<RangeCacheWriteBatchEntry>,
    range_in_loading: bool,
    loading_range_buffer: Vec<RangeCacheWriteBatchEntry>,
    engine: RangeCacheMemoryEngine,
    save_points: Vec<usize>,
    sequence_number: Option<u64>,
}

impl std::fmt::Debug for RangeCacheWriteBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeCacheWriteBatch")
            .field("buffer", &self.buffer)
            .field("save_points", &self.save_points)
            .field("sequence_number", &self.sequence_number)
            .finish()
    }
}

impl From<&RangeCacheMemoryEngine> for RangeCacheWriteBatch {
    fn from(engine: &RangeCacheMemoryEngine) -> Self {
        Self {
            range_in_cache: false,
            range_in_loading: false,
            buffer: Vec::new(),
            loading_range_buffer: Vec::new(),
            engine: engine.clone(),
            save_points: Vec::new(),
            sequence_number: None,
        }
    }
}

impl RangeCacheWriteBatch {
    pub fn with_capacity(engine: &RangeCacheMemoryEngine, cap: usize) -> Self {
        Self {
            range_in_cache: false,
            range_in_loading: false,
            buffer: Vec::with_capacity(cap),
            // cache_buffer should need small capacity
            loading_range_buffer: Vec::new(),
            engine: engine.clone(),
            save_points: Vec::new(),
            sequence_number: None,
        }
    }

    /// Sets the sequence number for this batch. This should only be called
    /// prior to writing the batch.
    pub fn set_sequence_number(&mut self, seq: u64) -> Result<()> {
        if let Some(seqno) = self.sequence_number {
            return Err(box_err!("Sequence number {} already set", seqno));
        };
        self.sequence_number = Some(seq);
        Ok(())
    }

    fn write_impl(&mut self, seq: u64) -> Result<()> {
        let (entries_to_write, engine) = self
            .engine
            .handle_loading_range_buffer(seq, std::mem::take(&mut self.loading_range_buffer));
        self.engine.handle_loading_range();
        entries_to_write
            .into_iter()
            .chain(std::mem::take(&mut self.buffer).into_iter())
            .try_for_each(|e| e.write_to_memory(&engine, seq))
    }
}

#[derive(Clone, Debug)]
enum WriteBatchEntryInternal {
    PutValue(Bytes),
    Deletion,
}

impl WriteBatchEntryInternal {
    fn encode(&self, key: &[u8], seq: u64) -> (Bytes, Bytes) {
        match self {
            WriteBatchEntryInternal::PutValue(value) => {
                (encode_key(key, seq, ValueType::Value), value.clone())
            }
            WriteBatchEntryInternal::Deletion => {
                (encode_key(key, seq, ValueType::Deletion), Bytes::new())
            }
        }
    }
    fn data_size(&self) -> usize {
        match self {
            WriteBatchEntryInternal::PutValue(value) => value.len(),
            WriteBatchEntryInternal::Deletion => 0,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RangeCacheWriteBatchEntry {
    cf: usize,
    key: Bytes,
    inner: WriteBatchEntryInternal,
}

impl RangeCacheWriteBatchEntry {
    pub fn put_value(cf: &str, key: &[u8], value: &[u8]) -> Self {
        Self {
            cf: cf_to_id(cf),
            key: Bytes::copy_from_slice(key),
            inner: WriteBatchEntryInternal::PutValue(Bytes::copy_from_slice(value)),
        }
    }

    pub fn deletion(cf: &str, key: &[u8]) -> Self {
        Self {
            cf: cf_to_id(cf),
            key: Bytes::copy_from_slice(key),
            inner: WriteBatchEntryInternal::Deletion,
        }
    }

    #[inline]
    pub fn encode(&self, seq: u64) -> (Bytes, Bytes) {
        self.inner.encode(&self.key, seq)
    }

    pub fn data_size(&self) -> usize {
        self.key.len() + std::mem::size_of::<u64>() + self.inner.data_size()
    }

    #[inline]
    pub fn should_write_to_memory(&self, range_manager: &RangeManager) -> bool {
        range_manager.contains(&self.key)
    }

    #[inline]
    pub fn write_to_memory(&self, skiplist_engine: &SkiplistEngine, seq: u64) -> Result<()> {
        let handle = &skiplist_engine.data[self.cf];
        let (key, value) = self.encode(seq);
        let _ = handle.put(key, value);
        Ok(())
    }
}

// group_write_batch_entries classifies the entries to two categories according
// to the infomation in range manager:
// 1. entreis that can be written to memory engine directly
// 2. entreis that need to be cached
// For 2, we group the entries according to the range. The method uses the
// property that entries in the same range are neighbors. Though that the method
// still handles corretly even they are randomly positioned.
pub fn group_write_batch_entries(
    mut entries: Vec<RangeCacheWriteBatchEntry>,
    range_manager: &RangeManager,
) -> (
    Vec<(CacheRange, Vec<RangeCacheWriteBatchEntry>)>,
    Vec<RangeCacheWriteBatchEntry>,
) {
    let mut group_entries_to_cache: Vec<(CacheRange, Vec<RangeCacheWriteBatchEntry>)> = vec![];
    let mut entries_to_write: Vec<RangeCacheWriteBatchEntry> = vec![];
    let mut drain = entries.drain(..).peekable();
    while let Some(mut e) = drain.next() {
        let mut cache_range = None;
        for r in &range_manager.ranges_loading_snapshot {
            if r.0.contains_key(&e.key) {
                cache_range = Some(r.0.clone());
                break;
            }
        }
        if cache_range.is_none() {
            for r in &range_manager.ranges_loading_cached_write {
                if r.contains_key(&e.key) {
                    cache_range = Some(r.clone());
                }
            }
        }
        if let Some(cache_range) = cache_range {
            let mut current_group = vec![];
            // This range of this write batch entry is still in loading status
            loop {
                current_group.push(e);
                if let Some(next_e) = drain.peek()
                    && cache_range.contains_key(&next_e.key)
                {
                    e = drain.next().unwrap();
                } else {
                    break;
                }
            }
            group_entries_to_cache.push((cache_range, current_group));
        } else {
            // cache_range is still None, it means the range has finished loading and
            // became a normal cache range
            for r in range_manager.ranges().keys() {
                if r.contains_key(&e.key) {
                    cache_range = Some(r.clone());
                }
            }
            let cache_range = cache_range.unwrap();
            loop {
                entries_to_write.push(e);
                if let Some(next_e) = drain.peek()
                    && cache_range.contains_key(&next_e.key)
                {
                    e = drain.next().unwrap();
                } else {
                    break;
                }
            }
        }
    }
    (group_entries_to_cache, entries_to_write)
}

impl WriteBatchExt for RangeCacheMemoryEngine {
    type WriteBatch = RangeCacheWriteBatch;
    // todo: adjust it
    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        RangeCacheWriteBatch::from(self)
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        RangeCacheWriteBatch::with_capacity(self, cap)
    }
}

impl WriteBatch for RangeCacheWriteBatch {
    fn write_opt(&mut self, _: &WriteOptions) -> Result<u64> {
        self.sequence_number
            .map(|seq| self.write_impl(seq).map(|()| seq))
            .transpose()
            .map(|o| o.ok_or_else(|| box_err!("sequence_number must be set!")))?
    }

    fn data_size(&self) -> usize {
        self.buffer
            .iter()
            .map(RangeCacheWriteBatchEntry::data_size)
            .sum()
    }

    fn count(&self) -> usize {
        self.buffer.len()
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        unimplemented!()
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.save_points.clear();
        _ = self.sequence_number.take();
    }

    fn set_save_point(&mut self) {
        self.save_points.push(self.buffer.len())
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.save_points
            .pop()
            .map(|_| ())
            .ok_or_else(|| box_err!("no save points available"))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.save_points
            .pop()
            .map(|sp| {
                self.buffer.truncate(sp);
            })
            .ok_or_else(|| box_err!("no save point available!"))
    }

    fn merge(&mut self, mut other: Self) -> Result<()> {
        self.buffer.append(&mut other.buffer);
        Ok(())
    }

    fn prepare_for_range(&mut self, range: &CacheRange) {
        let (range_in_cache, range_in_loading) = self.engine.prepare_for_apply(range);
        self.range_in_cache = range_in_cache;
        self.range_in_loading = range_in_loading;
    }
}

impl Mutable for RangeCacheWriteBatch {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, val)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], val: &[u8]) -> Result<()> {
        if self.range_in_cache {
            self.buffer
                .push(RangeCacheWriteBatchEntry::put_value(cf, key, val));
        } else if self.range_in_loading {
            self.loading_range_buffer
                .push(RangeCacheWriteBatchEntry::put_value(cf, key, val));
        }
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.buffer
            .push(RangeCacheWriteBatchEntry::deletion(cf, key));
        Ok(())
    }

    fn delete_range(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use engine_rocks::util::new_engine;
    use engine_traits::{
        CacheRange, KvEngine, Peekable, RangeCacheEngine, WriteBatch, CF_WRITE, DATA_CFS,
    };
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_write_to_skiplist() {
        let engine = RangeCacheMemoryEngine::new(Arc::default(), Duration::from_secs(1));
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_range_readable(&r, true);
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        assert_eq!(wb.write().unwrap(), 1);
        let sl = engine.core.read().engine().data[cf_to_id(CF_DEFAULT)].clone();
        let actual = sl.get(&encode_key(b"aaa", 1, ValueType::Value)).unwrap();
        assert_eq!(&b"bbb"[..], actual.value())
    }

    #[test]
    fn test_savepoints() {
        let engine = RangeCacheMemoryEngine::new(Arc::default(), Duration::from_secs(1));
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_range_readable(&r, true);
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_save_point();
        wb.put(b"aaa", b"ccc").unwrap();
        wb.put(b"ccc", b"ddd").unwrap();
        wb.rollback_to_save_point().unwrap();
        wb.set_sequence_number(1).unwrap();
        assert_eq!(wb.write().unwrap(), 1);
        let sl = engine.core.read().engine().data[cf_to_id(CF_DEFAULT)].clone();
        let actual = sl.get(&encode_key(b"aaa", 1, ValueType::Value)).unwrap();
        assert_eq!(&b"bbb"[..], actual.value());
        assert!(sl.get(&encode_key(b"ccc", 1, ValueType::Value)).is_none())
    }

    #[test]
    fn test_put_write_clear_delete_put_write() {
        let engine = RangeCacheMemoryEngine::new(Arc::default(), Duration::from_secs(1));
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().set_range_readable(&r, true);
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        _ = wb.write();
        wb.clear();
        wb.put(b"bbb", b"ccc").unwrap();
        wb.delete(b"aaa").unwrap();
        wb.set_sequence_number(2).unwrap();
        _ = wb.write();
        let snapshot = engine.snapshot(r, u64::MAX, 2).unwrap();
        assert_eq!(
            snapshot.get_value(&b"bbb"[..]).unwrap().unwrap(),
            &b"ccc"[..]
        );
        assert!(snapshot.get_value(&b"aaa"[..]).unwrap().is_none())
    }

    #[test]
    fn test_group_entries() {
        let path = Builder::new().prefix("test_group").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
        let snap = rocks_engine.snapshot(None);

        let mut range_manager = RangeManager::default();
        let r1 = CacheRange::new(b"k00".to_vec(), b"k10".to_vec());
        let r2 = CacheRange::new(b"k10".to_vec(), b"k20".to_vec());
        let r3 = CacheRange::new(b"k20".to_vec(), b"k30".to_vec());
        range_manager.new_range(r1.clone());
        range_manager
            .ranges_loading_snapshot
            .push_back((r2.clone(), Arc::new(snap)));
        range_manager.ranges_loading_cached_write.push(r3.clone());

        let mut entries = vec![];
        entries.push(RangeCacheWriteBatchEntry::put_value(
            CF_DEFAULT, b"k22", b"val",
        ));
        entries.push(RangeCacheWriteBatchEntry::put_value(
            CF_DEFAULT, b"k21", b"val",
        ));
        entries.push(RangeCacheWriteBatchEntry::deletion(CF_DEFAULT, b"k25"));
        entries.push(RangeCacheWriteBatchEntry::put_value(
            CF_DEFAULT, b"k28", b"val",
        ));

        entries.push(RangeCacheWriteBatchEntry::put_value(
            CF_WRITE, b"k03", b"val",
        ));
        entries.push(RangeCacheWriteBatchEntry::put_value(
            CF_WRITE, b"k05", b"val",
        ));
        entries.push(RangeCacheWriteBatchEntry::put_value(
            CF_WRITE, b"k09", b"val",
        ));

        entries.push(RangeCacheWriteBatchEntry::put_value(
            CF_WRITE, b"k10", b"val",
        ));
        entries.push(RangeCacheWriteBatchEntry::put_value(
            CF_WRITE, b"k19", b"val",
        ));

        let (group_entries_to_cache, entries_to_write) =
            group_write_batch_entries(entries, &range_manager);
        assert_eq!(group_entries_to_cache.len(), 2);
        assert_eq!(entries_to_write.len(), 3);
        entries_to_write
            .iter()
            .for_each(|e| assert!(r1.contains_key(&e.key)));
        group_entries_to_cache.iter().for_each(|(range, entries)| {
            if *range == r2 {
                assert_eq!(entries.len(), 2);
            } else if *range == r3 {
                assert_eq!(entries.len(), 4);
            } else {
                unreachable!();
            }
            entries
                .iter()
                .for_each(|e| assert!(range.contains_key(&e.key)))
        });
    }
}
