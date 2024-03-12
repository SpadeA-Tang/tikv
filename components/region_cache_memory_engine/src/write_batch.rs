use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use engine_traits::{
    CacheRange, Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT,
};
use tikv_util::{box_err, config::ReadableSize, error, warn};

use crate::{
    background::BackgroundTask,
    engine::{cf_to_id, RangeCacheMemoryEngineCore, SkiplistEngine},
    keys::{encode_key, ValueType, ENC_KEY_SEQ_LENGTH},
    memory_limiter::{MemoryController, MemoryUsage},
    range_manager::RangeManager,
    RangeCacheMemoryEngine,
};

const NODE_OVERHEAD_SIZE_EXPECTATION: usize = 96;

pub struct RangeCacheWriteBatch {
    buffer: Vec<RangeCacheWriteBatchEntry>,
    engine: RangeCacheMemoryEngine,
    save_points: Vec<usize>,
    sequence_number: Option<u64>,
    memory_controller: Arc<MemoryController>,
    memory_usage_reach_hard_limit: bool,
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
            buffer: Vec::new(),
            engine: engine.clone(),
            save_points: Vec::new(),
            sequence_number: None,
            memory_controller: engine.memory_controller(),
            memory_usage_reach_hard_limit: false,
        }
    }
}

impl RangeCacheWriteBatch {
    pub fn with_capacity(engine: &RangeCacheMemoryEngine, cap: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(cap),
            engine: engine.clone(),
            save_points: Vec::new(),
            sequence_number: None,
            memory_controller: engine.memory_controller(),
            memory_usage_reach_hard_limit: false,
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

    // todo(SpadeA): now, we cache all keys even for those that will not be written
    // in to the memory engine.
    fn write_impl(&mut self, seq: u64) -> Result<()> {
        self.engine.handle_pending_load();
        let mut keys_to_cache: BTreeMap<CacheRange, Vec<(u64, RangeCacheWriteBatchEntry)>> =
            BTreeMap::new();
        let (engine, filtered_keys) = {
            let core = self.engine.core().read().unwrap();
            if core.range_manager().has_range_to_cache_write() {
                self.buffer
                    .iter()
                    .for_each(|e| e.maybe_cached(seq, &core, &mut keys_to_cache));
            }

            (
                core.engine().clone(),
                self.buffer
                    .iter()
                    .filter(|&e| e.should_write_to_memory(core.range_manager()))
                    .collect::<Vec<_>>(),
            )
        };
        if !keys_to_cache.is_empty() {
            let mut core = self.engine.core().write().unwrap();
            for (range, write_batches) in keys_to_cache {
                core.cached_write_batch
                    .entry(range)
                    .or_default()
                    .extend(write_batches.into_iter());
            }
        }
        filtered_keys
            .into_iter()
            .try_for_each(|e| e.write_to_memory(&engine, seq))
    }

    fn schedule_memory_check(&self) {
        if self.memory_controller.memory_checking() {
            return;
        }
        self.memory_controller.set_memory_checking(true);
        if let Err(e) = self
            .engine
            .bg_worker_manager()
            .schedule_task(BackgroundTask::MemoryCheck)
        {
            error!(
                "schedule memory check failed";
                "err" => ?e,
            );
            assert!(tikv_util::thread_group::is_shutdown(!cfg!(test)));
        }
    }

    fn memory_acquire(&mut self, mem_required: usize) -> bool {
        match self.memory_controller.acquire(mem_required) {
            MemoryUsage::HardLimitReached(n) => {
                self.memory_usage_reach_hard_limit = true;
                warn!(
                    "the memory usage of in-memory engine reaches to hard limit";
                    "memory_usage(MB)" => ReadableSize(n as u64).as_mb_f64(),
                    "memory_acquire(MB)" => ReadableSize(mem_required as u64).as_mb_f64(),
                );
                self.schedule_memory_check();
                return false;
            }
            MemoryUsage::SoftLimitReached(n) => {
                warn!(
                    "the memory usage of in-memory engine reaches to soft limit";
                    "memory_usage(MB)" => ReadableSize(n as u64).as_mb_f64(),
                    "memory_acquire(MB)" => ReadableSize(mem_required as u64).as_mb_f64(),
                );
                self.schedule_memory_check();
            }
            _ => {}
        }
        true
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

    // keys will be inserted in `keys_to_cache` if they are to cached.
    #[inline]
    pub fn maybe_cached(
        &self,
        seq: u64,
        engine_core: &RangeCacheMemoryEngineCore,
        keys_to_cache: &mut BTreeMap<CacheRange, Vec<(u64, RangeCacheWriteBatchEntry)>>,
    ) {
        for r in &engine_core.range_manager().ranges_loading_snapshot {
            if r.0.contains_key(&self.key) {
                let range = r.0.clone();
                keys_to_cache
                    .entry(range)
                    .or_default()
                    .push((seq, self.clone()));
                return;
            }
        }
        for r in &engine_core.range_manager().ranges_loading_cached_write {
            if r.contains_key(&self.key) {
                let range = r.clone();
                keys_to_cache
                    .entry(range)
                    .or_default()
                    .push((seq, self.clone()));
                return;
            }
        }
    }

    #[inline]
    pub fn write_to_memory(&self, skiplist_engine: &SkiplistEngine, seq: u64) -> Result<()> {
        let handle = &skiplist_engine.data[self.cf];
        let (key, value) = self.encode(seq);
        let _ = handle.put(key, value);
        Ok(())
    }
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
}

impl Mutable for RangeCacheWriteBatch {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, val)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], val: &[u8]) -> Result<()> {
        if self.memory_usage_reach_hard_limit {
            return Ok(());
        }
        let expected_memory =
            NODE_OVERHEAD_SIZE_EXPECTATION + key.len() + val.len() + ENC_KEY_SEQ_LENGTH;
        if !self.memory_acquire(expected_memory) {
            return Ok(());
        }

        self.buffer
            .push(RangeCacheWriteBatchEntry::put_value(cf, key, val));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        if self.memory_usage_reach_hard_limit {
            return Ok(());
        }
        let expected_memory = NODE_OVERHEAD_SIZE_EXPECTATION + key.len() + ENC_KEY_SEQ_LENGTH;
        if !self.memory_acquire(expected_memory) {
            return Ok(());
        }

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
    use std::sync::Arc;

    use engine_traits::{CacheRange, Peekable, RangeCacheEngine, WriteBatch};

    use super::*;
    use crate::EngineConfig;

    #[test]
    fn test_write_to_skiplist() {
        let engine = RangeCacheMemoryEngine::new(Arc::default(), EngineConfig::config_for_test());
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write().unwrap();
            core.mut_range_manager().set_range_readable(&r, true);
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        assert_eq!(wb.write().unwrap(), 1);
        let sl = engine.core.read().unwrap().engine().data[cf_to_id(CF_DEFAULT)].clone();
        let actual = sl.get(&encode_key(b"aaa", 1, ValueType::Value)).unwrap();
        assert_eq!(&b"bbb"[..], actual.value())
    }

    #[test]
    fn test_savepoints() {
        let engine = RangeCacheMemoryEngine::new(Arc::default(), EngineConfig::config_for_test());
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write().unwrap();
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
        let sl = engine.core.read().unwrap().engine().data[cf_to_id(CF_DEFAULT)].clone();
        let actual = sl.get(&encode_key(b"aaa", 1, ValueType::Value)).unwrap();
        assert_eq!(&b"bbb"[..], actual.value());
        assert!(sl.get(&encode_key(b"ccc", 1, ValueType::Value)).is_none())
    }

    #[test]
    fn test_put_write_clear_delete_put_write() {
        let engine = RangeCacheMemoryEngine::new(Arc::default(), EngineConfig::config_for_test());
        let r = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(r.clone());
        {
            let mut core = engine.core.write().unwrap();
            core.mut_range_manager().set_range_readable(&r, true);
            core.mut_range_manager().set_safe_point(&r, 10);
        }
        let mut wb = RangeCacheWriteBatch::from(&engine);
        wb.put(b"aaa", b"bbb").unwrap();
        wb.set_sequence_number(1).unwrap();
        _ = wb.write().unwrap();
        wb.clear();
        wb.put(b"bbb", b"ccc").unwrap();
        wb.delete(b"aaa").unwrap();
        wb.set_sequence_number(2).unwrap();
        _ = wb.write().unwrap();
        let snapshot = engine.snapshot(r, u64::MAX, 2).unwrap();
        assert_eq!(
            snapshot.get_value(&b"bbb"[..]).unwrap().unwrap(),
            &b"ccc"[..]
        );
        assert!(snapshot.get_value(&b"aaa"[..]).unwrap().is_none())
    }
}
