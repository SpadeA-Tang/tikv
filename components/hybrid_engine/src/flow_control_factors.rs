// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{FlowControlFactorsExt, KvEngine, MemoryEngine, Result};

use crate::engine::HybridEngine;

impl<EK, EM> FlowControlFactorsExt for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    fn get_cf_num_files_at_level(&self, cf: &str, level: usize) -> Result<Option<u64>> {
        self.disk_engine().get_cf_num_files_at_level(cf, level)
    }

    fn get_cf_num_immutable_mem_table(&self, cf: &str) -> Result<Option<u64>> {
        self.disk_engine().get_cf_num_immutable_mem_table(cf)
    }

    fn get_cf_pending_compaction_bytes(&self, cf: &str) -> Result<Option<u64>> {
        self.disk_engine().get_cf_pending_compaction_bytes(cf)
    }
}
