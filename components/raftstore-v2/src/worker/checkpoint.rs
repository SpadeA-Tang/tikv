use std::{fmt::Display, path::PathBuf};

use engine_traits::{Checkpointer, KvEngine, TabletRegistry};
use futures::channel::oneshot::Sender;
use raftstore::store::RAFT_INIT_LOG_INDEX;
use slog::Logger;
use tikv_util::{slog_panic, worker::Runnable};

use crate::operation::SPLIT_PREFIX;

pub enum Task<EK> {
    Checkpoint {
        tablet: EK,
        log_index: u64,
        parent_region: u64,
        split_regions: Vec<u64>,
        sender: Sender<bool>,
    },
}

impl<EK> Display for Task<EK> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Task::Checkpoint {
                log_index,
                parent_region,
                split_regions,
                ..
            } => write!(
                f,
                "create checkpoint for batch split, parent region_id {}, source region_ids {}, log_index {}",
                parent_region, split_regions, log_index,
            ),
        }
    }
}

pub struct Runner<EK: KvEngine> {
    logger: Logger,
    tablet_registry: TabletRegistry<EK>,
}

pub fn temp_split_path<EK>(registry: &TabletRegistry<EK>, region_id: u64) -> PathBuf {
    let tablet_name = registry.tablet_name(SPLIT_PREFIX, region_id, RAFT_INIT_LOG_INDEX);
    registry.tablet_root().join(tablet_name)
}

impl<EK: KvEngine> Runner<EK> {
    pub fn new(logger: Logger, tablet_registry: TabletRegistry<EK>) -> Self {
        Self {
            logger,
            tablet_registry,
        }
    }

    fn checkpoint(
        &self,
        tablet: EK,
        parent_region: u64,
        split_regions: Vec<u64>,
        log_index: u64,
        sender: Sender<bool>,
    ) {
        let mut checkpointer = tablet.new_checkpointer().unwrap_or_else(|e| {
            slog_panic!(
                self.logger,
                "fails to create checkpoint object";
                "error" => ?e
            )
        });

        for id in split_regions {
            let split_temp_path = temp_split_path(&self.tablet_registry, id);
            checkpointer
                .create_at(&split_temp_path, None, 0)
                .unwrap_or_else(|e| {
                    slog_panic!(
                        self.logger,
                        "fails to create checkpoint";
                        "path" => %split_temp_path.display(),
                        "error" => ?e
                    )
                });
        }

        let derived_path = self.tablet_registry.tablet_path(parent_region, log_index);
        if !derived_path.exists() {
            checkpointer
                .create_at(&derived_path, None, 0)
                .unwrap_or_else(|e| {
                    slog_panic!(
                        self.logger,
                        "fails to create checkpoint";
                        "path" => %derived_path.display(),
                        "error" => ?e
                    )
                });
        }

        let _ = sender.send(true);
    }
}

impl<EK: KvEngine> Runnable for Runner<EK> {
    type Task = Task<EK>;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::Checkpoint {
                tablet: EK,
                log_index,
                parent_region,
                split_regions,
                sender,
            } => {}
        }
    }
}
