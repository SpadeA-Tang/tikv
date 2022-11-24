// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements the interactions with pd.

use std::cmp;

use engine_traits::{KvEngine, RaftEngine, TabletFactory};
use fail::fail_point;
use kvproto::{metapb, pdpb};
use raftstore::store::Transport;
use slog::error;
use tikv_util::time::InstantExt;

use crate::{
    batch::StoreContext,
    fsm::{PeerFsmDelegate, Store, StoreFsmDelegate},
    raft::Peer,
    router::{PeerTick, StoreTick},
    worker::{PdHeartbeatTask, PdTask},
};

impl<'a, EK: KvEngine, ER: RaftEngine, T> StoreFsmDelegate<'a, EK, ER, T> {
    pub fn on_pd_store_heartbeat(&mut self) {
        self.fsm.store.store_heartbeat_pd(self.store_ctx);
        self.schedule_tick(
            StoreTick::PdStoreHeartbeat,
            self.store_ctx.cfg.pd_store_heartbeat_tick_interval.0,
        );
    }
}

impl Store {
    pub fn store_heartbeat_pd<EK, ER, T>(&mut self, ctx: &mut StoreContext<EK, ER, T>)
    where
        EK: KvEngine,
        ER: RaftEngine,
    {
        let mut stats = pdpb::StoreStats::default();

        stats.set_store_id(self.store_id());
        {
            let meta = ctx.store_meta.lock().unwrap();
            stats.set_region_count(meta.tablet_caches.len() as u32);
        }

        stats.set_sending_snap_count(0);
        stats.set_receiving_snap_count(0);

        stats.set_start_time(self.start_time().unwrap() as u32);

        stats.set_bytes_written(0);
        stats.set_keys_written(0);
        stats.set_is_busy(false);

        // stats.set_query_stats(query_stats);

        let task = PdTask::StoreHeartbeat { stats };
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(self.logger(), "notify pd failed";
                "store_id" => self.store_id(),
                "err" => ?e
            );
        }
    }
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_pd_heartbeat(&mut self) {
        self.fsm.peer_mut().update_peer_statistics();
        if self.fsm.peer().is_leader() {
            self.fsm.peer_mut().heartbeat_pd(self.store_ctx);
        }
        // TODO: hibernate region
        self.schedule_tick(PeerTick::PdHeartbeat);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn heartbeat_pd<T>(&mut self, ctx: &StoreContext<EK, ER, T>) {
        let task = PdTask::Heartbeat(PdHeartbeatTask {
            term: self.term(),
            region: self.region().clone(),
            down_peers: self.collect_down_peers(ctx.cfg.max_peer_down_duration.0),
            peer: self.peer().clone(),
            pending_peers: self.collect_pending_peers(ctx),
            written_bytes: self.self_stat().written_bytes,
            written_keys: self.self_stat().written_keys,
            approximate_size: None,
            approximate_keys: None,
            wait_data_peers: Vec::new(),
        });
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "err" => ?e,
            );
            return;
        }
        fail_point!("schedule_check_split");
    }

    /// Collects all pending peers and update `peers_start_pending_time`.
    fn collect_pending_peers<T>(&mut self, ctx: &StoreContext<EK, ER, T>) -> Vec<metapb::Peer> {
        let mut pending_peers = Vec::with_capacity(self.region().get_peers().len());
        let status = self.raft_group().status();
        let truncated_idx = self
            .storage()
            .apply_state()
            .get_truncated_state()
            .get_index();

        if status.progress.is_none() {
            return pending_peers;
        }

        // TODO: update `peers_start_pending_time`.

        let progresses = status.progress.unwrap().iter();
        for (&id, progress) in progresses {
            if id == self.peer_id() {
                continue;
            }
            // The `matched` is 0 only in these two cases:
            // 1. Current leader hasn't communicated with this peer.
            // 2. This peer does not exist yet(maybe it is created but not initialized)
            //
            // The correctness of region merge depends on the fact that all target peers
            // must exist during merging. (PD rely on `pending_peers` to check whether all
            // target peers exist)
            //
            // So if the `matched` is 0, it must be a pending peer.
            // It can be ensured because `truncated_index` must be greater than
            // `RAFT_INIT_LOG_INDEX`(5).
            if progress.matched < truncated_idx {
                if let Some(p) = self.peer_from_cache(id) {
                    pending_peers.push(p);
                } else {
                    if ctx.cfg.dev_assert {
                        panic!(
                            "{:?} failed to get peer {} from cache",
                            self.logger.list(),
                            id
                        );
                    }
                    error!(
                        self.logger,
                        "failed to get peer from cache";
                        "region_id" => self.region_id(),
                        "peer_id" => self.peer_id(),
                        "get_peer_id" => id,
                    );
                }
            }
        }
        pending_peers
    }

    pub fn destroy_peer_pd<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        let task = PdTask::DestroyPeer {
            region_id: self.region_id(),
        };
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "err" => %e,
            );
        }
    }

    pub fn ask_batch_split_pd<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        split_keys: Vec<Vec<u8>>,
    ) {
        let task = PdTask::AskBatchSplit {
            region: self.region().clone(),
            split_keys,
            peer: self.peer().clone(),
            right_derive: ctx.cfg.right_derive_when_split,
            ch: None,
        };
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd to split";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "err" => %e,
            );
        }
    }
}
