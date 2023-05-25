// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use fail::fail_point;
use kvproto::{
    raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, RaftCmdRequest},
    raft_serverpb::RegionLocalState,
};
use protobuf::Message;
use raftstore::{coprocessor::RegionChangeReason, store::metrics::PEER_ADMIN_CMD_COUNTER, Result};

use super::AdminCmdResult;
use crate::{
    batch::StoreContext,
    fsm::ApplyResReporter,
    raft::{Apply, Peer},
};

#[derive(Debug)]
pub struct FlashbackResult {
    index: u64,
    region_state: RegionLocalState,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn propose_flashback<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        let data = req.write_to_bytes().unwrap();
        self.propose(store_ctx, data)
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn apply_flashback(
        &mut self,
        index: u64,
        req: &AdminRequest,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        // Modify flashback fields in region state.
        //
        // Note: region state is persisted by `Peer::on_apply_res_flashback`.
        let region = self.region_state_mut().mut_region();
        match req.get_cmd_type() {
            AdminCmdType::PrepareFlashback => {
                PEER_ADMIN_CMD_COUNTER.prepare_flashback.success.inc();

                region.set_is_in_flashback(true);
                region.set_flashback_start_ts(req.get_prepare_flashback().get_start_ts());
            }
            AdminCmdType::FinishFlashback => {
                PEER_ADMIN_CMD_COUNTER.finish_flashback.success.inc();

                region.set_is_in_flashback(false);
                region.clear_flashback_start_ts();
            }
            _ => unreachable!(),
        }
        Ok((
            AdminResponse::default(),
            AdminCmdResult::Flashback(FlashbackResult {
                index,
                region_state: self.region_state().clone(),
            }),
        ))
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Match v1 on_set_flashback_state.
    #[allow(unused_mut)]
    pub fn on_apply_res_flashback<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut res: FlashbackResult,
    ) {
        (|| {
            fail_point!("keep_peer_fsm_flashback_state_false", |_| {
                res.region_state.mut_region().set_is_in_flashback(false);
            })
        })();
        slog::debug!(self.logger,
            "flashback update region";
            "region" => ?res.region_state.get_region());
        let region_id = self.region_id();
        {
            let mut meta = store_ctx.store_meta.lock().unwrap();
            meta.set_region(res.region_state.get_region(), true, &self.logger);
            let (reader, _) = meta.readers.get_mut(&region_id).unwrap();
            self.set_region(
                &store_ctx.coprocessor_host,
                reader,
                res.region_state.get_region().clone(),
                RegionChangeReason::Flashback,
                res.region_state.get_tablet_index(),
            );
        }

        self.state_changes_mut()
            .put_region_state(region_id, res.index, &res.region_state)
            .unwrap();
        self.set_has_extra_write();

        // Compares to v1, v2 does not expire remote lease, because only
        // local reader can serve read requests.
    }
}
