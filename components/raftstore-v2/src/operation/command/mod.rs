// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains implementations of commmands that will be replicated to
//! all replicas and executed in the same order. Typical commands include:
//! - normal writes like put, delete, etc.
//! - admin commands like split, compact, etc.
//!
//! General proceessing is:
//! - Propose a command to the leader via PeerMsg::Command,
//! - The leader batch up commands and replicates them to followers,
//! - Once they are replicated to majority, leader considers it committed and
//!   send to another thread for execution via
//!   `schedule_apply_committed_entries`,
//! - The apply thread executes the commands in buffer, and write to LSM tree
//!   via `flush`,
//! - Applied result are sent back to peer fsm, and update memory state in
//!   `on_apply_res`.

use std::{cmp, collections::VecDeque};

use batch_system::{Fsm, FsmScheduler, Mailbox};
use engine_traits::{KvEngine, RaftEngine, WriteBatch, WriteOptions};
use kvproto::{
    raft_cmdpb::{self, AdminCmdType, CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader},
    raft_serverpb::RegionLocalState,
};
use protobuf::Message;
use raft::eraftpb::Entry;
use raftstore::{
    store::{
        cmd_resp,
        fsm::{
            apply::{
                ApplyResult, APPLY_WB_SHRINK_SIZE, DEFAULT_APPLY_WB_SIZE,
                SHRINK_PENDING_CMD_QUEUE_CAP,
            },
            Proposal,
        },
        local_metrics::RaftMetrics,
        metrics::*,
        msg::ErrorCallback,
        util, WriteCallback,
    },
    Error, Result,
};
use slog::{error, info};
use tikv_util::{box_err, time::monotonic_raw_now};

use crate::{
    batch::StoreContext,
    fsm::{ApplyFsm, ApplyResReporter, PeerFsmDelegate},
    raft::{Apply, Peer},
    router::{ApplyRes, ApplyTask, CmdResChannel, ExecResult, PeerMsg},
};

mod write;

pub use write::{SimpleWriteDecoder, SimpleWriteEncoder};

use self::write::SimpleWrite;

#[derive(Debug)]
pub struct CommittedEntries {
    /// Entries need to be applied. Note some entries may not be included for
    /// flow control.
    entry_and_proposals: Vec<(Entry, Vec<CmdResChannel>)>,
}

fn new_response(header: &RaftRequestHeader) -> RaftCmdResponse {
    let mut resp = RaftCmdResponse::default();
    if !header.get_uuid().is_empty() {
        let uuid = header.get_uuid().to_vec();
        resp.mut_header().set_uuid(uuid);
    }
    resp
}

impl<'a, EK: KvEngine, ER: RaftEngine, T> PeerFsmDelegate<'a, EK, ER, T> {
    #[inline]
    pub fn on_command(&mut self, req: RaftCmdRequest, ch: CmdResChannel) {
        if !req.get_requests().is_empty() {
            self.fsm
                .peer_mut()
                .on_write_command(self.store_ctx, req, ch)
        } else if req.has_admin_request() {
            self.fsm
                .peer_mut()
                .on_admin_request(self.store_ctx, req, ch)
        } else if req.has_status_request() {
            error!(self.fsm.logger(), "status command should be sent by Query");
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Schedule an apply fsm to apply logs in the background.
    ///
    /// Everytime a snapshot is applied or peer is just started, it will
    /// schedule a new apply fsm. The old fsm will stopped automatically
    /// when the old apply scheduler is dropped.
    #[inline]
    pub fn schedule_apply_fsm<T>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        let region_state = self.storage().region_state().clone();
        let mailbox = store_ctx.router.mailbox(self.region_id()).unwrap();
        let tablet = self.tablet().clone();
        let logger = self.logger.clone();
        let (apply_scheduler, mut apply_fsm) = ApplyFsm::new(
            store_ctx.store_id,
            region_state,
            mailbox,
            tablet,
            store_ctx.engine.clone(),
            store_ctx.tablet_factory.clone(),
            logger,
        );
        store_ctx
            .apply_pool
            .spawn(async move { apply_fsm.handle_all_tasks().await })
            .unwrap();
        self.set_apply_scheduler(apply_scheduler);
    }

    #[inline]
    fn validate_command(&self, req: &RaftCmdRequest, metrics: &mut RaftMetrics) -> Result<()> {
        if let Err(e) = util::check_store_id(req, self.peer().get_store_id()) {
            metrics.invalid_proposal.mismatch_store_id.inc();
            return Err(e);
        }
        for r in req.get_requests() {
            if let CmdType::Get | CmdType::Snap | CmdType::ReadIndex = r.get_cmd_type() {
                return Err(box_err!("internal error: query can't be sent as command"));
            }
        }
        if let Err(e) = util::check_peer_id(req, self.peer().get_id()) {
            metrics.invalid_proposal.mismatch_peer_id.inc();
            return Err(e);
        }
        if !self.is_leader() {
            metrics.invalid_proposal.not_leader.inc();
            return Err(Error::NotLeader(self.region_id(), self.leader()));
        }
        if let Err(e) = util::check_term(req, self.term()) {
            metrics.invalid_proposal.stale_command.inc();
            return Err(e);
        }
        if let Err(mut e) = util::check_region_epoch(req, self.region(), true) {
            if let Error::EpochNotMatch(_, new_regions) = &mut e {
                // TODO: query sibling regions.
                metrics.invalid_proposal.epoch_not_match.inc();
            }
            return Err(e);
        }
        Ok(())
    }

    #[inline]
    fn propose_command<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        let data = req.write_to_bytes().unwrap();
        self.propose(ctx, data)
    }

    #[inline]
    fn propose<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>, data: Vec<u8>) -> Result<u64> {
        ctx.raft_metrics.propose.normal.inc();
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);
        if data.len() as u64 > ctx.cfg.raft_entry_max_size.0 {
            return Err(Error::RaftEntryTooLarge {
                region_id: self.region_id(),
                entry_size: data.len() as u64,
            });
        }
        let last_index = self.raft_group().raft.raft_log.last_index();
        self.raft_group_mut().propose(vec![], data)?;
        if self.raft_group().raft.raft_log.last_index() == last_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id(), None));
        }
        Ok(last_index + 1)
    }

    #[inline]
    fn enqueue_pending_proposal<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        mut proposal: Proposal<Vec<CmdResChannel>>,
    ) {
        let applied_to_current_term = self.applied_to_current_term();
        if applied_to_current_term {
            proposal.cb.notify_proposed();
        }
        proposal.must_pass_epoch_check = applied_to_current_term;
        proposal.propose_time = Some(*ctx.current_time.get_or_insert_with(monotonic_raw_now));
        self.proposals_mut().push(proposal);
    }

    #[inline]
    pub fn schedule_apply_committed_entries<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        committed_entries: Vec<Entry>,
    ) {
        let last_entry = match committed_entries.last() {
            Some(e) => e,
            None => return,
        };
        let current_term = self.term();
        let mut entry_and_proposals = vec![];
        let queue = self.proposals_mut();
        if !queue.is_empty() {
            for e in committed_entries {
                let mut proposal = queue.find_proposal(e.term, e.index, current_term);
                if let Some(p) = &mut proposal && p.must_pass_epoch_check {
                    // In this case the apply can be guaranteed to be successful. Invoke the
                    // on_committed callback if necessary.
                    p.cb.notify_committed();
                }
                entry_and_proposals.push((e, proposal.map_or_else(Vec::new, |p| p.cb)));
            }
        } else {
            entry_and_proposals = committed_entries.into_iter().map(|e| (e, vec![])).collect();
        }
        // Unlike v1, v2 doesn't need to persist commit index and commit term. The
        // point of persist commit index/term of raft apply state is to recover commit
        // index when the writes to raft engine is lost but writes to kv engine is
        // persisted. But in v2, writes to raft engine must be persisted before
        // memtables in kv engine is flushed.
        let apply = CommittedEntries {
            entry_and_proposals,
        };
        self.apply_scheduler()
            .send(ApplyTask::CommittedEntries(apply));
    }

    fn on_ready_result<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        exec_results: &mut VecDeque<ExecResult>,
    ) {
        // handle executing commiited log results
        while let Some(result) = exec_results.pop_front() {
            match result {
                ExecResult::SplitRegion {
                    regions,
                    derived,
                    new_split_regions,
                } => self.on_ready_split_region(store_ctx, derived, regions, new_split_regions),
            }
        }
    }

    pub fn on_apply_res<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut apply_res: ApplyRes,
    ) {
        if !self.serving() {
            return;
        }
        // It must just applied a snapshot.
        if apply_res.applied_index < self.entry_storage().first_index() {
            // TODO: handle admin side effects like split/merge.
            return;
        }

        self.on_ready_result(store_ctx, &mut apply_res.exec_res);

        self.raft_group_mut()
            .advance_apply_to(apply_res.applied_index);
        let is_leader = self.is_leader();
        let entry_storage = self.entry_storage_mut();
        entry_storage
            .apply_state_mut()
            .set_applied_index(apply_res.applied_index);
        entry_storage.set_applied_term(apply_res.applied_term);
        if !is_leader {
            entry_storage.compact_entry_cache(apply_res.applied_index + 1);
            // TODO: handle read.
        } else {
            // TODO: handle read.
        }
    }

    fn on_admin_request<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
        ch: CmdResChannel,
    ) {
        let res = self.propose_command(ctx, req);
        self.post_propose_admin(ctx, res, vec![ch]);
    }

    fn post_propose_admin<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        res: Result<u64>,
        ch: Vec<CmdResChannel>,
    ) {
        let idx = match res {
            Ok(i) => i,
            Err(e) => {
                ch.report_error(cmd_resp::err_resp(e, self.term()));
                return;
            }
        };
        let p = Proposal::new(idx, self.term(), ch);
        self.enqueue_pending_proposal(ctx, p);
        self.set_has_ready();
    }
}

impl<EK: KvEngine, ER: RaftEngine, R: ApplyResReporter> Apply<EK, ER, R> {
    #[inline]
    pub async fn apply_committed_entries(&mut self, ce: CommittedEntries) {
        fail::fail_point!("APPLY_COMMITTED_ENTRIES");
        for (e, ch) in ce.entry_and_proposals {
            if !e.get_data().is_empty() {
                let mut set_save_point = false;
                if let Some(wb) = self.write_batch_mut() {
                    wb.set_save_point();
                    set_save_point = true;
                }
                let resp = match self.apply_entry(&e).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        if let Some(wb) = self.write_batch_mut() {
                            if set_save_point {
                                wb.rollback_to_save_point().unwrap();
                            } else {
                                wb.clear();
                            }
                        }
                        cmd_resp::new_error(e)
                    }
                };
                self.callbacks_mut().push((ch, resp));
            } else {
                assert!(ch.is_empty());
            }
            // Flush may be triggerred in the middle, so always update the index and term.
            self.set_apply_progress(e.index, e.term);
        }
    }

    #[inline]
    async fn apply_entry(&mut self, entry: &Entry) -> Result<RaftCmdResponse> {
        match SimpleWriteDecoder::new(entry.get_data()) {
            Ok(decoder) => {
                util::compare_region_epoch(
                    decoder.header().get_region_epoch(),
                    self.region_state().get_region(),
                    false,
                    true,
                    true,
                )?;
                let res = Ok(new_response(decoder.header()));
                for req in decoder {
                    match req {
                        SimpleWrite::Put(put) => self.apply_put(put.cf, put.key, put.value)?,
                        SimpleWrite::Delete(delete) => self.apply_delete(delete.cf, delete.key)?,
                        SimpleWrite::DeleteRange(dr) => self.apply_delete_range(
                            dr.cf,
                            dr.start_key,
                            dr.end_key,
                            dr.notify_only,
                        )?,
                    }
                }
                res
            }
            Err(req) => {
                util::check_region_epoch(&req, self.region_state().get_region(), true)?;
                if req.has_admin_request() {
                    // TODO: implement admin request.
                    let (resp, exec_res) = self.exec_admin_cmd(&req, entry)?;
                    self.push_exec_result(exec_res);
                    Ok(resp)
                } else {
                    for r in req.get_requests() {
                        match r.get_cmd_type() {
                            // These three writes should all use the new codec. Keep them here for
                            // backward compatibility.
                            CmdType::Put => {
                                let put = r.get_put();
                                self.apply_put(put.get_cf(), put.get_key(), put.get_value())?;
                            }
                            CmdType::Delete => {
                                let delete = r.get_delete();
                                self.apply_delete(delete.get_cf(), delete.get_key())?;
                            }
                            CmdType::DeleteRange => {
                                let dr = r.get_delete_range();
                                self.apply_delete_range(
                                    dr.get_cf(),
                                    dr.get_start_key(),
                                    dr.get_end_key(),
                                    dr.get_notify_only(),
                                )?;
                            }
                            _ => unimplemented!(),
                        }
                    }
                    Ok(new_response(req.get_header()))
                }
            }
        }
    }

    fn exec_admin_cmd(
        &mut self,
        req: &RaftCmdRequest,
        entry: &Entry,
    ) -> Result<(RaftCmdResponse, ExecResult)> {
        let request = req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        if cmd_type != AdminCmdType::CompactLog && cmd_type != AdminCmdType::CommitMerge {
            // info!(
            //     self.logger,
            //     "execute admin command";
            //     "region_id" => self.region_state().get_region().id,
            //     "peer_id" => self.id(),
            //     "term" => ctx.exec_log_term,
            //     "index" => ctx.exec_log_index,
            //     "command" => ?request,
            // );
        }

        let (mut response, exec_result) = match cmd_type {
            AdminCmdType::Split => unimplemented!(),
            AdminCmdType::BatchSplit => self.exec_batch_split(request, entry.index),
            _ => unimplemented!(),
        }?;

        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::default();
        if !req.get_header().get_uuid().is_empty() {
            let uuid = req.get_header().get_uuid().to_vec();
            resp.mut_header().set_uuid(uuid);
        }
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    #[inline]
    pub fn flush(&mut self) {
        if let Some(wb) = self.write_batch_mut() && !wb.is_empty() {
            let mut write_opt = WriteOptions::default();
            write_opt.set_disable_wal(true);
            if let Err(e) = wb.write_opt(&write_opt) {
                panic!("failed to write data: {:?}", self.logger.list());
            }
            if wb.data_size() <= APPLY_WB_SHRINK_SIZE {
                wb.clear();
            } else {
                self.write_batch_mut().take();
            }
        }
        let callbacks = self.callbacks_mut();
        for (ch, resp) in callbacks.drain(..) {
            ch.set_result(resp);
        }
        if callbacks.capacity() > SHRINK_PENDING_CMD_QUEUE_CAP {
            callbacks.shrink_to(SHRINK_PENDING_CMD_QUEUE_CAP);
        }
        let mut apply_res = ApplyRes::default();
        let (index, term) = self.apply_progress();
        apply_res.applied_index = index;
        apply_res.applied_term = term;
        if self.reset_state_changed() {
            apply_res.region_state = Some(self.region_state().clone());
        }
        apply_res.exec_res = self.take_exec_result();
        self.res_reporter().report(apply_res);
    }
}
