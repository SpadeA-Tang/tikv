// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use engine_traits::{KvEngine, RaftEngine};
use futures::{executor::block_on, prelude::*};
use keys::Prefix;
use kvproto::{
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RaftMessage,
};
use raftstore::{
    router::handle_send_error,
    store::{RegionSnapshot, Transport},
    Result, Result as RaftStoreResult,
};
use raftstore_v2::router::{CmdResSubscriber, PeerMsg, QueryResSubscriber, RaftRouter};
use test_raftstore::{filter_send, Filter};
use tikv_util::HandyRwLock;

#[derive(Clone)]
pub struct SimulateTransport<C> {
    filters: Arc<RwLock<Vec<Box<dyn Filter>>>>,
    ch: C,
}

impl<C> SimulateTransport<C> {
    pub fn new(ch: C) -> SimulateTransport<C> {
        Self {
            filters: Arc::new(RwLock::new(vec![])),
            ch,
        }
    }

    pub fn clear_filters(&mut self) {
        self.filters.wl().clear();
    }

    pub fn add_filter(&mut self, filter: Box<dyn Filter>) {
        self.filters.wl().push(filter);
    }
}

impl<C: Transport> Transport for SimulateTransport<C> {
    fn send(&mut self, m: RaftMessage) -> Result<()> {
        let ch = &mut self.ch;
        filter_send(&self.filters, m, |m| ch.send(m))
    }

    fn set_store_allowlist(&mut self, allowlist: Vec<u64>) {
        self.ch.set_store_allowlist(allowlist);
    }

    fn need_flush(&self) -> bool {
        self.ch.need_flush()
    }

    fn flush(&mut self) {
        self.ch.flush();
    }
}

pub trait SnapshotRouter<E: KvEngine> {
    fn snapshot(
        &mut self,
        req: RaftCmdRequest,
        timeout: Duration,
    ) -> std::result::Result<RegionSnapshot<E::Snapshot, Prefix>, RaftCmdResponse>;
}

impl<EK: KvEngine, ER: RaftEngine> SnapshotRouter<EK> for RaftRouter<EK, ER> {
    fn snapshot(
        &mut self,
        req: RaftCmdRequest,
        timeout: Duration,
    ) -> std::result::Result<RegionSnapshot<EK::Snapshot, Prefix>, RaftCmdResponse> {
        block_on(self.snapshot(req))
        // let timeout_f = GLOBAL_TIMER_HANDLE.delay(Instant::now() + timeout);
        // futures::executor::block_on(async move {
        //     futures::select! {
        //         res = self.snapshot(req).fuse() => res,
        //         e = timeout_f.compat().fuse() => {
        //             Err(cmd_resp::new_error(Error::Timeout(format!("request
        // timeout for {:?}: {:?}", timeout,e))))         },
        //     }
        // })
    }
}

impl<E: KvEngine, C: SnapshotRouter<E>> SnapshotRouter<E> for SimulateTransport<C> {
    fn snapshot(
        &mut self,
        req: RaftCmdRequest,
        timeout: Duration,
    ) -> std::result::Result<RegionSnapshot<E::Snapshot, Prefix>, RaftCmdResponse> {
        self.ch.snapshot(req, timeout)
    }
}

pub trait RaftStoreRouter {
    /// Sends RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest) -> RaftStoreResult<CmdResSubscriber>;

    fn send_query(&self, req: RaftCmdRequest) -> RaftStoreResult<QueryResSubscriber>;

    fn send_peer_msg(&self, region_id: u64, msg: PeerMsg) -> Result<()>;

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()>;
}

impl<EK: KvEngine, ER: RaftEngine> RaftStoreRouter for RaftRouter<EK, ER> {
    fn send_command(&self, req: RaftCmdRequest) -> RaftStoreResult<CmdResSubscriber> {
        let region_id = req.get_header().get_region_id();
        let (msg, sub) = PeerMsg::raft_command(req);
        self.send_peer_msg(region_id, msg)?;
        Ok(sub)
    }

    fn send_query(&self, req: RaftCmdRequest) -> RaftStoreResult<QueryResSubscriber> {
        let region_id = req.get_header().get_region_id();
        let (msg, sub) = PeerMsg::raft_query(req);
        self.send_peer_msg(region_id, msg)?;
        Ok(sub)
    }

    fn send_peer_msg(&self, region_id: u64, msg: PeerMsg) -> RaftStoreResult<()> {
        self.send(region_id, msg)
            .map_err(|e| handle_send_error(region_id, &e))
    }

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let region_id = msg.get_region_id();
        self.send_raft_message(Box::new(msg))
            .map_err(|e| handle_send_error(region_id, &e))
    }
}

impl<C: RaftStoreRouter> RaftStoreRouter for SimulateTransport<C> {
    fn send_command(&self, req: RaftCmdRequest) -> RaftStoreResult<CmdResSubscriber> {
        self.ch.send_command(req)
    }

    fn send_query(&self, req: RaftCmdRequest) -> RaftStoreResult<QueryResSubscriber> {
        self.ch.send_query(req)
    }

    fn send_peer_msg(&self, region_id: u64, msg: PeerMsg) -> RaftStoreResult<()> {
        self.ch.send_peer_msg(region_id, msg)
    }

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        filter_send(&self.filters, msg, |m| self.ch.send_raft_msg(m))
    }
}
