// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cell::Cell,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    marker::PhantomData,
    ops::Deref,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use batch_system::Router;
use crossbeam::{atomic::AtomicCell, channel::TrySendError};
use engine_traits::{KvEngine, RaftEngine, Snapshot, TabletFactory};
use fail::fail_point;
use kvproto::{
    errorpb, metapb,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, ReadIndexResponse, Request, Response},
};
use pd_client::BucketMeta;
use raftstore::{
    store::{
        cmd_resp,
        util::{self, LeaseState, RegionReadProgress, RemoteLease},
        LocalReaderCore, ProposalRouter, ReadDelegate, ReadExecutor, ReadExecutorProvider,
        ReadProgress, RegionSnapshot, RequestInspector, RequestPolicy, TrackVer, TxnExt,
        TLS_LOCAL_READ_METRICS,
    },
    Error, Result,
};
use slog::{debug, error, info, o, warn, Logger};
use tikv_util::{
    box_err,
    codec::number::decode_u64,
    lru::LruCache,
    time::{monotonic_raw_now, Instant, ThreadReadId},
};
use time::Timespec;
use txn_types::WriteBatchFlags;

use crate::{fsm::StoreMeta, router::PeerMsg, tablet::CachedTablet, StoreRouter};

pub trait MsgRouter: Send {
    fn send(&self, addr: u64, msg: PeerMsg) -> std::result::Result<(), TrySendError<PeerMsg>>;
}

impl<EK, ER> MsgRouter for StoreRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn send(&self, addr: u64, msg: PeerMsg) -> std::result::Result<(), TrySendError<PeerMsg>> {
        Router::send(self, addr, msg)
    }
}

#[derive(Clone)]
pub struct LocalReader<C, D, S>
where
    C: MsgRouter,
    D: ReadExecutor + Deref<Target = ReadDelegate>,
    S: ReadExecutorProvider<Executor = D>,
{
    local_reader: LocalReaderCore<D, S>,
    router: C,

    logger: Logger,
}

impl<E, C, D, S> LocalReader<C, D, S>
where
    E: KvEngine,
    C: MsgRouter,
    D: ReadExecutor<Tablet = E> + Deref<Target = ReadDelegate> + Clone,
    S: ReadExecutorProvider<Executor = D> + Clone,
{
    pub fn new(store_meta: S, router: C, logger: Logger) -> Self {
        let cache_read_id = ThreadReadId::new();
        Self {
            local_reader: LocalReaderCore::new(store_meta),
            router,
            logger,
        }
    }

    pub fn pre_propose_raft_command(
        &mut self,
        req: &RaftCmdRequest,
    ) -> Result<Option<(D, RequestPolicy)>> {
        if let Some(delegate) = self.local_reader.validate_request(req)? {
            let mut inspector = SnapRequestInspector {
                delegate: &delegate,
                logger: &self.logger,
            };
            match inspector.inspect(req) {
                Ok(RequestPolicy::ReadLocal) => Ok(Some((delegate, RequestPolicy::ReadLocal))),
                Ok(RequestPolicy::StaleRead) => Ok(Some((delegate, RequestPolicy::StaleRead))),
                // It can not handle other policies.
                Ok(_) => Ok(None),
                Err(e) => Err(e),
            }
        } else {
            Err(Error::RegionNotFound(req.get_header().get_region_id()))
        }
    }

    fn try_get_snapshot(
        &mut self,
        req: RaftCmdRequest,
    ) -> std::result::Result<Option<RegionSnapshot<E::Snapshot>>, RaftCmdResponse> {
        match self.pre_propose_raft_command(&req) {
            Ok(Some((mut delegate, policy))) => match policy {
                RequestPolicy::ReadLocal => {
                    let snapshot_ts = monotonic_raw_now();
                    if !delegate.is_in_leader_lease(snapshot_ts) {
                        return Ok(None);
                    }

                    let region = Arc::clone(&delegate.region);
                    let snap = RegionSnapshot::from_snapshot(
                        delegate.get_snapshot(None, &mut None),
                        region,
                    );

                    // Try renew lease in advance
                    self.maybe_renew_lease_in_advance(&delegate, &req, snapshot_ts);
                    Ok(Some(snap))
                }
                RequestPolicy::StaleRead => {
                    let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
                    delegate.check_stale_read_safe::<E>(read_ts)?;

                    let region = Arc::clone(&delegate.region);
                    let snap = RegionSnapshot::from_snapshot(
                        delegate.get_snapshot(None, &mut None),
                        region,
                    );

                    delegate.check_stale_read_safe::<E>(read_ts)?;

                    TLS_LOCAL_READ_METRICS
                        .with(|m| m.borrow_mut().local_executed_stale_read_requests.inc());
                    Ok(Some(snap))
                }
                _ => unreachable!(),
            },
            Ok(None) => Ok(None),
            Err(e) => {
                let mut response = cmd_resp::new_error(e);
                if let Some(delegate) = self
                    .local_reader
                    .delegates
                    .get(&req.get_header().get_region_id())
                {
                    cmd_resp::bind_term(&mut response, delegate.term);
                }
                Err(response)
            }
        }
    }

    pub async fn snapshot(
        &mut self,
        mut req: RaftCmdRequest,
    ) -> std::result::Result<RegionSnapshot<E::Snapshot>, RaftCmdResponse> {
        let region_id = req.header.get_ref().region_id;
        if let Some(snap) = self.try_get_snapshot(req.clone())? {
            return Ok(snap);
        }

        // try to renew the lease by sending read query where the reading process may
        // renew the lease
        let (msg, mut sub) = PeerMsg::raft_query(req.clone());
        MsgRouter::send(&self.router, region_id, msg);
        if let Some(query_res) = sub.result().await {
            // If query successful, try again.
            if query_res.read().is_some() {
                req.mut_header().set_read_quorum(false);
                if let Some(snap) = self.try_get_snapshot(req)? {
                    return Ok(snap);
                }
            }
        }

        Err(not_leader_err(region_id))
    }

    // If the remote lease will be expired in near future send message
    // to `raftstore` to renew it
    fn maybe_renew_lease_in_advance(
        &self,
        delegate: &ReadDelegate,
        req: &RaftCmdRequest,
        ts: Timespec,
    ) {
        if !delegate.need_renew_lease(ts) {
            return;
        }

        let region_id = req.header.get_ref().region_id;
        TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().renew_lease_advance.inc());
        // Send a read query which may renew the lease
        let (msg, mut sub) = PeerMsg::raft_query(req.clone());
        MsgRouter::send(&self.router, region_id, msg);
    }
}

fn not_leader_err(region_id: u64) -> RaftCmdResponse {
    let mut err = errorpb::Error::default();
    err.set_not_leader(Default::default());
    let mut resp = RaftCmdResponse::default();
    resp.mut_header().set_error(err);
    resp
}

fn delegate_not_found_err(region_id: u64) -> RaftCmdResponse {
    let mut err = errorpb::Error::default();
    err.set_region_not_found(Default::default());
    let mut resp = RaftCmdResponse::default();
    resp.mut_header().set_error(err);
    resp
}

/// CachedReadDelegate is a wrapper the ReadDelegate and CachedTablet.
/// CachedTablet can fetch the latest tablet of this ReadDelegate's region. The
/// main purpose of this wrapping is to implement ReadExecutor where the latest
/// tablet is needed.
pub struct CachedReadDelegate<E>
where
    E: KvEngine,
{
    // The reason for this to be Arc, see the comment on get_delegate in
    // raftstore/src/store/worker/read.rs
    delegate: Arc<ReadDelegate>,
    cached_tablet: CachedTablet<E>,
}

impl<E> Deref for CachedReadDelegate<E>
where
    E: KvEngine,
{
    type Target = ReadDelegate;

    fn deref(&self) -> &Self::Target {
        self.delegate.as_ref()
    }
}

impl<E> Clone for CachedReadDelegate<E>
where
    E: KvEngine,
{
    fn clone(&self) -> Self {
        CachedReadDelegate {
            delegate: Arc::clone(&self.delegate),
            cached_tablet: self.cached_tablet.clone(),
        }
    }
}

impl<E> ReadExecutor for CachedReadDelegate<E>
where
    E: KvEngine,
{
    type Tablet = E;

    fn get_tablet(&mut self) -> &E {
        self.cached_tablet.latest().unwrap()
    }

    fn get_snapshot(
        &mut self,
        _: Option<ThreadReadId>,
        _: &mut Option<raftstore::store::LocalReadContext<'_, E>>,
    ) -> Arc<E::Snapshot> {
        Arc::new(self.cached_tablet.latest().unwrap().snapshot())
    }
}

#[derive(Clone)]
struct StoreMetaDelegate<E>
where
    E: KvEngine,
{
    store_meta: Arc<Mutex<StoreMeta<E>>>,
}

impl<E> StoreMetaDelegate<E>
where
    E: KvEngine,
{
    pub fn new(store_meta: Arc<Mutex<StoreMeta<E>>>) -> StoreMetaDelegate<E> {
        StoreMetaDelegate { store_meta }
    }
}

impl<E> ReadExecutorProvider for StoreMetaDelegate<E>
where
    E: KvEngine,
{
    type Executor = CachedReadDelegate<E>;

    fn store_id(&self) -> Option<u64> {
        self.store_meta.as_ref().lock().unwrap().store_id
    }

    /// get the ReadDelegate with region_id and the number of delegates in the
    /// StoreMeta
    fn get_executor_and_len(&self, region_id: u64) -> (usize, Option<Self::Executor>) {
        let meta = self.store_meta.as_ref().lock().unwrap();
        let reader = meta.readers.get(&region_id).cloned();
        if let Some(reader) = reader {
            // If reader is not None, cache must not be None.
            let cached_tablet = meta.tablet_caches.get(&region_id).cloned().unwrap();
            return (
                meta.readers.len(),
                Some(CachedReadDelegate {
                    delegate: Arc::new(reader),
                    cached_tablet,
                }),
            );
        }
        (meta.readers.len(), None)
    }
}

struct SnapRequestInspector<'r> {
    delegate: &'r ReadDelegate,
    logger: &'r Logger,
}

impl<'r> RequestInspector for SnapRequestInspector<'r> {
    fn inspect(&mut self, req: &RaftCmdRequest) -> Result<RequestPolicy> {
        assert!(!req.has_admin_request());
        if req.get_requests().len() != 1
            || req.get_requests().first().unwrap().get_cmd_type() != CmdType::Snap
        {
            return Err(box_err!(
                "LocalReader can only serve for exactly one Snap request"
            ));
        }

        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            return Ok(RequestPolicy::StaleRead);
        }

        if req.get_header().get_read_quorum() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if !self.has_applied_to_current_term() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // Local read should be performed, if and only if leader is in lease.
        // None for now.
        match self.inspect_lease() {
            LeaseState::Valid => Ok(RequestPolicy::ReadLocal),
            LeaseState::Expired | LeaseState::Suspect => {
                // Perform a consistent read to Raft quorum and try to renew the leader lease.
                Ok(RequestPolicy::ReadIndex)
            }
        }
    }

    fn has_applied_to_current_term(&mut self) -> bool {
        if self.delegate.applied_term == self.delegate.term {
            true
        } else {
            debug!(
                self.logger,
                "rejected by term check";
                "tag" => &self.delegate.tag,
                "applied_term" => self.delegate.applied_term,
                "delegate_term" => ?self.delegate.term,
            );

            // only for metric.
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.applied_term.inc());
            false
        }
    }

    fn inspect_lease(&mut self) -> LeaseState {
        // TODO: disable localreader if we did not enable raft's check_quorum.
        if self.delegate.leader_lease.is_some() {
            // We skip lease check, because it is postponed until `handle_read`.
            LeaseState::Valid
        } else {
            debug!(self.logger, "rejected by leader lease"; "tag" => &self.delegate.tag);
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.no_lease.inc());
            LeaseState::Expired
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::Borrow,
        sync::mpsc::*,
        thread::{self, JoinHandle},
    };

    use crossbeam::channel::TrySendError;
    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{KvTestEngine, KvTestSnapshot, TestTabletFactoryV2},
    };
    use engine_traits::{OpenOptions, Peekable, SyncMutable, ALL_CFS, CF_DEFAULT};
    use futures::executor::block_on;
    use kvproto::{kvrpcpb::ExtraOp as TxnExtraOp, metapb::Region, raft_cmdpb::*};
    use raftstore::store::{
        util::{new_peer, Lease},
        Callback, LocalReaderCore, ProposalRouter, RaftCommand, ReadCallback,
        TLS_LOCAL_READ_METRICS,
    };
    use tempfile::{Builder, TempDir};
    use tikv_util::{codec::number::NumberEncoder, time::monotonic_raw_now};
    use time::Duration;
    use txn_types::{Key, Lock, LockType, WriteBatchFlags};

    use super::*;
    use crate::router::{QueryResult, ReadResponse};

    fn new_read_delegate(
        region: &Region,
        peer_id: u64,
        term: u64,
        applied_index_term: u64,
    ) -> ReadDelegate {
        let mut read_delegate_core = ReadDelegate::mock(region.id);
        read_delegate_core.peer_id = peer_id;
        read_delegate_core.term = term;
        read_delegate_core.applied_term = applied_index_term;
        read_delegate_core.region = Arc::new(region.clone());
        read_delegate_core
    }

    struct MockRouter {
        p_router: SyncSender<(u64, PeerMsg)>,
    }

    impl MockRouter {
        fn new() -> (MockRouter, Receiver<(u64, PeerMsg)>) {
            let (p_ch, p_rx) = sync_channel(1);
            (MockRouter { p_router: p_ch }, p_rx)
        }
    }

    impl MsgRouter for MockRouter {
        fn send(&self, addr: u64, cmd: PeerMsg) -> std::result::Result<(), TrySendError<PeerMsg>> {
            self.p_router.send((addr, cmd)).unwrap();
            Ok(())
        }
    }

    #[allow(clippy::type_complexity)]
    fn new_reader(
        store_id: u64,
        store_meta: Arc<Mutex<StoreMeta<KvTestEngine>>>,
    ) -> (
        LocalReader<MockRouter, CachedReadDelegate<KvTestEngine>, StoreMetaDelegate<KvTestEngine>>,
        Receiver<(u64, PeerMsg)>,
    ) {
        let (ch, rx) = MockRouter::new();
        let mut reader = LocalReader::new(
            StoreMetaDelegate::new(store_meta),
            ch,
            Logger::root(slog::Discard, o!("key1" => "value1")),
        );
        reader.local_reader.store_id = Cell::new(Some(store_id));
        (reader, rx)
    }

    fn new_peers(store_id: u64, pr_ids: Vec<u64>) -> Vec<metapb::Peer> {
        pr_ids
            .into_iter()
            .map(|id| {
                let mut pr = metapb::Peer::default();
                pr.set_store_id(store_id);
                pr.set_id(id);
                pr
            })
            .collect()
    }

    #[test]
    fn test_read() {
        fn handle_msg<F: FnOnce() + Send + 'static>(
            f: F,
            rx: Receiver<(u64, PeerMsg)>,
            ch_tx: SyncSender<Receiver<(u64, PeerMsg)>>,
        ) -> JoinHandle<()> {
            thread::spawn(move || {
                // Msg for query will be sent
                let (region_id, msg) = rx.recv().unwrap();

                f();
                match msg {
                    PeerMsg::RaftQuery(query) => ReadCallback::set_result(
                        query.ch,
                        QueryResult::Read(ReadResponse {
                            read_index: 0,
                            txn_extra_op: Default::default(),
                        }),
                    ),
                    _ => unreachable!(),
                }
                ch_tx.send(rx);
            })
        }

        let store_id = 1;

        // Building a tablet factory
        let ops = DbOptions::default();
        let cf_opts = ALL_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
        let path = Builder::new()
            .prefix("test-local-reader")
            .tempdir()
            .unwrap();
        let factory = Arc::new(TestTabletFactoryV2::new(path.path(), ops, cf_opts));

        let store_meta = Arc::new(Mutex::new(StoreMeta::new()));
        let (mut reader, mut rx) = new_reader(store_id, store_meta.clone());

        let mut region1 = metapb::Region::default();
        region1.set_id(1);
        let prs = new_peers(store_id, vec![1, 2, 3]);
        region1.set_peers(prs.clone().into());
        let epoch13 = {
            let mut ep = metapb::RegionEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let leader2 = prs[0].clone();
        region1.set_region_epoch(epoch13.clone());
        let term6 = 6;
        let mut lease = Lease::new(Duration::seconds(1), Duration::milliseconds(250));
        let read_progress = Arc::new(RegionReadProgress::new(&region1, 1, 1, "".to_owned()));

        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader2);
        header.set_region_epoch(epoch13);
        header.set_term(term6);
        cmd.set_header(header);
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        // The region is not register yet.
        let res = block_on(reader.snapshot(cmd.clone())).unwrap_err();
        assert!(
            res.header
                .as_ref()
                .unwrap()
                .get_error()
                .has_region_not_found()
        );
        // No msg will ben sent
        rx.try_recv().unwrap_err();
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.no_region.get()),
            1
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            1
        );
        assert!(reader.local_reader.delegates.get(&1).is_none());

        // Register region 1
        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        {
            let mut meta = store_meta.as_ref().lock().unwrap();

            // Create read_delegate with region id 1
            let read_delegate = ReadDelegate {
                tag: String::new(),
                region: Arc::new(region1.clone()),
                peer_id: 1,
                term: term6,
                applied_term: term6 - 1,
                leader_lease: Some(remote),
                last_valid_ts: Timespec::new(0, 0),
                txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
                txn_ext: Arc::new(TxnExt::default()),
                read_progress: read_progress.clone(),
                pending_remove: false,
                track_ver: TrackVer::new(),
                bucket_meta: None,
            };
            meta.readers.insert(1, read_delegate);
            // create tablet with region_id 1 and prepare some data
            let tablet1 = factory
                .open_tablet(1, Some(10), OpenOptions::default().set_create_new(true))
                .unwrap();
            let cache = CachedTablet::new(Some(tablet1));
            meta.tablet_caches.insert(1, cache);
        }

        let (ch_tx, ch_rx) = sync_channel(1);

        let handler = handle_msg(
            move || {
                let mut meta = store_meta.lock().unwrap();

                meta.readers
                    .get_mut(&1)
                    .unwrap()
                    .update(ReadProgress::applied_term(term6));
            },
            rx,
            ch_tx.clone(),
        );

        // The first try will be rejected due to unmatched applied term but after update
        // the applied term by the above thread, the snapshot will be acquired by
        // retrying.
        let snap = block_on(reader.snapshot(cmd.clone())).unwrap();
        assert_eq!(*snap.get_region(), region1);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            3
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.applied_term.get()),
            1
        );
        handler.join();
        rx = ch_rx.recv().unwrap();

        // Read quorum.
        let mut cmd_read_quorum = cmd.clone();
        cmd_read_quorum.mut_header().set_read_quorum(true);
        let handler = handle_msg(|| {}, rx, ch_tx);
        let snap = block_on(reader.snapshot(cmd_read_quorum.clone())).unwrap();
        handler.join();
        rx = ch_rx.recv().unwrap();

        // Stale read
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.safe_ts.get()),
            0
        );
        read_progress.update_safe_ts(1, 1);
        assert_eq!(read_progress.safe_ts(), 1);
        let data = {
            let mut d = [0u8; 8];
            (&mut d[..]).encode_u64(2).unwrap();
            d
        };
        cmd.mut_header()
            .set_flags(WriteBatchFlags::STALE_READ.bits());
        cmd.mut_header().set_flag_data(data.into());
        let res = block_on(reader.snapshot(cmd.clone())).unwrap_err();
        assert!(res.get_header().get_error().has_data_is_not_ready());
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.safe_ts.get()),
            1
        );
        read_progress.update_safe_ts(1, 2);
        assert_eq!(read_progress.safe_ts(), 2);
        let snap = block_on(reader.snapshot(cmd.clone())).unwrap();
        assert_eq!(*snap.get_region(), region1);
    }

    #[test]
    fn test_read_delegate() {
        // Building a tablet factory
        let ops = DbOptions::default();
        let cf_opts = ALL_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
        let path = Builder::new()
            .prefix("test-local-reader")
            .tempdir()
            .unwrap();
        let factory = Arc::new(TestTabletFactoryV2::new(path.path(), ops, cf_opts));

        let store_meta =
            StoreMetaDelegate::new(Arc::new(Mutex::new(StoreMeta::<KvTestEngine>::new())));

        let tablet1;
        let tablet2;
        {
            let mut meta = store_meta.store_meta.as_ref().lock().unwrap();

            // Create read_delegate with region id 1
            let mut read_delegate = ReadDelegate::mock(1);
            meta.readers.insert(1, read_delegate);

            // create tablet with region_id 1 and prepare some data
            tablet1 = factory
                .open_tablet(1, Some(10), OpenOptions::default().set_create_new(true))
                .unwrap();
            tablet1.put(b"a1", b"val1").unwrap();
            let cache = CachedTablet::new(Some(tablet1.clone()));
            meta.tablet_caches.insert(1, cache);

            // Create read_delegate with region id 1
            let mut read_delegate = ReadDelegate::mock(2);
            let cache = CachedTablet::new(Some(read_delegate.clone()));
            meta.readers.insert(2, read_delegate);

            // create tablet with region_id 1 and prepare some data
            tablet2 = factory
                .open_tablet(2, Some(10), OpenOptions::default().set_create_new(true))
                .unwrap();
            tablet2.put(b"a2", b"val2").unwrap();
            let cache = CachedTablet::new(Some(tablet2.clone()));
            meta.tablet_caches.insert(2, cache);
        }

        let (_, delegate) = store_meta.get_executor_and_len(1);
        let mut delegate = delegate.unwrap();
        let tablet = delegate.get_tablet();
        assert_eq!(tablet1.as_inner().path(), tablet.as_inner().path());
        let snapshot = delegate.get_snapshot(None, &mut None);
        assert_eq!(
            b"val1".to_vec(),
            *snapshot.get_value(b"a1").unwrap().unwrap()
        );

        let (_, delegate) = store_meta.get_executor_and_len(2);
        let mut delegate = delegate.unwrap();
        let tablet = delegate.get_tablet();
        assert_eq!(tablet2.as_inner().path(), tablet.as_inner().path());
        let snapshot = delegate.get_snapshot(None, &mut None);
        assert_eq!(
            b"val2".to_vec(),
            *snapshot.get_value(b"a2").unwrap().unwrap()
        );
    }
}
