// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cell::Cell,
    fmt::{self, Display, Formatter},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use crossbeam::{atomic::AtomicCell, channel::TrySendError};
use engine_traits::{KvEngine, RaftEngine, Snapshot};
use fail::fail_point;
use kvproto::{
    errorpb,
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, ReadIndexResponse, Request, Response},
};
use pd_client::BucketMeta;
use tikv_util::{
    codec::number::decode_u64,
    debug, error,
    lru::LruCache,
    time::{monotonic_raw_now, Instant, ThreadReadId},
};
use time::Timespec;

use super::metrics::*;
use crate::{
    errors::RAFTSTORE_IS_BUSY,
    store::{
        cmd_resp,
        fsm::store::StoreMeta,
        util::{self, LeaseState, RegionReadProgress, RemoteLease},
        Callback, CasualMessage, CasualRouter, Peer, ProposalRouter, RaftCommand, ReadResponse,
        RegionSnapshot, RequestInspector, RequestPolicy, TxnExt,
    },
    Error, Result,
};

pub trait ReadExecutor<E: KvEngine> {
    fn get_tablet(&mut self) -> &E;
    fn get_snapshot(&mut self, ts: Option<ThreadReadId>) -> Arc<E::Snapshot>;

    fn get_value(&mut self, req: &Request, region: &metapb::Region) -> Result<Response> {
        let key = req.get_get().get_key();
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, region)?;

        let engine = self.get_tablet();
        let mut resp = Response::default();
        let res = if !req.get_get().get_cf().is_empty() {
            let cf = req.get_get().get_cf();
            engine
                .get_value_cf(cf, &keys::data_key(key))
                .unwrap_or_else(|e| {
                    panic!(
                        "[region {}] failed to get {} with cf {}: {:?}",
                        region.get_id(),
                        log_wrappers::Value::key(key),
                        cf,
                        e
                    )
                })
        } else {
            engine.get_value(&keys::data_key(key)).unwrap_or_else(|e| {
                panic!(
                    "[region {}] failed to get {}: {:?}",
                    region.get_id(),
                    log_wrappers::Value::key(key),
                    e
                )
            })
        };
        if let Some(res) = res {
            resp.mut_get().set_value(res.to_vec());
        }

        Ok(resp)
    }

    fn execute(
        &mut self,
        msg: &RaftCmdRequest,
        region: &Arc<metapb::Region>,
        read_index: Option<u64>,
        mut ts: Option<ThreadReadId>,
    ) -> ReadResponse<E::Snapshot> {
        let requests = msg.get_requests();
        let mut response = ReadResponse {
            response: RaftCmdResponse::default(),
            snapshot: None,
            txn_extra_op: TxnExtraOp::Noop,
        };
        let mut responses = Vec::with_capacity(requests.len());
        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Get => match self.get_value(req, region.as_ref()) {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!(?e;
                            "failed to execute get command";
                            "region_id" => region.get_id(),
                        );
                        response.response = cmd_resp::new_error(e);
                        return response;
                    }
                },
                CmdType::Snap => {
                    let snapshot =
                        RegionSnapshot::from_snapshot(self.get_snapshot(ts.take()), region.clone());
                    response.snapshot = Some(snapshot);
                    Response::default()
                }
                CmdType::ReadIndex => {
                    let mut resp = Response::default();
                    if let Some(read_index) = read_index {
                        let mut res = ReadIndexResponse::default();
                        res.set_read_index(read_index);
                        resp.set_read_index(res);
                    } else {
                        panic!("[region {}] can not get readindex", region.get_id());
                    }
                    resp
                }
                CmdType::Prewrite
                | CmdType::Put
                | CmdType::Delete
                | CmdType::DeleteRange
                | CmdType::IngestSst
                | CmdType::Invalid => unreachable!(),
            };
            resp.set_cmd_type(cmd_type);
            responses.push(resp);
        }
        response.response.set_responses(responses.into());
        response
    }
}

/// A read only delegate of `Peer`.
#[derive(Clone, Debug)]
pub struct ReadDelegateCore {
    pub region: Arc<metapb::Region>,
    pub peer_id: u64,
    pub term: u64,
    pub applied_index_term: u64,
    pub leader_lease: Option<RemoteLease>,
    pub last_valid_ts: Timespec,

    pub tag: String,
    pub bucket_meta: Option<Arc<BucketMeta>>,
    pub txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    pub txn_ext: Arc<TxnExt>,
    pub read_progress: Arc<RegionReadProgress>,
    pub pending_remove: bool,

    // `track_ver` used to keep the local `ReadDelegate` in `LocalReader`
    // up-to-date with the global `ReadDelegate` stored at `StoreMeta`
    pub track_ver: TrackVer,
}

pub trait ReadDelegateTrait: Send + Sync + Clone + 'static {
    fn delegate(&self) -> &ReadDelegateCore;
}

pub struct ReadDelegate {
    delegate: Arc<ReadDelegateCore>,
}

impl ReadDelegateTrait for ReadDelegate {
    fn delegate(&self) -> &ReadDelegateCore {
        self.delegate.as_ref()
    }
}

impl Clone for ReadDelegate {
    fn clone(&self) -> Self {
        ReadDelegate {
            delegate: Arc::clone(&self.delegate),
        }
    }
}

pub trait ReadDelegateExtTrait<'a, E>: ReadExecutor<E>
where
    E: KvEngine,
{
    type Delegate: ReadDelegateTrait;

    fn from_read_delegate(
        delegate: Self::Delegate,
        kv_engine: &'a E,
        metrics: &'a mut ReadMetrics,
        read_id: &'a mut ThreadReadId,
        snap_cache: &'a mut Option<Arc<E::Snapshot>>,
    ) -> Self;

    fn delegate(&self) -> &ReadDelegateCore;

    fn take_delegate(&mut self) -> Self::Delegate;

    fn metrics(&mut self) -> &mut ReadMetrics;
}

pub struct ReadDelegateExt<'a, E>
where
    E: KvEngine,
{
    delegate: Option<ReadDelegate>,
    kv_engine: &'a E,
    metrics: &'a mut ReadMetrics,
    read_id: &'a mut ThreadReadId,
    snap_cache: &'a mut Option<Arc<E::Snapshot>>,
}

impl<'a, E> ReadDelegateExtTrait<'a, E> for ReadDelegateExt<'a, E>
where
    E: KvEngine,
{
    type Delegate = ReadDelegate;

    fn from_read_delegate(
        delegate: Self::Delegate,
        kv_engine: &'a E,
        metrics: &'a mut ReadMetrics,
        read_id: &'a mut ThreadReadId,
        snap_cache: &'a mut Option<Arc<E::Snapshot>>,
    ) -> Self {
        ReadDelegateExt::<'a, _> {
            delegate: Some(delegate),
            kv_engine,
            metrics,
            read_id,
            snap_cache,
        }
    }

    fn delegate(&self) -> &ReadDelegateCore {
        // It should not be called after calling take_delegate
        assert!(self.delegate.is_some());
        self.delegate.as_ref().unwrap().delegate()
    }

    fn take_delegate(&mut self) -> Self::Delegate {
        self.delegate.take().unwrap()
    }

    fn metrics(&mut self) -> &mut ReadMetrics {
        self.metrics
    }
}

impl Drop for ReadDelegateCore {
    fn drop(&mut self) {
        // call `inc` to notify the source `ReadDelegate` is dropped
        self.track_ver.inc();
    }
}

pub trait DelegateStore: Send + Clone + 'static {
    type Delegate: ReadDelegateTrait;

    fn store_id(&self) -> Option<u64>;

    fn get_delegate_and_len(&self, region_id: u64) -> (usize, Option<Self::Delegate>);
}

impl DelegateStore for Arc<Mutex<StoreMeta>> {
    type Delegate = ReadDelegate;

    fn store_id(&self) -> Option<u64> {
        self.as_ref().lock().unwrap().store_id
    }

    fn get_delegate_and_len(&self, region_id: u64) -> (usize, Option<Self::Delegate>) {
        let meta = self.as_ref().lock().unwrap();
        let reader = meta.readers.get(&region_id).cloned();
        if let Some(reader) = reader {
            return (
                meta.readers.len(),
                Some(ReadDelegate {
                    delegate: Arc::new(reader),
                }),
            );
        }
        (meta.readers.len(), None)
    }
}

#[derive(Debug)]
pub struct TrackVer {
    version: Arc<AtomicU64>,
    local_ver: u64,
    // source set to `true` means the `TrackVer` is created by `TrackVer::new` instead
    // of `TrackVer::clone`, more specific, only the `ReadDelegate` created by `ReadDelegate::new`
    // will have source `TrackVer` and be able to increase `TrackVer::version`, because these
    // `ReadDelegate` are store at `StoreMeta` and only them will invoke `ReadDelegate::update`
    source: bool,
}

impl TrackVer {
    pub fn new() -> TrackVer {
        TrackVer {
            version: Arc::new(AtomicU64::from(0)),
            local_ver: 0,
            source: true,
        }
    }

    // Take `&mut self` to prevent calling `inc` and `clone` at the same time
    pub fn inc(&mut self) {
        // Only the source `TrackVer` can increase version
        if self.source {
            self.version.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn any_new(&self) -> bool {
        self.version.load(Ordering::Relaxed) > self.local_ver
    }
}

impl Default for TrackVer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TrackVer {
    fn clone(&self) -> Self {
        TrackVer {
            version: Arc::clone(&self.version),
            local_ver: self.version.load(Ordering::Relaxed),
            source: false,
        }
    }
}

impl ReadDelegateCore {
    pub fn from_peer<EK: KvEngine, ER: RaftEngine>(peer: &Peer<EK, ER>) -> ReadDelegateCore {
        let region = peer.region().clone();
        let region_id = region.get_id();
        let peer_id = peer.peer.get_id();
        ReadDelegateCore {
            region: Arc::new(region),
            peer_id,
            term: peer.term(),
            applied_index_term: peer.get_store().applied_index_term(),
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            tag: format!("[region {}] {}", region_id, peer_id),
            txn_extra_op: peer.txn_extra_op.clone(),
            txn_ext: peer.txn_ext.clone(),
            read_progress: peer.read_progress.clone(),
            pending_remove: false,
            bucket_meta: peer.region_buckets.as_ref().map(|b| b.meta.clone()),
            track_ver: TrackVer::new(),
        }
    }

    pub fn fresh_valid_ts(&mut self) {
        self.last_valid_ts = monotonic_raw_now();
    }

    pub fn mark_pending_remove(&mut self) {
        self.pending_remove = true;
        self.track_ver.inc();
    }

    pub fn update(&mut self, progress: Progress) {
        self.fresh_valid_ts();
        self.track_ver.inc();
        match progress {
            Progress::Region(region) => {
                self.region = Arc::new(region);
            }
            Progress::Term(term) => {
                self.term = term;
            }
            Progress::AppliedIndexTerm(applied_index_term) => {
                self.applied_index_term = applied_index_term;
            }
            Progress::LeaderLease(leader_lease) => {
                self.leader_lease = Some(leader_lease);
            }
            Progress::RegionBuckets(bucket_meta) => {
                self.bucket_meta = Some(bucket_meta);
            }
        }
    }

    // If the remote lease will be expired in near future send message
    // to `raftstore` renew it
    pub fn maybe_renew_lease_advance<EK: KvEngine>(
        &self,
        router: &dyn CasualRouter<EK>,
        ts: Timespec,
        metrics: &mut ReadMetrics,
    ) {
        if !self
            .leader_lease
            .as_ref()
            .map(|lease| lease.need_renew(ts))
            .unwrap_or(false)
        {
            return;
        }
        metrics.renew_lease_advance += 1;
        let region_id = self.region.get_id();
        if let Err(e) = router.send(region_id, CasualMessage::RenewLease) {
            debug!(
                "failed to send renew lease message";
                "region" => region_id,
                "error" => ?e
            )
        }
    }

    pub fn is_in_leader_lease(&self, ts: Timespec, metrics: &mut ReadMetrics) -> bool {
        if let Some(ref lease) = self.leader_lease {
            let term = lease.term();
            if term == self.term {
                if lease.inspect(Some(ts)) == LeaseState::Valid {
                    return true;
                } else {
                    metrics.rejected_by_lease_expire += 1;
                    debug!("rejected by lease expire"; "tag" => &self.tag);
                }
            } else {
                metrics.rejected_by_term_mismatch += 1;
                debug!("rejected by term mismatch"; "tag" => &self.tag);
            }
        }

        false
    }

    pub fn check_stale_read_safe<S: Snapshot>(
        &self,
        read_ts: u64,
        metrics: &mut ReadMetrics,
    ) -> std::result::Result<(), ReadResponse<S>> {
        let safe_ts = self.read_progress.safe_ts();
        if safe_ts >= read_ts {
            return Ok(());
        }
        debug!(
            "reject stale read by safe ts";
            "tag" => &self.tag,
            "safe ts" => safe_ts,
            "read ts" => read_ts
        );
        metrics.rejected_by_safe_timestamp += 1;
        let mut response = cmd_resp::new_error(Error::DataIsNotReady {
            region_id: self.region.get_id(),
            peer_id: self.peer_id,
            safe_ts,
        });
        cmd_resp::bind_term(&mut response, self.term);
        Err(ReadResponse {
            response,
            snapshot: None,
            txn_extra_op: TxnExtraOp::Noop,
        })
    }

    /// Used in some external tests.
    pub fn mock(region_id: u64) -> Self {
        let mut region: metapb::Region = Default::default();
        region.set_id(region_id);
        let read_progress = Arc::new(RegionReadProgress::new(&region, 0, 0, "mock".to_owned()));
        ReadDelegateCore {
            region: Arc::new(region),
            peer_id: 1,
            term: 1,
            applied_index_term: 1,
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            tag: format!("[region {}] {}", region_id, 1),
            txn_extra_op: Default::default(),
            txn_ext: Default::default(),
            read_progress,
            pending_remove: false,
            track_ver: TrackVer::new(),
            bucket_meta: None,
        }
    }
}

impl Display for ReadDelegateCore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReadDelegate for region {}, \
             leader {} at term {}, applied_index_term {}, has lease {}",
            self.region.get_id(),
            self.peer_id,
            self.term,
            self.applied_index_term,
            self.leader_lease.is_some(),
        )
    }
}

#[derive(Debug)]
pub enum Progress {
    Region(metapb::Region),
    Term(u64),
    AppliedIndexTerm(u64),
    LeaderLease(RemoteLease),
    RegionBuckets(Arc<BucketMeta>),
}

impl Progress {
    pub fn region(region: metapb::Region) -> Progress {
        Progress::Region(region)
    }

    pub fn term(term: u64) -> Progress {
        Progress::Term(term)
    }

    pub fn applied_index_term(applied_index_term: u64) -> Progress {
        Progress::AppliedIndexTerm(applied_index_term)
    }

    pub fn leader_lease(lease: RemoteLease) -> Progress {
        Progress::LeaderLease(lease)
    }

    pub fn region_buckets(bucket_meta: Arc<BucketMeta>) -> Progress {
        Progress::RegionBuckets(bucket_meta)
    }
}

pub struct LocalReader<C, E, D, S>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E>,
    E: KvEngine,
    D: ReadDelegateTrait,
    S: DelegateStore<Delegate = D>,
{
    pub store_id: Cell<Option<u64>>,
    store_meta: S,
    kv_engine: E,
    metrics: ReadMetrics,
    // region id -> ReadDelegate
    // The use of `Arc` here is a workaround, see the comment at `get_delegate`
    delegates: LruCache<u64, D>,
    snap_cache: Option<Arc<E::Snapshot>>,
    cache_read_id: ThreadReadId,
    // A channel to raftstore.
    router: C,
}

impl<'a, E> ReadExecutor<E> for ReadDelegateExt<'_, E>
where
    E: KvEngine,
{
    fn get_tablet(&mut self) -> &E {
        self.kv_engine
    }

    fn get_snapshot(&mut self, create_time: Option<ThreadReadId>) -> Arc<E::Snapshot> {
        self.metrics.local_executed_requests += 1;
        if let Some(ts) = create_time {
            if ts == *self.read_id {
                if let Some(snap) = self.snap_cache.as_ref() {
                    self.metrics.local_executed_snapshot_cache_hit += 1;
                    return snap.clone();
                }
            }
            let snap = Arc::new(self.kv_engine.snapshot());
            self.read_id.update(ts);
            *self.snap_cache.as_mut().unwrap() = snap.clone();
            return snap;
        }
        Arc::new(self.kv_engine.snapshot())
    }
}

impl<C, E, D, S> LocalReader<C, E, D, S>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E>,
    E: KvEngine,
    D: ReadDelegateTrait,
    S: DelegateStore<Delegate = D>,
{
    pub fn new(kv_engine: E, store_meta: S, router: C) -> Self {
        let cache_read_id = ThreadReadId::new();
        LocalReader {
            store_meta,
            kv_engine,
            router,
            snap_cache: None,
            cache_read_id,
            store_id: Cell::new(None),
            metrics: Default::default(),
            delegates: LruCache::with_capacity_and_sample(0, 7),
        }
    }

    fn redirect(&mut self, mut cmd: RaftCommand<E::Snapshot>) {
        debug!("localreader redirects command"; "command" => ?cmd);
        let region_id = cmd.request.get_header().get_region_id();
        let mut err = errorpb::Error::default();
        match ProposalRouter::send(&self.router, cmd) {
            Ok(()) => return,
            Err(TrySendError::Full(c)) => {
                self.metrics.rejected_by_channel_full += 1;
                err.set_message(RAFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(RAFTSTORE_IS_BUSY.to_owned());
                cmd = c;
            }
            Err(TrySendError::Disconnected(c)) => {
                self.metrics.rejected_by_no_region += 1;
                err.set_message(format!("region {} is missing", region_id));
                err.mut_region_not_found().set_region_id(region_id);
                cmd = c;
            }
        }

        let mut resp = RaftCmdResponse::default();
        resp.mut_header().set_error(err);
        let read_resp = ReadResponse {
            response: resp,
            snapshot: None,
            txn_extra_op: TxnExtraOp::Noop,
        };

        cmd.callback.invoke_read(read_resp);
    }

    // Ideally `get_delegate` should return `Option<&ReadDelegate>`, but if so the lifetime of
    // the returned `&ReadDelegate` will bind to `self`, and make it impossible to use `&mut self`
    // while the `&ReadDelegate` is alive, a better choice is use `Rc` but `LocalReader: Send` will be
    // violated, which is required by `LocalReadRouter: Send`, use `Arc` will introduce extra cost but
    // make the logic clear
    pub fn get_delegate(&mut self, region_id: u64) -> Option<D> {
        let rd = match self.delegates.get(&region_id) {
            // The local `ReadDelegate` is up to date
            Some(d) if !d.delegate().track_ver.any_new() => Some(d.clone()),
            _ => {
                debug!("update local read delegate"; "region_id" => region_id);
                self.metrics.rejected_by_cache_miss += 1;

                let (meta_len, meta_reader) = { self.store_meta.get_delegate_and_len(region_id) };

                // Remove the stale delegate
                self.delegates.remove(&region_id);
                self.delegates.resize(meta_len);
                match meta_reader {
                    Some(reader) => {
                        self.delegates.insert(region_id, reader.clone());
                        Some(reader)
                    }
                    None => None,
                }
            }
        };
        // Return `None` if the read delegate is pending remove
        rd.filter(|r| !r.delegate().pending_remove)
    }

    pub fn pre_propose_raft_command(
        &mut self,
        req: &RaftCmdRequest,
    ) -> Result<Option<(D, RequestPolicy)>> {
        // Check store id.
        if self.store_id.get().is_none() {
            let store_id = self.store_meta.store_id();
            self.store_id.set(store_id);
        }
        let store_id = self.store_id.get().unwrap();

        if let Err(e) = util::check_store_id(req, store_id) {
            self.metrics.rejected_by_store_id_mismatch += 1;
            debug!("rejected by store id not match"; "err" => %e);
            return Err(e);
        }

        // Check region id.
        let region_id = req.get_header().get_region_id();
        let delegate = match self.get_delegate(region_id) {
            Some(d) => d,
            None => {
                self.metrics.rejected_by_no_region += 1;
                debug!("rejected by no region"; "region_id" => region_id);
                return Ok(None);
            }
        };

        fail_point!("localreader_on_find_delegate");

        // Check peer id.
        if let Err(e) = util::check_peer_id(req, delegate.delegate().peer_id) {
            self.metrics.rejected_by_peer_id_mismatch += 1;
            return Err(e);
        }

        // Check term.
        if let Err(e) = util::check_term(req, delegate.delegate().term) {
            debug!(
                "check term";
                "delegate_term" => delegate.delegate().term,
                "header_term" => req.get_header().get_term(),
            );
            self.metrics.rejected_by_term_mismatch += 1;
            return Err(e);
        }

        // Check region epoch.
        if util::check_region_epoch(req, &delegate.delegate().region, false).is_err() {
            self.metrics.rejected_by_epoch += 1;
            // Stale epoch, redirect it to raftstore to get the latest region.
            debug!("rejected by epoch not match"; "tag" => &delegate.delegate().tag);
            return Ok(None);
        }

        let mut inspector = Inspector {
            delegate: &delegate.delegate(),
            metrics: &mut self.metrics,
        };
        match inspector.inspect(req) {
            Ok(RequestPolicy::ReadLocal) => Ok(Some((delegate, RequestPolicy::ReadLocal))),
            Ok(RequestPolicy::StaleRead) => Ok(Some((delegate, RequestPolicy::StaleRead))),
            // It can not handle other policies.
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn propose_raft_command<'a, Ext>(
        &'a mut self,
        mut read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) where
        Ext: ReadDelegateExtTrait<'a, E, Delegate = D>,
    {
        match self.pre_propose_raft_command(&req) {
            Ok(Some((mut delegate, policy))) => {
                let mut delegate_ext: Ext;
                let mut response = match policy {
                    // Leader can read local if and only if it is in lease.
                    RequestPolicy::ReadLocal => {
                        let snapshot_ts = match read_id.as_mut() {
                            // If this peer became Leader not long ago and just after the cached
                            // snapshot was created, this snapshot can not see all data of the peer.
                            Some(id) => {
                                if id.create_time <= delegate.delegate().last_valid_ts {
                                    id.create_time = monotonic_raw_now();
                                }
                                id.create_time
                            }
                            None => monotonic_raw_now(),
                        };
                        if !delegate
                            .delegate()
                            .is_in_leader_lease(snapshot_ts, &mut self.metrics)
                        {
                            // Forward to raftstore.
                            self.redirect(RaftCommand::new(req, cb));
                            return;
                        }

                        delegate_ext = Ext::from_read_delegate(
                            delegate,
                            &self.kv_engine,
                            &mut self.metrics,
                            &mut self.cache_read_id,
                            &mut self.snap_cache,
                        );

                        let region = Arc::clone(&delegate_ext.delegate().region);
                        let response = delegate_ext.execute(&req, &region, None, read_id);
                        // Try renew lease in advance
                        delegate = delegate_ext.take_delegate();

                        delegate.delegate().maybe_renew_lease_advance(
                            &self.router,
                            snapshot_ts,
                            delegate_ext.metrics(),
                        );
                        response
                    }
                    // Replica can serve stale read if and only if its `safe_ts` >= `read_ts`
                    RequestPolicy::StaleRead => {
                        let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
                        assert!(read_ts > 0);
                        if let Err(resp) = delegate
                            .delegate()
                            .check_stale_read_safe(read_ts, &mut self.metrics)
                        {
                            cb.invoke_read(resp);
                            return;
                        }

                        delegate_ext = Ext::from_read_delegate(
                            delegate,
                            &self.kv_engine,
                            &mut self.metrics,
                            &mut self.cache_read_id,
                            &mut self.snap_cache,
                        );

                        let region = Arc::clone(&delegate_ext.delegate().region);
                        // Getting the snapshot
                        let response = delegate_ext.execute(&req, &region, None, read_id);

                        delegate = delegate_ext.take_delegate();
                        // Double check in case `safe_ts` change after the first check and before getting snapshot
                        if let Err(resp) = delegate
                            .delegate()
                            .check_stale_read_safe(read_ts, delegate_ext.metrics())
                        {
                            cb.invoke_read(resp);
                            return;
                        }
                        delegate_ext.metrics().local_executed_stale_read_requests += 1;
                        response
                    }
                    _ => unreachable!(),
                };
                cmd_resp::bind_term(&mut response.response, delegate.delegate().term);
                if let Some(snap) = response.snapshot.as_mut() {
                    snap.txn_ext = Some(delegate.delegate().txn_ext.clone());
                    snap.bucket_meta = delegate.delegate().bucket_meta.clone();
                }
                response.txn_extra_op = delegate.delegate().txn_extra_op.load();
                cb.invoke_read(response);
            }
            // Forward to raftstore.
            Ok(None) => self.redirect(RaftCommand::new(req, cb)),
            Err(e) => {
                let mut response = cmd_resp::new_error(e);
                if let Some(delegate) = self.delegates.get(&req.get_header().get_region_id()) {
                    cmd_resp::bind_term(&mut response, delegate.delegate().term);
                }
                cb.invoke_read(ReadResponse {
                    response,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                });
            }
        }
    }

    /// If read requests are received at the same RPC request, we can create one snapshot for all
    /// of them and check whether the time when the snapshot was created is in lease. We use
    /// ThreadReadId to figure out whether this RaftCommand comes from the same RPC request with
    /// the last RaftCommand which left a snapshot cached in LocalReader. ThreadReadId is composed
    /// by thread_id and a thread_local incremental sequence.
    #[inline]
    pub fn read<Ext>(
        &mut self,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) where
        for<'a> Ext: ReadDelegateExtTrait<'a, E, Delegate = D>,
    {
        self.propose_raft_command::<Ext>(read_id, req, cb);
        self.metrics.maybe_flush();
    }

    pub fn release_snapshot_cache(&mut self) {
        self.snap_cache.take();
    }
}

impl<C, E, D, S> Clone for LocalReader<C, E, D, S>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E> + Clone,
    E: KvEngine,
    D: ReadDelegateTrait,
    S: DelegateStore<Delegate = D>,
{
    fn clone(&self) -> Self {
        LocalReader {
            store_meta: self.store_meta.clone(),
            kv_engine: self.kv_engine.clone(),
            router: self.router.clone(),
            store_id: self.store_id.clone(),
            metrics: Default::default(),
            delegates: LruCache::with_capacity_and_sample(0, 7),
            snap_cache: self.snap_cache.clone(),
            cache_read_id: self.cache_read_id.clone(),
        }
    }
}

struct Inspector<'r, 'm> {
    delegate: &'r ReadDelegateCore,
    metrics: &'m mut ReadMetrics,
}

impl<'r, 'm> RequestInspector for Inspector<'r, 'm> {
    fn has_applied_to_current_term(&mut self) -> bool {
        if self.delegate.applied_index_term == self.delegate.term {
            true
        } else {
            debug!(
                "rejected by term check";
                "tag" => &self.delegate.tag,
                "applied_index_term" => self.delegate.applied_index_term,
                "delegate_term" => ?self.delegate.term,
            );

            // only for metric.
            self.metrics.rejected_by_applied_term += 1;
            false
        }
    }

    fn inspect_lease(&mut self) -> LeaseState {
        // TODO: disable localreader if we did not enable raft's check_quorum.
        if self.delegate.leader_lease.is_some() {
            // We skip lease check, because it is postponed until `handle_read`.
            LeaseState::Valid
        } else {
            debug!("rejected by leader lease"; "tag" => &self.delegate.tag);
            self.metrics.rejected_by_no_lease += 1;
            LeaseState::Expired
        }
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 15_000; // 15s

/// The reason for it to be pub is that it is shared with raftstore v2.
#[derive(Clone)]
pub struct ReadMetrics {
    pub local_executed_requests: u64,
    pub local_executed_stale_read_requests: u64,
    pub local_executed_snapshot_cache_hit: u64,
    // TODO: record rejected_by_read_quorum.
    pub rejected_by_store_id_mismatch: u64,
    pub rejected_by_peer_id_mismatch: u64,
    pub rejected_by_term_mismatch: u64,
    pub rejected_by_lease_expire: u64,
    pub rejected_by_no_region: u64,
    pub rejected_by_no_lease: u64,
    pub rejected_by_epoch: u64,
    pub rejected_by_applied_term: u64,
    pub rejected_by_channel_full: u64,
    pub rejected_by_cache_miss: u64,
    pub rejected_by_safe_timestamp: u64,
    pub renew_lease_advance: u64,

    pub last_flush_time: Instant,
}

impl Default for ReadMetrics {
    fn default() -> ReadMetrics {
        ReadMetrics {
            local_executed_requests: 0,
            local_executed_stale_read_requests: 0,
            local_executed_snapshot_cache_hit: 0,
            rejected_by_store_id_mismatch: 0,
            rejected_by_peer_id_mismatch: 0,
            rejected_by_term_mismatch: 0,
            rejected_by_lease_expire: 0,
            rejected_by_no_region: 0,
            rejected_by_no_lease: 0,
            rejected_by_epoch: 0,
            rejected_by_applied_term: 0,
            rejected_by_channel_full: 0,
            rejected_by_cache_miss: 0,
            rejected_by_safe_timestamp: 0,
            renew_lease_advance: 0,
            last_flush_time: Instant::now(),
        }
    }
}

impl ReadMetrics {
    pub fn maybe_flush(&mut self) {
        if self.last_flush_time.saturating_elapsed()
            >= Duration::from_millis(METRICS_FLUSH_INTERVAL)
        {
            self.flush();
            self.last_flush_time = Instant::now();
        }
    }

    fn flush(&mut self) {
        if self.rejected_by_store_id_mismatch > 0 {
            LOCAL_READ_REJECT
                .store_id_mismatch
                .inc_by(self.rejected_by_store_id_mismatch);
            self.rejected_by_store_id_mismatch = 0;
        }
        if self.rejected_by_peer_id_mismatch > 0 {
            LOCAL_READ_REJECT
                .peer_id_mismatch
                .inc_by(self.rejected_by_peer_id_mismatch);
            self.rejected_by_peer_id_mismatch = 0;
        }
        if self.rejected_by_term_mismatch > 0 {
            LOCAL_READ_REJECT
                .term_mismatch
                .inc_by(self.rejected_by_term_mismatch);
            self.rejected_by_term_mismatch = 0;
        }
        if self.rejected_by_lease_expire > 0 {
            LOCAL_READ_REJECT
                .lease_expire
                .inc_by(self.rejected_by_lease_expire);
            self.rejected_by_lease_expire = 0;
        }
        if self.rejected_by_no_region > 0 {
            LOCAL_READ_REJECT
                .no_region
                .inc_by(self.rejected_by_no_region);
            self.rejected_by_no_region = 0;
        }
        if self.rejected_by_no_lease > 0 {
            LOCAL_READ_REJECT.no_lease.inc_by(self.rejected_by_no_lease);
            self.rejected_by_no_lease = 0;
        }
        if self.rejected_by_epoch > 0 {
            LOCAL_READ_REJECT.epoch.inc_by(self.rejected_by_epoch);
            self.rejected_by_epoch = 0;
        }
        if self.rejected_by_applied_term > 0 {
            LOCAL_READ_REJECT
                .applied_term
                .inc_by(self.rejected_by_applied_term);
            self.rejected_by_applied_term = 0;
        }
        if self.rejected_by_channel_full > 0 {
            LOCAL_READ_REJECT
                .channel_full
                .inc_by(self.rejected_by_channel_full);
            self.rejected_by_channel_full = 0;
        }
        if self.rejected_by_safe_timestamp > 0 {
            LOCAL_READ_REJECT
                .safe_ts
                .inc_by(self.rejected_by_safe_timestamp);
            self.rejected_by_safe_timestamp = 0;
        }
        if self.local_executed_snapshot_cache_hit > 0 {
            LOCAL_READ_EXECUTED_CACHE_REQUESTS.inc_by(self.local_executed_snapshot_cache_hit);
            self.local_executed_snapshot_cache_hit = 0;
        }
        if self.local_executed_requests > 0 {
            LOCAL_READ_EXECUTED_REQUESTS.inc_by(self.local_executed_requests);
            self.local_executed_requests = 0;
        }
        if self.local_executed_stale_read_requests > 0 {
            LOCAL_READ_EXECUTED_STALE_READ_REQUESTS.inc_by(self.local_executed_stale_read_requests);
            self.local_executed_stale_read_requests = 0;
        }
        if self.renew_lease_advance > 0 {
            LOCAL_READ_RENEW_LEASE_ADVANCE_COUNTER.inc_by(self.renew_lease_advance);
            self.renew_lease_advance = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc::*, thread};

    use crossbeam::channel::TrySendError;
    use engine_test::kv::{KvTestEngine, KvTestSnapshot};
    use engine_traits::ALL_CFS;
    use kvproto::raft_cmdpb::*;
    use tempfile::{Builder, TempDir};
    use tikv_util::{codec::number::NumberEncoder, time::monotonic_raw_now};
    use time::Duration;
    use txn_types::WriteBatchFlags;

    use super::*;
    use crate::store::{util::Lease, Callback};

    struct MockRouter {
        p_router: SyncSender<RaftCommand<KvTestSnapshot>>,
        c_router: SyncSender<(u64, CasualMessage<KvTestEngine>)>,
    }

    impl MockRouter {
        #[allow(clippy::type_complexity)]
        fn new() -> (
            MockRouter,
            Receiver<RaftCommand<KvTestSnapshot>>,
            Receiver<(u64, CasualMessage<KvTestEngine>)>,
        ) {
            let (p_ch, p_rx) = sync_channel(1);
            let (c_ch, c_rx) = sync_channel(1);
            (
                MockRouter {
                    p_router: p_ch,
                    c_router: c_ch,
                },
                p_rx,
                c_rx,
            )
        }
    }

    impl ProposalRouter<KvTestSnapshot> for MockRouter {
        fn send(
            &self,
            cmd: RaftCommand<KvTestSnapshot>,
        ) -> std::result::Result<(), TrySendError<RaftCommand<KvTestSnapshot>>> {
            ProposalRouter::send(&self.p_router, cmd)
        }
    }

    impl CasualRouter<KvTestEngine> for MockRouter {
        fn send(&self, region_id: u64, msg: CasualMessage<KvTestEngine>) -> Result<()> {
            CasualRouter::send(&self.c_router, region_id, msg)
        }
    }

    #[allow(clippy::type_complexity)]
    fn new_reader(
        path: &str,
        store_id: u64,
        store_meta: Arc<Mutex<StoreMeta>>,
    ) -> (
        TempDir,
        LocalReader<MockRouter, KvTestEngine, ReadDelegate, Arc<Mutex<StoreMeta>>>,
        Receiver<RaftCommand<KvTestSnapshot>>,
    ) {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        let db = engine_test::kv::new_engine(path.path().to_str().unwrap(), None, ALL_CFS, None)
            .unwrap();
        let (ch, rx, _) = MockRouter::new();
        let mut reader = LocalReader::new(db, store_meta, ch);
        reader.store_id = Cell::new(Some(store_id));
        (path, reader, rx)
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

    fn must_redirect(
        reader: &mut LocalReader<MockRouter, KvTestEngine, ReadDelegate, Arc<Mutex<StoreMeta>>>,
        rx: &Receiver<RaftCommand<KvTestSnapshot>>,
        cmd: RaftCmdRequest,
    ) {
        reader.propose_raft_command::<ReadDelegateExt<'_, KvTestEngine, ReadDelegate>>(
            None,
            cmd.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        );
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd
        );
    }

    fn must_not_redirect(
        reader: &mut LocalReader<MockRouter, KvTestEngine, ReadDelegate, Arc<Mutex<StoreMeta>>>,
        rx: &Receiver<RaftCommand<KvTestSnapshot>>,
        task: RaftCommand<KvTestSnapshot>,
    ) {
        reader.propose_raft_command::<ReadDelegateExt<'_, KvTestEngine, ReadDelegate>>(
            None,
            task.request,
            task.callback,
        );
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    #[test]
    fn test_read() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx) = new_reader("test-local-reader", store_id, store_meta.clone());

        // region: 1,
        // peers: 2, 3, 4,
        // leader:2,
        // from "" to "",
        // epoch 1, 1,
        // term 6.
        let mut region1 = metapb::Region::default();
        region1.set_id(1);
        let prs = new_peers(store_id, vec![2, 3, 4]);
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
        let mut lease = Lease::new(Duration::seconds(1), Duration::milliseconds(250)); // 1s is long enough.
        let read_progress = Arc::new(RegionReadProgress::new(&region1, 1, 1, "".to_owned()));

        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader2.clone());
        header.set_region_epoch(epoch13.clone());
        header.set_term(term6);
        cmd.set_header(header);
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        // The region is not register yet.
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_no_region, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 1);
        assert!(reader.delegates.get(&1).is_none());

        // Register region 1
        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        // But the applied_index_term is stale.
        {
            let mut meta = store_meta.lock().unwrap();
            let read_delegate = ReadDelegateCore {
                tag: String::new(),
                region: Arc::new(region1.clone()),
                peer_id: leader2.get_id(),
                term: term6,
                applied_index_term: term6 - 1,
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
        }

        // The applied_index_term is stale
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_cache_miss, 2);
        assert_eq!(reader.metrics.rejected_by_applied_term, 1);

        // Make the applied_index_term matches current term.
        let pg = Progress::applied_index_term(term6);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(pg);
        }
        let task =
            RaftCommand::<KvTestSnapshot>::new(cmd.clone(), Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 3);

        // Let's read.
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_region(), &region1);
            })),
        );
        must_not_redirect(&mut reader, &rx, task);

        // Wait for expiration.
        thread::sleep(Duration::seconds(1).to_std().unwrap());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_lease_expire, 1);

        // Renew lease.
        lease.renew(monotonic_raw_now());

        // Store id mismatch.
        let mut cmd_store_id = cmd.clone();
        cmd_store_id
            .mut_header()
            .mut_peer()
            .set_store_id(store_id + 1);
        reader.propose_raft_command::<ReadDelegateExt<'_, KvTestEngine, ReadDelegate>>(
            None,
            cmd_store_id,
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_store_not_match());
                assert!(resp.snapshot.is_none());
            })),
        );
        assert_eq!(reader.metrics.rejected_by_store_id_mismatch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 3);

        // metapb::Peer id mismatch.
        let mut cmd_peer_id = cmd.clone();
        cmd_peer_id
            .mut_header()
            .mut_peer()
            .set_id(leader2.get_id() + 1);
        reader.propose_raft_command::<ReadDelegateExt<'_, KvTestEngine, ReadDelegate>>(
            None,
            cmd_peer_id,
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                assert!(
                    resp.response.get_header().has_error(),
                    "{:?}",
                    resp.response
                );
                assert!(resp.snapshot.is_none());
            })),
        );
        assert_eq!(reader.metrics.rejected_by_peer_id_mismatch, 1);

        // Read quorum.
        let mut cmd_read_quorum = cmd.clone();
        cmd_read_quorum.mut_header().set_read_quorum(true);
        must_redirect(&mut reader, &rx, cmd_read_quorum);

        // Term mismatch.
        let mut cmd_term = cmd.clone();
        cmd_term.mut_header().set_term(term6 - 2);
        reader.propose_raft_command::<ReadDelegateExt<'_, KvTestEngine, ReadDelegate>>(
            None,
            cmd_term,
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_stale_command(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        );
        assert_eq!(reader.metrics.rejected_by_term_mismatch, 1);

        // Stale epoch.
        let mut epoch12 = epoch13;
        epoch12.set_version(2);
        let mut cmd_epoch = cmd.clone();
        cmd_epoch.mut_header().set_region_epoch(epoch12);
        must_redirect(&mut reader, &rx, cmd_epoch);
        assert_eq!(reader.metrics.rejected_by_epoch, 1);

        // Expire lease manually, and it can not be renewed.
        let previous_lease_rejection = reader.metrics.rejected_by_lease_expire;
        lease.expire();
        lease.renew(monotonic_raw_now());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(
            reader.metrics.rejected_by_lease_expire,
            previous_lease_rejection + 1
        );

        // Channel full.
        reader.propose_raft_command::<ReadDelegateExt<'_, KvTestEngine, ReadDelegate>>(
            None,
            cmd.clone(),
            Callback::None,
        );
        reader.propose_raft_command::<ReadDelegateExt<'_, KvTestEngine, ReadDelegate>>(
            None,
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_server_is_busy(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        );
        rx.try_recv().unwrap();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(reader.metrics.rejected_by_channel_full, 1);

        // Reject by term mismatch in lease.
        let previous_term_rejection = reader.metrics.rejected_by_term_mismatch;
        let mut cmd9 = cmd.clone();
        cmd9.mut_header().set_term(term6 + 3);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::term(term6 + 3));
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::applied_index_term(term6 + 3));
        }
        reader.propose_raft_command::<ReadDelegateExt<'_, KvTestEngine, ReadDelegate>>(
            None,
            cmd9.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        );
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd9
        );
        assert_eq!(
            reader.metrics.rejected_by_term_mismatch,
            previous_term_rejection + 1,
        );
        assert_eq!(reader.metrics.rejected_by_cache_miss, 4);

        // Stale local ReadDelegate
        cmd.mut_header().set_term(term6 + 3);
        lease.expire_remote_lease();
        let remote_lease = lease.maybe_new_remote_lease(term6 + 3).unwrap();
        let pg = Progress::leader_lease(remote_lease);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(pg);
        }
        let task =
            RaftCommand::<KvTestSnapshot>::new(cmd.clone(), Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 5);

        // Stale read
        assert_eq!(reader.metrics.rejected_by_safe_timestamp, 0);
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
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_data_is_not_ready());
                assert!(resp.snapshot.is_none());
            })),
        );
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(reader.metrics.rejected_by_safe_timestamp, 1);

        read_progress.update_safe_ts(1, 2);
        assert_eq!(read_progress.safe_ts(), 2);
        let task = RaftCommand::<KvTestSnapshot>::new(cmd, Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(reader.metrics.rejected_by_safe_timestamp, 1);

        // Remove invalid delegate
        let reader_clone = store_meta.lock().unwrap().readers.get(&1).unwrap().clone();
        assert!(reader.get_delegate(1).is_some());

        // dropping the non-source `reader` will not make other readers invalid
        drop(reader_clone);
        assert!(reader.get_delegate(1).is_some());

        // drop the source `reader`
        store_meta.lock().unwrap().readers.remove(&1).unwrap();
        // the invalid delegate should be removed
        assert!(reader.get_delegate(1).is_none());
    }

    #[test]
    fn test_read_delegate_cache_update() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, _) = new_reader("test-local-reader", store_id, store_meta.clone());
        let mut region = metapb::Region::default();
        region.set_id(1);
        {
            let mut meta = store_meta.lock().unwrap();
            let read_delegate = ReadDelegateCore {
                tag: String::new(),
                region: Arc::new(region.clone()),
                peer_id: 1,
                term: 1,
                applied_index_term: 1,
                leader_lease: None,
                last_valid_ts: Timespec::new(0, 0),
                txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
                txn_ext: Arc::new(TxnExt::default()),
                track_ver: TrackVer::new(),
                read_progress: Arc::new(RegionReadProgress::new(&region, 0, 0, "".to_owned())),
                pending_remove: false,
                bucket_meta: None,
            };
            meta.readers.insert(1, read_delegate);
        }

        let d = reader.get_delegate(1).unwrap();
        assert_eq!(&*d.delegate().region, &region);
        assert_eq!(d.delegate().term, 1);
        assert_eq!(d.delegate().applied_index_term, 1);
        assert!(d.delegate().leader_lease.is_none());
        drop(d);

        {
            region.mut_region_epoch().set_version(10);
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::region(region.clone()));
        }
        assert_eq!(&*reader.get_delegate(1).unwrap().delegate().region, &region);

        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(Progress::term(2));
        }
        assert_eq!(reader.get_delegate(1).unwrap().delegate().term, 2);

        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::applied_index_term(2));
        }
        assert_eq!(
            reader
                .get_delegate(1)
                .unwrap()
                .delegate()
                .applied_index_term,
            2
        );

        {
            let mut lease = Lease::new(Duration::seconds(1), Duration::milliseconds(250)); // 1s is long enough.
            let remote = lease.maybe_new_remote_lease(3).unwrap();
            let pg = Progress::leader_lease(remote);
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(pg);
        }
        let d = reader.get_delegate(1).unwrap();
        assert_eq!(d.delegate().leader_lease.clone().unwrap().term(), 3);
    }

    // #[test]
    // fn test_snap_cache_hit() {
    //     let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
    //     let (_tmp, mut reader, _) = new_reader("test-local-reader", 1, store_meta);
    //     let read_id = Some(ThreadReadId::new());
    //     for _ in 0..10 {
    //         // Different region id should reuse the cache
    //         let _ = reader.get_snapshot(read_id.clone());
    //     }
    //     // We should hit cache 9 times
    //     assert_eq!(reader.metrics.local_executed_snapshot_cache_hit, 9);

    //     let read_id = Some(ThreadReadId::new());
    //     let _ = reader.get_snapshot(read_id.clone());
    //     // This time, we will miss the cache
    //     assert_eq!(reader.metrics.local_executed_snapshot_cache_hit, 9);
    //     let _ = reader.get_snapshot(read_id.clone());
    //     // We can hit it again.
    //     assert_eq!(reader.metrics.local_executed_snapshot_cache_hit, 10);

    //     reader.release_snapshot_cache();
    //     let _ = reader.get_snapshot(read_id.clone());
    //     // After release, we will mss the cache even with the prevsiou read_id.
    //     assert_eq!(reader.metrics.local_executed_snapshot_cache_hit, 10);
    //     let _ = reader.get_snapshot(read_id);
    //     // We can hit it again.
    //     assert_eq!(reader.metrics.local_executed_snapshot_cache_hit, 11);
    // }
}
