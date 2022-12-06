// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::fmt;

use engine_traits::Snapshot;
use kvproto::{metapb::RegionEpoch, raft_cmdpb::RaftCmdRequest, raft_serverpb::RaftMessage};
use raft::{eraftpb::Snapshot as RaftSnapshot, SnapshotStatus};
use raftstore::store::{metrics::RaftEventDurationType, FetchedLogs, GenSnapRes};
use tikv_util::time::Instant;

use super::{
    response_channel::{
        CmdResChannel, CmdResSubscriber, DebugInfoChannel, QueryResChannel, QueryResSubscriber,
    },
    ApplyRes,
};
use crate::operation::{SplitInit, SplitRegion};

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
#[repr(u8)]
pub enum PeerTick {
    Raft = 0,
    RaftLogGc = 1,
    SplitRegionCheck = 2,
    PdHeartbeat = 3,
    CheckMerge = 4,
    CheckPeerStaleState = 5,
    EntryCacheEvict = 6,
    CheckLeaderLease = 7,
    ReactivateMemoryLock = 8,
    ReportBuckets = 9,
    CheckLongUncommitted = 10,
}

impl PeerTick {
    pub const VARIANT_COUNT: usize = Self::all_ticks().len();

    #[inline]
    pub fn tag(self) -> &'static str {
        match self {
            PeerTick::Raft => "raft",
            PeerTick::RaftLogGc => "raft_log_gc",
            PeerTick::SplitRegionCheck => "split_region_check",
            PeerTick::PdHeartbeat => "pd_heartbeat",
            PeerTick::CheckMerge => "check_merge",
            PeerTick::CheckPeerStaleState => "check_peer_stale_state",
            PeerTick::EntryCacheEvict => "entry_cache_evict",
            PeerTick::CheckLeaderLease => "check_leader_lease",
            PeerTick::ReactivateMemoryLock => "reactivate_memory_lock",
            PeerTick::ReportBuckets => "report_buckets",
            PeerTick::CheckLongUncommitted => "check_long_uncommitted",
        }
    }

    pub const fn all_ticks() -> &'static [PeerTick] {
        const TICKS: &[PeerTick] = &[
            PeerTick::Raft,
            PeerTick::RaftLogGc,
            PeerTick::SplitRegionCheck,
            PeerTick::PdHeartbeat,
            PeerTick::CheckMerge,
            PeerTick::CheckPeerStaleState,
            PeerTick::EntryCacheEvict,
            PeerTick::CheckLeaderLease,
            PeerTick::ReactivateMemoryLock,
            PeerTick::ReportBuckets,
            PeerTick::CheckLongUncommitted,
        ];
        TICKS
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StoreTick {
    // No CompactLock and CompactCheck as they should be implemented by peer itself.
    PdStoreHeartbeat,
    SnapGc,
    ConsistencyCheck,
    CleanupImportSst,
}

impl StoreTick {
    #[inline]
    pub fn tag(self) -> RaftEventDurationType {
        match self {
            StoreTick::PdStoreHeartbeat => RaftEventDurationType::pd_store_heartbeat,
            StoreTick::SnapGc => RaftEventDurationType::snap_gc,
            StoreTick::ConsistencyCheck => RaftEventDurationType::consistency_check,
            StoreTick::CleanupImportSst => RaftEventDurationType::cleanup_import_sst,
        }
    }
}

/// Command that can be handled by raftstore.
pub struct RaftRequest<C> {
    pub send_time: Instant,
    pub request: RaftCmdRequest,
    pub ch: C,
}

impl<C> RaftRequest<C> {
    pub fn new(request: RaftCmdRequest, ch: C) -> Self {
        RaftRequest {
            send_time: Instant::now(),
            request,
            ch,
        }
    }
}

/// Message that can be sent to a peer.
pub enum PeerMsg {
    /// Raft message is the message sent between raft nodes in the same
    /// raft group. Messages need to be redirected to raftstore if target
    /// peer doesn't exist.
    RaftMessage(Box<RaftMessage>),
    /// Query won't change any state. A typical query is KV read. In most cases,
    /// it will be processed using lease or read index.
    RaftQuery(RaftRequest<QueryResChannel>),
    /// Command changes the inernal states. It will be transformed into logs and
    /// applied on all replicas.
    RaftCommand(RaftRequest<CmdResChannel>),
    /// Tick is periodical task. If target peer doesn't exist there is a
    /// potential that the raft node will not work anymore.
    Tick(PeerTick),
    /// Result of applying committed entries. The message can't be lost.
    ApplyRes(ApplyRes),
    LogsFetched(FetchedLogs),
    SnapshotGenerated(GenSnapRes),
    /// Start the FSM.
    Start,
    /// Messages from peer to peer in the same store
    SplitInit(Box<SplitInit>),
    /// A message only used to notify a peer.
    Noop,
    /// A message that indicates an asynchronous write has finished.
    Persisted {
        peer_id: u64,
        ready_number: u64,
    },
    QueryDebugInfo(DebugInfoChannel),
    SplitRegion(SplitRegion),
    /// A message that used to check if a flush is happened.
    #[cfg(feature = "testexport")]
    WaitFlush(super::FlushChannel),

    // A message that used to report status status after sending snapshot.
    SnapshotReportStatus {
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    },
    // /// Message that can't be lost but rarely created. If they are lost, real
    // /// bad things happen like some peers will be considered dead in the
    // /// group.
    // SignificantMsg(SignificantMsg<EK::Snapshot>),
}

impl PeerMsg {
    pub fn raft_query(req: RaftCmdRequest) -> (Self, QueryResSubscriber) {
        let (ch, sub) = QueryResChannel::pair();
        (PeerMsg::RaftQuery(RaftRequest::new(req, ch)), sub)
    }

    pub fn raft_command(req: RaftCmdRequest) -> (Self, CmdResSubscriber) {
        let (ch, sub) = CmdResChannel::pair();
        (PeerMsg::RaftCommand(RaftRequest::new(req, ch)), sub)
    }

    pub fn split_request(
        region_epoch: RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: String,
    ) -> (Self, CmdResSubscriber) {
        let (ch, sub) = CmdResChannel::pair();
        (
            PeerMsg::SplitRegion(SplitRegion {
                region_epoch,
                split_keys,
                source: source.into(),
                ch,
            }),
            sub,
        )
    }
}

impl fmt::Debug for PeerMsg {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            PeerMsg::RaftQuery(_) => write!(fmt, "Raft Query"),
            PeerMsg::RaftCommand(_) => write!(fmt, "Raft Command"),
            PeerMsg::Tick(tick) => write! {
                fmt,
                "{:?}",
                tick
            },
            PeerMsg::ApplyRes(res) => write!(fmt, "ApplyRes {:?}", res),
            PeerMsg::Start => write!(fmt, "Startup"),
            PeerMsg::SplitInit(_) => {
                write!(fmt, "Split initialization")
            }
            PeerMsg::Noop => write!(fmt, "Noop"),
            PeerMsg::Persisted {
                peer_id,
                ready_number,
            } => write!(
                fmt,
                "Persisted peer_id {}, ready_number {}",
                peer_id, ready_number
            ),
            PeerMsg::LogsFetched(fetched) => write!(fmt, "LogsFetched {:?}", fetched),
            PeerMsg::SnapshotGenerated(_) => write!(fmt, "SnapshotGenerated"),
            PeerMsg::QueryDebugInfo(_) => write!(fmt, "QueryDebugInfo"),
            PeerMsg::SplitRegion(_) => write!(fmt, "SplitRegion"),
            #[cfg(feature = "testexport")]
            PeerMsg::WaitFlush(_) => write!(fmt, "FlushMessages"),
            PeerMsg::SnapshotReportStatus {
                region_id,
                to_peer_id,
                status,
            } => write!(
                fmt,
                "snapshot report status, region_id {}, peer_id {}, status {:?}",
                region_id, to_peer_id, status
            ),
        }
    }
}

pub enum StoreMsg {
    RaftMessage(Box<RaftMessage>),
    SplitInit(Box<SplitInit>),
    Tick(StoreTick),
    Start(u64),
}

impl fmt::Debug for StoreMsg {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            StoreMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            StoreMsg::SplitInit(_) => write!(fmt, "Split initialization"),
            StoreMsg::Tick(tick) => write!(fmt, "StoreTick {:?}", tick),
            StoreMsg::Start(store_id) => write!(fmt, "Start store {}", store_id),
        }
    }
}
