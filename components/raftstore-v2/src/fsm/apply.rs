// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

use batch_system::{Fsm, FsmScheduler, Mailbox};
use collections::HashMap;
use crossbeam::channel::TryRecvError;
use engine_traits::{KvEngine, RaftEngine, TabletFactory};
use futures::{Future, StreamExt};
use kvproto::raft_serverpb::RegionLocalState;
use slog::Logger;
use tikv_util::mpsc::future::{self, Receiver, Sender, WakePolicy};

use crate::{
    raft::Apply,
    router::{ApplyRes, ApplyTask, PeerMsg},
    tablet::CachedTablet,
};

/// A trait for reporting apply result.
///
/// Using a trait to make signiture simpler.
pub trait ApplyResReporter {
    fn report(&self, apply_res: ApplyRes);
}

impl<F: Fsm<Message = PeerMsg>, S: FsmScheduler<Fsm = F>> ApplyResReporter for Mailbox<F, S> {
    fn report(&self, apply_res: ApplyRes) {
        // TODO: check shutdown.
        self.force_send(PeerMsg::ApplyRes(apply_res)).unwrap();
    }
}

/// Schedule task to `ApplyFsm`.
pub struct ApplyScheduler {
    sender: Sender<ApplyTask>,
}

impl ApplyScheduler {
    #[inline]
    pub fn send(&self, task: ApplyTask) {
        // TODO: ignore error when shutting down.
        self.sender.send(task).unwrap();
    }
}

pub struct ApplyFsm<EK: KvEngine, ER: RaftEngine, R> {
    apply: Apply<EK, ER, R>,
    receiver: Receiver<ApplyTask>,
}

impl<EK: KvEngine, ER: RaftEngine, R> ApplyFsm<EK, ER, R> {
    pub fn new(
        store_id: u64,
        region_state: RegionLocalState,
        res_reporter: R,
        remote_tablet: CachedTablet<EK>,
        raft_engine: ER,
        tablet_factory: Arc<dyn TabletFactory<EK>>,
        pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
        logger: Logger,
    ) -> (ApplyScheduler, Self) {
        let (tx, rx) = future::unbounded(WakePolicy::Immediately);
        let apply = Apply::new(
            store_id,
            region_state,
            res_reporter,
            remote_tablet,
            raft_engine,
            tablet_factory,
            pending_create_peers,
            logger,
        );
        (
            ApplyScheduler { sender: tx },
            Self {
                apply,
                receiver: rx,
            },
        )
    }
}

impl<EK: KvEngine, ER: RaftEngine, R: ApplyResReporter> ApplyFsm<EK, ER, R> {
    pub async fn handle_all_tasks(&mut self) {
        loop {
            let mut task = match self.receiver.next().await {
                Some(t) => t,
                None => return,
            };
            loop {
                match task {
                    // TODO: flush by buffer size.
                    ApplyTask::CommittedEntries(ce) => self.apply.apply_committed_entries(ce).await,
                }

                // TODO: yield after some time.

                // Perhaps spin sometime?
                match self.receiver.try_recv() {
                    Ok(t) => task = t,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }
            self.apply.flush();
        }
    }
}
