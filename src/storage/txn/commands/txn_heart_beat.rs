// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, SnapshotReader},
    txn::{
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        Result,
    },
    ProcessResult, Snapshot, TxnStatus,
};

command! {
    /// Heart beat of a transaction. It enlarges the primary lock's TTL.
    ///
    /// This is invoked on a transaction's primary lock. The lock may be generated by either
    /// [`AcquirePessimisticLock`](Command::AcquirePessimisticLock) or
    /// [`Prewrite`](Command::Prewrite).
    TxnHeartBeat:
        cmd_ty => TxnStatus,
        display => {
            "kv::command::txn_heart_beat {} @ {} ttl {} | {:?}",
            (primary_key, start_ts, advise_ttl, ctx),
        }
        content => {
            /// The primary key of the transaction.
            primary_key: Key,
            /// The transaction's start_ts.
            start_ts: TimeStamp,
            /// The new TTL that will be used to update the lock's TTL. If the lock's TTL is already
            /// greater than `advise_ttl`, nothing will happen.
            advise_ttl: u64,
            /// The updated min_commit_ts of the primary key. Piggybacked on the heartbeat command.
            min_commit_ts: u64,
        }
        in_heap => {
            primary_key,
        }
}

impl CommandExt for TxnHeartBeat {
    ctx!();
    tag!(txn_heart_beat);
    request_type!(KvTxnHeartBeat);
    ts!(start_ts);
    write_bytes!(primary_key);
    gen_lock!(primary_key);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for TxnHeartBeat {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        // TxnHeartBeat never remove locks. No need to wake up waiters.
        let mut txn = MvccTxn::new(self.start_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.start_ts, snapshot, &self.ctx),
            context.statistics,
        );
        fail_point!("txn_heart_beat", |err| Err(
            crate::storage::mvcc::Error::from(crate::storage::mvcc::txn::make_txn_error(
                err,
                &self.primary_key,
                self.start_ts,
            ))
            .into()
        ));

        let lock = match reader.load_lock(&self.primary_key)? {
            Some(mut lock) if lock.ts == self.start_ts => {
                let mut updated = false;

                if lock.ttl < self.advise_ttl {
                    lock.ttl = self.advise_ttl;
                    updated = true;
                }

                // only for non-async-commit pipelined transactions, we can update the
                // min_commit_ts
                if !lock.use_async_commit
                    && lock.generation > 0
                    && self.min_commit_ts > 0
                    && lock.min_commit_ts < self.min_commit_ts.into()
                {
                    lock.min_commit_ts = self.min_commit_ts.into();
                    updated = true;
                }

                if updated {
                    txn.put_lock(self.primary_key.clone(), &lock, false);
                }

                lock
            }
            _ => {
                return Err(MvccError::from(MvccErrorInner::TxnNotFound {
                    start_ts: self.start_ts,
                    key: self.primary_key.into_raw()?,
                })
                .into());
            }
        };

        let pr = ProcessResult::TxnStatus {
            txn_status: TxnStatus::uncommitted(lock, false),
        };
        let new_acquired_locks = txn.take_new_locks();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows: 1,
            pr,
            lock_info: vec![],
            released_locks: ReleasedLocks::new(),
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status: vec![],
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use tikv_util::deadline::Deadline;

    use super::*;
    use crate::storage::{
        kv::TestEngineBuilder,
        lock_manager::MockLockManager,
        mvcc::tests::*,
        txn::{
            commands::WriteCommand, scheduler::DEFAULT_EXECUTION_DURATION_LIMIT, tests::*,
            txn_status_cache::TxnStatusCache,
        },
        Engine,
    };

    pub fn must_success<E: Engine>(
        engine: &mut E,
        primary_key: &[u8],
        start_ts: impl Into<TimeStamp>,
        advise_ttl: u64,
        expect_ttl: u64,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let command = TxnHeartBeat {
            ctx: Context::default(),
            primary_key: Key::from_raw(primary_key),
            start_ts,
            advise_ttl,
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
            min_commit_ts: 0,
        };
        let result = command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &MockLockManager::new(),
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    async_apply_prewrite: false,
                    raw_ext: None,
                    txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
                },
            )
            .unwrap();
        if let ProcessResult::TxnStatus {
            txn_status: TxnStatus::Uncommitted { lock, .. },
        } = result.pr
        {
            write(engine, &ctx, result.to_be_write.modifies);
            assert_eq!(lock.ttl, expect_ttl);
        } else {
            unreachable!();
        }
    }

    pub fn must_err<E: Engine>(
        engine: &mut E,
        primary_key: &[u8],
        start_ts: impl Into<TimeStamp>,
        advise_ttl: u64,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let start_ts = start_ts.into();
        let cm = ConcurrencyManager::new(start_ts);
        let command = TxnHeartBeat {
            ctx,
            primary_key: Key::from_raw(primary_key),
            start_ts,
            advise_ttl,
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
            min_commit_ts: 0,
        };
        assert!(
            command
                .process_write(
                    snapshot,
                    WriteContext {
                        lock_mgr: &MockLockManager::new(),
                        concurrency_manager: cm,
                        extra_op: Default::default(),
                        statistics: &mut Default::default(),
                        async_apply_prewrite: false,
                        raw_ext: None,
                        txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn test_txn_heart_beat() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");

        fn test(ts: u64, k: &[u8], engine: &mut impl Engine) {
            // Do nothing if advise_ttl is less smaller than current TTL.
            must_success(engine, k, ts, 90, 100);
            // Return the new TTL if the TTL when the TTL is updated.
            must_success(engine, k, ts, 110, 110);
            // The lock's TTL is updated and persisted into the db.
            must_success(engine, k, ts, 90, 110);
            // Heart beat another transaction's lock will lead to an error.
            must_err(engine, k, ts - 1, 150);
            must_err(engine, k, ts + 1, 150);
            // The existing lock is not changed.
            must_success(engine, k, ts, 90, 110);
        }

        // No lock.
        must_err(&mut engine, k, 5, 100);

        // Create a lock with TTL=100.
        // The initial TTL will be set to 0 after calling must_prewrite_put. Update it
        // first.
        must_prewrite_put(&mut engine, k, v, k, 5);
        must_locked(&mut engine, k, 5);
        must_success(&mut engine, k, 5, 100, 100);

        test(5, k, &mut engine);

        must_locked(&mut engine, k, 5);
        must_commit(&mut engine, k, 5, 10);
        must_unlocked(&mut engine, k);

        // No lock.
        must_err(&mut engine, k, 5, 100);
        must_err(&mut engine, k, 10, 100);

        must_acquire_pessimistic_lock(&mut engine, k, k, 8, 15);
        must_pessimistic_locked(&mut engine, k, 8, 15);
        must_success(&mut engine, k, 8, 100, 100);

        test(8, k, &mut engine);

        must_pessimistic_locked(&mut engine, k, 8, 15);
    }

    #[test]
    fn test_heartbeat_piggyback_min_commit_ts() {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");

        // Create a lock with TTL=100.
        must_flush_put(&mut engine, k, v, k, 5, 1);
        must_locked(&mut engine, k, 5);

        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let start_ts = 5.into();
        let cm = ConcurrencyManager::new(start_ts);
        let command = TxnHeartBeat {
            ctx: ctx.clone(),
            primary_key: Key::from_raw(k),
            start_ts,
            advise_ttl: 3333,
            min_commit_ts: 10,
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
        };
        let result = command
            .process_write(
                snapshot,
                WriteContext {
                    lock_mgr: &MockLockManager::new(),
                    concurrency_manager: cm,
                    extra_op: Default::default(),
                    statistics: &mut Default::default(),
                    async_apply_prewrite: false,
                    raw_ext: None,
                    txn_status_cache: Arc::new(TxnStatusCache::new_for_test()),
                },
            )
            .unwrap();
        if let ProcessResult::TxnStatus {
            txn_status: TxnStatus::Uncommitted { lock, .. },
        } = result.pr
        {
            write(&engine, &ctx, result.to_be_write.modifies);
            assert_eq!(lock.ttl, 3333);
            assert_eq!(lock.min_commit_ts, 10.into());
        } else {
            unreachable!();
        }
    }
}
