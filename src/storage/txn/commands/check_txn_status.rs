// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{MvccTxn, SnapshotReader},
    txn::{
        actions::check_txn_status::*,
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResponsePolicy, TypedCommand,
            WriteCommand, WriteContext, WriteResult,
        },
        Result,
    },
    ProcessResult, Snapshot, TxnStatus,
};

command! {
    /// Check the status of a transaction. This is usually invoked by a transaction that meets
    /// another transaction's lock. If the primary lock is expired, it will rollback the primary
    /// lock. If the primary lock exists but is not expired, it may update the transaction's
    /// `min_commit_ts`. Returns a [`TxnStatus`](TxnStatus) to represent the status.
    ///
    /// This is invoked on a transaction's primary lock. The lock may be generated by either
    /// [`AcquirePessimisticLock`](Command::AcquirePessimisticLock) or
    /// [`Prewrite`](Command::Prewrite).
    CheckTxnStatus:
        cmd_ty => TxnStatus,
        display => "kv::command::check_txn_status {} @ {} curr({}, {}, {}, {}, {}) | {:?}",
           (primary_key, lock_ts, caller_start_ts, current_ts, rollback_if_not_exist,
               force_sync_commit, resolving_pessimistic_lock, ctx),
        content => {
            /// The primary key of the transaction.
            primary_key: Key,
            /// The lock's ts, namely the transaction's start_ts.
            lock_ts: TimeStamp,
            /// The start_ts of the transaction that invokes this command.
            caller_start_ts: TimeStamp,
            /// The approximate current_ts when the command is invoked.
            current_ts: TimeStamp,
            /// Specifies the behavior when neither commit/rollback record nor lock is found. If true,
            /// rollbacks that transaction; otherwise returns an error.
            rollback_if_not_exist: bool,
            // This field is set to true only if the transaction is known to fall back from async commit.
            // CheckTxnStatus treats the transaction as non-async-commit if this field is true.
            force_sync_commit: bool,
            // If the check request is used to resolve or decide the transaction status for a input pessimistic
            // lock, the transaction status could not be decided if the primary lock is pessimistic too and
            // it's still uncertain.
            resolving_pessimistic_lock: bool,
        }
}

impl CommandExt for CheckTxnStatus {
    ctx!();
    tag!(check_txn_status);
    request_type!(KvCheckTxnStatus);
    ts!(lock_ts);
    write_bytes!(primary_key);
    gen_lock!(primary_key);
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for CheckTxnStatus {
    /// checks whether a transaction has expired its primary lock's TTL,
    /// rollback the transaction if expired, or update the transaction's
    /// min_commit_ts according to the metadata in the primary lock.
    /// When transaction T1 meets T2's lock, it may invoke this on T2's primary
    /// key. In this situation, `self.start_ts` is T2's `start_ts`,
    /// `caller_start_ts` is T1's `start_ts`, and the `current_ts` is
    /// literally the timestamp when this function is invoked; it may not be
    /// accurate.
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let mut new_max_ts = self.lock_ts;
        if !self.current_ts.is_max() && self.current_ts > new_max_ts {
            new_max_ts = self.current_ts;
        }
        if !self.caller_start_ts.is_max() && self.caller_start_ts > new_max_ts {
            new_max_ts = self.caller_start_ts;
        }
        context.concurrency_manager.update_max_ts(new_max_ts);

        let mut txn = MvccTxn::new(self.lock_ts, context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(self.lock_ts, snapshot, &self.ctx),
            context.statistics,
        );

        fail_point!("check_txn_status", |err| Err(
            crate::storage::mvcc::Error::from(crate::storage::mvcc::txn::make_txn_error(
                err,
                &self.primary_key,
                self.lock_ts
            ))
            .into()
        ));

        let (txn_status, released) = match reader.load_lock(&self.primary_key)? {
            Some(lock) if lock.ts == self.lock_ts => check_txn_status_lock_exists(
                &mut txn,
                &mut reader,
                self.primary_key,
                lock,
                self.current_ts,
                self.caller_start_ts,
                self.force_sync_commit,
                self.resolving_pessimistic_lock,
            )?,
            l => (
                check_txn_status_missing_lock(
                    &mut txn,
                    &mut reader,
                    self.primary_key,
                    l,
                    MissingLockAction::rollback(self.rollback_if_not_exist),
                    self.resolving_pessimistic_lock,
                )?,
                None,
            ),
        };

        let mut released_locks = ReleasedLocks::new();
        released_locks.push(released);

        let pr = ProcessResult::TxnStatus { txn_status };
        let new_acquired_locks = txn.take_new_locks();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx: self.ctx,
            to_be_write: write_data,
            rows: 1,
            pr,
            lock_info: vec![],
            released_locks,
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::{Context, PrewriteRequestPessimisticAction::*};
    use tikv_util::deadline::Deadline;
    use txn_types::{Key, WriteType};

    use super::{TxnStatus::*, *};
    use crate::storage::{
        kv::Engine,
        lock_manager::MockLockManager,
        mvcc::tests::*,
        txn::{
            commands::{pessimistic_rollback, WriteCommand, WriteContext},
            scheduler::DEFAULT_EXECUTION_DURATION_LIMIT,
            tests::*,
        },
        types::TxnStatus,
        ProcessResult, TestEngineBuilder,
    };

    pub fn must_success<E: Engine>(
        engine: &mut E,
        primary_key: &[u8],
        lock_ts: impl Into<TimeStamp>,
        caller_start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
        rollback_if_not_exist: bool,
        force_sync_commit: bool,
        resolving_pessimistic_lock: bool,
        status_pred: impl FnOnce(TxnStatus) -> bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let lock_ts: TimeStamp = lock_ts.into();
        let command = crate::storage::txn::commands::CheckTxnStatus {
            ctx: Context::default(),
            primary_key: Key::from_raw(primary_key),
            lock_ts,
            caller_start_ts: caller_start_ts.into(),
            current_ts,
            rollback_if_not_exist,
            force_sync_commit,
            resolving_pessimistic_lock,
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
                },
            )
            .unwrap();
        if let ProcessResult::TxnStatus { txn_status } = result.pr {
            assert!(status_pred(txn_status));
        } else {
            unreachable!();
        }
        write(engine, &ctx, result.to_be_write.modifies);
    }

    pub fn must_err<E: Engine>(
        engine: &mut E,
        primary_key: &[u8],
        lock_ts: impl Into<TimeStamp>,
        caller_start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
        rollback_if_not_exist: bool,
        force_sync_commit: bool,
        resolving_pessimistic_lock: bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let current_ts = current_ts.into();
        let cm = ConcurrencyManager::new(current_ts);
        let lock_ts: TimeStamp = lock_ts.into();
        let command = crate::storage::txn::commands::CheckTxnStatus {
            ctx,
            primary_key: Key::from_raw(primary_key),
            lock_ts,
            caller_start_ts: caller_start_ts.into(),
            current_ts,
            rollback_if_not_exist,
            force_sync_commit,
            resolving_pessimistic_lock,
            deadline: Deadline::from_now(DEFAULT_EXECUTION_DURATION_LIMIT),
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
                    },
                )
                .is_err()
        );
    }

    fn committed(commit_ts: impl Into<TimeStamp>) -> impl FnOnce(TxnStatus) -> bool {
        move |s| {
            s == TxnStatus::Committed {
                commit_ts: commit_ts.into(),
            }
        }
    }

    fn uncommitted(
        ttl: u64,
        min_commit_ts: impl Into<TimeStamp>,
        should_be_pushed: bool,
    ) -> impl FnOnce(TxnStatus) -> bool {
        move |s| {
            if let TxnStatus::Uncommitted {
                lock,
                min_commit_ts_pushed,
            } = s
            {
                lock.ttl == ttl
                    && lock.min_commit_ts == min_commit_ts.into()
                    && min_commit_ts_pushed == should_be_pushed
            } else {
                false
            }
        }
    }

    #[test]
    fn test_check_async_commit_txn_status() {
        let do_test = |rollback_if_not_exist: bool| {
            let mut engine = TestEngineBuilder::new().build().unwrap();
            let r = rollback_if_not_exist;

            // case 1: primary is prewritten (optimistic)
            must_prewrite_put_async_commit(&mut engine, b"k1", b"v", b"k1", &Some(vec![]), 1, 2);
            // All following check_txn_status should return the unchanged lock information
            // caller_start_ts == current_ts == 0
            must_success(
                &mut engine,
                b"k1",
                1,
                0,
                0,
                r,
                false,
                false,
                uncommitted(100, 2, false),
            );
            // caller_start_ts != 0
            must_success(
                &mut engine,
                b"k1",
                1,
                5,
                0,
                r,
                false,
                false,
                uncommitted(100, 2, false),
            );
            // current_ts != 0
            must_success(
                &mut engine,
                b"k1",
                1,
                0,
                8,
                r,
                false,
                false,
                uncommitted(100, 2, false),
            );
            // caller_start_ts != 0 && current_ts != 0
            must_success(
                &mut engine,
                b"k1",
                1,
                10,
                12,
                r,
                false,
                false,
                uncommitted(100, 2, false),
            );
            // caller_start_ts == u64::MAX
            must_success(
                &mut engine,
                b"k1",
                1,
                TimeStamp::max(),
                12,
                r,
                false,
                false,
                uncommitted(100, 2, false),
            );
            // current_ts == u64::MAX
            must_success(
                &mut engine,
                b"k1",
                1,
                12,
                TimeStamp::max(),
                r,
                false,
                false,
                uncommitted(100, 2, false),
            );
            // force_sync_commit = true
            must_success(
                &mut engine,
                b"k1",
                1,
                12,
                TimeStamp::max(),
                r,
                true,
                false,
                |s| s == TtlExpire,
            );
            must_unlocked(&mut engine, b"k1");
            must_get_rollback_protected(&mut engine, b"k1", 1, false);

            // case 2: primary is prewritten (pessimistic)
            must_acquire_pessimistic_lock(&mut engine, b"k2", b"k2", 15, 15);
            must_pessimistic_prewrite_put_async_commit(
                &mut engine,
                b"k2",
                b"v",
                b"k2",
                &Some(vec![]),
                15,
                16,
                DoPessimisticCheck,
                17,
            );
            // All following check_txn_status should return the unchanged lock information
            // caller_start_ts == current_ts == 0
            must_success(
                &mut engine,
                b"k2",
                15,
                0,
                0,
                r,
                false,
                false,
                uncommitted(100, 17, false),
            );
            // caller_start_ts != 0
            must_success(
                &mut engine,
                b"k2",
                15,
                18,
                0,
                r,
                false,
                false,
                uncommitted(100, 17, false),
            );
            // current_ts != 0
            must_success(
                &mut engine,
                b"k2",
                15,
                0,
                18,
                r,
                false,
                false,
                uncommitted(100, 17, false),
            );
            // caller_start_ts != 0 && current_ts != 0
            must_success(
                &mut engine,
                b"k2",
                15,
                19,
                20,
                r,
                false,
                false,
                uncommitted(100, 17, false),
            );
            // caller_start_ts == u64::MAX
            must_success(
                &mut engine,
                b"k2",
                15,
                TimeStamp::max(),
                20,
                r,
                false,
                false,
                uncommitted(100, 17, false),
            );
            // current_ts == u64::MAX
            must_success(
                &mut engine,
                b"k2",
                15,
                20,
                TimeStamp::max(),
                r,
                false,
                false,
                uncommitted(100, 17, false),
            );
            // force_sync_commit = true
            must_success(
                &mut engine,
                b"k2",
                15,
                20,
                TimeStamp::max(),
                r,
                true,
                false,
                |s| s == TtlExpire,
            );
            must_unlocked(&mut engine, b"k2");
            must_get_rollback_protected(&mut engine, b"k2", 15, true);

            // case 3: pessimistic transaction with two keys (large txn), secondary is
            // prewritten first
            must_acquire_pessimistic_lock_for_large_txn(&mut engine, b"k3", b"k3", 20, 20, 100);
            must_acquire_pessimistic_lock_for_large_txn(&mut engine, b"k4", b"k3", 20, 25, 100);
            must_pessimistic_prewrite_put_async_commit(
                &mut engine,
                b"k4",
                b"v",
                b"k3",
                &Some(vec![]),
                20,
                25,
                DoPessimisticCheck,
                28,
            );
            // the client must call check_txn_status with caller_start_ts == current_ts ==
            // 0, should not push
            must_success(
                &mut engine,
                b"k3",
                20,
                0,
                0,
                r,
                false,
                false,
                uncommitted(100, 21, false),
            );

            // case 4: pessimistic transaction with two keys (not large txn), secondary is
            // prewritten first
            must_acquire_pessimistic_lock_with_ttl(&mut engine, b"k5", b"k5", 30, 30, 100);
            must_acquire_pessimistic_lock_with_ttl(&mut engine, b"k6", b"k5", 30, 35, 100);
            must_pessimistic_prewrite_put_async_commit(
                &mut engine,
                b"k6",
                b"v",
                b"k5",
                &Some(vec![]),
                30,
                35,
                DoPessimisticCheck,
                36,
            );
            // the client must call check_txn_status with caller_start_ts == current_ts ==
            // 0, should not push
            must_success(
                &mut engine,
                b"k5",
                30,
                0,
                0,
                r,
                false,
                false,
                uncommitted(100, 0, false),
            );
        };

        do_test(true);
        do_test(false);
    }

    fn test_check_txn_status_impl(rollback_if_not_exist: bool) {
        let mut engine = TestEngineBuilder::new().build().unwrap();

        let (k, v) = (b"k1", b"v1");

        let r = rollback_if_not_exist;

        let ts = TimeStamp::compose;

        // Try to check a not exist thing.
        if r {
            must_success(
                &mut engine,
                k,
                ts(3, 0),
                ts(3, 1),
                ts(3, 2),
                r,
                false,
                false,
                |s| s == LockNotExist,
            );
            // A protected rollback record will be written.
            must_get_rollback_protected(&mut engine, k, ts(3, 0), true);
        } else {
            must_err(
                &mut engine,
                k,
                ts(3, 0),
                ts(3, 1),
                ts(3, 2),
                r,
                false,
                false,
            );
        }

        // Lock the key with TTL=100.
        must_prewrite_put_for_large_txn(&mut engine, k, v, k, ts(5, 0), 100, 0);
        // The initial min_commit_ts is start_ts + 1.
        must_large_txn_locked(&mut engine, k, ts(5, 0), 100, ts(5, 1), false);

        // CheckTxnStatus with caller_start_ts = 0 and current_ts = 0 should just return
        // the information of the lock without changing it.
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            0,
            0,
            r,
            false,
            false,
            uncommitted(100, ts(5, 1), false),
        );

        // Update min_commit_ts to current_ts.
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            ts(6, 0),
            ts(7, 0),
            r,
            false,
            false,
            uncommitted(100, ts(7, 0), true),
        );
        must_large_txn_locked(&mut engine, k, ts(5, 0), 100, ts(7, 0), false);

        // Update min_commit_ts to caller_start_ts + 1 if current_ts < caller_start_ts.
        // This case should be impossible. But if it happens, we prevents it.
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            ts(9, 0),
            ts(8, 0),
            r,
            false,
            false,
            uncommitted(100, ts(9, 1), true),
        );
        must_large_txn_locked(&mut engine, k, ts(5, 0), 100, ts(9, 1), false);

        // caller_start_ts < lock.min_commit_ts < current_ts
        // When caller_start_ts < lock.min_commit_ts, no need to update it, but pushed
        // should be true.
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            ts(8, 0),
            ts(10, 0),
            r,
            false,
            false,
            uncommitted(100, ts(9, 1), true),
        );
        must_large_txn_locked(&mut engine, k, ts(5, 0), 100, ts(9, 1), false);

        // current_ts < lock.min_commit_ts < caller_start_ts
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            ts(11, 0),
            ts(9, 0),
            r,
            false,
            false,
            uncommitted(100, ts(11, 1), true),
        );
        must_large_txn_locked(&mut engine, k, ts(5, 0), 100, ts(11, 1), false);

        // For same caller_start_ts and current_ts, update min_commit_ts to
        // caller_start_ts + 1
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            ts(12, 0),
            ts(12, 0),
            r,
            false,
            false,
            uncommitted(100, ts(12, 1), true),
        );
        must_large_txn_locked(&mut engine, k, ts(5, 0), 100, ts(12, 1), false);

        // Logical time is also considered in the comparing
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            ts(13, 1),
            ts(13, 3),
            r,
            false,
            false,
            uncommitted(100, ts(13, 3), true),
        );
        must_large_txn_locked(&mut engine, k, ts(5, 0), 100, ts(13, 3), false);

        must_commit(&mut engine, k, ts(5, 0), ts(15, 0));
        must_unlocked(&mut engine, k);

        // Check committed key will get the commit ts.
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            ts(12, 0),
            ts(12, 0),
            r,
            false,
            false,
            committed(ts(15, 0)),
        );
        must_unlocked(&mut engine, k);

        must_prewrite_put_for_large_txn(&mut engine, k, v, k, ts(20, 0), 100, 0);

        // Check a committed transaction when there is another lock. Expect getting the
        // commit ts.
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            ts(12, 0),
            ts(12, 0),
            r,
            false,
            false,
            committed(ts(15, 0)),
        );

        // Check a not existing transaction, the result depends on whether
        // `rollback_if_not_exist` is set.
        if r {
            must_success(
                &mut engine,
                k,
                ts(6, 0),
                ts(12, 0),
                ts(12, 0),
                r,
                false,
                false,
                |s| s == LockNotExist,
            );
            // And a rollback record will be written.
            must_seek_write(
                &mut engine,
                k,
                ts(6, 0),
                ts(6, 0),
                ts(6, 0),
                WriteType::Rollback,
            );
        } else {
            must_err(
                &mut engine,
                k,
                ts(6, 0),
                ts(12, 0),
                ts(12, 0),
                r,
                false,
                false,
            );
        }

        // TTL check is based on physical time (in ms). When logical time's difference
        // is larger than TTL, the lock won't be resolved.
        must_success(
            &mut engine,
            k,
            ts(20, 0),
            ts(21, 105),
            ts(21, 105),
            r,
            false,
            false,
            uncommitted(100, ts(21, 106), true),
        );
        must_large_txn_locked(&mut engine, k, ts(20, 0), 100, ts(21, 106), false);

        // If physical time's difference exceeds TTL, lock will be resolved.
        must_success(
            &mut engine,
            k,
            ts(20, 0),
            ts(121, 0),
            ts(121, 0),
            r,
            false,
            false,
            |s| s == TtlExpire,
        );
        must_unlocked(&mut engine, k);
        must_seek_write(
            &mut engine,
            k,
            TimeStamp::max(),
            ts(20, 0),
            ts(20, 0),
            WriteType::Rollback,
        );

        // Push the min_commit_ts of pessimistic locks.
        must_acquire_pessimistic_lock_for_large_txn(&mut engine, k, k, ts(4, 0), ts(130, 0), 200);
        must_large_txn_locked(&mut engine, k, ts(4, 0), 200, ts(130, 1), true);
        must_success(
            &mut engine,
            k,
            ts(4, 0),
            ts(135, 0),
            ts(135, 0),
            r,
            false,
            false,
            uncommitted(200, ts(135, 1), true),
        );
        must_large_txn_locked(&mut engine, k, ts(4, 0), 200, ts(135, 1), true);

        // Commit the key.
        must_pessimistic_prewrite_put(
            &mut engine,
            k,
            v,
            k,
            ts(4, 0),
            ts(130, 0),
            DoPessimisticCheck,
        );
        must_commit(&mut engine, k, ts(4, 0), ts(140, 0));
        must_unlocked(&mut engine, k);
        must_get_commit_ts(&mut engine, k, ts(4, 0), ts(140, 0));

        // Now the transactions are intersecting:
        // T1: start_ts = 5, commit_ts = 15
        // T2: start_ts = 20, rollback
        // T3: start_ts = 4, commit_ts = 140
        must_success(
            &mut engine,
            k,
            ts(4, 0),
            ts(10, 0),
            ts(10, 0),
            r,
            false,
            false,
            committed(ts(140, 0)),
        );
        must_success(
            &mut engine,
            k,
            ts(5, 0),
            ts(10, 0),
            ts(10, 0),
            r,
            false,
            false,
            committed(ts(15, 0)),
        );
        must_success(
            &mut engine,
            k,
            ts(20, 0),
            ts(10, 0),
            ts(10, 0),
            r,
            false,
            false,
            |s| s == RolledBack,
        );

        // Rollback expired pessimistic lock.
        must_acquire_pessimistic_lock_for_large_txn(&mut engine, k, k, ts(150, 0), ts(150, 0), 100);
        must_success(
            &mut engine,
            k,
            ts(150, 0),
            ts(160, 0),
            ts(160, 0),
            r,
            false,
            false,
            uncommitted(100, ts(160, 1), true),
        );
        must_large_txn_locked(&mut engine, k, ts(150, 0), 100, ts(160, 1), true);
        must_success(
            &mut engine,
            k,
            ts(150, 0),
            ts(160, 0),
            ts(260, 0),
            r,
            false,
            false,
            |s| s == TtlExpire,
        );
        must_unlocked(&mut engine, k);
        // Rolling back a pessimistic lock should leave Rollback mark.
        must_seek_write(
            &mut engine,
            k,
            TimeStamp::max(),
            ts(150, 0),
            ts(150, 0),
            WriteType::Rollback,
        );

        // Rollback when current_ts is u64::max_value()
        must_prewrite_put_for_large_txn(&mut engine, k, v, k, ts(270, 0), 100, 0);
        must_large_txn_locked(&mut engine, k, ts(270, 0), 100, ts(270, 1), false);
        must_success(
            &mut engine,
            k,
            ts(270, 0),
            ts(271, 0),
            TimeStamp::max(),
            r,
            false,
            false,
            |s| s == TtlExpire,
        );
        must_unlocked(&mut engine, k);
        must_seek_write(
            &mut engine,
            k,
            TimeStamp::max(),
            ts(270, 0),
            ts(270, 0),
            WriteType::Rollback,
        );

        must_acquire_pessimistic_lock_for_large_txn(&mut engine, k, k, ts(280, 0), ts(280, 0), 100);
        must_large_txn_locked(&mut engine, k, ts(280, 0), 100, ts(280, 1), true);
        must_success(
            &mut engine,
            k,
            ts(280, 0),
            ts(281, 0),
            TimeStamp::max(),
            r,
            false,
            false,
            |s| s == TtlExpire,
        );
        must_unlocked(&mut engine, k);
        must_seek_write(
            &mut engine,
            k,
            TimeStamp::max(),
            ts(280, 0),
            ts(280, 0),
            WriteType::Rollback,
        );

        // Don't push forward the min_commit_ts if the min_commit_ts of the lock is 0.
        must_acquire_pessimistic_lock_with_ttl(&mut engine, k, k, ts(290, 0), ts(290, 0), 100);
        must_success(
            &mut engine,
            k,
            ts(290, 0),
            ts(300, 0),
            ts(300, 0),
            r,
            false,
            false,
            uncommitted(100, TimeStamp::zero(), false),
        );
        must_large_txn_locked(&mut engine, k, ts(290, 0), 100, TimeStamp::zero(), true);
        pessimistic_rollback::tests::must_success(&mut engine, k, ts(290, 0), ts(290, 0));

        must_prewrite_put_impl(
            &mut engine,
            k,
            v,
            k,
            &None,
            ts(300, 0),
            SkipPessimisticCheck,
            100,
            TimeStamp::zero(),
            1,
            // min_commit_ts
            TimeStamp::zero(),
            // max_commit_ts
            TimeStamp::zero(),
            false,
            kvproto::kvrpcpb::Assertion::None,
            kvproto::kvrpcpb::AssertionLevel::Off,
        );
        must_success(
            &mut engine,
            k,
            ts(300, 0),
            ts(310, 0),
            ts(310, 0),
            r,
            false,
            false,
            uncommitted(100, TimeStamp::zero(), false),
        );
        must_large_txn_locked(&mut engine, k, ts(300, 0), 100, TimeStamp::zero(), false);
        must_rollback(&mut engine, k, ts(300, 0), false);

        must_prewrite_put_for_large_txn(&mut engine, k, v, k, ts(310, 0), 100, 0);
        must_large_txn_locked(&mut engine, k, ts(310, 0), 100, ts(310, 1), false);
        // Don't push forward the min_commit_ts if caller_start_ts is max, but pushed
        // should be true.
        must_success(
            &mut engine,
            k,
            ts(310, 0),
            TimeStamp::max(),
            ts(320, 0),
            r,
            false,
            false,
            uncommitted(100, ts(310, 1), true),
        );
        must_commit(&mut engine, k, ts(310, 0), ts(315, 0));
        must_success(
            &mut engine,
            k,
            ts(310, 0),
            TimeStamp::max(),
            ts(320, 0),
            r,
            false,
            false,
            committed(ts(315, 0)),
        );
    }

    #[test]
    fn test_check_txn_status() {
        test_check_txn_status_impl(false);
        test_check_txn_status_impl(true);
    }

    #[test]
    fn test_check_txn_status_resolving_pessimistic_lock() {
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let k = b"k1";
        let v = b"v1";
        let ts = TimeStamp::compose;

        // Check with resolving_pessimistic_lock flag.
        // Path: there is no commit or rollback record, no rollback record should be
        // written.
        must_success(
            &mut engine,
            k,
            ts(3, 0),
            ts(3, 0),
            ts(4, 0),
            true,
            false,
            true,
            |s| s == LockNotExistDoNothing,
        );
        must_get_rollback_ts_none(&mut engine, k, ts(5, 0));

        // Path: there is no commit or rollback record, error should be reported if
        // rollback_if_not_exist is set to false.
        must_err(
            &mut engine,
            k,
            ts(3, 0),
            ts(5, 0),
            ts(5, 0),
            false,
            false,
            true,
        );

        // Path: the pessimistic primary key lock does exist, and it's not expired yet.
        must_acquire_pessimistic_lock_with_ttl(&mut engine, k, k, ts(10, 0), ts(10, 0), 10);
        must_pessimistic_locked(&mut engine, k, ts(10, 0), ts(10, 0));
        must_success(
            &mut engine,
            k,
            ts(10, 0),
            ts(11, 0),
            ts(11, 0),
            true,
            false,
            true,
            uncommitted(10, TimeStamp::zero(), false),
        );

        // Path: the pessimistic primary key lock does exist, and it's expired, the
        // primary lock will be pessimistically rolled back but there will not
        // be a rollback record.
        must_success(
            &mut engine,
            k,
            ts(10, 0),
            ts(21, 0),
            ts(21, 0),
            true,
            false,
            true,
            |s| s == PessimisticRollBack,
        );
        must_unlocked(&mut engine, k);
        must_get_rollback_ts_none(&mut engine, k, ts(22, 0));

        // Path: the prewrite primary key lock does exist, and it's not expired yet.
        // Should return locked status.
        must_prewrite_put_impl(
            &mut engine,
            k,
            v,
            k,
            &None,
            ts(30, 0),
            SkipPessimisticCheck,
            10,
            TimeStamp::zero(),
            1,
            // min_commit_ts
            TimeStamp::zero(),
            // max_commit_ts
            TimeStamp::zero(),
            false,
            kvproto::kvrpcpb::Assertion::None,
            kvproto::kvrpcpb::AssertionLevel::Off,
        );
        must_success(
            &mut engine,
            k,
            ts(30, 0),
            ts(31, 0),
            ts(31, 0),
            true,
            false,
            true,
            uncommitted(10, TimeStamp::zero(), false),
        );

        // Path: the prewrite primary key expired and the solving key is a pessimistic
        // lock, rollback record should be written and the transaction status is
        // certain.
        must_success(
            &mut engine,
            k,
            ts(30, 0),
            ts(41, 0),
            ts(41, 0),
            true,
            false,
            true,
            |s| s == TtlExpire,
        );
        must_unlocked(&mut engine, k);
        must_get_rollback_ts(&mut engine, k, ts(30, 0));

        // Path: the resolving_pessimistic_lock is false and the primary key lock is
        // pessimistic lock, the transaction is in commit phase and the rollback
        // record should be written.
        must_acquire_pessimistic_lock_with_ttl(&mut engine, k, k, ts(50, 0), ts(50, 0), 10);
        must_pessimistic_locked(&mut engine, k, ts(50, 0), ts(50, 0));
        must_success(
            &mut engine,
            k,
            ts(50, 0),
            ts(61, 0),
            ts(61, 0),
            true,
            false,
            // resolving_pessimistic_lock
            false,
            |s| s == TtlExpire,
        );
        must_unlocked(&mut engine, k);
        must_get_rollback_ts(&mut engine, k, ts(50, 0));
    }

    #[test]
    fn test_rollback_calculate_last_change_info() {
        let mut engine = crate::storage::TestEngineBuilder::new().build().unwrap();
        let k = b"k";

        // Below is a case explaining why we don't calculate last_change_ts for
        // rollback.

        must_prewrite_put(&mut engine, k, b"v1", k, 5);
        must_commit(&mut engine, k, 5, 6);

        must_prewrite_put(&mut engine, k, b"v2", k, 7);
        // When we calculate last_change_ts here, we will get 6.
        must_rollback(&mut engine, k, 10, true);
        // But we can still commit with ts 8, then the last_change_ts of the rollback
        // will be incorrect.
        must_commit(&mut engine, k, 7, 8);

        let rollback = must_written(&mut engine, k, 10, 10, WriteType::Rollback);
        assert!(rollback.last_change_ts.is_zero());
        assert_eq!(rollback.versions_to_last_change, 0);
    }
}
