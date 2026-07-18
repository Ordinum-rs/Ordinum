pub(crate) mod manager;
pub(crate) mod writer;

// --------------------------------------

use crate::sync::Arc;
use crate::sync::Condvar;
use crate::sync::Mutex;
use crate::sync::atomic::AtomicBool;
use crate::sync::atomic::AtomicU8;
use crate::sync::atomic::AtomicU64;
use crate::sync::atomic::Ordering;
use crate::sync::spin_loop;
use crate::{Error, Result};

// TODO: Need to make SyncQueue

// ---- SyncQueue Sem ---- //

pub(crate) type SyncQueueSem = Arc<SyncSem>;

/// Global WAL sync semaphore.
///
/// This is pipeline-level backpressure, not a per-batch completion signal. The
/// write path uses it to bound outstanding fsync work so writers cannot enqueue
/// unbounded WAL durability requests while batches remain live.
pub(crate) struct SyncSem {
    sync_occupancy: AtomicU64,
    sync_cv: Condvar,
    sync_mu: Mutex<()>,
}

impl Default for SyncSem {
    fn default() -> Self {
        Self {
            sync_occupancy: AtomicU64::new(0),
            sync_cv: Condvar::new(),
            sync_mu: Mutex::new(()),
        }
    }
}

// TODO: Finish the SyncSem
// NOTE: I wonder if we can write a util for a sem which we can wrap in our own type?

// ---- Sync Log Waiter ---- //
//

// Note on error handling

// Sync completion is tracked per-batch through WalSyncState rather than storing
// a full error object on every batch.
//
// The state answers:
//
//     "Did my batch succeed or fail?"
//
// while the DB-level background error answers:
//
//     "Why did the failure occur?"
//
// Example:
//
//     Group 1
//     -------
//     Batch A
//     fsync succeeds
//
//     Group 2
//     -------
//     Batch B
//     Batch C
//     fsync fails with EIO
//
// Completion state:
//
//     Batch A -> SyncDone
//     Batch B -> IoError
//     Batch C -> IoError
//
// DB background error:
//
//     db.error() -> EIO
//
// This separation allows callers to determine whether a specific batch was
// affected by a failure while avoiding duplication of heavyweight error objects
// on the write path.
//
// Importantly, a later DB error does not retroactively affect successful
// batches:
//
//     batch_a.wait() -> Ok(())
//     batch_b.wait() -> Err(IoError)
//     batch_c.wait() -> Err(IoError)
//
//     db.error() -> EIO
//
// The batch state identifies the failing operation, while the DB error provides
// detailed diagnostic information for the underlying failure.

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalSyncResultState {
    Init = 0,
    SyncDone = 1,
    IoError = 2,
    WalError = 3,
}

impl From<WalSyncResultState> for u8 {
    fn from(state: WalSyncResultState) -> Self {
        state as u8
    }
}

impl From<u8> for WalSyncResultState {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Init,
            1 => Self::SyncDone,
            2 => Self::IoError,
            3 => Self::WalError,
            _ => unreachable!("invalid WAL sync state"),
        }
    }
}

pub(crate) type SyncWaiter = Arc<SyncLogWaiter>;

/// Per-batch WAL sync completion signal.
///
/// A Batch owns one stable waiter for its lifetime. The write path clones this
/// Arc and hands the clone to the WAL/fsync worker. Once the worker has made
/// that batch's WAL bytes durable, it signals this waiter so the batch owner can
/// safely return, reset, or recycle the batch.
///
/// `state` is atomic so callers can spin briefly on the fast path before
/// falling back to the mutex/condvar path for longer waits. The mutex still
/// coordinates condvar sleep/wake transitions to avoid missed notifications.
pub(crate) struct SyncLogWaiter {
    state: AtomicU8,
    mu: Mutex<()>,
    cv: Condvar,
}

impl Default for SyncLogWaiter {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(WalSyncResultState::Init.into()),
            mu: Mutex::new(()),
            cv: Condvar::new(),
            // NOTE: We can potentially store a Arc<WalError> BUT - it would be quite heavy
            // error: Mutex<Option<Arc<WalError>>>,
        }
    }
}

// Impl for SyncLogWaiter

impl SyncLogWaiter {
    #[inline(always)]
    fn is_terminal(&self, state: WalSyncResultState) -> bool {
        state != WalSyncResultState::Init
    }

    #[inline(always)]
    fn to_result(state: WalSyncResultState) -> Option<std::result::Result<(), WalSyncResultState>> {
        match state {
            WalSyncResultState::SyncDone => Some(Ok(())),
            WalSyncResultState::IoError => Some(Err(WalSyncResultState::IoError)),
            WalSyncResultState::WalError => Some(Err(WalSyncResultState::WalError)),
            WalSyncResultState::Init => None,
        }
    }

    // TODO: Test this
    pub(crate) fn wait(&self) -> std::result::Result<(), WalSyncResultState> {
        // Fast path - may not need to wait on condvar
        for _ in 0..200 {
            let state = WalSyncResultState::from(self.state.load(Ordering::Acquire));

            if state == WalSyncResultState::Init {
                spin_loop();
                continue;
            }

            if let Some(result) = Self::to_result(state) {
                return result;
            }

            unreachable!("invalid terminal WAL sync state");
        }

        // TODO: Insert TEST_SYNC_POINT

        // Fallback to condvar

        let mut guard = self
            .mu
            .lock()
            .unwrap_or_else(|e| panic!("error on unwrapping the sync waiter mutex: {e}"));

        while WalSyncResultState::from(self.state.load(Ordering::Acquire))
            == WalSyncResultState::Init
        {
            guard = self
                .cv
                .wait(guard)
                .unwrap_or_else(|e| panic!("error waiting on sync waiter condvar: {e}"));
        }

        match Self::to_result(WalSyncResultState::from(self.state.load(Ordering::Acquire))) {
            Some(r) => return r,
            None => {
                // Should panic but we need to make sure that unwinding the thread doesn't cause UB through other
                // processes that may have references or access to thread local state

                // Do this for now
                return Err(WalSyncResultState::WalError);
            }
        }
    }

    // Signalling

    pub(crate) fn signal(&self, state: WalSyncResultState) {
        assert!(state != WalSyncResultState::Init);

        let _guard = self
            .mu
            .lock()
            .unwrap_or_else(|e| panic!("error on unwrapping the sync waiter mutex: {e}"));

        self.state.store(state as u8, Ordering::Release);
        self.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_sync_waiter() {
        //

        //
    }
}
