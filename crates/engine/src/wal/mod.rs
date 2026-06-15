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

// ---- Sync Log Waiter ---- //

/* NOTE: Currently Im thinking to keep WalSyncError very lightweight when we pass them back to Batches. We want to give enough of a distinction
// between error types to make decision, but for the heavy error, I want to store in the WAL state itself. That way, the batch (or db level once propagated)
// can return the heavy error to the caller
//
// NOTE: Do we actually need Error distinctions here?
// Need to think about what the Batch actually needs to get back in terms of WAL error and how we get this to the caller
// */
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
        }
    }
}

// Impl for SyncLogWaiter

impl SyncLogWaiter {
    // TODO: Test this
    pub(crate) fn wait(&self) -> std::result::Result<(), WalSyncResultState> {
        // Fast path - may not need to wait on condvar
        for _ in 0..200 {
            let state = WalSyncResultState::from(self.state.load(Ordering::Acquire));

            // TODO: Do full match branch here

            spin_loop();
        }

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

        // TODO: Do full match branch here to return a result

        Ok(())
    }
}
