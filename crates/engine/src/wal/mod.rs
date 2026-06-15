pub(crate) mod writer;

// --------------------------------------

use crate::sync::Arc;
use crate::sync::Condvar;
use crate::sync::Mutex;
use crate::sync::atomic::AtomicBool;
use crate::sync::atomic::AtomicU64;
use crate::{Error, Result};

pub(crate) type SyncQueueSem = Arc<SyncSem>;

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

enum SyncState {
    Pending,
    Complete(Result<()>),
}

pub(crate) type SyncWaiter = Arc<SyncLogWaiter>;

pub(crate) struct SyncLogWaiter {
    state: Mutex<SyncState>,
    cv: Condvar,
}

impl Default for SyncLogWaiter {
    fn default() -> Self {
        Self {
            state: Mutex::new(SyncState::Pending),
            cv: Condvar::new(),
        }
    }
}

// Impl for SyncLogWaiter

impl SyncLogWaiter {
    pub(crate) fn wait(&self) -> Result<()> {
        // TODO: Finish this

        Ok(())
    }
}
