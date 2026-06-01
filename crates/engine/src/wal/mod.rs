pub(crate) mod writer;

// --------------------------------------

use crate::sync::Arc;
use crate::sync::Condvar;
use crate::sync::Mutex;
use crate::sync::atomic::AtomicU64;

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
