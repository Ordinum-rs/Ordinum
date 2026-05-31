use crate::sync::Condvar;
use crate::sync::Mutex;
use crate::sync::atomic::AtomicU64;

//
pub(crate) struct SyncPermit {
    sync_occupancy: AtomicU64,
    sync_cv: Condvar,
    sync_mu: Mutex<()>,
}

impl Default for SyncPermit {
    fn default() -> Self {
        Self {
            sync_occupancy: AtomicU64::new(0),
            sync_cv: Condvar::new(),
            sync_mu: Mutex::new(()),
        }
    }
}

// Implement drop for if prepare fails and we can return occupancy
