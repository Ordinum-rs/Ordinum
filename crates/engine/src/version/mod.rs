pub(crate) mod memtable_list;
pub(crate) mod superversion;
pub(crate) mod version_set;

use crate::sync::atomic::AtomicU64;
use crate::sync::atomic::Ordering;

// ----------------- Sequence Number ----------------- //

pub(crate) struct SeqNumState {
    //
    /// Highest sequence number that readers are allowed to observe.
    pub(crate) visible_seq_num: AtomicU64,
    //
    /// Upper bound of sequence numbers reserved / assigned into the commit pipeline.
    pub(crate) log_seq_num: AtomicU64,
}

impl Default for SeqNumState {
    fn default() -> Self {
        SeqNumState::new()
    }
}

impl SeqNumState {
    pub(crate) fn new() -> Self {
        Self {
            // TODO: Need to look into if we initialise with zero or is there some minimal number we initialise a seq_num with to reserve lower number space for
            log_seq_num: AtomicU64::new(0),
            visible_seq_num: AtomicU64::new(0),
        }
    }

    pub(crate) fn load_log_seq_num(&self, ordering: Ordering) -> u64 {
        self.log_seq_num.load(ordering)
    }
}
