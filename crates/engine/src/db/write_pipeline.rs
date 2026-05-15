//
// Protyping a commit pipeline similar to pebble - rather than a leader/follower rocks style write_thread
//

use std::sync::{Condvar, Mutex, atomic::AtomicU64};

use crate::utils::{self, cache_padded::CachePadded};

// Logic
//
// Queue:
// [B1][B2][B3][B4]
//
// Producer:
// Current thread holding BatchQueue Mutex
//
// Consumer:
// All Committing Threads
//
//
//
//
//
//
//

// Referencing
// https://github.com/cockroachdb/pebble/blob/a3b8dfe9/commit.go#L24
struct BatchQueue {
    head_tail: CachePadded<AtomicU64>,
    slots: [u8; 1], // TODO:
}

pub(crate) struct WritePipeline {
    batch_queue: BatchQueue,

    // NOTE: Need some sort of capacity reservation for batch queue

    //
    mu: Mutex<()>,
    signal: Condvar,
}
