//
// Protyping a commit pipeline similar to pebble - rather than a leader/follower rocks style write_thread
//

use std::{
    array,
    ptr::null_mut,
    sync::{
        Condvar, Mutex,
        atomic::{AtomicPtr, AtomicU64},
    },
};

use crate::{
    db::batch::BatchInner,
    utils::{self, cache_padded::CachePadded},
};

// NOTE:
// We'd want this compile time constant but may want it also to configurable
// CONFIG: Compile constant choices for config?
pub(crate) const WRITE_PIPELINE_SIZE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeadTail(u64);

impl HeadTail {
    const DEQUEUE_BITS: u32 = 32;
    const MASK: u64 = (1u64 << Self::DEQUEUE_BITS) - 1;

    #[inline(always)]
    fn pack(head: u32, tail: u32) -> Self {
        Self(((head as u64) << Self::DEQUEUE_BITS) | ((tail as u64) & Self::MASK))
    }

    #[inline(always)]
    fn unpack(self) -> (u32, u32) {
        let head = ((self.0 >> Self::DEQUEUE_BITS) & Self::MASK) as u32;
        let tail = (self.0 & Self::MASK) as u32;

        (head, tail)
    }

    #[inline(always)]
    fn raw(self) -> u64 {
        self.0
    }

    #[inline(always)]
    fn from_raw(raw: u64) -> Self {
        Self(raw)
    }
}

// Logic
//
// Queue:
// [B1][B2][B3][B4]
//
// Producer:
// Current thread holding BatchQueue Mutex
// Consumer:
// All Committing Threads
//
//index:  0   1   2   3   4   5   6   7
//      [ _ |B1 |B2 | _ | _ | _ | _ | _ ]
//
// head = 3
// tail = 1
// Range = [tail, head) i.e. [1,3)
//
// Each slot = AtomicPtr<BatchInner>
// Null slot = Producer Owns Slot
// Non Null  = Consumer Owns Slot
//
//

// Referencing
// https://github.com/cockroachdb/pebble/blob/a3b8dfe9/commit.go#L24
struct BatchQueue<const N: usize> {
    head_tail: CachePadded<AtomicU64>,
    slots: [AtomicPtr<BatchInner>; N],
}

impl<const N: usize> BatchQueue<N> {
    pub(crate) fn new() -> Self {
        assert!(N.is_power_of_two());
        assert!(N <= 1024); // TODO: Make constant MAX Queue size
        Self {
            head_tail: CachePadded::new(AtomicU64::new(0)),
            slots: array::from_fn(|_| AtomicPtr::new(null_mut())),
        }
    }
}

// NOTE: Think about this more - needs to be cleaner
pub(crate) type BatchQueueDefault = BatchQueue<WRITE_PIPELINE_SIZE>;

pub(crate) struct WritePipeline {
    batch_queue: BatchQueueDefault,
    batch_permits: AtomicU64,

    // NOTE: Need some sort of capacity reservation for batch queue

    //
    mu: Mutex<()>,
    signal: Condvar,
}

#[cfg(test)]
mod tests {
    use std::{ptr, thread};

    use crate::db::batch::Batch;

    use super::*;

    #[test]
    fn head_tail_masking() {
        let head = 2;
        let tail = 4;

        let mut packed = HeadTail::pack(head, tail);

        let (h, t) = packed.unpack();
        assert!(h == 2);
        assert!(t == 4);
    }

    #[test]
    fn two_threads_see_batch() {
        //
        //
        //
        let global_queue = BatchQueue::<4>::new();
        //
        // Setting the headtail at beginning of scope because we know what slots are
        // going to be used and by what batch
        //
        global_queue.head_tail.store(
            HeadTail::pack(3, 1).raw(),
            std::sync::atomic::Ordering::Release,
        );

        thread::scope(|s| {
            // SAFETY:
            // Each spawn thread should null it's global queue pointer before exiting
            // This simulates the commit lifetime

            // Batch 1
            s.spawn(|| {
                //
                let b1 = Batch::new();

                // We would be taking the mutex here after reserving space
                global_queue.slots[0].store(
                    ptr::from_ref(b1.as_ref()).cast_mut(),
                    std::sync::atomic::Ordering::Release,
                );

                // Need to null global pointer
            });

            // Batch 2
            s.spawn(|| {
                let b2 = Batch::new();

                // We would be taking the mutex here after reserving space
                global_queue.slots[1].store(
                    ptr::from_ref(b2.as_ref()).cast_mut(),
                    std::sync::atomic::Ordering::Release,
                );
                //

                // Need to null global pointer
            });
        })
    }
}
