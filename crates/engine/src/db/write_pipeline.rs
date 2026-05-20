//
// Protyping a commit pipeline similar to pebble - rather than a leader/follower rocks style write_thread
//

use std::{
    array,
    hint::spin_loop,
    ptr::{self, NonNull, null_mut},
    sync::{
        Condvar, Mutex,
        atomic::{AtomicPtr, AtomicU64, Ordering},
    },
};

use crate::{
    db::batch::{Batch, BatchInner, Sealed},
    db::options::DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE,
    utils::{self, cache_padded::CachePadded},
};

//
//
// HeadTail
// +-------------------+-------------------+
// |   head (upper)    |   tail (lower)    |
// +-------------------+-------------------+
// 63                32 31                0

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
    fn unpack_unchecked(packed: u64) -> (u32, u32) {
        let head = ((packed >> Self::DEQUEUE_BITS) & Self::MASK) as u32;
        let tail = (packed & Self::MASK) as u32;

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

// XXX: Later we may want to move this to a configurable type
pub(crate) type BatchQueueDefault = BatchQueue<DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE>;

// Referencing
// https://github.com/cockroachdb/pebble/blob/a3b8dfe9/commit.go#L24
//
/// BatchQueue is a bounded SPMC (single-producer, multi-consumer)
/// ring buffer of commit-ready batches.
///
/// Producer-side invariants:
/// - The queue itself does not synchronize producers.
/// - The commit pipeline guarantees that only one producer may call
///   `enqueue()` at a time (typically via the commit mutex).
/// - The producer owns the slot at `head` until `head` is advanced,
///   at which point ownership transfers to consumers.
///
/// Consumer-side invariants:
/// - Consumers run lock-free and may concurrently inspect and dequeue
///   published batches.
/// - Consumers compete to atomically advance `tail` via CAS.
/// - Once a consumer successfully claims a slot, it clears the slot,
///   returning ownership back to the producer for reuse.
///
/// `head_tail` packs:
/// - upper 32 bits: next logical head position (producer-owned)
/// - lower 32 bits: oldest logical tail position (consumer-owned)
#[derive(Debug)]
struct BatchQueue<const N: usize> {
    head_tail: CachePadded<AtomicU64>,
    slots: [AtomicPtr<Batch<Sealed>>; N],
}

impl<const N: usize> BatchQueue<N> {
    //
    pub(crate) const fn size() -> usize {
        N
    }

    pub(crate) fn new() -> Self {
        assert!(N.is_power_of_two());
        assert!(N <= 1024); // TODO: Make constant MAX Queue size
        Self {
            head_tail: CachePadded::new(AtomicU64::new(0)),
            slots: array::from_fn(|_| AtomicPtr::new(null_mut())),
        }
    }

    // Enqueueing into the BatchQueue should be done under a Mutex lock
    pub(crate) fn enqueue(&self, batch: NonNull<Batch<Sealed>>) {
        let (head, tail) = HeadTail::unpack_unchecked(self.head_tail.load(Ordering::Relaxed));

        // Queue should not be full as we should have reserved space already - if it is we need to panic
        if tail.wrapping_add(N as u32) == head {
            panic!("Queue full - reservation failed")
        }

        let slot = &self.slots[(head & N as u32 - 1) as usize];

        // Need to check if the slot is null - if is not, then another consumer is still processing and we must wait
        if !slot.load(Ordering::Acquire).is_null() {
            spin_loop();
            //
        }

        // Once we're here we own the slot - all consumers are finished on it
        slot.store(batch.as_ptr(), Ordering::Release);

        // Increment the head for the next producer and trasnfers ownership to consumers which will see the newly published slot and be
        // able to load it
        self.head_tail
            .fetch_add(1u64 << HeadTail::DEQUEUE_BITS, Ordering::Release);
    }

    // try_dequeue attempts to remove the oldest batch in the queue and advance the tail if the batch is applied
    //
    // If an earlier batch is not yet applied or the queue is empty then we return nil
    pub(crate) fn try_dequeue(&self) -> Option<NonNull<Batch<Sealed>>> {
        //
        let mut ht = HeadTail::from_raw(self.head_tail.load(Ordering::Acquire)).raw();

        loop {
            let (head, tail) = HeadTail::unpack_unchecked(ht);
            if tail == head {
                return None;
            }

            // Get the slot
            let slot = &self.slots[(tail & (N as u32) - 1) as usize];
            let batch = slot.load(Ordering::Acquire);

            // If batch is null then it has been dequeue by another, if the batch is not yet applied then it is not ready
            if batch.is_null() || !unsafe { &*batch }.is_applied(Ordering::Acquire) {
                return None;
            }

            let new_ht = HeadTail::pack(head, tail.wrapping_add(1)).raw();

            match self
                .head_tail
                .compare_exchange(ht, new_ht, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    // We won ownership of this tail slot. Clearing the slot returns
                    // physical slot ownership to the producer so it may be reused
                    // after wraparound.
                    slot.store(ptr::null_mut(), Ordering::Release);
                    return Some(unsafe { NonNull::new_unchecked(batch) });
                }
                Err(actual_ht) => ht = actual_ht,
            }
        }
    }
}

// ------------------------- WriterEnv ------------------------- //
//
// The WriterEnv trait provides the boundary where the WritePipeline hands
// execution back to the storage engine once commit ordering has been
// established.
//
// Commit flow:
//
// 1. WritePipeline reserves queue capacity and establishes commit order
// 2. Sequence numbers are reserved and assigned to the batch
// 3. WriterEnv prepares storage state for the write
//      - detect/write stall if necessary
//      - rotate mutable memtables if required
//      - append batch to WAL
// 4. WriterEnv applies batch mutations into memtables
// 5. WritePipeline publishes completed batches in sequence order
//
// This separation keeps the WritePipeline focused on ordering semantics
// while allowing the DB layer to retain ownership of storage policy and
// lifecycle management.

// TODO: Finish the trait
pub(crate) trait WriterEnv {
    //
    // fn prepare_commit(&BatchSealed>) -> Result<(),()>
    //
    // fn apply_commit(&BatchSealed>) -> Result<(),()>
}

/// WritePipeline is the coordinator responsible for processing batches committed by caller threads on the write path.
/// Batches are queued into a Single-Producer-Multi-Consumer queue and committed through stages of a state machine
///
/// 1 - Synchronised Producers enqueue to preserve order
/// 2 - Sequence numbers are reserved and assigned to batches
/// 3 - Batches are written to the WAL
/// 4 - Caller threads concurrently insert their batches into memtables
/// 5 - Batches are made visible to Readers by dequeuing batches that have been applied whilst retaining order
///
/// Maintaining order is the key. Batches with a higher sequence number that are applied sooner than those with a lesser sequence number in the queue will not
/// be dequeued but must wait until previous batches in the queue have completed and made their sequence numbers visible to readers
/// This preserves the logical ordering of data which is committed and applied to the database
///
/// DOCS: Continue to work on the DOC
pub(crate) struct WritePipeline {
    batch_queue: BatchQueueDefault,
    batch_permits: AtomicU64,

    //
    mu: Mutex<()>,
    signal: Condvar,
}

impl WritePipeline {
    pub(crate) fn new() -> Self {
        Self {
            batch_queue: BatchQueueDefault::new(),
            batch_permits: AtomicU64::new(DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE as u64),
            mu: Mutex::new(()),
            signal: Condvar::new(),
        }
    }

    // TODO: Need try_acquire_token()
    // TODO: Need wait()

    pub(crate) fn commit(
        &self,
        batch: NonNull<Batch<Sealed>>,
        sync_wal: bool, /* NOTE: can possibly use options struct or config here */
    ) -> Result<(), ()> {
        // NOTE: Any assertions here?

        // Need to try_acquire a token - if not we wait()

        // Hand off to DB which will carry out the write

        //
        //
        //
        todo!()
    }

    pub(crate) fn prepare_commit(&self, batch: NonNull<Batch<Sealed>>) -> Result<(), ()> {
        todo!()
    }

    //
}

#[cfg(test)]
mod tests {
    use std::{ptr, thread};

    use crate::db::{batch::Batch, write_pipeline::tests::queue_harness::Harness};

    use super::*;

    mod queue_harness {
        use std::{
            ptr::NonNull,
            sync::{Condvar, Mutex},
            thread::{self, Scope, ScopedJoinHandle},
        };

        use crate::db::{
            batch::{Batch, Sealed},
            write_pipeline::BatchQueue,
        };

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(super) enum Stage {
            Init,
            Enqueued,
            Ready,
            Done,
        }

        #[derive(Debug)]
        struct GateState {
            stages: Vec<Stage>,
            permits: Vec<u64>,
        }

        pub(super) struct Harness<const N: usize> {
            pub(super) queue: BatchQueue<N>,
            mu: Mutex<GateState>,
            cv: Condvar,
        }

        #[derive(Debug)]
        pub(super) struct ConsumerCtx {
            pub(super) published: bool,
            pub(super) applied: bool,
            pub(super) self_dequeued: bool,
            pub(super) did_dequeue: bool,
        }

        impl<const N: usize> Harness<N> {
            pub(super) fn new(writer_count: usize) -> Self {
                Self {
                    queue: BatchQueue::<N>::new(),
                    mu: Mutex::new(GateState {
                        stages: vec![Stage::Init; writer_count],
                        permits: vec![0u64; writer_count],
                    }),
                    cv: Condvar::new(),
                }
            }

            fn set_stage(&self, id: usize, stage: Stage) {
                let mut g = self.mu.lock().unwrap();
                g.stages[id] = stage;
                self.cv.notify_all();
            }

            pub(super) fn wait_until(&self, id: usize, stage: Stage) {
                let mut g = self.mu.lock().unwrap();

                while g.stages[id] != stage {
                    g = self.cv.wait(g).unwrap();
                }
            }

            pub(super) fn release(&self, id: usize) {
                let mut g = self.mu.lock().unwrap();
                g.permits[id] += 1;
                self.cv.notify_all();
            }

            fn wait_released(&self, id: usize, permit: u64) {
                let mut g = self.mu.lock().unwrap();

                while g.permits[id] < permit {
                    g = self.cv.wait(g).unwrap();
                }
            }

            pub(super) fn spawn_batch<'scope, F, C>(
                &'scope self,
                scope: &'scope Scope<'scope, '_>,
                id: usize,
                config: C,
                f: F,
            ) -> ScopedJoinHandle<'scope, ConsumerCtx>
            where
                F: FnOnce(NonNull<Batch<Sealed>>, &BatchQueue<N>) -> Option<NonNull<Batch<Sealed>>>
                    + Send
                    + 'scope,
                C: FnOnce(&Batch<Sealed>) + Send + 'scope,
            {
                scope.spawn(move || {
                    let batch = Batch::new().seal();
                    let b_ptr = batch.non_null_ptr();

                    self.wait_released(id, 1);
                    self.queue.enqueue(b_ptr);
                    self.set_stage(id, Stage::Enqueued);

                    config(&batch);

                    self.wait_released(id, 2);
                    self.set_stage(id, Stage::Ready);

                    let r = f(b_ptr, &self.queue);

                    let self_dq = matches!(r, Some(ptr) if ptr == b_ptr);

                    self.set_stage(id, Stage::Done);

                    ConsumerCtx {
                        published: batch.is_published(std::sync::atomic::Ordering::Relaxed),
                        applied: batch.is_applied(std::sync::atomic::Ordering::Relaxed),
                        self_dequeued: self_dq,
                        did_dequeue: r.is_some(),
                    }
                })
            }
        }
    }

    #[test]
    fn two_consumers_race_dequeue() {
        //
        let h = Harness::<2>::new(2);

        thread::scope(|s| {
            let b1 = h.spawn_batch(
                s,
                0,
                |b| {
                    b.mark_applied(Ordering::Relaxed);
                },
                |ptr, q| q.try_dequeue(),
            );
            let b2 = h.spawn_batch(s, 1, |_| {}, |ptr, q| q.try_dequeue());

            h.release(0);
            h.wait_until(0, queue_harness::Stage::Enqueued);

            h.release(1);
            h.wait_until(1, queue_harness::Stage::Enqueued);

            h.release(0);
            h.release(1);

            let r1 = b1.join().unwrap();
            let r2 = b2.join().unwrap();

            // B1 should have applied and dequeued successfully while B2 should not have

            assert!(r1.did_dequeue == true);
            assert!(r2.did_dequeue == false);
        })
    }

    #[test]
    fn batch_size() {
        assert_eq!(BatchQueue::<4>::size(), 4);
    }

    #[test]
    fn head_tail_masking() {
        let head = 2;
        let tail = 4;

        let packed = HeadTail::pack(head, tail);

        let (h, t) = packed.unpack();
        assert!(h == 2);
        assert!(t == 4);
    }

    #[test]
    #[should_panic]
    fn full_queue() {
        // [B1, B2, B3, B4]
        //  T            H
        //
        let ht = HeadTail::pack(5, 1);

        let batch_q = BatchQueue::<4>::new();
        batch_q.head_tail.store(ht.raw(), Ordering::Release);

        let batch = Batch::new().seal();

        batch_q.enqueue(unsafe { NonNull::new_unchecked(ptr::from_ref(&batch).cast_mut()) });
    }

    #[test]
    fn enqueue_batch() {
        let batch = Batch::new().seal();

        let batch_q = BatchQueue::<4>::new();

        batch_q.enqueue(batch.non_null_ptr());

        assert!(batch_q.slots[0].load(Ordering::Relaxed) == ptr::from_ref(&batch).cast_mut());
        let (h, _) = HeadTail::unpack_unchecked(batch_q.head_tail.load(Ordering::Relaxed));
        assert!(h == 1);
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
                let mut b1 = Batch::new().seal();

                // We would be taking the mutex here after reserving space
                global_queue.slots[0].store(&raw mut b1, std::sync::atomic::Ordering::Release);

                // Need to null global pointer
            });

            // Batch 2
            s.spawn(|| {
                let mut b2 = Batch::new().seal();

                // We would be taking the mutex here after reserving space
                global_queue.slots[1].store(&raw mut b2, std::sync::atomic::Ordering::Release);
                //

                // Need to null global pointer
            });
        })
    }
}
