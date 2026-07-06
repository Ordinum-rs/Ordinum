use std::{
    array,
    ptr::{self, NonNull, null_mut},
    sync::atomic::AtomicBool,
};

use crate::{
    db::batch::{BatchRef, NonNullBatchPtr},
    sync::atomic::{AtomicPtr, AtomicU16, AtomicU64, Ordering},
    version::SeqNumState,
    wal::SyncQueueSem,
};

use crate::{
    Error, Result,
    db::{
        batch::{Batch, BatchObject, Sealed},
        options::DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE,
    },
    sync::Arc,
    sync::Condvar,
    sync::Mutex,
    sync::MutexGuard,
    sync::spin_loop,
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
// Each slot = AtomicPtr<Batch>
// Null slot = Producer Owns Slot
// Non Null  = Consumer Owns Slot
//
//

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
    slots: [AtomicPtr<Batch>; N],
}

impl<const N: usize> BatchQueue<N> {
    //
    pub(crate) const SIZE: usize = N;

    pub(crate) const fn size(&self) -> usize {
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
    pub(crate) fn enqueue(&self, batch: NonNull<Batch>) {
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
    pub(crate) fn try_dequeue(&self) -> Option<NonNull<Batch>> {
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
pub(crate) trait WriterEnv: Send + Sync {
    //
    // NOTE: Happens under Mutex lock
    fn prepare_commit<'env>(&self, batch: &'env BatchRef) -> Result<()>;
    //
    // NOTE: No Lock - concurrent application to memtables
    fn apply_commit<'env>(&self, batch: &'env BatchRef) -> Result<()>;
}

// --- Commit Permit --- //

pub(super) struct PipelineAdmission<const COUNTING_SEM_LIMIT: usize> {
    count: AtomicU16,
    sem_mu: Mutex<()>,
    sem_cv: Condvar,
}

impl<const COUNTING_SEM_LIMIT: usize> PipelineAdmission<COUNTING_SEM_LIMIT> {
    pub(super) fn new() -> Self {
        debug_assert!(COUNTING_SEM_LIMIT <= u16::MAX as usize);
        Self {
            count: AtomicU16::new(COUNTING_SEM_LIMIT as u16),
            sem_mu: Mutex::new(()),
            sem_cv: Condvar::new(),
        }
    }

    // This is the fast CAS path for the Semaphore without mutex locking
    pub(super) fn try_acquire(&self) -> bool {
        let mut cur = self.count.load(Ordering::Acquire);

        loop {
            if cur == 0 {
                return false;
            }

            match self
                .count
                .compare_exchange(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    // Slow path where we wait on Mutex and CondVar for space to become available
    pub(super) fn acquire(&self) {
        if self.try_acquire() {
            return;
        }

        let mut guard = self.sem_mu.lock().unwrap();

        loop {
            while self.count.load(Ordering::Acquire) == 0 {
                guard = self.sem_cv.wait(guard).unwrap();
            }

            if self.try_acquire() {
                return;
            }
        }
    }

    pub(super) fn release(&self) {
        let _guard = self.sem_mu.lock().unwrap();
        let prev = self.count.fetch_add(1, Ordering::AcqRel);

        assert!(
            (prev as usize) < COUNTING_SEM_LIMIT,
            "Semaphore released at count limit"
        );

        self.sem_cv.notify_one();
    }

    #[cfg(test)]
    pub(super) fn available_permits(&self, ordering: Ordering) -> usize {
        self.count.load(ordering) as usize
    }
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
pub(crate) struct WritePipeline<const N: usize, E: WriterEnv> {
    // Queue
    batch_queue: BatchQueue<N>,

    // Write Queue reservation
    commit_sem: PipelineAdmission<N>,

    // Global WAL fsync reservation/backpressure.
    //
    // This is separate from a batch's SyncWaiter. sync_sem bounds how much
    // WAL sync work may be outstanding across the whole pipeline; the
    // per-batch waiter records completion for one specific batch.
    sync_sem: SyncQueueSem,

    // Env trait
    env: Arc<E>,

    // Seq State
    seq_state: Arc<SeqNumState>,

    //
    q_mu: Mutex<()>,

    #[cfg(test)]
    condvar_waiters: AtomicU64,
}

impl<E> WritePipeline<DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE, E>
where
    E: WriterEnv,
{
    // New with specific size
    pub(crate) fn new(env: Arc<E>, seq_state: Arc<SeqNumState>, sync_sem: SyncQueueSem) -> Self {
        Self::new_with_size(env, seq_state, sync_sem)
    }
}

impl<const N: usize, E> WritePipeline<N, E>
where
    E: WriterEnv,
{
    pub(crate) fn new_with_size(
        env: Arc<E>,
        seq_state: Arc<SeqNumState>,
        sync_sem: SyncQueueSem,
    ) -> Self {
        Self {
            batch_queue: BatchQueue::<N>::new(),
            commit_sem: PipelineAdmission::new(),
            env,

            seq_state,
            sync_sem,

            q_mu: Mutex::new(()),

            #[cfg(test)]
            condvar_waiters: AtomicU64::new(0),
        }
    }

    pub(super) fn reserve_space(&self) {
        //
        //
        // 1. loop 200 times using a "pause" for 1 micro sec
        // 2. Thread::yield()
        // 3. Mutex and CondVar
        //
        // This is inspired by Rocks code see: https://github.com/facebook/rocksdb/blob/763401b595c8c1647908356e42525aadd0b90eae/db/write_thread.cc#L64
        for _ in 0..200 {
            if self.try_reserve_space() {
                return;
            }
            spin_loop();
        }

        // Slow path condvar wait
        self.commit_sem.acquire();

        return;
    }

    fn try_reserve_space(&self) -> bool {
        self.commit_sem.try_acquire()
    }

    pub(super) fn release_queue_space(&self) {
        self.commit_sem.release();
    }

    //

    pub(crate) fn commit_sync(&self, batch: &BatchObject<Sealed>) -> Result<()> {
        // NOTE: Any assertions here?
        //
        // NOTE: When we commit we do not need the type state anymore and can convert into inner heap allocated batch object

        // Need to try_acquire a token - if not we wait()

        // Hand off to DB which will carry out the write
        //

        // Commit Sync will not return until the batch has been applied, and fsynced

        //
        //
        //
        todo!()
    }

    pub(crate) fn commit(
        // TODO: Commit should take a (mutable?) reference to the BatchObject so the Caller retains ownership of the underlying NonNullBatchPtr
        // and can call Close() / Return() after commit()
        &self,
        batch: &BatchObject<Sealed>,
    ) -> Result<()> {
        // NOTE: Any assertions here?
        //
        // NOTE: When we commit we do not need the type state anymore and can convert into inner heap allocated batch object

        // Need to try_acquire a token - if not we wait()

        // Hand off to DB which will carry out the write

        //
        //
        //
        Ok(())
    }

    pub(crate) fn prepare(&self, batch: NonNull<Batch>) -> Result<()> {
        // XXX: In the future we may want to to have a SyncWal bool where we can decide if we want to fsync to WAL or not
        // Further to that we can also decide if we want to asynchronously wait for fsync to complete
        // But for now the commit will both wait for publish and fsync

        let _guard = self.q_mu.lock().unwrap();

        // Get reference (&Batch<Sealed>) to the batch to pass into methods
        let b = unsafe { &*batch.as_ptr() };

        self.batch_queue.enqueue(batch);

        // TODO: Need to check this and test
        // Assign the seq_no to the batch
        unsafe {
            b.assign_seq_num_once(
                self.seq_state
                    .log_seq_num
                    .fetch_add(b.get_batch_count(), Ordering::AcqRel)
                    - b.get_batch_count(),
            )
        };

        // Prepare
        self.env.prepare_commit(&BatchRef::from_batch(b))?;

        Ok(())
    }

    pub(crate) fn publish(&self, batch: &BatchObject<Sealed>) {
        todo!()
    }

    //
}

#[cfg(test)]
mod tests {
    use crate::sync::atomic::{AtomicBool, AtomicUsize};
    use std::{ptr, sync::Barrier, thread, time::Duration};

    use crate::db::{batch::Batch, write_pipeline::tests::queue_harness::Harness};

    use super::*;

    mod queue_harness {
        use std::{
            ptr::NonNull,
            thread::{self, Scope, ScopedJoinHandle},
        };

        use crate::{
            db::batch::{Batch, NonNullBatchPtr},
            sync::{Condvar, Mutex},
        };

        use crate::db::{
            batch::{BatchObject, Sealed},
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
                F: FnOnce(&Batch, &BatchQueue<N>) -> Option<NonNull<Batch>> + Send + 'scope,
                C: FnOnce(&Batch) + Send + 'scope,
            {
                scope.spawn(move || {
                    let mut sealed = BatchObject::new().seal();
                    let b_non_null = sealed.as_non_null();
                    let b_ptr = b_non_null.as_ptr();
                    let b_ref = unsafe { &*b_non_null.as_ptr() };

                    self.wait_released(id, 1);
                    self.queue.enqueue(b_non_null);
                    self.set_stage(id, Stage::Enqueued);

                    config(b_ref);

                    self.wait_released(id, 2);
                    self.set_stage(id, Stage::Ready);

                    let r = f(b_ref, &self.queue);

                    let self_dq = matches!(r, Some(ptr) if ptr.as_ptr() == b_ptr);

                    self.set_stage(id, Stage::Done);

                    ConsumerCtx {
                        applied: b_ref.is_applied(std::sync::atomic::Ordering::Relaxed),
                        self_dequeued: self_dq,
                        did_dequeue: r.is_some(),
                    }
                })
            }
        }
    }

    mod pipeline_harness {}

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

            // B1 is the only applied head batch. Either consumer may win the
            // dequeue race, but exactly one consumer should dequeue a batch.
            assert_eq!(
                [r1.did_dequeue, r2.did_dequeue]
                    .into_iter()
                    .filter(|did_dequeue| *did_dequeue)
                    .count(),
                1
            );
        })
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

        let mut batch = BatchObject::new().seal();
        let b_ptr = batch.as_non_null();

        batch_q.enqueue(b_ptr);
    }

    #[test]
    fn enqueue_batch() {
        let mut batch = BatchObject::new().seal();
        let b_ptr = batch.as_non_null();

        let batch_q = BatchQueue::<4>::new();

        batch_q.enqueue(b_ptr);

        assert!(batch_q.slots[0].load(Ordering::Relaxed) == batch.as_ptr());
        let (h, _) = HeadTail::unpack_unchecked(batch_q.head_tail.load(Ordering::Relaxed));
        assert!(h == 1);
    }

    // TODO: Test -> dequeue_preserves_fifo_order()
    //
    // TODO: Test -> applied_later_batch_does_not_skip_unapplied_head()
    //
    // TODO: Test -> wraparound_reuses_cleared_slots()

    // -------- Pipeline Tests -------- //

    // TODO: Need to fix this test
    #[test]
    fn try_reserve() {
        //
        struct EnvStub;
        impl WriterEnv for EnvStub {
            fn apply_commit<'env>(&'env self, batch: &'env BatchRef) -> Result<()> {
                Ok(())
            }
            fn prepare_commit<'env>(&'env self, batch: &'env BatchRef) -> Result<()> {
                Ok(())
            }
        }

        let env = Arc::new(EnvStub {});

        let seq_state = Arc::new(SeqNumState::default());
        let sync_sem = SyncQueueSem::default();

        let wp = WritePipeline::<1, EnvStub>::new_with_size(env, seq_state.clone(), sync_sem);

        assert!(wp.batch_queue.size() == 1);

        assert!(wp.try_reserve_space());
        assert!(!wp.try_reserve_space());
        assert_eq!(wp.commit_sem.available_permits(Ordering::Acquire), 0);

        let reserved = AtomicBool::new(false);

        thread::scope(|s| {
            s.spawn(|| {
                wp.reserve_space();
                reserved.store(true, Ordering::Release);
                wp.release_queue_space();
            });

            thread::sleep(Duration::from_millis(10));
            assert!(!reserved.load(Ordering::Acquire));

            wp.release_queue_space();

            while !reserved.load(Ordering::Acquire) {
                spin_loop();
            }
        });

        assert_eq!(wp.commit_sem.available_permits(Ordering::Acquire), 1);

        //
    }

    #[test]
    #[should_panic]
    fn release_queue_two_threads() {
        struct EnvStub;
        impl WriterEnv for EnvStub {
            fn apply_commit<'env>(&self, batch: &'env BatchRef) -> Result<()> {
                Ok(())
            }
            fn prepare_commit<'env>(&self, batch: &'env BatchRef) -> Result<()> {
                Ok(())
            }
        }

        let env = Arc::new(EnvStub {});

        let seq_state = Arc::new(SeqNumState::default());
        let sync_sem = SyncQueueSem::default();

        let wp = WritePipeline::<1, EnvStub>::new_with_size(env, seq_state.clone(), sync_sem);

        // We want two threads to race on releasing queue which has occupancy of 1
        // Should panic

        let barrier = Barrier::new(2);

        // Increase occupancy by one

        assert!(wp.try_reserve_space());
        assert_eq!(wp.commit_sem.available_permits(Ordering::Acquire), 0);

        thread::scope(|s| {
            s.spawn(|| {
                barrier.wait();

                wp.release_queue_space();
            });
            //
            barrier.wait();

            wp.release_queue_space();
            //
        });
    }
}

#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use crate::sync::atomic::*;
    use crate::sync::{Arc, Condvar, Mutex};

    // ----------------- Loom Tests ----------------- //

    // TODO: Need to fix test
    #[test]
    fn reserve_release_simple() {
        //
        struct EnvStub;
        impl WriterEnv for EnvStub {
            fn apply_commit<'env>(&self, batch: &'env BatchRef) -> Result<()> {
                Ok(())
            }
            fn prepare_commit<'env>(&self, batch: &'env BatchRef) -> Result<()> {
                Ok(())
            }
        }

        loom::model(|| {
            let env = Arc::new(EnvStub);
            let seq_state = Arc::new(SeqNumState::default());
            let sync_sem = SyncQueueSem::default();
            let wp = Arc::new(WritePipeline::<1, EnvStub>::new_with_size(
                env,
                seq_state.clone(),
                sync_sem,
            ));

            let inside = Arc::new(AtomicUsize::new(0));

            let wp1 = wp.clone();
            let inside1 = inside.clone();

            let t1 = loom::thread::spawn(move || {
                wp1.reserve_space();

                let prev = inside1.fetch_add(1, Ordering::SeqCst);
                assert_eq!(prev, 0);

                loom::thread::yield_now();

                let prev = inside1.fetch_sub(1, Ordering::SeqCst);
                assert_eq!(prev, 1);

                wp1.release_queue_space();
            });

            let wp2 = wp.clone();
            let inside2 = inside.clone();

            let t2 = loom::thread::spawn(move || {
                wp2.reserve_space();

                let prev = inside2.fetch_add(1, Ordering::SeqCst);
                assert_eq!(prev, 0);

                loom::thread::yield_now();

                let prev = inside2.fetch_sub(1, Ordering::SeqCst);
                assert_eq!(prev, 1);

                wp2.release_queue_space();
            });

            t1.join().unwrap();
            t2.join().unwrap();

            assert_eq!(wp.commit_sem.available_permits(Ordering::SeqCst), 1);
            assert_eq!(inside.load(Ordering::SeqCst), 0);
        });

        //
    }
}
