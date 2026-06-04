// Global batch pool for heap allocated batches
//
//

// 1. A batch in the pool is not visible to the write pipeline.
// 2. A batch in the write pipeline is not visible to the pool.
// 3. pool_next is only read/written by BatchPool.
// 4. reset_for_reuse happens before push publishes the batch.
// 5. acquire must clear/detach pool_next before returning the batch.
// 6. shutdown must drain the pool and free retained batches.

use std::{array, ptr::NonNull};

use crate::{
    db::batch::{Batch, BatchObject, NonNullBatchPtr, Pooled, UnCommitted},
    sync::{
        Mutex,
        atomic::{AtomicPtr, AtomicUsize},
    },
    utils::cache_padded::CachePadded,
};

const NUMBER_POOL_SHARDS: usize = 4;

// Default Batch Size = 1024
//
// We don't want the pool to retain a large amount of batch memory
//
// Shard Capacity    = 16
// Shard Cap         = 4
// Max Pool Retained = 65,536
//
// We can't estimate the number of retained batches in tls because writer threads are unbounded but we can cap the tls cache size

const MAX_BATCHES_PER_SHARD: usize = 16;
const SHARD_CAP: usize = 4;
const MAX_RETAINED_POOL_BYTES: usize =
    (MAX_BATCHES_PER_SHARD * SHARD_CAP) * crate::db::batch::DEFAULT_BATCH_INIT_SIZE;

const MAX_BATCHES_PER_THREAD_CACHE: usize = 4;

// For pooling we want to be very memory light and still rely on the allocator to do most of the work if we spill
//
// Pool Rules:
//
// - no diving (lol)
//
// Acquire:
// - Try pop one Batch from TLS.
// - If TLS is empty, refill TLS from the assigned shard.
// - If shard is empty, allocate a new Box<Batch>.
// - Return one Batch to caller.

// Release:
// - Sanitise/reset Batch.
// - If Batch retained capacity is too large, destroy it.
// - Else push Batch into TLS.
// - If TLS exceeds its cap, spill about half to the assigned shard.
// - If shard exceeds its cap, destroy the overflow.
//
// Pool invariants:
// - Batches are allocated with Box::new and converted with Box::into_raw.
// - TLS and shard pools store only NonNull<Batch>; they track availability, not active use.
// - An acquired Batch is exclusively owned by the caller/pipeline.
// - A Batch may be returned only after WAL, memtable apply, publish, signalling, and caller-visible completion are finished.
// - After return, no thread may hold or dereference any pointer/reference to that Batch.
// - The pool may destroy returned batches using Box::from_raw when retention limits are exceeded.
// - Thread-local cached batches are destroyed when ThreadCtx drops.

pub(crate) struct ThreadBatchCache {
    pub(crate) shard_idx: Option<usize>,
    pub(crate) batches: Vec<NonNull<Batch>>,
}

impl ThreadBatchCache {}

struct BatchPoolShard {
    batches: Mutex<Vec<NonNullBatchPtr>>,
}

impl Default for BatchPoolShard {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchPoolShard {
    fn new() -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
        }
    }
}

// Pool stores reusable Batch pointers.

// Batches are allocated lazily on demand and may be
// destroyed when the pool exceeds its retention limits.
pub(crate) struct BatchPool {
    pool: [CachePadded<BatchPoolShard>; SHARD_CAP],
    // XXX: Later we may want to hold ownership of the batches such as Vec<Box<Batch>> or custom Slab Allocator??
    // This would help with cache locality in memory and upfront allocation for predicted workloads
    next_shard: AtomicUsize,
}

impl BatchPool {
    pub(crate) fn new() -> Self {
        Self {
            pool: array::from_fn(|_| CachePadded::new(BatchPoolShard::default())),
            next_shard: AtomicUsize::new(0),
        }
    }

    pub(crate) fn acquire(&mut self) -> BatchObject<UnCommitted> {
        // Easy path for test

        if self.pool[0].batches.lock().unwrap().len() == 0 {
            println!("Allocating");
            BatchObject::new()
        } else {
            println!("Fetching from pool..");
            BatchObject::from_batch_ptr(self.pool[0].batches.lock().unwrap().pop().unwrap())
        }

        // ==============

        // assertions

        // 1. Get thread-local batch cache

        // 2. Lazily assign shard if cache has not yet been assigned one

        // 3. Try acquire from TLS cache
        //    - Return immediately on hit

        // 4. Try acquire from assigned shard
        //    - Refill TLS cache on hit
        //    - Return one batch

        // 5. Allocate a small batch refill
        //    - Return one batch
        //    - Place remaining batches into TLS cache
    }

    fn try_acquire(&self, thread_cache: &mut ThreadBatchCache) /* Enum return? */ {}

    pub(crate) fn thread_cache(&self, ctx: &mut ThreadBatchCache) -> NonNull<Batch> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::atomic::Ordering;
    use std::{sync::Barrier, thread};

    use super::*;

    #[test]
    fn basic_acquire() {
        //

        let mut pool = BatchPool::new();

        pool.pool[0]
            .batches
            .lock()
            .unwrap()
            .push(BatchObject::<UnCommitted>::new().batch_ptr());

        thread::scope(|s| {
            let batch = pool.acquire();

            s.spawn(|| {
                let batch = pool.acquire();
            });
        });

        //
        //
    }

    #[test]
    fn shard_index() {
        //
        //
        let pool = BatchPool::new();

        let barrier = Barrier::new(2);

        thread::scope(|s| {
            let t1 = s.spawn(|| {
                barrier.wait();
                pool.next_shard.fetch_add(1, Ordering::Release)
            });

            let t2 = s.spawn(|| {
                barrier.wait();
                pool.next_shard.fetch_add(1, Ordering::Release)
            });

            let r1 = t1.join().unwrap();
            println!("t1 index = {}", r1 % SHARD_CAP);
            let r2 = t2.join().unwrap();
            println!("t2 index = {}", r2 % SHARD_CAP);

            //
        });

        assert!(pool.next_shard.load(Ordering::Acquire) == 2);
        println!("{}", pool.next_shard.load(Ordering::Acquire));
    }
}
