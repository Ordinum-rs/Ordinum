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
    thread_local_storage::{thread_ctx, thread_db_instance_ctx},
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
    pub(crate) batches: Vec<NonNullBatchPtr>,
}

impl ThreadBatchCache {
    pub(crate) fn new() -> Self {
        Self {
            shard_idx: None,
            batches: Vec::new(),
        }
    }
}

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
    //
    // TODO: Add metrics
    // __
}

impl BatchPool {
    // XXX: Once we have a stable DB we can make this pub(super) so that only objects that hold a pool can create one
    pub(crate) fn new() -> Self {
        Self {
            pool: array::from_fn(|_| CachePadded::new(BatchPoolShard::default())),
            next_shard: AtomicUsize::new(0),
        }
    }

    //
    //
    //
    //
    fn try_acquire_from_tls(
        &self,
        cache: &mut ThreadBatchCache,
    ) -> Option<BatchObject<UnCommitted>> {
        cache.batches.pop().map_or(None, |batch| {
            Some(BatchObject::<UnCommitted>::from_batch_ptr(batch))
        })
    }

    fn refill_tls_cache(&self, cache: &mut ThreadBatchCache) {}

    pub(crate) fn acquire(&self) -> BatchObject<UnCommitted> {
        // Easy path for test

        // if self.pool[0].batches.lock().unwrap().len() == 0 {
        //     println!("Allocating");
        //     BatchObject::new()
        // } else {
        //     println!("Fetching from pool..");
        //     BatchObject::from_batch_ptr(self.pool[0].batches.lock().unwrap().pop().unwrap())
        // }

        // ==============

        // 0. Assertions

        thread_db_instance_ctx(0, |ctx| {
            //
            // Lazy shard assign check

            return ctx.thread_batch_cache_mut(|cache| {
                // 1. Try acquire from TLS cache
                //    - Return immediately on hit
                match self.try_acquire_from_tls(cache).or_else(|| {
                    //
                    // 2. Try to refill from pool (which will allocate if global is empty)
                    // TODO: Add the refill from global pool
                    // 3. Return a batch from the pool and avoid a second tls cache call

                    println!("Allocating..");
                    Some(BatchObject::new())
                }) {
                    Some(batch) => return batch,
                    None => {
                        panic!("Could not acquire from TLS or Pool and could not Allocate")
                    }
                }
            });
        })

        //
        //
        //
        //
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::atomic::Ordering;
    use std::{sync::Barrier, thread};

    use super::*;

    #[test]
    fn empty_try_acquire() {
        //
        let mut pool = BatchPool::new();

        thread_db_instance_ctx(0, |ctx| {
            // We don't have any db instances yet so just use 0 and let tls make a slot for us
            ctx.thread_batch_cache_mut(|cache| {
                let result = pool.try_acquire_from_tls(cache);

                assert!(result.is_none());

                // If we manually insert a Batch into the tls cache then we should get a Wrapped BatchObject<Uncommitted>
                cache.batches.push(BatchObject::new().into_inner());
            })
        });

        thread_db_instance_ctx(0, |ctx| {
            // We don't have any db instances yet so just use 0 and let tls make a slot for us
            ctx.thread_batch_cache_mut(|cache| {
                let result = pool.try_acquire_from_tls(cache);

                assert!(result.is_some());
            })
        });
    }

    #[test]
    fn basic_acquire() {
        //

        let mut pool = BatchPool::new();

        pool.pool[0]
            .batches
            .lock()
            .unwrap()
            .push(BatchObject::<UnCommitted>::new().into_inner());

        thread::scope(|s| {
            let batch = pool.acquire();

            s.spawn(|| {
                let batch = pool.acquire();
            });
        });

        // Assertions?
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
                pool.next_shard.fetch_add(1, Ordering::Relaxed)
            });

            let t2 = s.spawn(|| {
                barrier.wait();
                pool.next_shard.fetch_add(1, Ordering::Relaxed)
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
