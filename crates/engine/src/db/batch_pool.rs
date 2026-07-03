// Global batch pool for heap allocated batches
//
//

// 1. A batch in the pool is not visible to the write pipeline.
// 2. A batch in the write pipeline is not visible to the pool.
// 3. pool_next is only read/written by BatchPool.
// 4. reset_for_reuse happens before push publishes the batch.
// 5. acquire must clear/detach pool_next before returning the batch.
// 6. shutdown must drain the pool and free retained batches.

use std::{array, mem::MaybeUninit, ptr::NonNull};

use crate::{
    db::batch::{
        Batch, BatchCommitState, BatchObject, BatchObjectHandle, NonNullBatchPtr, UnCommitted,
    },
    sync::{
        Arc, Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
    thread_local_storage::{
        thread_ctx, thread_db_instance_ctx,
        thread_local_ptr::{ThreadLocalObject, ThreadLocalPtr, UnrefHandler},
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
const NUMBER_OF_SHARDS_FOR_POOL: usize = 4;
const MAX_RETAINED_POOL_BYTES: usize =
    (MAX_BATCHES_PER_SHARD * NUMBER_OF_SHARDS_FOR_POOL) * crate::db::batch::DEFAULT_BATCH_INIT_SIZE;

const MAX_BATCHES_PER_THREAD_CACHE: usize = 6;

// For pooling we want to be very memory light and still rely on the allocator to do most of the work if we spill
//
// Pool Rules:
//
// - no diving (lol)
//
// Acquire:
// - Try to pop one Batch from the thread-local cache.
// - If TLS is empty, refill TLS from this thread's assigned shard.
// - If the shard is empty, allocate a new Box<Batch>.
// - Return one exclusively-owned Batch to the caller.
//
// Release:
// - The Batch must already be in a terminal completion state.
// - Sanitise/reset the Batch.
// - If its retained buffers exceed the retention limit, destroy it.
// - Otherwise push it into TLS.
// - If TLS exceeds its cap, spill approximately half into the assigned shard
//   We do this because every subsequent release call will grab the global mutex.
// - If the shard exceeds its cap, destroy the overflow.
//
// Invariants:
// - Batches are allocated with Box::new and converted with Box::into_raw.
// - TLS and shard pools store only NonNull<Batch>; they track availability, not active use.
// - An acquired Batch is exclusively owned by the caller/pipeline.
// - A Batch may not be returned while it is queued, WAL-pending, memtable-pending,
//   publish-pending, signal-pending, or externally observable through a live handle.
// - Returning a Batch requires that no thread can still wait on it, inspect it,
//   reset it, or dereference any pointer/reference into it.
// - After return, the pool owns the Batch and may hand it to another writer immediately.
// - The pool may destroy returned batches using Box::from_raw when retention limits
//   are exceeded.
// - Thread-local caches are drained when the tls thread row drops: batches may be returned
//   to the global shard or destroyed according to the pool's retention policy.

// TODO: Move into thread_local_storage folder?
pub(crate) struct ThreadBatchCache<const CACHE_CAP: usize = MAX_BATCHES_PER_THREAD_CACHE> {
    pub(crate) shard_idx: Option<usize>,
    len: u8,
    pub(crate) batches: [MaybeUninit<NonNullBatchPtr>; CACHE_CAP],
    // Do we need an index here?
}

impl ThreadBatchCache {
    pub(crate) fn new() -> Self {
        Self::new_with_size()
    }

    // TODO: Make Tiny helpers to access elements in the batches array safely
}

impl<const CACHE_CAP: usize> ThreadBatchCache<CACHE_CAP> {
    pub(crate) fn new_with_size() -> Self {
        Self {
            shard_idx: None,
            len: 0,
            batches: array::from_fn(|_| MaybeUninit::zeroed()),
        }
    }
}

impl ThreadLocalObject for ThreadBatchCache {
    fn handler() -> Option<UnrefHandler> {
        Some(Self::unref_erased)
    }

    unsafe fn unref(ptr: *mut Self) {
        let _entry = unsafe { Box::from_raw(ptr) };
    }
}

/* NOTE: We would implement a drop to return cached batches back to pool on thread exit BUT this can be problematic as we'd have to hold a Weak Pointer back
to Pool and also may encounter some cyclic behaviour if we are shutting down so Pool is dropping and thread is exiting whilst trying to return to pool.
Decision is to just drop the cached batches for now and stay light */

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

// XXX: Need to think about how we might wire this into a standardised stats module which then plugs in to a wider engine level stats collection
struct BatchPoolStats {
    tls_misses: AtomicUsize,
    shard_hits: AtomicUsize,
    allocations: AtomicUsize,
    // XXX: Would like to show total_allocated_bytes
    // Can we do this over time? Or histogram this?
}

impl Default for BatchPoolStats {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchPoolStats {
    fn new() -> Self {
        Self {
            tls_misses: AtomicUsize::new(0),
            shard_hits: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
        }
    }
}

// Pool stores reusable Batch pointers.

// Batches are allocated lazily on demand and may be
// destroyed when the pool exceeds its retention limits.
pub(crate) struct BatchPool {
    pool: [CachePadded<BatchPoolShard>; NUMBER_OF_SHARDS_FOR_POOL],
    // XXX: Later we may want to hold ownership of the batches such as Vec<Box<Batch>> or custom Slab Allocator??
    // This would help with cache locality in memory and upfront allocation for predicted workloads
    next_shard: AtomicUsize,
    //
    stats: BatchPoolStats,
    //
    thread_local_ptr: ThreadLocalPtr<ThreadBatchCache>,
}

// TODO: Think about this - need justification
unsafe impl Sync for BatchPool {}

impl BatchPool {
    // XXX: Once we have a stable DB we can make this pub(super) so that only objects that hold a pool can create one
    pub(crate) fn new() -> Self {
        Self {
            pool: array::from_fn(|_| CachePadded::new(BatchPoolShard::default())),
            next_shard: AtomicUsize::new(0),
            stats: BatchPoolStats::default(),

            thread_local_ptr: ThreadLocalPtr::new(),
        }
    }

    //
    //
    //
    //

    fn assign_shard_idx(&self, cache: &mut ThreadBatchCache) -> usize {
        let id = self.next_shard.fetch_add(1, Ordering::Relaxed) % NUMBER_OF_SHARDS_FOR_POOL;
        cache.shard_idx = Some(id);
        id
    }

    fn shard_idx_for_cache(&self, cache: &mut ThreadBatchCache) -> usize {
        cache
            .shard_idx
            .unwrap_or_else(|| self.assign_shard_idx(cache))
    }

    // ----- Acquire Methods ----- //

    fn thread_local_batch_ptr<'a>(&self) -> &'a ThreadBatchCache {
        let ptr = self.thread_local_ptr.get_or_init(|| {
            // Need to allocate the thread batch cache
            unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(ThreadBatchCache::new()))) }
        });

        unsafe { &*ptr.as_ptr() }
        //
    }

    fn thread_local_batch_cache_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut ThreadBatchCache) -> R,
    {
        self.thread_local_ptr.get_or_init_mut(
            //
            || unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(ThreadBatchCache::new()))) },
            //
            // Nested closure, only really have this thread_local_batch_cache_mut method to avoid having to use two closures in a function signature
            // Nice API surface
            f,
        )
    }

    // TODO: Check these methods + test

    fn try_acquire_from_tls(
        &self,
        cache: &mut ThreadBatchCache,
    ) -> Option<BatchObject<UnCommitted>> {
        cache.batches.pop().map_or(None, |batch| {
            Some(BatchObject::<UnCommitted>::from_batch_ptr(batch))
        })
    }

    fn refill_tls_cache(&self, cache: &mut ThreadBatchCache) -> BatchObject<UnCommitted> {
        // First get the batches from the shard
        let mut shard = self.pool[self.shard_idx_for_cache(cache)]
            .batches
            .lock()
            .unwrap_or_else(|e| {
                // XXX: Maybe we want to think about if we handle another thread panicking? Do we want to recover?
                panic!()
            });

        self.stats.shard_hits.fetch_add(1, Ordering::Relaxed);

        // Grab a batch we can return straight away
        let returnable_batch = shard.pop().unwrap_or_else(|| {
            self.stats.allocations.fetch_add(1, Ordering::Relaxed);
            BatchObject::new().into_inner()
        });

        // While we're here we will also try to hydrate the tls cache by grabbing batches from global
        while cache.batches.len() < MAX_BATCHES_PER_THREAD_CACHE / 2 {
            // We want to pop from global pool - if pool is empty then we allocate a new batch
            match shard.pop() {
                Some(batch) => cache.batches.push(batch),
                None => {
                    // XXX: What i'd like to do here is get warmed up as possible by allocating on empty pop and refilling tls_cache eagerly
                    // BUT We need to understand the stats first because a cold thread could feasably over allocate and not use the cached batches
                    // So we will go with the natural approach first. When global pool is empty we just break and return the single batch and let the
                    // drop implementatino slowly build the cache
                    break;
                }
            }
        }

        BatchObject::from_batch_ptr(returnable_batch)
    }

    pub(crate) fn acquire(&self) -> BatchObject<UnCommitted> {
        // 0. Assertions

        self.thread_local_batch_cache_mut(|cache| {
            // 1. Try acquire from TLS cache
            //    - Return immediately on hit
            match self.try_acquire_from_tls(cache).or_else(|| {
                self.stats.tls_misses.fetch_add(1, Ordering::Relaxed);

                // 2. Try to refill from pool
                Some(self.refill_tls_cache(cache))
            }) {
                Some(batch) => return batch,
                None => {
                    panic!("Could not acquire from TLS or Pool and could not Allocate")
                }
            }
        })
    }

    // ----- Release Methods ----- //

    fn try_return_to_cache(
        &self,
        batch: BatchObject<UnCommitted>,
        cache: &ThreadBatchCache,
    ) -> Result<(), BatchObject<UnCommitted>> {
        //

        Ok(())
    }

    pub(crate) fn release<B: BatchCommitState>(&self, batch: BatchObject<B>) {
        // Want
        // 1. Extract the Batch
        // 2. Reset the batch to a cachable state
        // 3. Try to return to pool
        // 4. Destroy if no space
        //

        let batch = batch.reset_batch();

        self.thread_local_batch_cache_mut(|cache| ())
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

        // We should have only done 1 allocation
        assert_eq!(pool.stats.allocations.load(Ordering::Relaxed), 1);
        // We should have missed tls twice
        assert_eq!(pool.stats.tls_misses.load(Ordering::Relaxed), 2);
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
            println!("t1 index = {}", r1 % NUMBER_OF_SHARDS_FOR_POOL);
            let r2 = t2.join().unwrap();
            println!("t2 index = {}", r2 % NUMBER_OF_SHARDS_FOR_POOL);

            //
        });

        assert!(pool.next_shard.load(Ordering::Acquire) == 2);
        println!("{}", pool.next_shard.load(Ordering::Acquire));
    }
}
