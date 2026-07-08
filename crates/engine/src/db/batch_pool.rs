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
        Batch, BatchCommitState, BatchObject, BatchObjectHandle, DEFAULT_BATCH_INIT_SIZE,
        NonNullBatchPtr, UnCommitted,
    },
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
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
}

impl<const CACHE_CAP: usize> ThreadBatchCache<CACHE_CAP> {
    // Consts
    const TARGET_FILL: usize = CACHE_CAP / 2;

    pub(crate) fn new_with_size() -> Self {
        debug_assert!(CACHE_CAP <= MAX_BATCHES_PER_THREAD_CACHE);
        Self {
            shard_idx: None,
            len: 0,
            batches: array::from_fn(|_| MaybeUninit::zeroed()),
        }
    }

    pub(super) fn cache_len(&self) -> usize {
        self.len as usize
    }

    pub(super) fn push(&mut self, entry: NonNullBatchPtr) -> Result<(), NonNullBatchPtr> {
        debug_assert!(self.len as usize <= CACHE_CAP);

        if self.len as usize == CACHE_CAP {
            return Err(entry);
        }

        self.batches[self.len as usize].write(entry);
        self.len += 1;

        Ok(())
    }

    pub(super) fn pop(&mut self) -> Option<NonNullBatchPtr> {
        debug_assert!(self.len as usize <= CACHE_CAP);

        if self.len == 0 {
            return None;
        }
        let idx = self.len as usize - 1;

        let entry = std::mem::replace(&mut self.batches[idx], MaybeUninit::uninit());

        self.len -= 1;

        Some(unsafe { entry.assume_init() })
    }

    // TODO: Make target spill array to return - replacing with MaybeUninit::uninit()
}

impl ThreadLocalObject for ThreadBatchCache {
    fn handler() -> Option<UnrefHandler> {
        Some(Self::unref_erased)
    }

    unsafe fn unref(ptr: *mut Self) {
        // Need to drop the entries in the batch cache first before dropping the
        // batch container
        let mut entry = unsafe { Box::from_raw(ptr) };

        for i in 0..entry.len as usize {
            // Each entry is a NonNullBatchPtr which is a NonNull<Batch> to Batch Memory
            // It has a drop implementation which safely destroys the NonNullBatchPtr
            entry.batches[i].assume_init_drop();
        }
    }
}

struct BatchPoolShard {
    // NOTE: Can we make these arrays with MaybeUninit?
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

    global_batches_reused: AtomicUsize,

    allocations: AtomicUsize,
    allocated_bytes: AtomicUsize,
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
            global_batches_reused: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
            allocated_bytes: AtomicUsize::new(0),
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
        cache.pop().map(|ptr| BatchObject::from_batch_ptr(ptr))
        //
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

        let mut allocated: u8 = 0;
        let mut reused: u8 = 0;

        // Grab a batch we can return straight away
        let returnable_batch = match shard.pop() {
            Some(batch) => {
                reused += 1;
                batch
            }
            None => {
                allocated += 1;
                BatchObject::new().into_inner()
            }
        };

        // While we're here we will also try to hydrate the tls cache by grabbing batches from global
        while (cache.len as usize) < cache.batches.len() / 2 {
            // We want to pop from global pool - if pool is empty then we allocate a new batch
            match shard.pop() {
                Some(batch) => {
                    reused += 1;
                    // We know we have space because of the while check
                    let _ = cache.push(batch);
                }
                None => {
                    break;
                }
            }
        }

        // Update stats

        self.stats.tls_misses.fetch_add(1, Ordering::Relaxed);

        if allocated != 0 {
            self.stats
                .allocations
                .fetch_add(allocated as usize, Ordering::Relaxed);
        }
        if reused != 0 {
            self.stats
                .global_batches_reused
                .fetch_add(reused as usize, Ordering::Relaxed);
        }

        BatchObject::from_batch_ptr(returnable_batch)
    }

    pub(crate) fn acquire(&self) -> BatchObject<UnCommitted> {
        // 0. Assertions

        self.thread_local_batch_cache_mut(|cache| {
            // 1. Try acquire from TLS cache
            //    - Return immediately on hit
            match self.try_acquire_from_tls(cache).or_else(|| {
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
        cache: &mut ThreadBatchCache,
    ) -> Result<(), BatchObject<UnCommitted>> {
        //
        cache
            .push(batch.into_inner())
            .map_err(|b_ptr| BatchObject::from_batch_ptr(b_ptr))
        //
    }

    pub(crate) fn release<B: BatchCommitState>(&self, batch: BatchObject<B>) {
        //
        // This only resets the TypeState - we need to also think about resize here
        let mut batch = batch.reset_batch();

        // SAFETY:
        //
        // We have exclusive ownership of this batch and it's memory and can safely dereference as no other process or caller
        // has any references to this batch
        let len = unsafe { &mut *batch.as_ptr() }.get_batch_size();

        // NOTE:
        // We may want to abstract out a resize policy layer if we find that we are making decisions about resizing batches
        // in different places and around different invariants
        if len >= DEFAULT_BATCH_INIT_SIZE * 2 {
            batch.shrink_to(DEFAULT_BATCH_INIT_SIZE);
        }

        self.thread_local_batch_cache_mut(|cache| match cache.push(batch.into_inner()) {
            Ok(_) => return (),
            Err(batch) => {
                // We need to try to return to global pool
            }
        })
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
                cache.push(BatchObject::new().into_inner());
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

        // We should have missed tls twice
        assert_eq!(pool.stats.tls_misses.load(Ordering::Relaxed), 2);
        // We should have only allocated once
        assert_eq!(pool.stats.allocations.load(Ordering::Relaxed), 1);
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
            let r2 = t2.join().unwrap();

            //
        });

        // We should have two shards - next shard should be idx 2
        assert!(pool.next_shard.load(Ordering::Acquire) == 2);
    }

    #[test]
    fn thread_cache_pop_empty_returns_none() {
        // Create a fresh ThreadBatchCache.
        // Assert pop() returns None.
        // Assert len remains 0.

        let pool = BatchPool::new();

        thread::scope(|s| {
            s.spawn(|| {
                pool.thread_local_batch_cache_mut(|cache| {
                    assert!(cache.pop().is_none());
                    assert!(cache.len == 0);
                })
            });
        });
    }

    #[test]
    fn thread_cache_push_then_pop_returns_same_batch() {
        // Create a fresh ThreadBatchCache.
        // Push one BatchObject::new().into_inner().
        // Pop it and assert the returned pointer equals the inserted pointer.
        // Clean up the popped batch allocation.
    }

    #[test]
    fn thread_cache_pop_is_lifo() {
        // Push two distinct batch pointers.
        // Pop twice.
        // Assert the second pushed pointer is returned first.
        // Clean up both popped batch allocations.
    }

    #[test]
    fn thread_cache_push_full_returns_err() {
        // Fill ThreadBatchCache to MAX_BATCHES_PER_THREAD_CACHE.
        // Push one extra batch.
        // Assert Err(extra_batch) is returned.
        // Clean up all retained batch pointers plus the extra pointer.
    }

    #[test]
    fn try_acquire_from_tls_empty_returns_none() {
        // Use BatchPool::try_acquire_from_tls with an empty ThreadBatchCache.
        // Assert None.
    }

    #[test]
    fn try_acquire_from_tls_pops_cached_batch() {
        // Push one batch pointer into ThreadBatchCache.
        // Call try_acquire_from_tls.
        // Assert Some(BatchObject<UnCommitted>).
        // Assert a second call returns None.
    }

    #[test]
    fn acquire_allocates_when_tls_and_shard_empty() {
        // Create BatchPool with empty shards.
        // Call acquire().
        // Assert allocations increments by 1.
        // Assert tls_misses increments by 1.
    }

    #[test]
    fn acquire_uses_shard_before_allocating() {
        // Seed the assigned shard with one batch.
        // Call acquire().
        // Assert allocations does not increment.
        // Assert returned batch is the seeded pointer if pointer identity is observable.
    }

    #[test]
    fn refill_tls_hydrates_cache_from_shard() {
        // Seed a shard with several batches.
        // Call acquire() through the pool's TLS path.
        // Assert one batch is returned and up to half the TLS cache is filled.
    }

    #[test]
    fn shard_assignment_is_sticky_per_thread_cache() {
        // Create one ThreadBatchCache.
        // Call shard_idx_for_cache twice.
        // Assert both calls return the same shard index.
        // Assert next_shard only increments once.
    }

    #[test]
    fn shard_assignment_round_robins_across_caches() {
        // Create multiple ThreadBatchCache values.
        // Assign shard index for each.
        // Assert indexes wrap modulo NUMBER_OF_SHARDS_FOR_POOL.
    }

    #[test]
    fn thread_batch_cache_unref_drops_cached_batches() {
        // After implementing cache draining:
        // Create a boxed ThreadBatchCache with initialized batch pointers.
        // Call ThreadBatchCache::unref through the erased handler.
        // Assert no leak/double-free under miri or a drop-counting test batch helper.
    }
}
