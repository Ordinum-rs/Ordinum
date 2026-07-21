use std::{array, mem::MaybeUninit, ptr::NonNull, todo};

use crate::{
    db::batch::{
        Batch, BatchAllocation, BatchCommitState, BatchFactory, BatchInner, BatchObject,
        DEFAULT_BATCH_INIT_SIZE, IndexedBatch, IndexedBatchFactory, OwnedBatchFactory,
        OwnedBatchPtr, UnCommitted,
    },
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        cell::UnsafeCell,
    },
    thread_local_storage::{
        thread_ctx, thread_db_instance_ctx,
        thread_local_ptr::{ThreadLocalObject, ThreadLocalPtr, UnrefHandler},
    },
    utils::cache_padded::CachePadded,
};

const DEFAULT_SHARDS_PER_POOL: usize = 4;
const DEFAULT_MAX_BATCHES_PER_SHARD: usize = 16;
const MAX_RETAINED_POOL_BYTES: usize = (DEFAULT_MAX_BATCHES_PER_SHARD * DEFAULT_SHARDS_PER_POOL)
    * crate::db::batch::DEFAULT_BATCH_INIT_SIZE;

const DEFAULT_THREAD_BATCH_CACHE_CAPACITY: usize = 8;
const DEFAULT_THREAD_BATCH_CACHE_TARGET_RETAINED: usize = DEFAULT_THREAD_BATCH_CACHE_CAPACITY / 2;

// Compile-time sizing model:
//
// The pool keeps fixed-size arrays for the global shard pool and each
// thread-local cache. Those sizes are encoded as const generic parameters so
// the storage layout is known at compile time and does not require a Vec for the
// hot per-thread cache.
//
// BatchPool is the production alias with conservative defaults. Tests can use
// BatchPoolImpl directly to exercise shard and TLS spill boundaries with small
// deterministic capacities, for example BatchPoolImpl<1, 2, 2, 1>.

// ----------------------------------------------------------

// ---- Batch Pool ---- //
//
//
// Pool Rules:
//
// - no diving (lol)
//
// Acquire:
// - Try to pop one allocation from the thread-local cache.
// - If TLS is empty, refill TLS from this thread's assigned shard.
// - If the shard is empty, ask the pool's BatchFactory for a new allocation.
// - Return one exclusively-owned BatchObject to the caller.
//
// Release:
// - The batch must already be in a reset-safe completion state.
// - Clear the batch and apply the pool's retention policy.
// - If its retained buffers exceed the retention limit, destroy it.
// - Otherwise push it into TLS.
// - If TLS exceeds its cap, spill approximately half into the assigned shard
//   We do this because every subsequent release call will grab the global mutex.
// - If the shard exceeds its cap, destroy the overflow.
//
// Invariants:
// - TLS and shard pools own `F::Allocation` values; they track availability,
//   not active use.
// - An acquired allocation is exclusively owned by the caller/pipeline.
// - An allocation may not be returned while it is queued, WAL-pending, memtable-pending,
//   publish-pending, signal-pending, or externally observable through a live handle.
// - Returning an allocation requires that no thread can still wait on it, inspect it,
//   reset it, or dereference any pointer/reference into it.
// - After return, the pool may hand the allocation to another writer immediately
//   or destroy it by dropping its owning allocation wrapper.
// - Thread-local caches are drained when the tls thread row drops: batches may be returned
//   to the global shard or destroyed according to the pool's retention policy.

/// Per-thread batch cache stored as one cell in the engine TLS matrix.
///
/// Each BatchPool owns one ThreadLocalPtr column. Each thread that touches that
/// pool lazily creates one ThreadBatchCache in its TLS row for that column.
/// During normal acquire/release paths, only the owning thread accesses its
/// cache, so len and the MaybeUninit array do not need atomic synchronization.
///
/// The cache owns up to CACHE_CAP reusable heap-allocated batches. Entries are
/// stored as owning `BatchAllocation` values inside `MaybeUninit` slots so
/// push/pop can manage the initialized prefix without allocating a `Vec`.
///
/// CACHE_CAP is the hard per-thread capacity. TARGET_RETAINED is the number of
/// batches this cache should keep after spilling excess entries back to the
/// shared shard pool.
///
/// When the TLS row or BatchPool TLS column is reclaimed, ThreadBatchCache::unref
/// drains the initialized entries and destroys any retained batches that were
/// not returned to the shared pool.
pub(crate) struct ThreadBatchCache<
    const CACHE_CAP: usize = DEFAULT_THREAD_BATCH_CACHE_CAPACITY,
    const TARGET_RETAINED: usize = DEFAULT_THREAD_BATCH_CACHE_TARGET_RETAINED,
    P: BatchAllocation = OwnedBatchPtr,
> {
    pub(crate) shard_idx: Option<usize>,
    len: u8,
    pub(crate) batches: [MaybeUninit<P>; CACHE_CAP],
}

impl ThreadBatchCache {
    pub(crate) fn new() -> Self {
        Self::new_with_const_size()
    }
}

impl<const CACHE_CAP: usize, const TARGET_RETAINED: usize, P: BatchAllocation>
    ThreadBatchCache<CACHE_CAP, TARGET_RETAINED, P>
{
    pub(crate) fn new_with_const_size() -> Self {
        debug_assert!(CACHE_CAP > 0);
        debug_assert!(CACHE_CAP <= u8::MAX as usize);
        debug_assert!(TARGET_RETAINED > 0);
        debug_assert!(TARGET_RETAINED <= CACHE_CAP);

        Self {
            shard_idx: None,
            len: 0,
            batches: array::from_fn(|_| MaybeUninit::uninit()),
        }
    }

    pub(super) fn cache_len(&self) -> usize {
        self.len as usize
    }

    pub(super) fn push(&mut self, entry: P) -> Result<(), P> {
        debug_assert!(self.len as usize <= CACHE_CAP);

        if self.len as usize == CACHE_CAP {
            return Err(entry);
        }

        // The capacity check happens before the transition, so an Err returns
        // the pointer with its previous state unchanged. On success, this cache
        // takes exclusive ownership and marks the live allocation idle before
        // publishing it in the initialized cache prefix.

        unsafe { &*entry.batch_ptr().as_ptr() }
            .set_runtime_state(super::batch::BatchRuntimeState::Idle, Ordering::Release);

        self.batches[self.len as usize].write(entry);
        self.len += 1;

        Ok(())
    }

    pub(super) fn pop(&mut self) -> Option<P> {
        debug_assert!(self.len as usize <= CACHE_CAP);

        if self.len == 0 {
            return None;
        }
        let idx = self.len as usize - 1;

        let entry = std::mem::replace(&mut self.batches[idx], MaybeUninit::uninit());

        self.len -= 1;

        Some(unsafe { entry.assume_init() })
    }

    fn spill_cache_to_target_retained(&mut self, mut handle: impl FnMut(P)) {
        // Leave one slot within the target for the newly released hot batch.
        while (self.len as usize) >= TARGET_RETAINED {
            handle(self.pop().expect("cache length was above target retained"))
        }
    }
}

impl<const CACHE_CAP: usize, const TARGET_RETAINED: usize, P: BatchAllocation> ThreadLocalObject
    for ThreadBatchCache<CACHE_CAP, TARGET_RETAINED, P>
{
    fn handler() -> Option<UnrefHandler> {
        Some(Self::unref_erased)
    }

    unsafe fn unref(ptr: *mut Self) {
        // Drain the initialized prefix before dropping the cache container.
        let mut entry = unsafe { Box::from_raw(ptr) };

        for i in 0..entry.len as usize {
            unsafe { entry.batches[i].assume_init_read() };
        }
    }
}

struct BatchPoolShardInner<const CAP: usize, P: BatchAllocation = OwnedBatchPtr> {
    len: u8,
    batches: [MaybeUninit<P>; CAP],
}

impl<const CAP: usize, P: BatchAllocation> BatchPoolShardInner<CAP, P> {
    fn new() -> Self {
        Self {
            len: 0,
            batches: array::from_fn(|_| MaybeUninit::uninit()),
        }
    }

    // Must hold Mutex befor calling
    fn pop(&mut self) -> Option<P> {
        debug_assert!(self.len as usize <= CAP);

        if self.len == 0 {
            return None;
        }

        self.len -= 1;
        let idx = self.len as usize;

        let entry = std::mem::replace(&mut self.batches[idx], MaybeUninit::uninit());

        Some(unsafe { entry.assume_init() })
    }

    // Must hold Mutex before calling
    fn push(&mut self, batch: P) -> Result<(), P> {
        debug_assert!(self.len as usize <= CAP);

        if self.len as usize == CAP {
            return Err(batch);
        }

        // SAFETY:
        // The capacity check happens before the transition, so an Err returns
        // the pointer with its previous state unchanged. On success, this shard
        // takes exclusive ownership while its mutex is held and marks the live
        // allocation idle before publishing it in the shard array.
        unsafe { &*batch.batch_ptr().as_ptr() }
            .set_runtime_state(crate::db::batch::BatchRuntimeState::Idle, Ordering::Release);

        let idx = self.len as usize;

        self.batches[idx].write(batch);
        self.len += 1;

        Ok(())
    }
}

struct BatchPoolShard<const CAP: usize, P: BatchAllocation = OwnedBatchPtr> {
    inner: Mutex<BatchPoolShardInner<CAP, P>>,
}

impl<const CAP: usize, P: BatchAllocation> BatchPoolShard<CAP, P> {
    fn new() -> Self {
        debug_assert!(CAP > 0);
        debug_assert!(CAP <= u8::MAX as usize);
        Self {
            inner: Mutex::new(BatchPoolShardInner {
                len: 0,
                batches: array::from_fn(|_| MaybeUninit::uninit()),
            }),
        }
    }

    fn pop(&self) -> Option<P> {
        let mut inner = self.inner.lock().unwrap();
        inner.pop()
    }

    fn push(&self, batch: P) -> Result<(), P> {
        let mut inner = self.inner.lock().unwrap();
        inner.push(batch)
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

// DOCS: Need docs
pub(crate) struct BatchPoolImpl<
    const SHARDS_PER_POOL: usize = DEFAULT_SHARDS_PER_POOL,
    const MAX_BATCH_PER_SHARD: usize = DEFAULT_MAX_BATCHES_PER_SHARD,
    const TLS_CAP: usize = DEFAULT_THREAD_BATCH_CACHE_CAPACITY,
    const TLS_TARGET_RETAINED: usize = DEFAULT_THREAD_BATCH_CACHE_TARGET_RETAINED,
    F: BatchFactory = OwnedBatchFactory,
> {
    //
    pool: [CachePadded<BatchPoolShard<MAX_BATCH_PER_SHARD, F::Allocation>>; SHARDS_PER_POOL],

    factory: F,
    //
    next_shard: AtomicUsize,
    //
    stats: BatchPoolStats,
    //
    thread_local_ptr: ThreadLocalPtr<ThreadBatchCache<TLS_CAP, TLS_TARGET_RETAINED, F::Allocation>>,
}

// SAFETY:
//
// All mutable state shared through `BatchPool` is synchronized:
//
// - Each global shard protects its length and allocation array with a mutex.
//   A shard operation only transfers allocation ownership; the mutex does not
//   synchronize access to the pointed-to `BatchInner`.
// - The batch ownership protocol guarantees that a batch retained by a shard
//   or TLS cache has no active owner or outstanding references.
// - `next_shard` and pool statistics are accessed atomically.
// - `ThreadLocalPtr` maps each calling thread to a distinct
//   `ThreadBatchCache`, so normal pool operations cannot concurrently access
//   the same cache.
// - Cross-thread TLS teardown and column reclamation are serialized by the
//   TLS registry mutex. The owner must ensure all pool and TLS accesses have
//   quiesced before `BatchPool` and its TLS column are destroyed.
unsafe impl<
    const SHARDS_PER_POOL: usize,
    const MAX_BATCH_PER_SHARD: usize,
    const TLS_CAP: usize,
    const TLS_TARGET_RETAINED: usize,
    F: BatchFactory,
> Sync for BatchPoolImpl<SHARDS_PER_POOL, MAX_BATCH_PER_SHARD, TLS_CAP, TLS_TARGET_RETAINED, F>
{
}

impl<
    const SHARDS_PER_POOL: usize,
    const MAX_BATCH_PER_SHARD: usize,
    const TLS_CAP: usize,
    const TLS_TARGET_RETAINED: usize,
    F: BatchFactory,
> BatchPoolImpl<SHARDS_PER_POOL, MAX_BATCH_PER_SHARD, TLS_CAP, TLS_TARGET_RETAINED, F>
{
    pub(crate) fn new_with_const_size(factory: F) -> Self {
        Self {
            pool: array::from_fn(|_| CachePadded::new(BatchPoolShard::new())),
            factory,
            next_shard: AtomicUsize::new(0),
            stats: BatchPoolStats::default(),

            thread_local_ptr: ThreadLocalPtr::new(),
        }
    }

    fn assign_shard_idx(
        &self,
        cache: &mut ThreadBatchCache<TLS_CAP, TLS_TARGET_RETAINED, F::Allocation>,
    ) -> usize {
        let id = self.next_shard.fetch_add(1, Ordering::Relaxed) % SHARDS_PER_POOL;
        cache.shard_idx = Some(id);
        id
    }

    fn shard_idx_for_cache(
        &self,
        cache: &mut ThreadBatchCache<TLS_CAP, TLS_TARGET_RETAINED, F::Allocation>,
    ) -> usize {
        cache
            .shard_idx
            .unwrap_or_else(|| self.assign_shard_idx(cache))
    }

    fn push_to_global(
        &self,
        shard_idx: usize,
        batch_ptr: F::Allocation,
    ) -> Result<(), F::Allocation> {
        //
        self.pool[shard_idx].push(batch_ptr)
        //
    }

    fn thread_local_batch_cache_mut<C, R>(&self, f: C) -> R
    where
        C: FnOnce(&mut ThreadBatchCache<TLS_CAP, TLS_TARGET_RETAINED, F::Allocation>) -> R,
    {
        // SAFETY:
        // The initializer transfers a valid boxed cache into this TLP's
        // reclamation protocol. Each call accesses only the calling thread's
        // TLS row, this wrapper is not re-entered for the same cache while `f`
        // runs, and BatchPool destruction occurs only after pool accesses have
        // quiesced.
        unsafe {
            self.thread_local_ptr.get_or_init_mut(
                // We pass in an initaliser for thread local to use to init() if we don't yet have an initialised cache entry
                || {
                    let cache = Box::new(ThreadBatchCache::<
                        TLS_CAP,
                        TLS_TARGET_RETAINED,
                        F::Allocation,
                    >::new_with_const_size());
                    NonNull::new_unchecked(Box::into_raw(cache))
                },
                //
                //
                f,
            )
        }
    }

    fn try_acquire_from_tls(
        &self,
        cache: &mut ThreadBatchCache<TLS_CAP, TLS_TARGET_RETAINED, F::Allocation>,
    ) -> Option<BatchObject<UnCommitted, F::Allocation>> {
        cache.pop().map(|ptr| {
            // Removing the pointer from the calling thread's cache transfers
            // exclusive ownership to the returned BatchObject. Transition it
            // before the object can escape to caller code.
            let batch = BatchObject::acquire(ptr);
            batch
        })
        //
    }

    fn refill_tls_cache(
        &self,
        cache: &mut ThreadBatchCache<TLS_CAP, TLS_TARGET_RETAINED, F::Allocation>,
    ) -> BatchObject<UnCommitted, F::Allocation> {
        // First get the batches from the shard
        let shard = &self.pool[self.shard_idx_for_cache(cache)];

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
                self.factory.allocate()
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

        // `returnable_batch` is now owned only by this stack frame, whether it
        // came from the locked shard or a fresh allocation. Mark it acquired
        // before exposing the owning BatchObject to the caller.
        BatchObject::acquire(returnable_batch)
    }

    pub(crate) fn acquire(&self) -> BatchObject<UnCommitted, F::Allocation> {
        self.thread_local_batch_cache_mut(|cache| {
            self.try_acquire_from_tls(cache)
                .unwrap_or_else(|| self.refill_tls_cache(cache))
        })
    }

    // ----- Release Methods ----- //

    fn try_return_to_cache(
        &self,
        batch: BatchObject<UnCommitted, F::Allocation>,
        cache: &mut ThreadBatchCache<TLS_CAP, TLS_TARGET_RETAINED, F::Allocation>,
    ) -> Result<(), BatchObject<UnCommitted, F::Allocation>> {
        //
        cache
            .push(batch.into_inner())
            .map_err(|b_ptr| BatchObject::acquire(b_ptr))
        //
    }

    pub(crate) fn release<B: BatchCommitState>(&self, batch: BatchObject<B, F::Allocation>) {
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

        self.thread_local_batch_cache_mut(|cache| {
            // First try to return the batch to cache
            match cache.push(batch.into_inner()) {
                Ok(_) => return (),
                // If we fail then cache is full and we must try to push to global
                Err(batch) => {
                    // Want a method on global which will take the batch and cache spill

                    let shard_idx = self.shard_idx_for_cache(cache);

                    cache.spill_cache_to_target_retained(|b| {
                        if let Err(b) = self.push_to_global(shard_idx, b) {
                            drop(b)
                        }
                    });

                    // Once we are done we should push the hot batch back into the cache
                    if let Err(batch) = cache.push(batch) {
                        drop(batch);
                        unreachable!("spilling TLS cache must leave room for released batch");
                    }

                    return ();
                }
            }
        })
    }
}

/// Batch pool configuration used by the engine.
pub(crate) type BatchPool = BatchPoolImpl<
    DEFAULT_SHARDS_PER_POOL,
    DEFAULT_MAX_BATCHES_PER_SHARD,
    DEFAULT_THREAD_BATCH_CACHE_CAPACITY,
    DEFAULT_THREAD_BATCH_CACHE_TARGET_RETAINED,
    OwnedBatchFactory,
>;

impl BatchPoolImpl {
    pub(crate) fn new() -> Self {
        Self::new_with_const_size(OwnedBatchFactory)
    }

    // TODO: Add comments as to why we can only do this here
    pub(crate) fn acquire_batch(self: &Arc<Self>) -> Batch<UnCommitted> {
        Batch::new(Arc::clone(self), self.acquire())
    }
}

/// Indexed Batch pool configuration used by the engine.
pub(crate) type IndexedBatchPool = BatchPoolImpl<
    DEFAULT_SHARDS_PER_POOL,
    DEFAULT_MAX_BATCHES_PER_SHARD,
    DEFAULT_THREAD_BATCH_CACHE_CAPACITY,
    DEFAULT_THREAD_BATCH_CACHE_TARGET_RETAINED,
    IndexedBatchFactory,
>;

impl IndexedBatchPool {
    pub(crate) fn new() -> Self {
        Self::new_with_const_size(IndexedBatchFactory)
    }

    // TODO: Add comments as to why we can only do this here
    pub(crate) fn acquire_batch(self: &Arc<Self>) -> IndexedBatch<UnCommitted> {
        IndexedBatch::new(Arc::clone(self), self.acquire())
    }
}

#[cfg(test)]
mod tests {
    use crate::{db::batch::BatchRuntimeState, sync::atomic::Ordering};
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

                // A cached allocation is returned as an acquired BatchObject.
                cache.push(BatchObject::<UnCommitted>::new().into_inner());
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
            .push(BatchObject::<UnCommitted>::new().into_inner())
            .unwrap();

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
    fn spill_to_retain() {
        let mut thread_batch = ThreadBatchCache::<4, 2>::new_with_const_size();

        for _ in 0..4 {
            thread_batch
                .push(BatchObject::<UnCommitted>::new().into_inner())
                .unwrap();
        }

        assert_eq!(thread_batch.cache_len(), 4);

        let hot_batch = BatchObject::<UnCommitted>::new().into_inner();
        let hot_ptr = hot_batch.as_ptr();
        let mut spilled = Vec::new();

        thread_batch.spill_cache_to_target_retained(|batch| spilled.push(batch));

        assert_eq!(spilled.len(), 3);
        assert_eq!(thread_batch.cache_len(), 1);

        thread_batch.push(hot_batch).unwrap();

        assert_eq!(thread_batch.cache_len(), 2);
        assert_eq!(thread_batch.pop().unwrap().as_ptr(), hot_ptr);

        // This stack-local cache is not reclaimed through its TLS handler.
        drop(thread_batch.pop());
    }

    #[test]
    fn release_to_full_tls_back_to_global() {
        // Want small tls and small global

        // Even though we have 2 batches per global pool we set the cache retained to 2 and cap to 2 so there is no spill
        // Therefore if we fill up cache and then try to release one more batch it should be put in the global pool and nothing should spill or be destroyed
        let mut bp = BatchPoolImpl::<2, 2, 2, 2>::new_with_const_size(OwnedBatchFactory);

        // Fill up the tls cache so we have to spill to global
        bp.thread_local_batch_cache_mut(|cache| {
            let _ = cache.push(BatchObject::<UnCommitted>::new().into_inner());
            let _ = cache.push(BatchObject::<UnCommitted>::new().into_inner());
        });

        let caller_owned_batch = BatchObject::<UnCommitted>::new();

        bp.release(caller_owned_batch);

        assert_eq!(bp.pool[0].inner.lock().unwrap().len, 1);
    }

    #[test]
    fn state_lifecycle() {
        // New batch
        // Acquire = Idle -> Acquired
        // Release = Acquired -> Idle

        let pool = BatchPoolImpl::<1, 1, 1, 1>::new_with_const_size(OwnedBatchFactory);

        let batch = pool.acquire();
        assert_eq!(batch.state(Ordering::Relaxed), BatchRuntimeState::Acquired);

        pool.release(batch);

        pool.thread_local_batch_cache_mut(|cache| {
            let batch = BatchObject::<UnCommitted>::acquire(cache.pop().unwrap());
            assert_eq!(batch.state(Ordering::Relaxed), BatchRuntimeState::Acquired);
            cache.push(batch.into_inner())
        });

        let batch = pool.acquire();
        assert_eq!(batch.state(Ordering::Relaxed), BatchRuntimeState::Acquired);
        pool.release(batch);

        // TLS is full, so releasing another acquired batch spills one idle batch to global.

        let b = BatchObject::<UnCommitted>::new();
        assert_eq!(b.state(Ordering::Relaxed), BatchRuntimeState::Acquired);

        pool.release(b);
        let b = BatchObject::<UnCommitted>::acquire(pool.pool[0].pop().unwrap());
        assert_eq!(b.state(Ordering::Relaxed), BatchRuntimeState::Acquired);
    }

    #[test]
    #[should_panic]
    fn thread_cache_rejects_zero_target_retained() {
        let _ = ThreadBatchCache::<4, 0>::new_with_const_size();
    }

    #[test]
    #[should_panic]
    fn thread_cache_rejects_target_above_capacity() {
        let _ = ThreadBatchCache::<4, 5>::new_with_const_size();
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
        // Fill ThreadBatchCache to its configured capacity.
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
        // Assert indexes wrap modulo the pool's shard count.
    }

    #[test]
    fn thread_batch_cache_unref_drops_cached_batches() {
        // After implementing cache draining:
        // Create a boxed ThreadBatchCache with initialized batch pointers.
        // Call ThreadBatchCache::unref through the erased handler.
        // Assert no leak/double-free under miri or a drop-counting test batch helper.
    }
}
