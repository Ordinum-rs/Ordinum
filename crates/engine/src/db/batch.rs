use std::fmt::Display;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ptr::NonNull;
use std::slice::from_raw_parts_mut;
use std::thread::{self, Thread};
use std::{array, panic, ptr, todo};
use std::{marker::PhantomData, sync::atomic::AtomicU8};

use crate::arena::arena::Arena;
use crate::column_family::cf::ColumnFamilyHandle;
use crate::db::DEFAULT_CF_ID;
use crate::db::batch_pool::{BatchPool, BatchPoolHandle};
use crate::db::options::DEFAULT_MAX_WRITE_BATCH_BYTES;
use crate::db::write_batch::BatchOpType;
use crate::db::write_pipeline::WritePipeline;
use crate::db::{self, db_impl::DbImpl};
use crate::key::MAX_USER_KEY_BYTES;
use crate::memtable::memtable::{Memtable, Mutable};
use crate::sync::Arc;
use crate::sync::atomic::Ordering;
use crate::utils::read_u32_le_unsafe;
use crate::utils::skiplists::batch_index::BatchSkipList;
use crate::utils::var_int::VarInt;
use crate::wal::{SyncLogWaiter, SyncWaiter, WalSyncResultState};
use crate::{Error, Result};
use crate::{error, utils};

use super::batch_pool::BatchPoolImpl;
use super::batch_pool::IndexedBatchPool;

// ---- Constants ---- //

// Batch size policy flow
// ======================
//
// 1. Database policy
//
//    `DEFAULT_MAX_WRITE_BATCH_BYTES` is the default operational limit for one
//    atomic batch. `MAX_BATCH_SIZE` exposes that default within this module.
//    Each `BatchInner` stores the selected limit in `max_batch_size` so a future
//    `DBOptions` value can be copied into newly allocated batches without
//    changing the encoding or reservation logic.
//
// 2. API input and wire-format limits
//
//    Before reserving memory, the write path validates the user-key policy and
//    converts key/value lengths to the `u32` lengths used by the batch format.
//    It then calculates the complete encoded record size with checked
//    arithmetic. The size includes the kind, column-family ID, both varints,
//    and the key/value bytes; it does not include the batch header.
//
// 3. Atomic batch reservation
//
//    `reserve_operation` first checks whether `HEADER_SIZE + record_size` can
//    ever fit in an empty batch. Failure is `RecordTooLarge`. If the record fits
//    alone but not after the records already present, failure is `BatchFull`.
//    Both checks happen before the Vec is initialized or resized, so callers may
//    commit a full batch and retry a valid record in a fresh batch.
//
// 4. WAL and commit pipeline
//
//    `data.len()` is the serialized batch size: the 12-byte header followed by
//    encoded operations. These same bytes form the WAL record, so the batch
//    limit also bounds one atomic WAL write, recovery work, sequence reservation,
//    and the amount of memory retained while a commit is in flight. A batch is
//    never silently split because that would break its atomicity.
//
// 5. Memtable application
//
//    The batch limit is independent of a column family's write-buffer size.
//    During application, each operation must also fit in one memtable arena
//    allocation. The current arena rejects layouts larger than its regular block
//    size. Future large-batch handling may install a batch as a flushable LSM
//    layer, and future blob support may replace a durable value with a reference;
//    neither behavior is currently implemented, so oversized records are
//    rejected at the batch boundary.
//
// 6. Pool retention
//
//    `max_batch_size` controls what may be written, while the batch pool's
//    retention policy controls how much Vec capacity is kept after reset. A
//    previously large but valid batch may be shrunk before pooling; this does not
//    change its configured write limit and it may grow again on later use.

pub(crate) const MAX_BATCH_SIZE: usize = DEFAULT_MAX_WRITE_BATCH_BYTES;
pub(crate) const DEFAULT_BATCH_INIT_SIZE: usize = 1 << 10;

const DEFAULT_INLINE_CF_ARRAY: usize = 4;

// ---- Module Errors ---- //

// ---- Batch Meta ---- //

#[derive(Debug)]
struct BatchMeta {
    first_seen_cf_id: u64,
    multiple_cf_ids: bool,
}

impl BatchMeta {
    fn clear(&mut self) {
        self.first_seen_cf_id = 0;
        self.multiple_cf_ids = false;
    }
}

// ---- Batch CF Table ---- //

// TODO: Finish from here

/* NOTE:
 * It's ok to use Memtable<Mutable> here and not MemInner because the CF of that memtable will try to reserve space and rotate if it needs to
 * meaning we agree that there will be some stranded space in that frozen memtable
 * Other CF's will do the same to their repspective memtables
 * Future batches will not go back to old memtables to fill the space they will continue on the current memtable
 *
 * XXX: Future optimisation being flushable batches to avoid wasting current memtable space
*/
#[derive(Debug)]
struct CFTableEntry((u64, NonNull<Memtable<Mutable>>));

#[derive(Debug)]
struct CFTable {
    len: usize,
    vec: Vec<CFTableEntry>,
}

impl CFTable {
    fn new() -> Self {
        Self {
            len: 0,
            vec: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.len = 0;
        self.vec.clear();
    }
}

//
// ---- Batch Operations Enum ---- //

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatchRecordKind {
    Put = 1,
    Delete = 2,
    Merge = 3,
    RangeDel = 4,
    // XXX: More operations in later updates
}

// ---- Batch Runtime State ---- //

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum BatchRuntimeState {
    Idle,
    Acquired,
    InQueue,
    WaitingSync,
    Applied,
}

impl Display for BatchRuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchRuntimeState::Idle => write!(f, "Idle"),
            BatchRuntimeState::Acquired => write!(f, "Acquired"),
            BatchRuntimeState::InQueue => write!(f, "InQueue"),
            BatchRuntimeState::WaitingSync => write!(f, "WaitingSync"),
            BatchRuntimeState::Applied => write!(f, "Applied"),
        }
    }
}

impl From<u8> for BatchRuntimeState {
    fn from(value: u8) -> Self {
        match value {
            0 => BatchRuntimeState::Idle,
            1 => BatchRuntimeState::Acquired,
            2 => BatchRuntimeState::InQueue,
            3 => BatchRuntimeState::WaitingSync,
            4 => BatchRuntimeState::Applied,
            _ => unreachable!(),
        }
    }
}

impl BatchRuntimeState {
    const RESET_SAFE_STATES: [Self; 2] = [Self::Acquired, Self::Applied];

    pub(super) fn is_reset_safe(self) -> bool {
        Self::RESET_SAFE_STATES.contains(&self)
    }
}

// --- Batch Type States --- //

pub(crate) trait BatchCommitState {}

#[derive(Debug)]
pub(crate) struct UnCommitted {}
impl BatchCommitState for UnCommitted {}

pub(crate) struct Sealed {}
impl BatchCommitState for Sealed {}

pub(crate) trait SealedBatch {
    /// Returns the stable address of the encoded Batch consumed by the pipeline.
    fn batch_ptr(&self) -> NonNull<BatchInner>;
}

// ---- Batch Factory Trait ---- //

/// Creates fresh owning allocations when a batch pool cannot reuse one from
/// its thread-local cache or global shards.
///
/// The associated allocation type binds a pool and all of its storage tiers to
/// one concrete ownership representation, such as `OwnedBatchPtr` or
/// `OwnedIndexedBatchPtr`. A factory may be zero-sized or may hold allocation
/// configuration such as an initial capacity.
///
/// Factories are `Send + Sync` because a shared batch pool may allocate through
/// the same factory concurrently. The factory controls construction only;
/// runtime-state transitions and movement between ownership phases remain the
/// responsibility of `BatchPoolImpl` and `BatchObject`.
pub(crate) trait BatchFactory: Send + Sync {
    type Allocation: BatchAllocation;

    /// Returns one valid, uniquely owned batch allocation.
    fn allocate(&self) -> (usize, Self::Allocation);

    fn allocate_with_capacity(&self, cap: usize) -> (usize, Self::Allocation);
}

#[derive(Default)]
pub(crate) struct OwnedBatchFactory;

impl BatchFactory for OwnedBatchFactory {
    type Allocation = OwnedBatchPtr;

    fn allocate(&self) -> (usize, Self::Allocation) {
        let batch = Box::new(BatchInner::new());
        (batch.data.len(), batch.into())
    }

    fn allocate_with_capacity(&self, cap: usize) -> (usize, Self::Allocation) {
        let batch = Box::new(BatchInner::new_with_capacity(cap));
        (batch.data.len(), batch.into())
    }
}

// NOTE: Can hold state in {} if needed
#[derive(Default)]
pub(crate) struct IndexedBatchFactory;

impl BatchFactory for IndexedBatchFactory {
    type Allocation = OwnedIndexedBatchPtr;

    fn allocate(&self) -> (usize, Self::Allocation) {
        // TODO: finish indexed allocation
        todo!()
    }

    fn allocate_with_capacity(&self, cap: usize) -> (usize, Self::Allocation) {
        todo!()
    }
}

// ---- Batch Allocation Trait ---- //

pub(crate) unsafe trait BatchAllocation: Send + Sized {
    fn batch_ptr(&self) -> NonNull<BatchInner>;

    fn reset_for_reuse(&mut self);
}

/// Owning pointer to a heap-allocated `BatchInner`.
///
/// `OwnedBatchPtr` is the stable allocation identity used by the batch pool and
/// write pipeline. The pointed-to `BatchInner` is allocated with `Box::into_raw` and
/// must be destroyed exactly once with `Box::from_raw` when it is no longer
/// retained by the pool.
///
/// # Invariants
///
/// - The pointer is non-null, aligned, and was produced from `Box<BatchInner>`.
/// - At any time, ownership is in exactly one phase:
///   - retained by TLS cache,
///   - retained by a global pool shard,
///   - owned by an active `BatchObject<S, P>`,
///   - or visible to the write pipeline until commit publication completes.
/// - A batch must not be returned to TLS/global pool while any queue slot,
///   write pipeline stage, caller, or worker thread may still access it.
/// - Non-atomic batch fields may be mutated only by the current owner before
///   publication, or by the write pipeline at protocol points that have
///   exclusive access.
/// - Cross-thread state changes after publication must use atomics or other
///   synchronization.
#[derive(Debug)]
pub(crate) struct OwnedBatchPtr {
    ptr: NonNull<BatchInner>,
}

impl OwnedBatchPtr {
    pub(super) fn as_ptr(&self) -> *mut BatchInner {
        self.ptr.as_ptr()
    }

    pub(super) fn as_non_null(&self) -> NonNull<BatchInner> {
        self.ptr
    }

    // Destroy takes the heap allocated Batch and de-alloacates.
    //
    // # Safety
    //
    // The caller must ensure that when calling destroy() no other references to the Batch are stored and no Pointers are still held
    pub(super) unsafe fn destroy(self) {
        drop(self);
    }
}

impl From<Box<BatchInner>> for OwnedBatchPtr {
    fn from(batch: Box<BatchInner>) -> Self {
        let ptr = Box::into_raw(batch);

        // SAFETY: Box::into_raw never returns a null pointer.
        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
        }
    }
}

// SAFETY:
//
// OwnedBatchPtr uniquely owns a Box<BatchInner>. The returned pointer is non-null,
// correctly aligned, and remains valid and stable until this owner is dropped.
unsafe impl BatchAllocation for OwnedBatchPtr {
    fn batch_ptr(&self) -> NonNull<BatchInner> {
        self.ptr
    }

    fn reset_for_reuse(&mut self) {
        let batch = unsafe { &mut *self.as_ptr() };
        batch.clear();
    }
}

// SAFETY:
//
// `OwnedBatchPtr` transfers ownership of a stable heap allocation between threads.
// The pointer itself does not permit shared mutation. Safe APIs must preserve
// the phase invariant: only one owner may mutate non-atomic batch state, and a
// batch visible to the write pipeline may not be reused or destroyed.
unsafe impl Send for OwnedBatchPtr {}

impl Drop for OwnedBatchPtr {
    fn drop(&mut self) {
        drop(unsafe { Box::from_raw(self.ptr.as_ptr()) })
    }
}

// TODO: Integrate indexed batches with their caller-facing object and pool.
#[derive(Debug)]
pub(crate) struct OwnedIndexedBatchPtr {
    ptr: NonNull<IndexedBatchInner>,
}

impl OwnedIndexedBatchPtr {
    pub(super) fn as_ptr(&self) -> *mut IndexedBatchInner {
        self.ptr.as_ptr()
    }

    pub(super) fn as_non_null(&self) -> NonNull<IndexedBatchInner> {
        self.ptr
    }

    pub(super) fn batch_ptr(&self) -> NonNull<BatchInner> {
        // SAFETY:
        //
        // `self.ptr` owns a live IndexedBatch allocation. &raw mut
        // projects the embedded Batch field without creating an intermediate
        // reference, and that field remains stable for the allocation's life.
        let ptr = unsafe { &raw mut (*self.ptr.as_ptr()).batch };
        unsafe { NonNull::new_unchecked(ptr) }
    }

    // # Safety
    //
    // The caller must ensure that no references or non-owning pointers to the
    // IndexedBatch or its embedded Batch remain.
    pub(super) unsafe fn destroy(self) {
        drop(self);
    }
}

impl From<Box<IndexedBatchInner>> for OwnedIndexedBatchPtr {
    fn from(batch: Box<IndexedBatchInner>) -> Self {
        let ptr = Box::into_raw(batch);

        // SAFETY:
        //
        // Box::into_raw never returns a null pointer.
        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
        }
    }
}

// SAFETY:
//
// OwnedIndexedBatchPtr uniquely owns a Box<IndexedBatchInner>. `batch` is embedded
// within that stable heap allocation and therefore remains valid until the
// owning IndexedBatchInner is dropped.
unsafe impl BatchAllocation for OwnedIndexedBatchPtr {
    fn batch_ptr(&self) -> NonNull<BatchInner> {
        unsafe { NonNull::new_unchecked(&raw mut (*self.as_ptr()).batch) }
    }

    fn reset_for_reuse(&mut self) {
        let batch = unsafe { &mut *self.as_ptr() };

        // TODO: Clear the skiplist
        // TODO: Clear range-del skiplist
        batch.arena.reset();
        batch.batch.clear();
    }
}

// SAFETY:
//
// OwnedIndexedBatchPtr uniquely owns a stable IndexedBatchInner allocation. Moving
// the owning pointer to another thread does not itself permit aliased access.
unsafe impl Send for OwnedIndexedBatchPtr {}

impl Drop for OwnedIndexedBatchPtr {
    fn drop(&mut self) {
        // TODO:
        // If BatchSkipList gains teardown that dereferences arena-backed
        // nodes, destroy the indexes before allowing IndexedBatchInner::arena to drop.
        //
        // SAFETY:
        //
        // `ptr` came from Box::into_raw in the Box conversion, and this
        // owning wrapper is responsible for reconstructing that Box exactly once.
        drop(unsafe { Box::from_raw(self.ptr.as_ptr()) })
    }
}

// ---- Batch ---- //

pub(crate) struct Batch<S, P = BatchPool>
where
    S: BatchCommitState,
    P: BatchPoolHandle<Allocation = OwnedBatchPtr>,
{
    pool: Arc<P>,
    batch: BatchObject<S, OwnedBatchPtr>,
}

impl<S, P> Batch<S, P>
where
    S: BatchCommitState,
    P: BatchPoolHandle<Allocation = OwnedBatchPtr>,
{
    pub(crate) fn new(pool: Arc<P>, batch: BatchObject<S, OwnedBatchPtr>) -> Self {
        Self { pool, batch }
    }

    pub(crate) fn inner(&self) -> &BatchObject<S, OwnedBatchPtr> {
        &self.batch
    }

    pub(crate) fn reset(mut self) -> Batch<UnCommitted, P> {
        //
        debug_assert!(
            self.batch.can_reset(),
            "batch cannot be reset while the pipeline still owns access"
        );

        self.wait().expect("batch wait failed before reset");

        Batch {
            pool: self.pool,
            batch: self.batch.reset_batch(),
        }
    }

    fn wait(&self) -> Result<()> {
        self.batch.wait_until_reusable()?;
        Ok(())
    }

    // TODO:
    pub(crate) fn close(self) -> Result<()> {
        self.wait()?;
        self.pool.release(self.batch);
        Ok(())
    }
}

impl<P> Batch<UnCommitted, P>
where
    P: BatchPoolHandle<Allocation = OwnedBatchPtr>,
{
    pub(crate) fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.batch.put(key, value);
    }

    pub(crate) fn seal(self) -> Batch<Sealed, P> {
        Batch {
            pool: self.pool,
            batch: self.batch.seal(),
        }
    }
}

impl<P> SealedBatch for Batch<Sealed, P>
where
    P: BatchPoolHandle<Allocation = OwnedBatchPtr>,
{
    fn batch_ptr(&self) -> NonNull<BatchInner> {
        self.batch.as_non_null()
    }
}

// ---- Index Batch ---- //

pub(crate) struct IndexedBatch<S, P = IndexedBatchPool>
where
    S: BatchCommitState,
    P: BatchPoolHandle<Allocation = OwnedIndexedBatchPtr>,
{
    pool: Arc<P>,
    batch: BatchObject<S, OwnedIndexedBatchPtr>,
}

// TODO: Implement user facing indexed batch methods

impl<S, P> IndexedBatch<S, P>
where
    S: BatchCommitState,
    P: BatchPoolHandle<Allocation = OwnedIndexedBatchPtr>,
{
    pub(crate) fn new(pool: Arc<P>, batch: BatchObject<S, OwnedIndexedBatchPtr>) -> Self {
        Self { pool, batch }
    }

    pub(crate) fn inner(&self) -> &BatchObject<S, OwnedIndexedBatchPtr> {
        &self.batch
    }

    fn wait(&self) -> Result<()> {
        self.batch.wait_until_reusable()?;
        Ok(())
    }

    // TODO: Needs looking at
    pub(crate) fn close(self) -> Result<()> {
        self.wait()?;
        self.pool.release(self.batch);
        // NOTE: Does drops for arena etc kick in here
        Ok(())
    }

    pub(crate) fn reset(mut self) -> IndexedBatch<UnCommitted, P> {
        self.wait().expect("batch wait failed before reset");

        // Must reset the arena?
        unsafe { &mut *self.batch.inner.as_ptr() }.arena.reset();

        IndexedBatch {
            pool: self.pool,
            batch: self.batch.reset_batch(),
        }
    }
}

impl<P> SealedBatch for IndexedBatch<Sealed, P>
where
    P: BatchPoolHandle<Allocation = OwnedIndexedBatchPtr>,
{
    fn batch_ptr(&self) -> NonNull<BatchInner> {
        self.batch.as_non_null()
    }
}

/// Batches use a compact binary representation where all operations are encoded sequentially into a byte slice
/// the binary representation is so that batches can form the records of the WAL without any additional changes
/// We are free to choose the endianness and for optimisation on x86 architectures we choose little endian here.
///
/// This applies only to the structure of the batch i.e batch count, varint and column_family_id. For the internal key, we defer to the endianness it uses which is
/// big endian for seq number comparison
///
/// A batch holds a set of operations to be committed atomically as part of the write path.
/// Each operation is binary encoded and appended to a contiguous Vec<u8> buffer.
/// The buffer begins with a 12-byte header:
///   - 8 bytes: starting sequence number (assigned at commit time)
///   - 4 bytes: operation count
///
/// Batches are created both implicitly (e.g. DB::put) and explicitly by users.
///
/// A single DB::put() creates a batch containing one operation, allowing the
/// write path to uniformly operate on batches regardless of origin.
///
/// Example (Pseudo code):
///
/// ```
/// DB::put("key1", "value1");
///
/// // Internally:
///
/// fn put(&self, key: &[u8], value: &[u8]) {
///     let mut batch = self.acquire_batch();
///     batch.put(DEFAULT_CF, key, value);
///     self.commit(&batch)?;
///     //
///     batch.reset();
/// }
///
/// ```
///
/// Batch holds a group of operations for a writer/caller thread. [Put, Delete, Merge ...].
///
/// A batch should be 1:1 with a writer thread. A writer/caller should create a batch and push operations into the batch
/// before calling Commit to have the batch processed by the [write_pipeline.rs]('WritePipeline').
///
/// Batches are heap allocated and may be retained by a batch pool for reuse.
/// A sealed batch may be passed through the WritePipeline using non-owning pointers.
///
/// A batch allocation must remain alive and must not return to the pool while it
/// is visible to the WritePipeline or while another thread may still access it.
#[derive(Debug)]
pub(crate) struct BatchObject<B: BatchCommitState, P: BatchAllocation = OwnedBatchPtr> {
    _state: PhantomData<B>,
    inner: P,
}

// ---- Generic Batch Object Impl ---- //

impl<B: BatchCommitState, P: BatchAllocation> BatchObject<B, P> {
    //
    fn transition<S: BatchCommitState>(self) -> BatchObject<S, P> {
        BatchObject {
            _state: PhantomData,
            inner: self.inner,
        }
    }

    pub(super) fn as_inner_ptr(&self) -> *mut BatchInner {
        self.inner.batch_ptr().as_ptr()
    }

    pub(super) fn as_non_null(&self) -> NonNull<BatchInner> {
        self.inner.batch_ptr()
    }

    /// Atomically sets the runtime state on the heap-allocated batch.
    ///
    /// While a batch is retained by TLS or the global pool, it is exclusively owned
    /// by that storage and no references to it may exist. The unsafe boundary starts
    /// once the batch has been acquired and references/pointers may be handed to the
    /// caller or write pipeline.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the batch allocation is still live and has not
    /// been returned to TLS/global pool or destroyed.
    ///
    /// The caller must also guarantee that this state transition is valid for the
    /// current ownership phase. This method does not enforce legal transitions or
    /// prevent a batch from being recycled while another thread still holds a
    /// pointer to it.
    ///
    /// Concurrent access to `runtime_commit_state` itself is safe because it is
    /// atomic. This method does not protect any non-atomic batch fields.
    pub(super) unsafe fn set_runtime_state(&self, state: BatchRuntimeState, ordering: Ordering) {
        //
        // SAFETY:
        // The caller guarantees that the batch pointer is live and not currently
        // retained by the pool. We only access the atomic runtime state field.
        unsafe { &*self.as_inner_ptr() }
            .runtime_commit_state
            .store(state as u8, ordering)
    }

    pub(crate) fn state(&self, ordering: Ordering) -> BatchRuntimeState {
        BatchRuntimeState::from(
            unsafe { &*self.as_inner_ptr() }
                .runtime_commit_state
                .load(ordering),
        )
    }

    pub(crate) fn is_state(&self, state: BatchRuntimeState) -> bool {
        // SAFTEY:
        //
        // We are safe to dereference here because the BatchObject ensures the underlying batch heap allocation is alive and we are
        // accessing an atomic field only
        unsafe { &*self.as_inner_ptr() }
            .runtime_commit_state
            .load(Ordering::Relaxed)
            == state as u8
    }

    pub(super) fn wait_until_reusable(&self) -> Result<()> {
        // SAFETY:
        //
        // BatchObject owns the allocation and therefore keeps it alive while this
        // method accesses the embedded waiter. Ownership alone does not exclude
        // non-owning pipeline pointers.
        //
        // The write-pipeline contract requires all accesses through those pointers
        // to finish before commit returns and publishes `Applied`. `reset_batch`
        // verifies that state with an Acquire load before mutating the allocation.
        //
        // At this point only the WAL worker may remain active, and it holds a clone
        // of `sync_waiter`, not a pointer into the mutable batch data. Waiting here
        // prevents the batch and its waiter from being reset or pooled before that
        // WAL operation completes.
        let batch = unsafe { &*self.as_inner_ptr() };

        // TODO: Need to wait on the sync signal - do we need a timeout?

        match batch.sync_waiter.state() {
            WalSyncResultState::Init => Ok(()),
            WalSyncResultState::Primed => batch.sync_waiter.wait().map_err(|_| Error::WalError),
            WalSyncResultState::SyncDone => Ok(()),
            WalSyncResultState::IoError | WalSyncResultState::WalError => Err(Error::WalError),
        }
    }

    pub(crate) fn can_reset(&self) -> bool {
        let state = self.state(Ordering::Acquire);
        if !state.is_reset_safe() {
            return false;
        } else {
            return true;
        }
    }

    /// Clears the batch for immediate reuse while retaining its current buffer
    /// capacity. Pool release applies its own retention and shrinking policy.
    pub(crate) fn reset_batch(mut self) -> BatchObject<UnCommitted, P> {
        //
        assert!(self.state(Ordering::Acquire).is_reset_safe());
        self.inner.reset_for_reuse();

        unsafe { self.set_runtime_state(BatchRuntimeState::Acquired, Ordering::Release) };
        self.transition()
    }
}

//
// ---- Uncommitted Non-Indexed ---- //

impl BatchObject<UnCommitted, OwnedBatchPtr> {
    pub(super) fn new() -> Self {
        let inner = Box::new(BatchInner::new());

        Self {
            _state: PhantomData,
            inner: OwnedBatchPtr::from(inner),
        }
    }

    pub(super) fn new_with_capacity(cap: usize) -> Self {
        let inner = Box::new(BatchInner::new_with_capacity(cap));
        Self {
            _state: PhantomData,
            inner: OwnedBatchPtr::from(inner),
        }
    }

    pub(crate) fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        // Any assertions
        self.put_cf(Self::default_cf(), key, value);
    }

    pub(crate) fn put_cf<K, V>(&mut self, cf_id: u64, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        //
        // TODO: Finish this when we have column families and can use a resolver

        // # SAFETY
        //
        // BatchObject owns the inner allocation and keeps it alive while this method is active. The BatchCommitState <Uncommitted>
        // ensures that the batch object is not in the write pipeline and because we hold the BatchObject
        // we are not in the pool or batch cache so we are safe to dereference into &BatchInner
        let batch_inner =
            unsafe { &mut *self.as_inner_ptr() }.put(cf_id, key.as_ref(), value.as_ref());
    }
}

// ---- Uncommitted Indexed ---- //

impl BatchObject<UnCommitted, OwnedIndexedBatchPtr> {
    pub(super) fn new() -> Self {
        let inner = Box::new(IndexedBatchInner::new());

        Self {
            _state: PhantomData,
            inner: OwnedIndexedBatchPtr::from(inner),
        }
    }

    pub(crate) fn put<K, V>(&self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        // Any assertions
        self.put_cf(Self::default_cf(), key, value);
    }

    pub(crate) fn put_cf<K, V>(&self, cf_id: u64, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        //
        // TODO: Finish this when we have column families and can use a resolver
    }
}

// ---- Uncommitted Generic ---- //

impl<P: BatchAllocation> BatchObject<UnCommitted, P> {
    fn default_cf() -> u64 {
        DEFAULT_CF_ID
    }

    pub(super) fn acquire(inner: P) -> Self {
        let object = Self {
            inner,
            _state: PhantomData,
        };

        unsafe { object.set_runtime_state(BatchRuntimeState::Acquired, Ordering::Release) };
        object
    }

    pub(super) fn into_inner(self) -> P {
        self.inner
    }

    pub(super) fn set_acquired_state(&self) {
        // SAFETY:
        // BatchObject owns the allocation. Callers use this only after removing
        // the pointer from idle storage, before the object is exposed outside
        // the ownership-transfer operation.
        unsafe { self.set_runtime_state(BatchRuntimeState::Acquired, Ordering::Release) };
    }

    pub(crate) fn seal(self) -> BatchObject<Sealed, P> {
        BatchObject {
            _state: PhantomData,
            inner: self.inner,
        }
    }

    pub(crate) fn resize_to(&mut self, new_size: usize) {
        // TODO: Add safety comments
        let batch = unsafe { &mut *self.as_inner_ptr() };

        batch.data.reserve(new_size);
    }

    pub(crate) fn shrink_to(&mut self, new_size: usize) {
        // TODO: Add safety comments
        let batch = unsafe { &mut *self.as_inner_ptr() };
        batch.shrink_batch_to(new_size);
    }
}

// ---- Sealed Non-Indexed ---- //

impl BatchObject<Sealed, OwnedBatchPtr> {
    //
}

// ---- Sealed Indexed ---- //

impl BatchObject<Sealed, OwnedIndexedBatchPtr> {
    //
}

// ---- IndexedBatchInner ---- //

// Index Batch Objects

// TODO: Make the Objects

pub(super) struct IndexedBatchInner {
    batch: BatchInner,
    arena: Arena,
    index: BatchSkipList,
    // XXX: range-del skiplist indexes are allocated lazily on first operation
    range_del_index: Option<BatchSkipList>,
    // XXX: Need tombstone cache which is invalidated on iterator creation
    //
}

impl IndexedBatchInner {
    fn new() -> Self {
        // TODO: Finish indexed batch inner
        todo!()
    }
}

// ------------------------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct DeferredOp<'batch> {
    _life: PhantomData<&'batch ()>,
    batch: Option<&'batch mut BatchInner>,
    reservation_start: usize,
    reservation_end: usize,
    key_offset: usize,
    key_len: usize,
    value_offset: usize,
    value_len: usize,
}

impl<'batch> DeferredOp<'batch> {
    fn new(
        batch: &'batch mut BatchInner,
        reservation_start: usize,
        reservation_end: usize,
        key_offset: usize,
        key_len: usize,
        value_offset: usize,
        value_len: usize,
    ) -> Self {
        debug_assert!(reservation_start <= reservation_end);

        Self {
            _life: PhantomData,
            batch: Some(batch),
            reservation_start,
            reservation_end,
            key_offset,
            key_len,
            value_offset,
            value_len,
        }
    }

    pub(crate) fn key_mut(&mut self) -> &mut [u8] {
        debug_assert!(self.batch.is_some());

        &mut self.batch.as_deref_mut().unwrap_or_else(|| panic!()).data
            [self.key_offset..self.key_offset + self.key_len]
    }

    pub(crate) fn value_mut(&mut self) -> &mut [u8] {
        debug_assert!(self.batch.is_some());

        &mut self.batch.as_deref_mut().unwrap_or_else(|| panic!()).data
            [self.value_offset..self.value_offset + self.value_len]
    }

    pub(crate) fn key_value_mut(&mut self) -> (&mut [u8], &mut [u8]) {
        //
        let batch = self.batch.as_deref_mut().expect("missing deferred batch");

        let region = &mut batch.data[self.key_offset..self.value_offset + self.value_len];

        // Region
        //  ---- Key ---- [X] --- Value ---
        // [0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0]
        //  0           4     6           10
        //
        //

        let (key, remainder) = region.split_at_mut(self.key_len);

        let value_varint_len = self.value_offset - (self.key_offset + self.key_len);

        let (_, value) = remainder.split_at_mut(value_varint_len);

        (key, &mut value[..self.value_len])
    }

    // We don't want this to consume self because we want to allow drop to handle the deferred op
    pub(crate) fn finish(&mut self) {
        self.batch = None
    }

    // TODO: Need rollback() method
    fn rollback(&mut self) {
        //
        debug_assert!(self.batch.is_some());
        // rollback is simply the mechanism to unreserve batch buffer space if we error during writing
        self.reservation_end = self.reservation_start;

        self.batch
            .as_deref_mut()
            .expect("found None for Batch when trying to Rollback")
            .data
            .resize(self.reservation_start, 0);

        //
    }
}

// TODO: Implement Drop for DeferredOp which rolls back the inner batch buffer to original reservation start IF batch is some()
impl<'batch> Drop for DeferredOp<'batch> {
    fn drop(&mut self) {
        // If batch is None then we do nothing else we need to rollback and then drop
        if self.batch.is_some() {
            println!("Rolling ...");
            self.rollback();
            return;
        }
        println!("Dropping normally");
    }
}

//

//TODO: Add sync waiting state and completion state so the batch can wait for fysync

//
// Batch:
// | --------- 12 byte header ----------|--------- Operations ---------|
// | Seq No (8 bytes) | Count (4 bytes) | Operation 1 ... Operation 2...
//
//
// Operation:
// | op_type (1 byte) | cf_id (u64 LE) | key_len (VarInt) | key ... | value_len (VarInt) | value ... |

// https://github.com/cockroachdb/pebble/blob/a3b8dfe9e85015110be33743718a7de47458a4d7/batch.go#L199
//
//
//
#[derive(Debug)]
pub(super) struct BatchInner {
    // ----
    // Operaton Data
    //
    data: Vec<u8>,
    /// The maximum total serialized size allowed for a single atomic Batch.
    ///
    /// This limit is a global operational safety bound, not a memtable-fit constraint.
    ///
    /// A Batch may span multiple column families, and its contents are applied
    /// independently into each destination memtable. As a result, the total batch
    /// size may legitimately exceed the configured write buffer size of any single
    /// column family.
    ///
    /// This limit exists to:
    /// - bound WAL write latency and recovery cost,
    /// - prevent pathological memory pressure during commit/replay,
    /// - avoid extremely large sequence reservations,
    /// - preserve fairness for concurrent writers,
    /// - and protect the write pipeline from oversized atomic operations.
    ///
    /// Memtable capacity is a separate concern. Per-column-family reservation
    /// and a flushable-batch path still need to be implemented before batches
    /// larger than an ordinary destination memtable can be accepted safely.
    max_batch_size: usize,
    //
    count: u32,
    //
    //
    //
    //

    // ----
    // Commit Pipeline State
    //
    runtime_commit_state: AtomicU8,

    batch_meta: BatchMeta,
    cf_table: CFTable,

    // Per-batch WAL fsync completion.
    //
    // The batch owns this stable Arc for its whole allocation lifetime. The WAL
    // worker receives a clone when this batch is written, then signals it when
    // the batch's WAL bytes are durable. Reset/reuse must wait on this waiter
    // when the batch has outstanding sync work.
    pub(crate) sync_waiter: SyncWaiter,
}

impl BatchInner {
    const SEQ_NO_OFFSET: usize = 0; // seq starts at byte 0
    const BATCH_COUNT_OFFSET: usize = size_of::<u64>(); // count starts at byte 8
    const HEADER_SIZE: usize = size_of::<u64>() + size_of::<u32>(); // = 12

    fn new() -> Self {
        let mut data = Vec::with_capacity(DEFAULT_BATCH_INIT_SIZE);
        data.resize(Self::HEADER_SIZE, 0);
        Self {
            data,
            max_batch_size: MAX_BATCH_SIZE,
            count: 0,
            runtime_commit_state: AtomicU8::new(BatchRuntimeState::Acquired as u8),
            batch_meta: BatchMeta {
                first_seen_cf_id: 0,
                multiple_cf_ids: false,
            },
            cf_table: CFTable::new(),
            sync_waiter: Arc::new(SyncLogWaiter::default()),
        }
    }

    fn new_with_capacity(cap: usize) -> Self {
        let capacity = cap
            .checked_add(Self::HEADER_SIZE)
            .expect("batch initial capacity overflow");

        assert!(capacity <= MAX_BATCH_SIZE);
        let mut data = Vec::with_capacity(capacity);
        data.resize(Self::HEADER_SIZE, 0);
        Self {
            data,
            max_batch_size: MAX_BATCH_SIZE,
            count: 0,
            runtime_commit_state: AtomicU8::new(BatchRuntimeState::Acquired as u8),
            batch_meta: BatchMeta {
                first_seen_cf_id: 0,
                multiple_cf_ids: false,
            },
            cf_table: CFTable::new(),
            sync_waiter: Arc::new(SyncLogWaiter::default()),
        }
    }

    fn init_buffer(&mut self, capacity_hint: usize) {
        debug_assert!(self.data.len() == 0);

        let desired_capacity = Self::HEADER_SIZE + capacity_hint;
        debug_assert!(desired_capacity <= self.max_batch_size);

        self.data.reserve(desired_capacity);

        // We've validated that we have the capacity - now we want len to start after the header so we can write operations to the region
        self.data.resize(Self::HEADER_SIZE, 0);
    }

    fn seq_num(&self) -> u64 {
        debug_assert!(self.data.len() > Self::BATCH_COUNT_OFFSET);
        let ptr = self.data[..Self::BATCH_COUNT_OFFSET].as_ptr();

        // SAFETY
        //
        // We know that the data slice is greater than 8 bytes
        // Batches are created always with enough bytes for the header to exist. The Vec initialises the data so read_unaligned is safe for the first 8 bytes
        unsafe { utils::read_u64_unsafe(ptr) }
    }

    /// assign_seq_num_once stamps the reserved sequence number into the
    /// batch header.
    ///
    /// The sequence number occupies the first 8 bytes of the encoded batch
    /// representation and is written exactly once by the commit pipeline
    /// after global sequence reservation succeeds.
    ///
    /// # Safety
    ///
    /// This method performs interior mutation through a shared reference by
    /// mutating the underlying encoded batch bytes directly.
    ///
    /// The caller must guarantee:
    ///
    /// - No concurrent mutation of the sequence number field occurs.
    /// - The sequence number write must happen-before any concurrent
    ///   observation of the batch by readers or writers.
    ///
    /// Violating these invariants may result in undefined behavior, torn
    /// visibility of sequence metadata, or corruption of commit ordering
    /// semantics.
    pub(super) unsafe fn assign_seq_num_once(&self, seq_num: u64) {
        debug_assert!(self.data.len() > Self::BATCH_COUNT_OFFSET);
        let b_ptr = self.data[..Self::BATCH_COUNT_OFFSET].as_ptr().cast_mut();
        // # SAFETY
        //
        // We assert that data slice is greater than 8 bytes
        // Batches are created always with enough bytes for the header to exist. The Vec initialises the data so copy_non_overlapping is safe for the first 8 bytes
        unsafe {
            utils::write_u64_unsafe(b_ptr, seq_num);
        }
    }

    /// Stores a lifecycle state after the caller has established the ownership
    /// or pipeline boundary represented by `state`.
    ///
    /// The atomic store synchronizes observation of the state; it does not by
    /// itself transfer ownership or make an otherwise invalid transition valid.
    pub(super) fn set_runtime_state(&self, state: BatchRuntimeState, ordering: Ordering) {
        self.runtime_commit_state.store(state as u8, ordering);
    }

    pub(super) fn is_applied(&self, ordering: Ordering) -> bool {
        BatchRuntimeState::from(self.runtime_commit_state.load(ordering))
            == BatchRuntimeState::Applied
    }

    pub(super) fn mark_applied(&self) {
        // The write pipeline calls this only after it has finished every access
        // that must precede reuse. Publishing Applied allows the owner waiting
        // with an Acquire load to proceed to reset.
        self.runtime_commit_state
            .store(BatchRuntimeState::Applied as u8, Ordering::Release)
    }

    pub(super) fn get_batch_count(&self) -> u32 {
        self.count
    }

    pub(super) fn get_batch_size(&self) -> usize {
        self.data.len()
    }

    pub(super) fn shrink_batch_to(&mut self, new_size: usize) {
        // We are not interested why we're being resized
        self.data.shrink_to(new_size);
    }

    // This is for calcualting the estimated memtable size needed in the cf_table
    fn estimate_entry_size(&self, key_len: usize, value: usize) -> usize {
        0
    }

    fn reserve_operation(&mut self, record_size: usize) -> Result<(usize, usize)> {
        // Start with calculating what an empty batch size would be for this record - reason is if the record itself is too big for a batch
        // we want to return that error immediately so caller can handle that and so that we don't try to create a new batch only to fail
        // again because the record size is actually too big for any batch even empty ones
        let empty_batch_size =
            Self::HEADER_SIZE
                .checked_add(record_size)
                .ok_or(Error::RecordTooLarge {
                    encoded_size: usize::MAX,
                    max_batch_size: self.max_batch_size,
                })?;

        if empty_batch_size > self.max_batch_size {
            return Err(Error::RecordTooLarge {
                encoded_size: empty_batch_size,
                max_batch_size: self.max_batch_size,
            });
        }
        // We can safely fit into an empty batch if we need to.

        // Check if we need to intialise the batch buffer where len will be after Self::HEADER_SIZE
        let current_size = if self.data.is_empty() {
            // If data buffer is empty then we need to set the current size to header and also init the buffer
            self.init_buffer(record_size);
            Self::HEADER_SIZE
        } else {
            self.data.len()
        };

        // Reserve the end of the space we need inside the buffer
        // Checking to make sure that we don't spill the buffer meaning it's full
        let end = current_size
            .checked_add(record_size)
            .ok_or(Error::BatchFull {
                record_size,
                current_size,
                max_batch_size: self.max_batch_size,
            })?;

        if end > self.max_batch_size {
            return Err(Error::BatchFull {
                record_size,
                current_size,
                max_batch_size: self.max_batch_size,
            });
        }

        // len here will be the beginning of the reservation because we have only written up until that point - capacity is the full buffer capacity
        // and end is the end point within the capacity after len giving us the span of memory in buffer we want
        let start = self.data.len();

        // By resizing we are effectively setting the len to the end of the reserved memory in buffer so future writers can begin writing there and do not
        // touch our reserved space. Because we can return a DeferredOp struct it's important that we do this so we can fill the space when we want as we've reserved it
        self.data.resize(end, 0);

        Ok((start, end))
    }

    pub(crate) fn send_sync_waiter(&self) -> SyncWaiter {
        /*
         * NOTE: Do we want to assert strong count is 1 here? so we know we're only sending this to WAL and not anywhere else
         *  where we might be able to signal pipeline logic?
         */
        debug_assert!(Arc::strong_count(&self.sync_waiter) == 1);
        Arc::clone(&self.sync_waiter)
    }

    pub(crate) fn prime_sync_waiter(&self) {
        self.sync_waiter.prime();
    }

    pub(crate) fn sync_waiter_state(&self) -> WalSyncResultState {
        self.sync_waiter.state()
    }

    // ---- Header ---- //

    fn adjust_count_in_header(&mut self, count: u32) -> u32 {
        assert!(
            self.data.len() >= Self::HEADER_SIZE,
            "batch header must be initialised"
        );

        let count_bytes = &mut self.data[Self::BATCH_COUNT_OFFSET..Self::HEADER_SIZE];

        let old = u32::from_le_bytes(
            (*count_bytes)
                .try_into()
                .expect("count field must be exactly four bytes"),
        );

        count_bytes.copy_from_slice(&count.to_le_bytes());

        old
    }

    // ---- Writing ---- //

    fn key_value_record_layout(
        &self,
        key_len: usize,
        value_len: usize,
    ) -> Result<(VarInt, VarInt, usize)> {
        if key_len > MAX_USER_KEY_BYTES {
            return Err(Error::KeyTooLarge {
                size: key_len,
                max: MAX_USER_KEY_BYTES,
            });
        }

        // Key
        let encoded_key_len = u32::try_from(key_len).map_err(|_| Error::KeyTooLarge {
            size: key_len,
            max: u32::MAX as usize,
        })?;

        let key_varint = VarInt::new(encoded_key_len);

        // Value
        let encoded_value_len = u32::try_from(value_len).map_err(|_| Error::ValueTooLarge {
            size: value_len,
            max: u32::MAX as usize,
        })?;

        let value_varint = VarInt::new(encoded_value_len);

        // Record Binary:
        // | op_type (1 byte) | cf_id (u64 LE) | key_len (VarInt) | key ... | value_len (VarInt) | value ... |
        let record_size = 1usize
            .checked_add(size_of::<u64>())
            .and_then(|size| size.checked_add(key_varint.size()))
            .and_then(|size| size.checked_add(key_len))
            .and_then(|size| size.checked_add(value_varint.size()))
            .and_then(|size| size.checked_add(value_len))
            .ok_or(Error::RecordTooLarge {
                encoded_size: usize::MAX,
                max_batch_size: self.max_batch_size,
            })?;

        Ok((key_varint, value_varint, record_size))
    }

    fn prepare_with_key_value_impl<'batch>(
        &'batch mut self,
        cf_id: u64,
        key_len: usize,
        value_len: usize,
        kind: BatchRecordKind,
    ) -> Result<DeferredOp<'batch>> {
        debug_assert!(
            self.runtime_commit_state.load(Ordering::Acquire) != BatchRuntimeState::InQueue as u8
        );

        let (key_varint, value_varint, record_size) =
            self.key_value_record_layout(key_len, value_len)?;

        let k_varint_size = key_varint.size();
        let v_varint_size = value_varint.size();

        let (reservation_start, reservation_end) = self.reserve_operation(record_size)?;

        let mut index = reservation_start;

        self.data[index] = kind as u8;
        index += 1;

        self.data[index..index + 8].copy_from_slice(&cf_id.to_le_bytes());
        index += 8;

        self.data[index..index + k_varint_size].copy_from_slice(key_varint.as_slice());
        index += k_varint_size;

        // Capture key offset
        let key_offset = index;
        // Skip key value
        index += key_len;

        self.data[index..index + v_varint_size].copy_from_slice(value_varint.as_slice());
        index += v_varint_size;

        // Capture value offset
        let value_offset = index;

        debug_assert_eq!(reservation_end, index + value_len);

        Ok(DeferredOp::new(
            self,
            reservation_start,
            reservation_end,
            key_offset,
            key_len,
            value_offset,
            value_len,
        ))
    }

    fn prepare_with_key_record_impl(&mut self, key_len: usize) {}

    pub(crate) fn put(&mut self, cf_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let k_len = key.len();
        let v_len = value.len();

        let (key_varint, value_varint, record_size) = self.key_value_record_layout(k_len, v_len)?;

        let k_varint_size = key_varint.size();
        let v_varint_size = value_varint.size();

        let (start, end) = self.reserve_operation(record_size)?;

        let mut index = start;

        self.data[index] = (BatchRecordKind::Put as u8);

        index += 1;

        self.data[index..index + 8].copy_from_slice(&cf_id.to_le_bytes());
        index += 8;

        self.data[index..index + k_varint_size].copy_from_slice(key_varint.as_slice());
        index += k_varint_size;

        self.data[index..index + k_len].copy_from_slice(key);
        index += k_len;

        self.data[index..index + v_varint_size].copy_from_slice(value_varint.as_slice());
        index += v_varint_size;

        self.data[index..index + v_len].copy_from_slice(value);
        index += v_len;

        debug_assert!(index == end);

        self.count += 1;

        // Update the header
        let _ = self.adjust_count_in_header(self.count);

        Ok(())
    }

    /* NOTE: Keeping this because i want to benchmark against using closures - pebble avoids closures because of the
     *  function calling overhead and to avoid allocation through closure capture. This may be just a Go optimisation, Rust stack allocates closures
     *  and compiles them down to inline structs.
     */
    // pub(crate) fn put_deferred<'batch>(
    //     &'batch mut self,
    //     cf_id: u64,
    //     key_len: usize,
    //     value_len: usize,
    //     kind: BatchRecordKind,
    // ) -> Result<DeferredOp<'batch>> {
    //     self.prepare_with_key_value_impl(cf_id, key_len, value_len, kind)
    // }

    pub(crate) fn put_with<F>(
        &mut self,
        cf_id: u64,
        key_len: usize,
        value_len: usize,
        kind: BatchRecordKind,
        f: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut DeferredOp),
    {
        // Deferred op inside the closure
        self.prepare_with_key_value_impl(cf_id, key_len, value_len, kind)
            .map_or_else(
                // FIX: Do we want to panic here? May need better error handling
                |err| Err(err),
                |mut def| {
                    f(&mut def);
                    def.finish();
                    Ok(())
                },
            )

        // After this closure finishes - if we haven't panicked then we should call deferred.finish()
    }

    pub(super) fn clear(&mut self) {
        // NOTE:
        // We do NOT wait on signals here - once we reach here we should have exclusive ownership and
        // the type state batch objects should have done the runtime waiting for us

        debug_assert!(
            BatchRuntimeState::from(self.runtime_commit_state.load(Ordering::Acquire))
                .is_reset_safe()
        );

        // NOTE:
        // Do we need to clear the sync waiters

        self.count = 0;

        // Reset the data buffer
        self.data.clear();

        // Clear the batch meta and cf table
        self.batch_meta.clear();
        self.cf_table.clear();

        //
    }
}

pub(crate) struct BatchRef<'env> {
    batch: &'env BatchInner,
}

impl<'env> BatchRef<'env> {
    pub(crate) fn from_batch(batch: &'env BatchInner) -> Self {
        Self { batch }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_ptr_drop() {
        let b = BatchObject::<UnCommitted, OwnedBatchPtr>::new();

        let b_ptr = b.as_non_null();
        let b_ptr2 = b.as_non_null();

        assert_eq!(b_ptr, b_ptr2);
    }

    #[test]
    fn batch_new() {
        let mut batch = BatchObject::<UnCommitted, OwnedBatchPtr>::new();
        let b_ref = unsafe { &*batch.as_inner_ptr() };
        assert!(b_ref.count == 0);
    }

    #[should_panic]
    #[test]
    fn batch_reset() {
        let mut batch = BatchInner::new();

        batch
            .runtime_commit_state
            .store(BatchRuntimeState::InQueue as u8, Ordering::Relaxed);

        batch.clear();
    }

    #[should_panic]
    #[test]
    fn batch_object_reset_error() {
        let batch = BatchObject::<UnCommitted, OwnedBatchPtr>::new();

        unsafe { batch.set_runtime_state(BatchRuntimeState::InQueue, Ordering::Relaxed) };

        // Now if we try and reset we should get the error message

        batch.reset_batch();
    }

    #[test]
    fn assign_seq_num() {
        let mut batch = BatchObject::<UnCommitted, OwnedBatchPtr>::new_with_capacity(10);

        let b_ref = unsafe { &*batch.as_inner_ptr() };

        assert_eq!(b_ref.seq_num(), 0);

        unsafe { b_ref.assign_seq_num_once(10) };

        assert_eq!(b_ref.seq_num(), 10);
    }

    #[test]
    fn inner_put() {
        let mut inner = BatchInner::new_with_capacity(100);

        let expected_len = 33;

        inner.put(DEFAULT_CF_ID, b"hello", b"world").unwrap();

        assert_eq!(inner.data.len(), 33);
    }

    #[test]
    fn put_deferred_ok() {
        let mut inner = BatchInner::new_with_capacity(40);

        let key = b"Hello";
        let value = b"World";

        inner.put_with(0, key.len(), value.len(), BatchRecordKind::Put, |def| {
            let (k, v) = def.key_value_mut();

            k.copy_from_slice(key);
            v.copy_from_slice(value);

            let result_k = String::from_utf8_lossy(k);
            let result_v = String::from_utf8_lossy(v);

            // Length checks
            assert_eq!(key.len(), k.len());
            assert_eq!(key.len(), v.len());

            // Bytes checks
            assert_eq!(result_k.as_bytes(), key);
            assert_eq!(result_v.as_bytes(), value);

            // Write checks
            assert_eq!(key, def.key_mut());
        });
    }

    // TODO: Check my rollback works
    #[should_panic]
    #[test]
    fn put_deferred_rollback() {
        let mut inner = BatchInner::new_with_capacity(40);

        let key = b"Hello";
        let wrong_key = b"GoodAfternoon"; // We use a wrong key here to inject into the deferred closure which is more than the reserved space
        let value = b"World";

        // Reserved should equal
        // HEADER  - 12 +
        // OP TYPE - 1  +
        // CF ID   - 8  +
        // VAR INT - 1  +
        // KEY     - 5  +
        // VAR INT - 1  +
        // VALUE   - 5  +
        //        = 31

        inner.put_with(0, key.len(), value.len(), BatchRecordKind::Put, |def| {
            let (k, v) = def.key_value_mut();

            // Assert that we have correct reserved space
            assert_eq!(def.reservation_end, )

            k.copy_from_slice(wrong_key);
            v.copy_from_slice(value);

            let result_k = String::from_utf8_lossy(k);
            let result_v = String::from_utf8_lossy(v);
        });

        // Deferred closure should panic and also rollback
        // We can assert buffer len in the closure and then out of the closure here to confirm
    }

    #[test]
    fn record_may_fill_batch_to_exact_limit() {
        let mut inner = BatchInner::new();
        let record_size = 1 + size_of::<u64>() + 1 + 1 + 1 + 1;
        inner.max_batch_size = BatchInner::HEADER_SIZE + record_size;

        inner.put(DEFAULT_CF_ID, b"k", b"v").unwrap();

        assert_eq!(inner.data.len(), inner.max_batch_size);
        assert_eq!(inner.count, 1);
    }

    #[test]
    fn oversized_record_is_rejected_without_initializing_batch() {
        let mut inner = BatchInner::new();
        inner.max_batch_size = BatchInner::HEADER_SIZE + 10;

        let error = inner.put(DEFAULT_CF_ID, b"k", b"v").unwrap_err();

        assert!(matches!(
            error,
            Error::RecordTooLarge {
                max_batch_size,
                ..
            } if max_batch_size == BatchInner::HEADER_SIZE + 10
        ));
        assert_eq!(inner.data.len(), BatchInner::HEADER_SIZE);
        assert_eq!(inner.count, 0);
    }

    #[test]
    fn full_batch_rejects_next_record_without_mutation() {
        let mut inner = BatchInner::new();
        inner.max_batch_size = 30;
        inner.put(DEFAULT_CF_ID, b"", b"").unwrap();

        let previous_data = inner.data.clone();
        let previous_count = inner.count;
        let error = inner.put(DEFAULT_CF_ID, b"", b"").unwrap_err();

        assert!(matches!(error, Error::BatchFull { .. }));
        assert_eq!(inner.data, previous_data);
        assert_eq!(inner.count, previous_count);
    }

    #[test]
    fn oversized_user_key_is_rejected_before_reservation() {
        let mut inner = BatchInner::new();

        let error = inner
            .put_with(
                DEFAULT_CF_ID,
                MAX_USER_KEY_BYTES + 1,
                0,
                BatchRecordKind::Put,
                |def| (),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            Error::KeyTooLarge {
                size,
                max: MAX_USER_KEY_BYTES,
            } if size == MAX_USER_KEY_BYTES + 1
        ));
        assert_eq!(inner.data.len(), BatchInner::HEADER_SIZE);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn value_length_that_cannot_be_encoded_is_rejected() {
        let mut inner = BatchInner::new();
        let value_len = u32::MAX as usize + 1;

        let error =
            match inner.put_with(DEFAULT_CF_ID, 0, value_len, BatchRecordKind::Put, |def| ()) {
                Ok(_) => panic!("unencodable value length unexpectedly reserved a record"),
                Err(error) => error,
            };

        assert!(matches!(
            error,
            Error::ValueTooLarge { size, max } if size == value_len && max == u32::MAX as usize
        ));
        assert_eq!(inner.data.len(), BatchInner::HEADER_SIZE);
    }

    #[test]
    #[should_panic]
    fn sync_waiter_send() {
        let batch_inner = BatchInner::new();

        let one = batch_inner.send_sync_waiter();
        let two = batch_inner.send_sync_waiter();

        let keep_alive = &one;
    }
}
