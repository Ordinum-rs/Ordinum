use std::fmt::Display;
use std::ops::Deref;
use std::ptr;
use std::ptr::NonNull;
use std::thread::{self, Thread};
use std::{marker::PhantomData, sync::atomic::AtomicU8};

use crate::db::DEFAULT_CF_ID;
use crate::db::batch::TryResetError::InvalidState;
use crate::db::batch_pool::BatchPool;
use crate::db::{self, db_impl::DbImpl};
use crate::sync::Arc;
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::utils;
use crate::utils::var_int::VarInt;
use crate::wal::{SyncLogWaiter, SyncWaiter};
use crate::{Error, Result};

// ---- Constants ---- //

pub(crate) const MAX_BATCH_SIZE: usize = 1 << 20;
pub(crate) const DEFAULT_BATCH_INIT_SIZE: usize = 1 << 10; // NOTE: This is where we'd like to get to if we pool batches
//
pub(crate) const RESET_SAFE_STATES: [BatchRuntimeState; 2] =
    [BatchRuntimeState::Acquired, BatchRuntimeState::Applied];

// ---- Module Errors ---- //

#[derive(Debug)]
pub(super) enum TryResetError<T> {
    InvalidState {
        object: T,
        expected: [BatchRuntimeState; 2],
        got: BatchRuntimeState,
    },
    Error {
        handle: T,
        error: Error,
    },
}

/* NOTE: We use std::result::Result<T, Error> as opposed to the alias in [errror.rs]("error.rs") because we want to change
// the error type we use the crate level error.*/
pub(super) type TryResetResult<T, E> = std::result::Result<T, TryResetError<E>>;
//
//
//
//
// ---- Batch Operations Enum ---- //

#[repr(align(8))]
#[derive(Debug)]
pub(crate) enum BatchOp {
    Put,
    Delete,
    Merge,
    // XXX: More operations in later updates
}

// ---- Batch Runtime State ---- //

#[repr(align(8))]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum BatchRuntimeState {
    Pooled,
    Acquired,
    Committed,
    InQueue,
    WaitingSync,
    Applied,
}

impl Display for BatchRuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchRuntimeState::Pooled => write!(f, "Pooled"),
            BatchRuntimeState::Acquired => write!(f, "Acquired"),
            BatchRuntimeState::Committed => write!(f, "Committed"),
            BatchRuntimeState::InQueue => write!(f, "InQueue"),
            BatchRuntimeState::WaitingSync => write!(f, "WaitingSync"),
            BatchRuntimeState::Applied => write!(f, "Applied"),
        }
    }
}

impl From<u8> for BatchRuntimeState {
    fn from(value: u8) -> Self {
        match value {
            1 => BatchRuntimeState::Pooled,
            2 => BatchRuntimeState::Acquired,
            3 => BatchRuntimeState::Committed,
            4 => BatchRuntimeState::InQueue,
            5 => BatchRuntimeState::WaitingSync,
            7 => BatchRuntimeState::Applied,
            _ => unreachable!(),
        }
    }
}

impl BatchRuntimeState {
    pub(super) fn is_reset_safe(self) -> bool {
        matches!(
            self,
            BatchRuntimeState::Acquired | BatchRuntimeState::Applied
        )
    }
}

// --- Batch Type States --- //

pub(crate) trait BatchCommitState {}

#[derive(Debug)]
pub(crate) struct UnCommitted {}
impl BatchCommitState for UnCommitted {}

pub(crate) struct Sealed {}
impl BatchCommitState for Sealed {}

/// Owning pointer to a heap-allocated batch object.
///
/// `NonNullBatchPtr` is the stable allocation identity used by the batch pool and
/// write pipeline. The pointed-to `Batch` is allocated with `Box::into_raw` and
/// must be destroyed exactly once with `Box::from_raw` when it is no longer
/// retained by the pool.
///
/// # Invariants
///
/// - The pointer is non-null, aligned, and was produced from `Box<Batch>`.
/// - At any time, ownership is in exactly one phase:
///   - retained by TLS cache,
///   - retained by a global pool shard,
///   - owned by an active `BatchObject<S>` handle,
///   - or visible to the write pipeline until commit publication completes.
/// - A batch must not be returned to TLS/global pool while any queue slot,
///   write pipeline stage, caller, or worker thread may still access it.
/// - Non-atomic batch fields may be mutated only by the current owner before
///   publication, or by the write pipeline at protocol points that have
///   exclusive access.
/// - Cross-thread state changes after publication must use atomics or other
///   synchronization.
#[derive(Debug)]
pub(super) struct NonNullBatchPtr {
    ptr: NonNull<Batch>,
}

impl NonNullBatchPtr {
    pub(super) fn as_ptr(&self) -> *mut Batch {
        self.ptr.as_ptr()
    }

    pub(super) fn as_non_null(&self) -> NonNull<Batch> {
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

impl From<NonNull<Batch>> for NonNullBatchPtr {
    fn from(ptr: NonNull<Batch>) -> Self {
        NonNullBatchPtr { ptr }
    }
}

// SAFETY:
//
// `BatchPtr` transfers ownership of a stable heap allocation between threads.
// The pointer itself does not permit shared mutation. Safe APIs must preserve
// the phase invariant: only one owner may mutate non-atomic batch state, and a
// batch visible to the write pipeline may not be reused or destroyed.
unsafe impl Send for NonNullBatchPtr {}

// NOTE: This is important - so we must carefully maintain that we are not creating UB when doing this
impl Drop for NonNullBatchPtr {
    fn drop(&mut self) {
        drop(unsafe { Box::from_raw(self.ptr.as_ptr()) })
    }
}

// https://github.com/cockroachdb/pebble/blob/a3b8dfe9e85015110be33743718a7de47458a4d7/batch.go#L199
//
// Batch:
// | --------- 12 byte header ----------|--------- Operations ---------|
// | Seq No (8 bytes) | Count (4 bytes) | Operation 1 ... Operation 2...
//
//
// Operation:
// | op_type (1 byte) | cf_id (VarInt) | key_len (VarInt) | key ... | value_len (VarInt) | value ... |

// ---- BatchObjectHandle ---- //

pub(crate) struct BatchObjectHandle<B: BatchCommitState> {
    pool: Arc<BatchPool>,
    batch: BatchObject<B>,
}

impl<B: BatchCommitState> BatchObjectHandle<B> {
    pub(crate) fn new(pool: Arc<BatchPool>, batch: BatchObject<B>) -> Self {
        Self { pool, batch }
    }

    pub(crate) fn inner(&self) -> &BatchObject<B> {
        &self.batch
    }

    pub(crate) fn reset(mut self) -> BatchObjectHandle<UnCommitted> {
        //
        self.wait().expect("batch wait failed before reset");

        BatchObjectHandle {
            pool: self.pool,
            batch: self.batch.reset_batch(),
        }
    }

    fn wait(&self) -> Result<()> {
        self.batch.wait_until_reusable()?;
        Ok(())
    }
}

impl BatchObjectHandle<UnCommitted> {
    pub(crate) fn put<K, V>(&self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.batch.put(key, value);
    }

    pub(crate) fn seal(self) -> BatchObjectHandle<Sealed> {
        BatchObjectHandle {
            pool: self.pool,
            batch: self.batch.seal(),
        }
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
pub(crate) struct BatchObject<B: BatchCommitState> {
    _state: PhantomData<B>,
    inner: NonNullBatchPtr,
}

// ---- Generic Impl ---- //

impl<B: BatchCommitState> BatchObject<B> {
    //
    fn transition<S: BatchCommitState>(self) -> BatchObject<S> {
        BatchObject {
            _state: PhantomData,
            inner: self.inner,
        }
    }

    pub(super) fn as_ptr(&self) -> *mut Batch {
        self.inner.as_ptr()
    }

    pub(super) fn as_non_null(&self) -> NonNull<Batch> {
        self.inner.as_non_null()
    }

    pub(super) fn from_batch_ptr(ptr: NonNullBatchPtr) -> Self {
        Self {
            _state: PhantomData,
            inner: ptr,
        }
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
        unsafe { &*self.as_ptr() }
            .runtime_commit_state
            .store(state as u8, ordering)
    }

    pub(crate) fn state(&self, ordering: Ordering) -> BatchRuntimeState {
        BatchRuntimeState::from(
            unsafe { &*self.as_ptr() }
                .runtime_commit_state
                .load(ordering),
        )
    }

    pub(crate) fn is_state(&self, state: BatchRuntimeState) -> bool {
        // SAFTEY:
        //
        // We are safe to dereference here because the BatchObject ensures the underlying batch heap allocation is alive and we are
        // accessing an atomic field only
        unsafe { &*self.as_ptr() }
            .runtime_commit_state
            .load(Ordering::Relaxed)
            == state as u8
    }

    pub(super) fn wait_until_reusable(&self) -> Result<()> {
        let batch = unsafe { &*self.as_ptr() };

        // TODO: Need to wait on the sync signal - do we need a timeout?

        batch.sync_waiter.wait().unwrap();

        Ok(())
    }

    pub(crate) fn can_reset(&self) -> bool {
        let state = self.state(Ordering::Acquire);
        if !state.is_reset_safe() { false } else { true }
    }

    pub(crate) fn reset_batch(mut self) -> BatchObject<UnCommitted> {
        //
        // Check our state
        debug_assert!(self.state(Ordering::Acquire).is_reset_safe());

        // SAFETY
        //
        // We are safe to dereference because we own exlcusive access to the BatchObject which
        // owns the NonNullBatchPtr which points to the stable batch allocation
        let batch = unsafe { &mut *self.as_ptr() };

        // If no errors; we have reset and are safe to transition state and return
        self.transition()
    }
}

//
// ---- Uncommitted ---- //

impl BatchObject<UnCommitted> {
    fn default_cf() -> VarInt {
        VarInt::new(DEFAULT_CF_ID)
    }

    // We need to document and maybe think of a safe way to avoid leaking the memory from box leak
    // Maybe through a custom NewType which wraps NonNull<Batch> and ensure through drop that we Box::from_raw(_) and destroy properly
    pub(super) fn new() -> Self {
        let inner = Box::new(Batch::new());

        Self {
            // XXX: Is there a safer way to do this - unwrap() is scary
            inner: NonNullBatchPtr::from(NonNull::new(Box::into_raw(inner)).unwrap()),
            _state: PhantomData,
        }
    }

    pub(super) fn new_with_capacity(cap: usize) -> Self {
        let batch = Box::new(Batch::new_with_capacity(cap));

        //

        Self {
            _state: PhantomData,
            // XXX: Is there a safer way to do this - unwrap() is scary
            inner: NonNullBatchPtr::from(NonNull::new(Box::into_raw(batch)).unwrap()),
        }
    }

    pub(super) fn into_inner(self) -> NonNullBatchPtr {
        self.inner
    }

    pub(crate) fn put<K, V>(&self, key: K, value: V)
    // XXX: Do we want this to return a Result with an Error?
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        // Any assertions
        self.put_cf(Self::default_cf(), key, value);
    }

    // XXX: May want to change the cf_id to a column family handle OR we allow the layers above to resolve the handle and we only deal with the id
    pub(crate) fn put_cf<K, V>(&self, cf_id: VarInt, key: K, value: V)
    // XXX: Result?
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        //
    }

    pub(crate) fn seal(self) -> BatchObject<Sealed> {
        BatchObject {
            _state: PhantomData,
            inner: self.inner,
        }
    }
}

// ---- Sealed ---- //

impl BatchObject<Sealed> {
    //
}

//TODO: Add sync waiting state and completion state so the batch can wait for fysync

// https://github.com/cockroachdb/pebble/blob/a3b8dfe9e85015110be33743718a7de47458a4d7/batch.go#L199
pub(super) struct Batch {
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
    /// Memtable flush and large-batch heuristics are evaluated separately on a
    /// per-column-family basis using the batch footprint for each destination
    /// memtable.
    max_batch_size: usize,
    count: u64,
    runtime_commit_state: AtomicU8,
    //
    /* NOTE: Need inline array for touched column families in this batch */
    //
    //
    //

    // Per-batch WAL fsync completion.
    //
    // The batch owns this stable Arc for its whole allocation lifetime. The WAL
    // worker receives a clone when this batch is written, then signals it when
    // the batch's WAL bytes are durable. Reset/reuse must wait on this waiter
    // when the batch has outstanding sync work.
    sync_waiter: SyncWaiter,
}

impl Batch {
    const SEQ_NO_OFFSET: usize = 0; // seq starts at byte 0
    const BATCH_COUNT_OFFSET: usize = size_of::<u64>(); // count starts at byte 8
    const HEADER_SIZE: usize = size_of::<u64>() + size_of::<u32>(); // = 12

    fn new() -> Self {
        let mut data = Vec::with_capacity(DEFAULT_BATCH_INIT_SIZE);
        Self {
            data,
            max_batch_size: MAX_BATCH_SIZE,
            count: 0,
            runtime_commit_state: AtomicU8::new(BatchRuntimeState::Pooled as u8),
            sync_waiter: Arc::new(SyncLogWaiter::default()),
        }
    }

    fn new_with_capacity(cap: usize) -> Self {
        let capacity = cap + Self::HEADER_SIZE;

        assert!(capacity <= MAX_BATCH_SIZE);
        let mut data = Vec::with_capacity(capacity);
        data.extend_from_slice(&[0u8; Self::HEADER_SIZE]);
        Self {
            data,
            max_batch_size: MAX_BATCH_SIZE,
            count: 0,
            runtime_commit_state: AtomicU8::new(0),
            sync_waiter: Arc::new(SyncLogWaiter::default()),
        }
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

    pub(super) fn is_applied(&self, ordering: Ordering) -> bool {
        BatchRuntimeState::from(self.runtime_commit_state.load(ordering))
            == BatchRuntimeState::Applied
    }

    pub(super) fn mark_applied(&self, ordering: Ordering) {
        self.runtime_commit_state
            .store(BatchRuntimeState::Applied as u8, ordering)
    }

    pub(super) fn get_batch_count(&self) -> u64 {
        self.count
    }

    pub(super) fn reset(&mut self) {
        // NOTE: We do NOT wait on signals here - once we reach here we should have exclusive ownership and
        // the type state batch objects should have done the runtime waiting for us
        //
        // Want:
        // - We need to assess the size of the data buf and decide if we want to resize

        self.count = 0;
        //
        self.runtime_commit_state
            .store(BatchRuntimeState::Acquired as u8, Ordering::Relaxed);
        //

        // Reset the data buffer
        self.data.clear();

        // Decide if we need to resize
        //
        //
    }
}

pub(crate) struct BatchRef<'env> {
    batch: &'env Batch,
}

impl<'env> BatchRef<'env> {
    pub(crate) fn from_batch(batch: &'env Batch) -> Self {
        Self { batch }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_ptr_drop() {
        let b = BatchObject::new();

        let b_ptr = b.as_non_null();
        let b_ptr2 = b.as_non_null();

        assert_eq!(b_ptr, b_ptr2);
    }

    #[test]
    fn batch_new() {
        let mut batch = BatchObject::new();
        let b_ref = unsafe { &*batch.as_ptr() };
        assert!(b_ref.count == 0);
    }

    #[should_panic]
    #[test]
    fn batch_reset() {
        let mut batch = Batch::new();

        batch
            .runtime_commit_state
            .store(BatchRuntimeState::InQueue as u8, Ordering::Relaxed);

        batch.reset();
    }

    #[should_panic]
    #[test]
    fn batch_object_reset_error() {
        let batch = BatchObject::new();

        unsafe { batch.set_runtime_state(BatchRuntimeState::InQueue, Ordering::Relaxed) };

        // Now if we try and reset we should get the error message

        batch.reset_batch();
    }

    #[test]
    fn assign_seq_num() {
        let mut batch = BatchObject::new_with_capacity(10);

        let b_ref = unsafe { &*batch.as_ptr() };

        assert_eq!(b_ref.seq_num(), 0);

        unsafe { b_ref.assign_seq_num_once(10) };

        assert_eq!(b_ref.seq_num(), 10);
    }
}
