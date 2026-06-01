//
// Tyring out pebble style approach of batch owns it's operations and commits them and parallel inserts into memtable - waiting to publish
//
//

// Batch::put()
//
// let b: Batch<Mutable = Batch::new();
//
// b.push()
// b.push()
//
// b.commit(b) // commit moves batch into it's scope and transitions state
//

use crate::sync::atomic::{AtomicBool, Ordering};
use crate::utils;
use std::ops::Deref;
use std::ptr;
use std::ptr::NonNull;
use std::thread::{self, Thread};
use std::{marker::PhantomData, sync::atomic::AtomicU8};

use crate::db::DEFAULT_CF_ID;
use crate::db::{self, db_impl::DbImpl};
use crate::utils::var_int::VarInt;

pub(crate) const MAX_BATCH_SIZE: usize = 1 << 20;
pub(crate) const DEFAULT_BATCH_INIT_SIZE: usize = 1 << 10; // NOTE: This is where we'd like to get to if we pool batches

pub(crate) trait BatchCommitState {}

pub(crate) struct UnCommitted {}

impl BatchCommitState for UnCommitted {}

// TODO: Move into BatchInner and expose only through type state methods
pub(crate) struct Sealed {}

impl BatchCommitState for Sealed {}

// Pooled is the default state of a batch when pooled in the heap object pool
pub(crate) struct Pooled {}

impl BatchCommitState for Pooled {}

#[repr(align(8))]
#[derive(Debug)]
pub(crate) enum BatchOp {
    Put,
    Delete,
    Merge,
    // XXX: More operations in later updates
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

/// Batches use a compact binary representation where all operations are encoded sequentially into a byte slice
/// the binary representation is so that batches can form the records of the WAL without any additional changes
/// We are free to choose the endianness and for optimisation on x86 architectures we choose little endian here.
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
///     let mut batch = Batch::new();
///     batch.put(DEFAULT_CF, key, value);
///     self.write(batch);
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
pub(crate) struct BatchObject<B: BatchCommitState> {
    _state: PhantomData<B>,

    // TODO: Once heap allocated this should be the heap object - either NonNull or Box
    inner: NonNull<Batch>,
}

impl<B: BatchCommitState> BatchObject<B> {
    pub(crate) fn into_inner(self) -> NonNull<Batch> {
        self.inner
    }
}

impl BatchObject<UnCommitted> {
    fn default_cf() -> VarInt {
        VarInt::new(DEFAULT_CF_ID)
    }

    #[cfg(test)]
    pub(crate) fn new() -> Self {
        let inner = Box::new(Batch::new());

        Self {
            inner: NonNull::from(Box::leak(inner)),
            _state: PhantomData,
        }
    }

    pub(crate) fn new_with_capacity(cap: usize) -> Self {
        let batch = Box::new(Batch::new_with_capacity(cap));

        //

        Self {
            _state: PhantomData,
            inner: NonNull::from(Box::leak(batch)),
        }
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

impl BatchObject<Sealed> {
    pub(crate) fn non_null_ptr(&self) -> NonNull<Self> {
        // # SAFETY
        //
        // `ptr::from_ref(self)` produces a non-null pointer to `self`.
        //
        // Casting to `*mut` is sound because this does not create an
        // exclusive `&mut Self`; it only produces a raw pointer for
        // publication into the commit queue.
        //
        // The caller must uphold:
        //
        // - `self` remains alive for the duration of queue publication.
        // - `self` is not moved or returned to the pool after its pointer is published.
        // - Any cross-thread mutation of `Batch<Sealed>` occurs only
        //   through atomics or other synchronization primitives.
        unsafe { NonNull::new_unchecked(ptr::from_ref(self).cast_mut()) }
    }
}

//TODO: Add sync waiting state and completion state so the batch can wait for fysync

// https://github.com/cockroachdb/pebble/blob/a3b8dfe9e85015110be33743718a7de47458a4d7/batch.go#L199
pub(super) struct Batch {
    data: Vec<u8>,
    /// The maximum total serialized size allowed for a single atomic WriteBatch.
    ///
    /// This limit is a global operational safety bound, not a memtable-fit constraint.
    ///
    /// A WriteBatch may span multiple column families, and its contents are applied
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
    // NOTE:
    // Need inline array for touched column families in this batch
    //
    is_applied: AtomicBool,
    is_published: AtomicBool,
    //
    //
    // Pool logic
    // TODO: Add field pool_next: ___,
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
            runtime_commit_state: AtomicU8::new(0),
            is_applied: AtomicBool::new(false),
            is_published: AtomicBool::new(false),
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
            is_applied: AtomicBool::new(false),
            is_published: AtomicBool::new(false),
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
        self.is_applied.load(ordering)
    }

    pub(super) fn mark_applied(&self, ordering: Ordering) {
        self.is_applied.store(true, ordering);
    }

    pub(super) fn is_published(&self, ordering: Ordering) -> bool {
        self.is_published.load(ordering)
    }

    pub(super) fn get_batch_count(&self) -> u64 {
        self.count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_new() {
        let batch = BatchObject::new().into_inner();
        let b_ref = unsafe { &*batch.as_ptr() };
        assert!(b_ref.count == 0);
    }

    #[test]
    fn assign_seq_num() {
        let batch = BatchObject::new_with_capacity(10).into_inner();

        let b_ref = unsafe { &*batch.as_ptr() };

        assert_eq!(b_ref.seq_num(), 0);

        unsafe { b_ref.assign_seq_num_once(10) };

        assert_eq!(b_ref.seq_num(), 10);
    }
}
