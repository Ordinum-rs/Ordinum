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

use std::ops::Deref;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, Thread};
use std::{marker::PhantomData, sync::atomic::AtomicU8};

use crate::db::write_batch::WBatch;
use crate::db::{self, db_impl::DbImpl};

pub(crate) const MAX_BATCH_SIZE: usize = 1 << 20;
pub(crate) const DEFAULT_BATCH_INIT_SIZE: usize = 1 << 10; // NOTE: This is where we'd like to get to if we pool batches

pub(crate) trait BatchCommitState {}

pub(crate) struct UnCommitted {}

impl BatchCommitState for UnCommitted {}

pub(crate) struct Flushable {}

impl BatchCommitState for Flushable {}

pub(crate) struct Sealed {
    applied: AtomicBool,
    published: AtomicBool,
    waiter: Thread,
}

impl BatchCommitState for Sealed {}

// https://github.com/cockroachdb/pebble/blob/a3b8dfe9e85015110be33743718a7de47458a4d7/batch.go#L199
//

/// Batch holds a group of operations for a writer/caller thread. [Put, Delete, Merge ...].
///
/// A batch should be 1:1 with a writer thread. A writer/caller should create a batch and push operations into the batch
/// before calling Commit to have the batch processed by the [write_pipeline.rs]('WritePipeline').
///
/// Batches are stack allocated. Ownership of the Batch is moved into Commit and passed to the WritePipeline once it is Sealed. Writers should
/// call Seal on the Batch to Commit.
///
/// Batches are safe to be accessed between threads because their lifetime is guranteed to outlive references and the stack allocation scope extends beyond
/// the objects and references which may store or reference it.
pub(crate) struct Batch<B: BatchCommitState> {
    state: B,
    inner: BatchInner,
}

impl Batch<UnCommitted> {
    pub(crate) fn new() -> Self {
        let batch = BatchInner::new();
        Self {
            state: UnCommitted {},
            inner: batch,
        }
    }

    pub(crate) fn new_with_capacity(cap: usize) -> Self {
        let batch = BatchInner::new_with_capacity(cap);

        //

        Self {
            state: UnCommitted {},
            inner: batch,
        }
    }

    pub(crate) fn estimate_size(&self) -> usize {
        todo!()
    }

    pub(crate) fn seal(self) -> Batch<Sealed> {
        // Checks sizes for if flushable

        Batch {
            state: Sealed {
                applied: AtomicBool::new(false),
                published: AtomicBool::new(false),
                waiter: thread::current(),
            },
            inner: self.inner,
        }
    }

    pub(crate) fn seal_batch(self) -> Batch<impl BatchCommitState> {
        // match self.estimate_size() {
        //      if
        // }
        Batch {
            inner: self.inner,
            state: Flushable {},
        }
    }
}

impl Batch<Sealed> {
    pub(crate) fn is_applied(&self, ordering: Ordering) -> bool {
        self.state.applied.load(ordering)
    }

    pub(crate) fn mark_applied(&self, ordering: Ordering) {
        self.state.applied.store(true, ordering);
    }

    pub(crate) fn is_published(&self, ordering: Ordering) -> bool {
        self.state.published.load(ordering)
    }

    pub(crate) fn non_null_ptr(&self) -> NonNull<Self> {
        // SAFETY:
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
        // - `self` is not moved after its pointer is published.
        // - Any cross-thread mutation of `Batch<Sealed>` occurs only
        //   through atomics or other synchronization primitives.
        unsafe { NonNull::new_unchecked(ptr::from_ref(self).cast_mut()) }
    }
}

// https://github.com/cockroachdb/pebble/blob/a3b8dfe9e85015110be33743718a7de47458a4d7/batch.go#L199
pub(super) struct BatchInner {
    data: Vec<u8>,
    max_batch_size: usize,
    count: u64,
    flushable: bool, // NOTE: bool for now until we implement flushable batches
    runtime_commit_state: AtomicU8,
    //
}

impl BatchInner {
    const SEQ_NO_OFFSET: usize = 0; // seq starts at byte 0
    const BATCH_COUNT_OFFSET: usize = size_of::<u64>(); // count starts at byte 8
    const HEADER_SIZE: usize = size_of::<u64>() + size_of::<u32>(); // = 12

    fn new() -> Self {
        let mut data = Vec::with_capacity(DEFAULT_BATCH_INIT_SIZE);
        Self {
            data,
            max_batch_size: MAX_BATCH_SIZE,
            count: 0,
            flushable: false,
            runtime_commit_state: AtomicU8::new(0),
        }
    }

    fn new_with_capacity(cap: usize) -> Self {
        assert!(cap <= MAX_BATCH_SIZE);
        let mut data = Vec::with_capacity(cap);
        data.extend_from_slice(&[0u8; Self::HEADER_SIZE]);
        Self {
            data,
            max_batch_size: MAX_BATCH_SIZE,
            count: 0,
            flushable: false,
            runtime_commit_state: AtomicU8::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_new() {
        let batch = Batch::new();
        assert!(batch.inner.count == 0);
    }
}
