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

pub(crate) struct Sealed {}

impl BatchCommitState for Sealed {}

pub(crate) struct Batch<B: BatchCommitState> {
    state: PhantomData<B>,
    inner: BatchInner,
    applied: AtomicBool,
}

impl Batch<UnCommitted> {
    pub(crate) fn new() -> Self {
        let batch = BatchInner::new();
        Self {
            state: PhantomData,
            inner: batch,
            applied: AtomicBool::new(false),
        }
    }

    pub(crate) fn new_with_capacity(cap: usize) -> Self {
        let batch = BatchInner::new_with_capacity(cap);
        Self {
            state: PhantomData,
            inner: batch,
            applied: AtomicBool::new(false),
        }
    }

    pub(crate) fn as_ref(&self) -> &BatchInner {
        &self.inner
    }

    pub(crate) fn seal(self) -> Batch<Sealed> {
        Batch {
            state: PhantomData,
            inner: self.inner,
            applied: self.applied,
        }
    }
}

impl Batch<Sealed> {
    pub(crate) fn is_applied(&self, ordering: Ordering) -> bool {
        self.applied.load(ordering)
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

pub(super) struct BatchInner {
    data: Vec<u8>,
    max_batch_size: usize,
    count: u64,
    flushable: bool, // NOTE: bool for now until we implement flushable batches
    runtime_commit_state: AtomicU8,
    waiter: Thread,
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
            waiter: thread::current(),
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
            waiter: thread::current(),
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
