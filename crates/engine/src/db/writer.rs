use std::{
    cell::UnsafeCell,
    cmp::Ordering,
    mem::MaybeUninit,
    ptr::{self, NonNull},
    sync::{
        Condvar, Mutex,
        atomic::{AtomicPtr, AtomicU8},
    },
    thread::{self, Thread},
};

use crate::db::write_batch::Batch;

#[non_exhaustive]
pub(super) struct WriterState;

impl WriterState {
    pub const INIT: u8 = 1 << 0;
    pub const LEADER: u8 = 1 << 1;
    pub const FOLLOWER: u8 = 1 << 2;
    pub const LOCKED_WAITING: u8 = 1 << 3;
    pub const COMPLETE: u8 = 1 << 4;
}

/// Writer is the calling threads write which holds a batch of operations.
///
/// A writer node is created on each Db operation (Put/Delete/Merge .. etc) and
/// will insert into the tail of the write thread becoming either the leader of a group of batches or a follower
///
/// The batch pointer is non-owning. The caller retains ownership and
/// responsibility for the Batch lifetime. The Writer destructor does
/// not drop the batch.
///
/// # Safety
///
/// Caller must guarantee batch outlives this Writer
pub(crate) struct Writer {
    batch: NonNull<Batch>,
    state: AtomicU8,
    next: AtomicPtr<Writer>,
    park: Thread,
}

impl Writer {
    pub(crate) fn new(batch: &Batch) -> Self {
        Self {
            batch: NonNull::from(batch),
            state: AtomicU8::new(0),
            next: AtomicPtr::new(ptr::null_mut()),
            park: thread::current(),
        }
    }

    /// wait() is used when the calling thread of a write has joined the write_thread and becomes a follower in the group.
    ///
    /// It must wait and block until the leader completes the write pipeline.
    ///
    /// The wait() method is implemented on the Writer and not on the WriteThread because Writer must be able to create a CondVar on
    /// demand and pass in it's local state to the Mutex in order to be signalled.
    pub(crate) fn wait(&self) {
        debug_assert!(
            self.state.load(std::sync::atomic::Ordering::Relaxed) & WriterState::FOLLOWER != 0
        );

        // We have joined on the write_thread and are a follower in the write group. We must wait until the leader is done with the write.
        // There are three stages we can efficiently wait to avoid the heavy syscall on Condvar each time. We start with the first stage and go through
        // until we fallback to Condvar or the write is complete at any point during.
        //
        //
        // Synchronisation is maintained through the state machine which is checked on each loop and in each stage
        //
        // 1. loop 200 times using a "pause" for 1 micro sec
        // 2. Thread::yield()
        // 3. Thread parking (rocks uses Mutex and CondVar)
        //
        // This is inspired by Rocks code see: https://github.com/facebook/rocksdb/blob/763401b595c8c1647908356e42525aadd0b90eae/db/write_thread.cc#L64

        // Wait logic
        todo!()
    }
}
