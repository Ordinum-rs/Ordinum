use std::{
    cell::UnsafeCell,
    fmt::Display,
    ptr::{self, NonNull},
    sync::atomic::{AtomicPtr, AtomicU8, Ordering},
    thread::{self, Thread},
};

use crate::db::{
    write_batch::Batch,
    write_thread::{WriteGroup, WriteThread},
};

// WriterState is a struct here and not an enum because it is a bitflag set
#[non_exhaustive]
pub(super) struct WriterState;

impl WriterState {
    pub const INIT: u8 = 1 << 0;
    pub const LEADER: u8 = 1 << 1;
    pub const LOCKED_WAITING: u8 = 1 << 2;
    pub const COMPLETE: u8 = 1 << 3;

    pub fn debug(state: u8) -> String {
        let mut states = Vec::new();

        if state & Self::INIT != 0 {
            states.push("INIT");
        }

        if state & Self::LEADER != 0 {
            states.push("LEADER");
        }

        if state & Self::LOCKED_WAITING != 0 {
            states.push("LOCKED_WAITING");
        }

        if state & Self::COMPLETE != 0 {
            states.push("COMPLETE");
        }

        if states.is_empty() {
            "NO_STATE".into()
        } else {
            states.join(" | ")
        }
    }
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
    pub(super) batch: NonNull<Batch>,
    pub(super) state: AtomicU8,
    // Writers which entered the queue before this Writer [newest_writer -> W3 -> W2 -> W1 -> leader]
    pub(super) link_older: UnsafeCell<*mut Writer>,
    // Writers which are ordered oldest->newest
    pub(super) group_next: UnsafeCell<*mut Writer>,
    // Thread handle to unpark waiting followers
    pub(super) thread_handle: Thread,
    pub(super) write_group: *const WriteGroup,
    // Options
    // seq_no_first: u64,
    // status: WriterStatus,
    pub(super) sync: bool,
    // slow_down: bool,
    // disable_wal: bool,
    // deep_wait: bool, Was this parked and did the wait reach stage 3?
    // wal_log_num: u64,
    // mem_log_num: u64,
    //
}

// SAFETY:
//
// Writer instances are shared between threads through intrusive queue links.
//
// Mutable access to `state` is synchronized through atomic operations.
//
// Mutable access to `link_older` and `group_next` is externally serialized
// by the WriteThread queue protocol such that no two threads concurrently
// mutate the same link field.
//
// `batch` is immutable after Writer construction and guaranteed to outlive
// the Writer.
//
// `write_group` is leader-owned transient state whose lifetime is bounded
// by the write-thread processing phase.
unsafe impl Sync for Writer {}

impl Writer {
    pub(crate) fn new(batch: &Batch) -> Self {
        Self {
            batch: NonNull::from(batch),
            state: AtomicU8::new(0),
            link_older: UnsafeCell::new(ptr::null_mut()),
            group_next: UnsafeCell::new(ptr::null_mut()),
            thread_handle: thread::current(),
            write_group: ptr::null(),
            sync: true,
        }
    }

    // TODO: Add bitfield methods to make semantic state clearer

    /// wait() is used when the calling thread of a write has joined the write_thread and becomes a follower in the group.
    ///
    /// It must wait and block until the leader completes the write pipeline.
    ///
    /// The wait() method is implemented on the Writer and not on the WriteThread because Writer must be able to create a CondVar on
    /// demand and pass in it's local state to the Mutex in order to be signalled.
    pub(crate) fn wait(&self) {
        debug_assert!(
            self.state.load(std::sync::atomic::Ordering::Relaxed) & WriterState::INIT != 0
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

        for _ in 0..200 {
            if self.state.load(Ordering::Acquire) & WriterState::COMPLETE != 0 {
                return;
            }
            std::hint::spin_loop();
        }

        // PERF: Include performance timings/collection here

        for _ in 0..WriteThread::YIELD_PAUSE_ITERATIONS {
            // XXX: Later if benchmarking shows contention, we can do what rocks did and add a predictive credit
            // based yield to determine if we should yield or fall through to block
            if self.state.load(Ordering::Acquire) & WriterState::COMPLETE != 0 {
                return;
            }
            thread::yield_now();
        }

        // Fall through to block
        self.wait_and_block();
    }

    #[inline]
    pub(super) fn wait_and_block(&self) {
        self.state
            .fetch_or(WriterState::LOCKED_WAITING, Ordering::Release);

        while self.state.load(Ordering::Acquire) & (WriterState::COMPLETE | WriterState::LEADER)
            == 0
        {
            thread::park();
        }
    }

    #[inline(always)]
    pub(crate) fn is_leader(&self) -> bool {
        self.state.load(Ordering::Relaxed) & WriterState::LEADER != 0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::{thread::scope, time::Duration};

    use super::*;

    // ================================================================================================
    // Writer Test Invariants
    // ================================================================================================
    //
    // These tests validate Writer synchronization and waiting semantics in isolation
    // from the full WriteThread queue implementation.
    //
    // Important invariants:
    //
    // 1. Writers are thread-owned
    // ---------------------------
    // A Writer captures thread::current() during construction:
    //
    //     thread_handle: Thread
    //
    // Therefore a Writer MUST be constructed inside the thread which may later
    // call wait()/wait_and_block() and become parked.
    //
    // Constructing a Writer on one thread and parking on another would invalidate
    // the wakeup protocol because unpark() would target the wrong thread.
    //
    //
    // 2. Writers are shared through pointer publication
    // -------------------------------------------------
    // Concurrent tests publish Writer pointers through AtomicPtr using
    // Release/Acquire synchronization.
    //
    // Writers remain stack allocated inside their owning thread.
    //
    // thread::scope() guarantees all spawned threads complete before stack
    // teardown, making temporary shared raw pointers valid for the scoped lifetime.
    //
    //
    // 3. Published Writers are concurrently shared
    // --------------------------------------------
    // After publication, Writers may be observed and mutated concurrently through:
    //
    //     - atomic state transitions
    //     - intrusive queue links
    //     - thread wakeup operations
    //
    // UnsafeCell fields are NOT independently synchronized and rely on higher-level
    // protocol invariants for correctness.
    //
    //
    // 4. These are protocol tests
    // ---------------------------
    // Tests here validate narrow synchronization edges such as:
    //
    //     - wait/wakeup correctness
    //     - COMPLETE publication
    //     - LEADER promotion
    //     - parking semantics
    //
    // They intentionally avoid reconstructing the full WriteThread queue or
    // DBImpl::Write() execution flow.
    //
    // ================================================================================================

    #[test]
    fn writer_state() {
        let batch = Batch::new();
        let writer = Writer::new(&batch);

        writer.state.store(WriterState::LEADER, Ordering::Relaxed);

        assert!(writer.is_leader());
    }

    #[test]
    fn wait_and_block_wakes_after_complete() {
        //
        let follower: AtomicPtr<Writer> = AtomicPtr::new(ptr::null_mut());

        thread::scope(|s| {
            s.spawn(|| {
                //
                loop {
                    let f = follower.load(Ordering::Acquire);
                    if f.is_null() {
                        std::hint::spin_loop();
                        continue;
                    } else {
                        debug_assert!(!f.is_null());
                        let f = unsafe { &*f };
                        while f.state.load(Ordering::Acquire) & WriterState::LOCKED_WAITING == 0 {
                            std::hint::spin_loop();
                        }

                        f.state.fetch_or(WriterState::COMPLETE, Ordering::Release);
                        f.thread_handle.unpark();
                        break;
                    }
                }
            });

            s.spawn(|| {
                let batch = Batch::new();
                let writer = Writer::new(&batch);
                writer.state.fetch_or(WriterState::INIT, Ordering::Release);
                follower.store(ptr::from_ref(&writer).cast_mut(), Ordering::Release);
                writer.wait_and_block();

                assert!(writer.state.load(Ordering::Acquire) & WriterState::COMPLETE != 0);
                assert!(writer.state.load(Ordering::Acquire) & WriterState::LOCKED_WAITING != 0);
            });
        });
    }

    #[test]
    fn follower_promoted_to_leader() {
        let follower: AtomicPtr<Writer> = AtomicPtr::new(ptr::null_mut());

        thread::scope(|t| {
            t.spawn(|| {
                loop {
                    let f = follower.load(Ordering::Acquire);

                    if f.is_null() {
                        std::hint::spin_loop();
                        continue;
                    } else {
                        // We have a follower - suppose we have processed a write group and this follower needs to be handed leadership
                        // to process the next group

                        let f = unsafe { &*f };

                        while f.state.load(Ordering::Acquire) & WriterState::LOCKED_WAITING == 0 {
                            std::hint::spin_loop();
                        }

                        f.state.fetch_or(WriterState::LEADER, Ordering::Release);
                        f.thread_handle.unpark();
                        break;
                    }
                }
                //
            });

            let batch = Batch::new();
            let writer = Writer::new(&batch);
            follower.store(ptr::from_ref(&writer).cast_mut(), Ordering::Release);

            writer.wait_and_block();

            // assert woke as leader not complete
            assert!(writer.state.load(Ordering::Acquire) & WriterState::LEADER != 0);
            assert!(writer.state.load(Ordering::Acquire) & WriterState::COMPLETE == 0);
        });
    }
}
