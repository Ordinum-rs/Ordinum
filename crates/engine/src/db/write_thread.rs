// db.write(batch)
//     │
//     ├─ create Writer node on stack
//     ├─ join writer queue
//     │
//     ├─ if FOLLOWER
//     │      block on condvar
//     │      wake up when done
//     │      return
//     │
//     └─ if LEADER
//            form write group
//            assign sequence numbers
//            WAL write
//            apply group to memtables
//            signal followers
//            return
//
//
// Logic:
// db_impl  — orchestrates the whole flow on the calling thread
//    │
//    ├── write_thread — just coordination, am I leader or follower?
//    │                  if follower: block here until signalled
//    │                  if leader: return and let caller thread do the work
//    │
//    └── if leader: caller thread continues executing through db_impl
//                   accessing self directly for WAL, memtables, CFs
//
//
// Leader Cutoff
// The leader determines cutoff during batch formation based on compatibility and size limits,
// and a new leader starts either when newest_writer_ is set to null
// or when the next writer's state is explicitly set to STATE_GROUP_LEADER

use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::{ptr, sync::atomic::AtomicPtr};

use crate::db::writer::WriterState;

use super::write_batch::Batch;
use super::writer::Writer;

pub(crate) struct WriteGroup {
    leader: NonNull<Writer>,
    last_writer: *mut Writer,
    assigned_seq_no: u64,
    size: u64,
    writers: u32,
}

impl WriteGroup {
    fn new(leader: *mut Writer) -> Self {
        assert!(!leader.is_null());
        Self {
            leader: unsafe { NonNull::new_unchecked(leader) },
            last_writer: ptr::null_mut(),
            assigned_seq_no: 0,
            size: 0,
            writers: 0,
        }
    }
}

/// WriteThread is the coordination mechanism for multiple writes. Each calling thread will creater a writer holding a batch of operations and try to join
/// the write thread queue. The write thread will group multiple writes and determine leader/followers.
/// Once complete, it will signal to followers and drop
///
///
/// SAFETY:
///
/// WriteThread stores raw pointers to stack-owned Writer nodes.
///
/// A Writer passed to join() must remain alive until join() returns. join()
/// may publish the pointer to other threads, but it will not return for a
/// follower until the writer has reached a terminal state, and it will not
/// allow the writer to be dropped while another thread may still access it.
///
/// Therefore any Writer pointer reachable from the queue points to a live
/// Writer.
pub(crate) struct WriteThread {
    newest_writer: AtomicPtr<Writer>,
}

impl Default for WriteThread {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteThread {
    // NOTE: Later move to config options on the write thread if we want this to be configurable

    // How many times do we want to asm!(PAUSE) on the fast path for Writer::wait()
    pub(crate) const WAIT_PAUSE_ITERATIONS: usize = 200;
    // How many time do we want to iterate and Thread::yield()
    pub(crate) const YIELD_PAUSE_ITERATIONS: usize = 64;

    pub(crate) const MAX_BATCH_SIZE_PER_GROUP: usize = 1048;
    pub(crate) const MIN_BATCH_SIZE_PER_GROUP: usize = Self::MAX_BATCH_SIZE_PER_GROUP / 8;

    pub(crate) fn new() -> Self {
        Self {
            newest_writer: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn link_writer(&self, writer: *mut Writer) -> bool {
        debug_assert!(unsafe { (*writer).state.load(Ordering::Relaxed) & WriterState::INIT != 0 });
        debug_assert!(!writer.is_null());

        // TODO: Double check ordering here
        let mut current_newest_writer = self.newest_writer.load(Ordering::Relaxed);

        loop {
            // XXX: We can put write stall blocking here
            //

            // # SAFETY:
            // We check that writer is not null so we are safe to dereference
            unsafe {
                (*writer)
                    .link_older
                    .store(current_newest_writer, Ordering::Relaxed);
            }

            // CAS on current newest writer
            match self.newest_writer.compare_exchange_weak(
                current_newest_writer,
                writer,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(ptr) => return ptr.is_null(),
                Err(ptr) => {
                    current_newest_writer = ptr;
                    continue;
                }
            }
        }
    }

    // Example:
    // [newest]                        [oldest/leader]
    //    4-----------3-----------2-----------1
    //  Head  ----> Next  ----> Next  ----> Next
    //      <--------┚  <--------┚  <--------┚
    //      group_next   group_next  group_next
    //
    /// Builds the execution chain for the current write group.
    ///
    /// Starting from the snapshot of the newest writer, this walks the
    /// intrusive `older` chain (`newest -> ... -> oldest`) established
    /// during `join()`.
    ///
    /// As each writer is visited, its `group_next` pointer is set to the
    /// previously visited writer, effectively materializing the logical
    /// execution order of the group (`oldest -> ... -> newest`).
    ///
    /// Traversal continues until the oldest writer in the group is reached,
    /// identified by `older == null`.
    ///
    /// This does not modify the global queue or discover newly joined
    /// writers. It operates only on the leader's snapshot of the group.
    fn set_new_links(&self, group_newest_writer: *mut Writer) {
        //

        assert!(!group_newest_writer.is_null());

        let mut current = group_newest_writer;

        loop {
            // # SAFTEY:
            // current is not null so we are safe to load link_older
            let older = unsafe { (*current).link_older.load(Ordering::Relaxed) };

            // If the older Writer is null (reached end) or the older Writers next link is set already we break
            if older.is_null()
                // # SAFETY:
                // if older was null we will have hit the first conditional check, therefore, older is safe to dereference here
                || !(unsafe { (*older).group_next.load(Ordering::Relaxed).is_null() })
            {
                debug_assert!(
                    (older.is_null())
                        || unsafe { (*older).group_next.load(Ordering::Relaxed) == current }
                );
                break;
            }

            // # SAFETY:
            // old is not null so we are safe to access the group_next to store current
            unsafe { (*older).group_next.store(current, Ordering::Relaxed) };
            current = older;
        }
    }

    // Method to enter group as leader
    // https://github.com/facebook/rocksdb/blob/763401b5/db/write_thread.cc#L440
    pub(crate) fn EnterBatchGroup(&self, leader: NonNull<Writer>, write_group: &mut WriteGroup) {
        //
        //

        // # SAFTEY:
        // leader is NonNull, batch is NonNull. leader Writer remains active on the stack and outlives this method
        let size = unsafe { leader.as_ref().batch.as_ref().batch_size() };

        // Limit the max size if the leader's batch is smaller than MIN_BATCH_GROUP_SIZE so that small writes are not
        // slowed by group mechanics
        let mut max_size = WriteThread::MAX_BATCH_SIZE_PER_GROUP;
        if size <= WriteThread::MAX_BATCH_SIZE_PER_GROUP {
            max_size = size + WriteThread::MIN_BATCH_SIZE_PER_GROUP;
        }

        write_group.size = 1;
        write_group.writers = 1;
        // Set last writer as leader for now until we process next writers in the group and reach newest_writer (last in group) to then set last_writer.
        write_group.last_writer = leader.as_ptr();

        // Get the newest_writer to use to link newer writers in the group
        let newest_writer = self.newest_writer.load(Ordering::Acquire);

        self.set_new_links(newest_writer);

        // Traverse the WriteGroup in contextual order (oldest->newest) and decide if we need to remove writers and append to end (next group)

        let w = leader.as_ptr();
        let we = leader.as_ptr();
        let r: *mut Writer = ptr::null_mut();
        let re: *mut Writer = ptr::null_mut();

        while w != newest_writer {
            //
            // # SAFTEY:
            // We know that writer is NonNull and that it is safe to dereference because the stack allocated writer will oulive this method
            let w = unsafe { (*w).group_next.load(Ordering::Relaxed) };

            // TODO: Finish from here

            // compatable check
            // append rejected_list
            // fix r_list links
            // update write group
            //

            break;
        }

        todo!()
    }

    pub(crate) fn join(&self, writer: &Writer) {
        //
        // Raw pointer form used for the intrusive queue. Lifetime is governed by
        // WriteThread::join's stack-writer invariant.
        let w = ptr::from_ref(writer).cast_mut();

        let linked_writer = self.link_writer(w);

        if linked_writer {
            debug_assert!(writer.is_leader());

            let mut write_group = WriteGroup::new(w);

            // Continue as Leader
            //
            //
        } else {
            writer.wait();
        }
    }
}

//

#[cfg(test)]
mod tests {
    use crate::db::writer::WriterState;

    use super::*;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::thread::{self};
    use std::time::Duration;

    // TODO: Need to make this deterministic with while loop so we can enforce thread join order
    #[test]
    fn writer_follower_to_leader() {
        // XXX: Replace naive implementation with writer_thread methods - eventually move to integration test
        //
        let group: AtomicPtr<Writer> = AtomicPtr::new(ptr::null_mut());

        // Want:
        // leader -> follower 1 -> follower 2
        // To become:
        // follower 1 (new leader) -> follower 2

        // To make this deterministic we'll make each spawned thread sleep so we can control the order
        // We are testing logic->follower with third follower blocking on leader change

        // Assertion state
        let follower_1_state = AtomicU8::new(0);
        let follower_2_state = AtomicU8::new(0);

        thread::scope(|t| {
            // Leader
            t.spawn(|| {
                let batch = Batch::new();
                let mut writer_1 = Writer::new(&batch);

                // No wait - we want this to be leader

                group.store(&raw mut writer_1, Ordering::Release);

                // Set as leader
                writer_1
                    .state
                    .fetch_or(WriterState::LEADER, Ordering::Release);

                // Now wait for 1000ms to simulate processing group write and then set next leader
                thread::sleep(Duration::from_millis(1000));

                // We don't need to unpark because the next follower is the one we want to make leader
                // normally we'd traverse the linked list and process the group before either nulling the global head or
                // assigning new leader

                let follower = writer_1.group_next.load(Ordering::Acquire);

                assert!(!follower.is_null());
                unsafe {
                    (*follower)
                        .state
                        .fetch_or(WriterState::LEADER, Ordering::Release);
                    (*follower).thread_handle.unpark();
                }

                //
            });

            // Follower 1 (next leader)
            t.spawn(|| {
                let batch = Batch::new();
                let mut writer_2 = Writer::new(&batch);

                thread::sleep(Duration::from_millis(10));

                match group.compare_exchange(
                    group.load(Ordering::Acquire),
                    &raw mut writer_2,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(ptr) => {
                        // We have pointer to the leader - we need to set it's back_link to us
                        unsafe {
                            (*ptr)
                                .group_next
                                .store(&raw mut writer_2, Ordering::Relaxed);
                        }
                        // Set our next pointer to ptr we just stole from group head
                        writer_2.link_older.store(ptr, Ordering::Relaxed);
                    }
                    Err(_) => panic!(),
                }

                // Now block
                writer_2.wait_and_block();
                //

                // If we do become leader (which we should) check, simulate work and unpark followers
                if writer_2.is_leader() {
                    // Simulate work

                    thread::sleep(Duration::from_millis(1000));

                    // assert out back link is not null
                    assert!(!writer_2.group_next.load(Ordering::Relaxed).is_null());
                    let follower = writer_2.group_next.load(Ordering::Relaxed);

                    unsafe {
                        (*follower)
                            .state
                            .fetch_or(WriterState::COMPLETE, Ordering::Release);
                        if (*follower).state.load(Ordering::Acquire) & WriterState::LOCKED_WAITING
                            != 0
                        {
                            (*follower).thread_handle.unpark();
                        }
                    }
                }
                // Before we exit - log our state for assertion
                follower_1_state.store(writer_2.state.load(Ordering::Relaxed), Ordering::Relaxed);
            });

            // Follower 2
            t.spawn(|| {
                let batch = Batch::new();
                let mut writer_3 = Writer::new(&batch);

                thread::sleep(Duration::from_millis(20));

                match group.compare_exchange(
                    group.load(Ordering::Acquire),
                    &raw mut writer_3,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(ptr) => {
                        // We have pointer to the leader - we need to set it's back_link to us
                        unsafe {
                            (*ptr)
                                .group_next
                                .store(&raw mut writer_3, Ordering::Release);
                        }
                        // Set our next pointer to ptr we just stole from group head
                        writer_3.link_older.store(ptr, Ordering::Release);
                    }
                    Err(_) => panic!(),
                }

                // Now block
                writer_3.wait_and_block();
                //

                // Before we exit - log our state for assertion
                follower_2_state.store(writer_3.state.load(Ordering::Relaxed), Ordering::Relaxed);
            });
        });

        // assertions:
        assert!(follower_1_state.load(Ordering::Relaxed) & WriterState::LEADER != 0);
        assert!(follower_2_state.load(Ordering::Relaxed) & WriterState::COMPLETE != 0);
    }
}
