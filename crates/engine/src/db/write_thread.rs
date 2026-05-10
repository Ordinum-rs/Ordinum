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
                *(*writer).link_older.get() = current_newest_writer;
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
            let older = unsafe { *(*current).link_older.get() };

            // If the older Writer is null (reached end) or the older Writers next link is set already we break
            if older.is_null()
                // # SAFETY:
                // if older was null we will have hit the first conditional check, therefore, older is safe to dereference here
                || !(unsafe { (*older).group_next.get().is_null() })
            {
                debug_assert!(
                    (older.is_null()) || unsafe { *(*older).group_next.get() == current }
                );
                break;
            }

            // # SAFETY:
            // old is not null so we are safe to access the group_next to store current
            unsafe { *(*older).group_next.get() = current };
            current = older;
        }
    }

    // Method to enter group as leader
    // https://github.com/facebook/rocksdb/blob/763401b5/db/write_thread.cc#L440
    pub(crate) fn EnterBatchGroup(&self, leader: NonNull<Writer>, write_group: &mut WriteGroup) {
        //
        //

        // SAFETY:
        // `leader` is `NonNull`, and `batch` is initialized during writer
        // construction and immutable after publication. Reading batch metadata
        // does not race with any concurrent mutation.
        let mut size = unsafe { leader.as_ref().batch.as_ref().batch_size() };

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
        let snapshot_newest_w = self.newest_writer.load(Ordering::Acquire);

        self.set_new_links(snapshot_newest_w);

        // Traverse the WriteGroup in contextual order (oldest->newest) and decide if we need to remove writers and append to end (next group)

        let mut current_writer = leader.as_ptr();
        let mut write_group_end = leader.as_ptr();
        let mut rejected_head: *mut Writer = ptr::null_mut();
        let mut rejected_tail: *mut Writer = ptr::null_mut();

        while current_writer != snapshot_newest_w {
            debug_assert!(!unsafe { *(*current_writer).group_next.get() }.is_null());
            //
            // SAFETY:
            // `w` is part of the current materialized execution chain.
            // `group_next` has been initialized by `set_new_links()` before
            // entering this loop, so reading it yields either the next writer
            // in this group or null at the group boundary.
            current_writer = unsafe { *(*current_writer).group_next.get() };

            // Compatibility checks

            // SAFETY:
            //
            // `w` traverses the materialized execution chain for this batch group,
            // starting at `leader` and advancing through `group_next` until the
            // snapshot `newest_writer` is reached.
            //
            // All writers in this chain remain live while linked into `WriteThread`,
            // and writer metadata (`batch`, `sync`, write options) is immutable after
            // publication, so reading these fields is race-free.
            //
            // This method is executed by the sole current batch-group leader. No other
            // thread mutates `link_older` or `group_next` for writers in this selected
            // group while this loop is active.
            //
            // Therefore it is sound to:
            //
            // - traverse writers via `group_next`
            // - inspect writer metadata for compatibility checks
            // - splice rejected writers out of the execution chain by rewiring
            //   `link_older` and `group_next`
            // - append rejected writers to `r_list` for handoff into the next group.
            unsafe {
                // Don't group empty batches
                if (*current_writer).batch.as_ref().is_empty() ||
                    // Remove batches which breach our max size
                    (*current_writer).batch.as_ref().batch_size() > max_size ||
                    // If sync modes do not match with leader, remove
                    (*current_writer).sync != (*leader.as_ptr()).sync
                // TODO: Add other conditions
                {
                    //
                    // Remove from the list by
                    //
                    // We take the next and previous writer's of current and re-link them so that they each skip the current writer
                    //
                    // Linking the current's older writer to current's newer writer so current's older skips current
                    // W4 --------> W3 --------> W2 --------> W1
                    //              |           |           |
                    //         link_newer <- current -> link_older
                    //               <---------<------------┚
                    //               link so W1 skips current

                    let older = *(*current_writer).link_older.get();
                    let newer = *(*current_writer).group_next.get();

                    // Set the current's older writer's group_next to the writer after current so current is skipped
                    *(*older).group_next.get() = newer;

                    // Do the inverse of above
                    if !newer.is_null() {
                        *(*newer).link_older.get() = older;
                    };

                    // Insert current into r_list
                    // Building by tail append: r = beginning (head/oldest) rb = end (tail/newest)

                    // If end of r_list is null we can just set both start and end pointers to current
                    if rejected_tail.is_null() {
                        rejected_tail = current_writer;
                        rejected_head = current_writer;
                        // current writers link_older should be changed to null as it is the oldest entry in the r_list
                        *(*current_writer).link_older.get() = ptr::null_mut();
                    } else {
                        // else we need to insert the current writer at the tail of the r_list

                        // Link current writer's link_older to point to the current re writer
                        *(*current_writer).link_older.get() = rejected_tail;
                        // Link the current re writer's group_next to point to the current writer
                        *(*rejected_tail).group_next.get() = current_writer;
                        // update new re tail to equal current writer
                        rejected_tail = current_writer;
                    }
                } else {
                    // We pass compatability checks on the writers and can now grow the group upwards
                    write_group_end = current_writer;
                    let cw = &*current_writer;
                    size += cw.batch.as_ref().batch_size();
                    write_group.last_writer = current_writer;
                    write_group.size += 1;
                };
            };
        } // Loop exit - current_writer should be snapshot_newest_w here

        // Once we've reached newest_writer (end of write group for this loop) we can append the rejected list to the end of the current_writer
        // so the next newest writer (or leader handoff) can process it
        if !rejected_head.is_null() {
            // SAFETY:
            // `rejected_head..rejected_tail` is a temporary rejected chain built
            // by this leader, and `write_group_end` is the last accepted writer.
            //
            // This leader has exclusive permission to mutate these intrusive links.
            // Grafting `rejected_head` after `write_group_end` and null-terminating
            // `rejected_tail.group_next` restores a single execution chain.
            unsafe {
                // link the rejected head to the write groups tail
                *(*rejected_head).link_older.get() = write_group_end;
                // Null the link_newer rejected tail because there will be no more appended (it is at end)
                *(*rejected_tail).group_next.get() = ptr::null_mut();
                // Now link the write_groups tail group_next to point to the rejected lists head
                *(*write_group_end).group_next.get() = rejected_head;
            }

            // Now we have a write group and have appended the rejected_list, we need to detect if newer writers have joined in the meantime. We do so through
            // CASing on the global newest writer and comparing against our view taken of the newest_writer (snapshot_newest_w) and the rejected_tail writer.
            // If we succeed the rejected_tail becomes the newest writer in the global linked list.
            // If we fail, then new writers have joined since then and we must link them correctly.
            //
            // For example:
            // [A = Comapatible R = Rejected]
            //
            // EnterGroupBatch start ->
            //
            // get global newest_writer list:
            // newest | W4 --> W3 -->  W2 -->  W1 -->  L | oldest
            //          C      C       R       R       C
            //
            // writer list:
            // tail   | L  --> W3 --> W4 | head
            // rejected list:
            // tail   | W1 --> W2        | head
            //
            // write_group list:
            // tail   | L  --> W3 --> W4 --> W1 --> W2 | head
            //         we             wb     re     rb
            //
            //
            // global newest writer list after new writers join:
            // newest | W6 --> W5 --> W4 --> W3 -->  W2 -->  W1 -->  L | oldest
            //          ^      ^
            //                 W5 link_older now points to the end of the compatible writer group W4
            //
            // The oldest writer newer than our snapshot must have its `link_older`
            // rewired from `snapshot_newest_w` to `rejected_tail`.
            // tail   | L  --> W3 --> W4 --> W1 --> W2 --> W5 --> W6 | head
            //        |   write group     |  rejected   |     new    |
            //

            match self.newest_writer.compare_exchange_weak(
                snapshot_newest_w,
                rejected_tail,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => { /* If ok drop out, our view on newest_writer hasn't changed and so we can assign rejected tail to newest writer global */
                }
                // If we error, we want the global newest writer, and then we walk it's link_older until we get to the writer's whose link_older equals
                // our snapshot_newest_w. Once we're there we can re-assign it's link_older to point to the rejected tail writer
                Err(mut current_global_newest) => {
                    // SAFETY:
                    //
                    // The failed CAS returned the current global head, which is a live writer
                    // published through `self.newest_writer`.
                    //
                    // Writers newer than `snapshot_newest_w` were linked by `link_writer()`
                    // before publication, so each has a valid non-null `link_older` pointing
                    // toward older writers.
                    //
                    // As the sole current batch-group leader, this thread has exclusive
                    // permission to mutate intrusive group links. Walking `link_older` from
                    // the current global head until reaching the first writer whose
                    // `link_older == snapshot_newest_w` identifies the oldest writer newer
                    // than our snapshot. Re-pointing that writer to `rejected_tail` restores
                    // queue continuity after local group surgery.
                    unsafe {
                        while *(*current_global_newest).link_older.get() != snapshot_newest_w {
                            current_global_newest = *(*current_global_newest).link_older.get();
                        }
                        // We are at the writer which needs to be linked to the rejected tail writer
                        *(*current_global_newest).link_older.get() = rejected_tail
                    }
                }
            }
        }
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

                let follower = unsafe { *writer_1.group_next.get() };

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
                        unsafe { *(*ptr).group_next.get() = &raw mut writer_2 }
                        // Set our next pointer to ptr we just stole from group head
                        unsafe { *writer_2.link_older.get() = ptr }
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
                    assert!(!unsafe { *writer_2.group_next.get() }.is_null());
                    let follower = unsafe { *writer_2.group_next.get() };

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
                            *(*ptr).group_next.get() = &raw mut writer_3;
                        }
                        // Set our next pointer to ptr we just stole from group head
                        unsafe { *writer_3.link_older.get() = ptr };
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

    #[test]
    fn entering_as_group() {
        todo!()
    }
}
