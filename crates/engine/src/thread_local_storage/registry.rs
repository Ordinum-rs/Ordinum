//
//
//
//
//

use crate::db::batch_pool::ThreadBatchCache;
use crate::db::db_impl::DbImpl;
use crate::sync::Mutex;
use crate::sync::atomic::AtomicPtr;
use crate::sync::atomic::Ordering;
use crate::sync::cell::Cell;
use crate::sync::cell::RefCell;
use crate::sync::cell::RefMut;
use crate::thread_local_storage::TCTX;
use crate::thread_local_storage::static_meta;
use crate::version::superversion::SVCache;

//
use std::cell::UnsafeCell;
use std::ops::Bound::Unbounded;
use std::pin::{self, Pin};
use std::ptr;
use std::ptr::NonNull;
use std::ptr::null_mut;

// Thread Local Storage
//
// TLS stores per-thread state plus per-thread-per-DB state.
//
// The global StaticMeta owns a linked list of all registered ThreadCtx objects.
// This list lets DB maintenance code walk every thread and inspect the entry
// for a specific DB.
//
// Each DBImpl owns a tls_id. The tls_id is a column index into every
// ThreadCtx.instances vector.
//
// Conceptually:
//
//                  DB A          DB B          DB C
//                  tls_id=0      tls_id=1      tls_id=2
//
// ThreadCtx 1      entries[0]    entries[1]    entries[2]
// ThreadCtx 2      entries[0]    entries[1]    entries[2]
// ThreadCtx 3      entries[0]    entries[1]    entries[2]
//
// StaticMeta.head links the rows:
//
//     head <-> ThreadCtx 1 <-> ThreadCtx 2 <-> ThreadCtx 3
//
// A DB does not own a linked-list head for its column. To invalidate DB A,
// DBImpl uses its tls_id and walks StaticMeta.head:
//
//     for each ThreadCtx:
//         if tls_id < instances.len():
//             invalidate instances[tls_id]
//
// ThreadCtx.instances is grown lazily. If a thread has never touched a DB
// whose tls_id is N, its vector may be shorter than N + 1. Walkers must check
// bounds and skip missing slots.

// TODO: Next is to build the close functionality where we walk all threads and de-register

/// One per-thread, per-DB TLS slot.
///
/// `Entry` owns an optional heap-allocated `DBInstanceCtx`. A null pointer means
/// this thread has not touched that DB slot yet.
///
/// Ownership:
/// - The pointed-to `DBInstanceCtx`, when present, was allocated with
///   `Box::into_raw`.
/// - `Entry::drop` reclaims it with `Box::from_raw`.
///
/// Synchronization:
/// - The owning thread may initialize/read its own entry through `&mut Entry`.
/// - Cross-thread walkers must hold `StaticMeta::thread_mu` while traversing
///   `ThreadCtx.instances` and must not retain returned pointers after
///   releasing that lock.
/// - The pointer is not atomic; do not read or write it concurrently without the
///   registry mutex or exclusive owner-thread access.
pub(super) struct Entry {
    ptr: UnsafeCell<*mut DBInstanceCtx>,
}

impl Default for Entry {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        // SAFETY
        //
        // We maintain that every Entry is stored in the instances of a ThreadCtx and is only accessed through the thread.mu Mutex by walkers
        // traversing the linked list of ThreadCtx's
        let ptr = unsafe { *self.ptr.get() };

        if !ptr.is_null() {
            // SAFETY
            //
            // We are safe to drop as we hold exclusive access and we should hold the thread_mu Mutex in static_meta()
            // We maintain through API that no ptr references exist outside of the mutex
            unsafe { drop(Box::from_raw(ptr)) }
        }
    }
}

impl Entry {
    pub(super) fn new() -> Self {
        Self {
            ptr: UnsafeCell::new(null_mut()),
        }
    }

    pub(super) fn get(&self) -> Option<&DBInstanceCtx> {
        let ptr = unsafe { *self.ptr.get() };

        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    }

    pub(super) fn get_or_insert_new(&mut self) -> &DBInstanceCtx {
        let ptr = unsafe { *self.ptr.get() };

        if !ptr.is_null() {
            return unsafe { &*ptr };
        }

        let new_ptr = Box::into_raw(Box::new(DBInstanceCtx::new()));

        // Add Safety
        unsafe {
            *self.ptr.get() = new_ptr;
            &*new_ptr
        }
    }
}

pub(crate) struct ThreadCtx {
    // Linked
    next: *mut ThreadCtx,
    prev: *mut ThreadCtx,
    registered: Cell<bool>,

    // Thread Local
    // NOTE: Add PerfContext/Metrics
    // NOTE: Add IOContext/Metrics

    //

    // DB Instanced
    instances: UnsafeCell<Vec<Entry>>,
    // NOTE: Can also be -> UnsafeCell<Vec<Entry<DBInstanceCtx>>> Where entry is Entry { ptr: AtomicPtr<()> } ??
}

// TODO: Need to implement thread ctx drop
//
impl Drop for ThreadCtx {
    fn drop(&mut self) {
        println!("Dropping tls ctx");
        // We will need to call the return method on ThreadBatchCache
    }
}

impl ThreadCtx {
    pub(crate) fn new() -> Self {
        Self {
            next: null_mut(),
            prev: null_mut(),

            registered: Cell::new(false),

            instances: UnsafeCell::new(Vec::new()),
        }
    }

    #[inline]
    fn resize_instances(&self, tls_id: usize) {
        let _guard = static_meta().thread_mu.lock().unwrap();

        let instances = unsafe { &mut *self.instances.get() };

        if tls_id >= instances.len() {
            instances.resize_with(tls_id + 1, Entry::new);
        }
    }

    pub(super) fn ensure_registered(&self) {
        if self.registered.get() {
            return;
        }

        // We need to register in the static meta and initialise the instances to tls_id global

        let meta = static_meta();

        let guard = meta.thread_mu.lock().unwrap_or_else(|e| panic!("{e}"));

        // Re-size instances first

        let tls_id = meta.next_tls_id.load(Ordering::Acquire);

        let instances = unsafe { &mut *self.instances.get() };

        if instances.len() < tls_id {
            instances.resize_with(tls_id, Entry::new);
        }

        // Insert ctx into doubly linked list
        //
        //            Prev <--- current_head ---> Next ---> null
        //             |             ^
        //  Prev <--- Self ----------┘

        let ptr = self as *const Self as *mut Self;
        let sentinel = unsafe { &mut *meta.head.get() };

        let old = sentinel.next;

        unsafe {
            (*ptr).prev = sentinel as *mut ThreadCtx;
            (*ptr).next = old;

            if !old.is_null() {
                (*old).prev = ptr;
            } else {
                sentinel.prev = ptr;
            }

            sentinel.next = ptr;
        }

        let _ = self.registered.replace(true);
    }

    pub(super) fn db_instance(&self, tls_id: usize) -> &DBInstanceCtx {
        {
            let instances = unsafe { &*self.instances.get() };

            if tls_id >= instances.len() {
                self.resize_instances(tls_id);
            }
        }

        let instances = unsafe { &mut *self.instances.get() };

        instances[tls_id].get_or_insert_new()
    }
}

// ---- DBInstanceCtx ---- //

pub(crate) struct DBInstanceCtx {
    // TODO: Writers will be able to walk the linked list of DBInstanceCtx for the DB when changing a superversion to invalidate
    // a Cached SV so we must maintain strict Safety invariants for this
    //
    // Should we Box this?
    // sv_cache: UnsafeCell<SVCache>,
    //
    // TODO: Document the strict invariants for accessing this field
    batch_cache: UnsafeCell<ThreadBatchCache>,
}

impl DBInstanceCtx {
    pub(super) fn new() -> Self {
        Self {
            batch_cache: UnsafeCell::new(ThreadBatchCache::new()),
        }
    }

    // TODO: Need SAFETY Comments
    pub(crate) fn thread_batch_cache_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut ThreadBatchCache) -> R,
    {
        let mut cache = unsafe { &mut *self.batch_cache.get() };
        f(&mut cache)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use crate::sync::spin_loop;
    use crate::thread_local_storage::{thread_ctx, thread_db_instance_ctx};

    use super::*;

    #[test]
    fn thread_ctx_registered() {
        // Now walk the static meta linked list and should have 1 entry

        let meta = static_meta();

        assert!(unsafe { &*meta.head.get() }.next.is_null());
        //
        thread_ctx(|ctx| assert_eq!(ctx.registered.get(), true));
        //
        assert!(!unsafe { &*meta.head.get() }.next.is_null());
    }

    #[test]
    fn mutli_thread_register() {
        let meta = static_meta();

        let mut registered_count = 0;

        thread::scope(|t| {
            //

            t.spawn(|| thread_ctx(|_| ()));
            //
            t.spawn(|| thread_ctx(|_| ()));
            //
            t.spawn(|| thread_ctx(|_| ()));
        });

        let mut next = unsafe { &*meta.head.get() }.next;

        while !next.is_null() {
            registered_count += 1;

            next = unsafe { &*next }.next
        }

        assert_eq!(registered_count, 3);
    }

    #[test]
    fn thread_instances_initialised_to_db_instances() {
        let meta = static_meta();

        meta.next_tls_id.store(3, Ordering::Release);

        thread::scope(|t| {
            //

            t.spawn(|| thread_ctx(|_| ()));
            //
            t.spawn(|| thread_ctx(|_| ()));
            //
            t.spawn(|| {
                thread_ctx(|ctx| {
                    // Check instances len
                    assert_eq!(unsafe { &*ctx.instances.get() }.len(), 3);
                })
            });
        });
    }

    #[test]
    fn check_thread_instance_resize_on_new_db_instance() {
        let meta = static_meta();

        meta.next_tls_id.store(3, Ordering::Release);

        // tls_id:
        //  1, 2, 3, 4, 5
        // index:
        // [0, 1, 2, 3, 4]
        //              ^
        //         tls_id = 5 = index 4

        thread::scope(|t| {
            t.spawn(|| {
                thread_ctx(|ctx| {
                    assert_eq!(unsafe { &*ctx.instances.get() }.len(), 3);
                });

                meta.next_tls_id.store(5, Ordering::Release);

                thread_ctx(|ctx| {
                    assert_eq!(unsafe { &*ctx.instances.get() }.len(), 3);
                });

                // Only resize when accessing the db instance

                thread_db_instance_ctx(4, |_| {});
                thread_ctx(|ctx| {
                    assert_eq!(unsafe { &*ctx.instances.get() }.len(), 5);
                });
            });
        });
    }
}
