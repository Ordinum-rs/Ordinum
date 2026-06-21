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
use std::ptr::NonNull;
use std::ptr::null_mut;

// Thread Local Storage
//
// TLS will house both Thread object and Per-DB instanced objects.
//
// Other threads may walk thread storage and access shared data, we need a way to store TLS and access it in a way that allows us to link
// between threads and also db_instances.
//
// For this, we must think about what has access to where:
//
// The DB is an instance and should own a vertical stack of threads which touch it
// Threads are a process and should own a horizontal stack of instances they touch
//
// Similar to Rocks this forms a sort of matrix
//
//                  column 0      column 1      column 2
//                  DB A          DB B          DB C
// ThreadData 1     entries[0]    entries[1]    entries[2]
// ThreadData 2     entries[0]    entries[1]    entries[2]
// ThreadData 3     entries[0]    entries[1]    entries[2]
//
//
// Each DB instance will own a ThreadLocalPtr sentinel which forms the head of a linked list to the threads which touch it. The DB will form a column
// in this matrix. It will acquire a TLS ID which new threads will use to index to columns (DB Instances).
//
// Each ThreadCtx will hold a list of Entries which are the columns that it touches. It will also hold a Next and a Prev pointer to form a doubly linked list
// allowing it to traverse the rows of that column.
//

// TODO: Next is to build the close functionality where we walk all threads and de-register

pub(super) struct Entry {
    ptr: AtomicPtr<DBInstanceCtx>,
}

impl Entry {
    pub(super) fn new() -> Self {
        Self {
            ptr: AtomicPtr::new(null_mut()),
        }
    }

    pub(super) fn from(ptr: AtomicPtr<DBInstanceCtx>) -> Self {
        Self { ptr }
    }

    pub(super) fn get_or_insert_new(&mut self) -> &DBInstanceCtx {
        let ptr = self.ptr.load(Ordering::Acquire);

        if !ptr.is_null() {
            return unsafe { &*ptr };
        }

        let new_ptr = Box::into_raw(Box::new(DBInstanceCtx::new()));
        self.ptr.store(new_ptr, Ordering::Release);

        unsafe { &*new_ptr }
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

    // pub(crate) fn sv_cache_mut(&self) -> &mut SVCache {
    // unsafe { &mut *self.sv_cache.get() }
    // }
    //

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
