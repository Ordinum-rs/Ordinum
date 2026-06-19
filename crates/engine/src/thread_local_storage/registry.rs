//
//
//
//
//

use crate::db::batch_pool::ThreadBatchCache;
use crate::sync::Mutex;
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
    instances: UnsafeCell<Vec<Option<Box<DBInstanceCtx>>>>,
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

    // TODO: Need to test
    pub(super) fn ensure_registered(&self) {
        if self.registered.get() {
            return;
        }

        // We need to register in the static meta

        let meta = static_meta();

        let guard = meta.thread_mu.lock().unwrap_or_else(|e| panic!("{e}"));

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
    }

    // pub(crate) fn sv_cache_mut(&self) -> &mut SVCache {
    // unsafe { &mut *self.sv_cache.get() }
    // }
    //

    pub(super) fn db_instance(&self, db_id: usize) -> &DBInstanceCtx {
        //

        // TODO: Fix after
        let dvec = unsafe { &mut *self.instances.get() };

        if dvec.len() <= db_id {
            dvec.resize_with(db_id + 1, || None);
        }

        dvec[db_id].get_or_insert_with(|| Box::new(DBInstanceCtx::new()))
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
