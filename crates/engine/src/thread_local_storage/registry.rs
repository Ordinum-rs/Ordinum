//
//
//
//
//

use crate::db::batch_pool::ThreadBatchCache;
use crate::sync::cell;
use crate::sync::cell::RefCell;
use crate::sync::cell::RefMut;
use crate::thread_local_storage::TCTX;
use crate::version::superversion::SVCache;

//
use std::cell::UnsafeCell;
use std::ops::Bound::Unbounded;
use std::pin::{self, Pin};
use std::ptr::NonNull;

pub(crate) struct DBInstanceCtx {
    // TODO: Writers will be able to walk the linked list of DBInstanceCtx for the DB when changing a superversion to invalidate
    // a Cached SV so we must maintain strict Safety invariants for this
    //
    // Should we Box this?
    // sv_cache: UnsafeCell<SVCache>,
    //
    // TODO: Document the strict invariants for accessing this field
    batch_cache: RefCell<ThreadBatchCache>,
    // NOTE: Add PerfContext/Metrics
    // NOTE: Add IOContext/Metrics
}

impl DBInstanceCtx {
    pub(crate) fn new() -> Self {
        Self {
            batch_cache: RefCell::new(ThreadBatchCache::new()),
        }
    }

    // TODO: Need SAFETY Comments
    pub(crate) fn thread_batch_cache_mut<F, R>(&self, db_id: usize, f: F) -> R
    where
        F: FnOnce(&mut ThreadBatchCache) -> R,
    {
        // unsafe { &mut *self.db_instance(db_id).batch_cache.borrow_mut() }
        let mut cache = self.batch_cache.borrow_mut();
        f(&mut cache)
    }
}

pub(crate) struct ThreadCtx {
    // XXX: Future optimisation baked in now
    // Indexed by tls_id/db_id
    //
    // Vec will change or move contents on re-allocation, we need to ensure that the objects stored are at stable addresses
    //
    // XXX: Make this into a cleaner type
    db_instance: UnsafeCell<Vec<Option<Pin<Box<DBInstanceCtx>>>>>,
}

// TODO: Need to implement thread ctx drop
//

impl ThreadCtx {
    pub(crate) fn new() -> Self {
        Self {
            db_instance: UnsafeCell::new(Vec::new()),
        }
    }

    // pub(crate) fn sv_cache_mut(&self) -> &mut SVCache {
    // unsafe { &mut *self.sv_cache.get() }
    // }
    //

    // TODO: Need SAFETY Comments
    pub(super) fn db_instance(&self, db_id: usize) -> &DBInstanceCtx {
        //
        let db_vec = unsafe { &mut *self.db_instance.get() };

        if db_vec.len() <= db_id {
            db_vec.resize_with(db_id + 1, || None);
        }

        db_vec[db_id].get_or_insert_with(|| Pin::new(Box::new(DBInstanceCtx::new())))
    }
}

#[test]

fn hzd_ptr() {
    TCTX.with(|ctx| {
        // Get the sv_cache
        // let cache = ctx.sv_cache_mut();
        // Access the generation number to check freshness
        // If fresh:
        // take sv pointer and protect() -- cheap because it should still be the same in the holder
        // Else:
        // get the global Atomic sv and store ptr and protect
    })
}
