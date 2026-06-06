//
//
//
//
//

use crate::db::batch_pool::ThreadBatchCache;
use crate::thread_local_storage::TCTX;
use crate::version::superversion::SVCache;

//
use crate::sync::cell;
use std::cell::UnsafeCell;
use std::ops::Bound::Unbounded;
use std::ptr::NonNull;

pub(crate) struct DBInstanceCtx {
    // sv_cache: UnsafeCell<SVCache>,
    batch_cache: UnsafeCell<ThreadBatchCache>,
    // NOTE: Add PerfContext/Metrics
    // NOTE: Add IOContext/Metrics
}

pub(crate) struct ThreadCtx {
    // XXX: Future optimisation baked in now
    // Indexed by tls_id/db_id
    db_instance: UnsafeCell<Vec<DBInstanceCtx>>,
}

// TODO: Need to implement thread ctx drop
//

impl ThreadCtx {
    pub(crate) fn new() -> Self {
        todo!()
    }

    // pub(crate) fn sv_cache_mut(&self) -> &mut SVCache {
    // unsafe { &mut *self.sv_cache.get() }
    // }
    //

    // TODO: Need SAFETY Comments
    fn db_instance(&self, db_id: usize) -> &DBInstanceCtx {
        //
        let db_vec = unsafe { &*self.db_instance.get() };

        assert!(db_id < db_vec.len());

        &db_vec[db_id]
    }

    // TODO: Need SAFETY Comments
    pub(crate) fn thread_batch_cache(&self, db_id: usize) -> &ThreadBatchCache {
        unsafe { &*self.db_instance(db_id).batch_cache.get() }
    }

    // TODO: Need SAFETY Comments
    pub(crate) fn thread_batch_cache_mut(&self, db_id: usize) -> &mut ThreadBatchCache {
        unsafe { &mut *self.db_instance(db_id).batch_cache.get() }
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
