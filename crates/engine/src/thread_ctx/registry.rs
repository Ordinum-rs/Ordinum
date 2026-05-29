//
//
//
//
//

use crate::thread_ctx::TCTX;
use crate::version::superversion::SVCache;

//
use std::cell::UnsafeCell;

pub(crate) struct ThreadCtx {
    // sv_cache: UnsafeCell<SVCache>,
    // NOTE: Add PerfContext/Metrics
    // NOTE: Add IOContext/Metrics
}

impl ThreadCtx {
    pub(crate) fn new() -> Self {
        Self {
            // sv_cache: UnsafeCell::new(SVCache::new()),
        }
    }

    // pub(crate) fn sv_cache_mut(&self) -> &mut SVCache {
    // unsafe { &mut *self.sv_cache.get() }
    // }
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
