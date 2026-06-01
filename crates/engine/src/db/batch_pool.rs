// Global batch pool for heap allocated batches
//
//

// 1. A batch in the pool is not visible to the write pipeline.
// 2. A batch in the write pipeline is not visible to the pool.
// 3. pool_next is only read/written by BatchPool.
// 4. reset_for_reuse happens before push publishes the batch.
// 5. acquire must clear/detach pool_next before returning the batch.
// 6. shutdown must drain the pool and free retained batches.

use crate::{
    db::batch::{BatchObject, Pooled},
    sync::atomic::{AtomicPtr, AtomicUsize},
    utils::tagged_pointer::AtomicTaggedPtr,
};

pub(crate) struct BatchPool {
    head: AtomicTaggedPtr<BatchObject<Pooled>>,
    retained: AtomicUsize,
}
