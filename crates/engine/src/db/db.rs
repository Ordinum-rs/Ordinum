//
//
//
//
//
//

use crate::db::db_impl::DbImpl;
use crate::db::options::DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE;

use crate::db::write_pipeline::WritePipeline;
use crate::sync::Arc;
use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::Ordering;
use crate::thread_local_storage::NEXT_TLS_ID;
use crate::wal::SyncQueueSem;

// -------------------------------------------------------------

pub struct DB {
    // XXX: Internal per-DB TLS slot. Currently there is typically one DB instance,
    //      but keeping this indexed avoids baking single-instance assumptions into
    //      ThreadCtx.
    db_id: usize,
    inner: Arc<DbImpl>,
    write_pipeline: Arc<WritePipeline<DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE, DbImpl>>,
    //
}

impl DB {
    pub fn open() -> Self {
        // Add more user specified stuff like file path, name etc
        //
        // Then make new()
        Self::new()
    }

    pub(crate) fn new() -> Self {
        let id = NEXT_TLS_ID.fetch_add(1, Ordering::Relaxed);

        let db_impl = Arc::new(DbImpl::new());

        Self {
            db_id: id,
            inner: Arc::clone(&db_impl),
            write_pipeline: Arc::new(WritePipeline::new(
                Arc::clone(&db_impl),
                db_impl.seq_state(),
                SyncQueueSem::default(),
            )),
        }
    }
}
