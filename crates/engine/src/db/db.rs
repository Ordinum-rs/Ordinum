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

// -------------------------------------------------------------

pub struct DB {
    // XXX: Internal per-DB TLS slot. Currently there is typically one DB instance,
    //      but keeping this indexed avoids baking single-instance assumptions into
    //      ThreadCtx.
    tls_idx: usize,
    inner: Arc<DbImpl>,
    write_pipeline: Arc<WritePipeline<DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE, DbImpl>>,
    //
}
