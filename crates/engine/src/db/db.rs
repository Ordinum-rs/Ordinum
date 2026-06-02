//
//
//
//
//
//

use crate::db::batch::{BatchObject, Sealed};
use crate::db::db_impl::DbImpl;
use crate::db::options::DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE;
use crate::{Error, Result};

use crate::db::write_pipeline::WritePipeline;
use crate::sync::Arc;

// -------------------------------------------------------------

pub struct DB {
    inner: Arc<DbImpl>,
    write_pipeline: Arc<WritePipeline<DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE, DbImpl>>,
    //
}

impl DB {
    //
    // XXX: Need to think carefully if we want to move ownership of the batch here - we can offer both approaches for explicit caller ownership but
    // maybe will have to treat this differently in the pool?
    pub(crate) fn write(&self, batch: BatchObject<Sealed> /* Other params? */) -> Result<()> {
        // Order of operations - process flow

        // validate the batch

        // Does DB assertions and checks

        // self.write_pipeline.commit(batch, /* Pass in writer trait? */)
        // Inside commit
        //      - Enqueue -> Prepare -> Call trait to insert WAL -> Call trait to insert memtable -> try_apply()

        Ok(())
    }
}
