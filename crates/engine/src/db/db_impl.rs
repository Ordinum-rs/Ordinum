use crate::{
    Error, Result,
    column_family::cf::ColumnFamilyData,
    db::{
        batch::{Batch, BatchObject, Sealed},
        options::DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE,
        write_batch::WBatch,
        write_pipeline::{WritePipeline, WriterEnv},
    },
    sync::{Arc, atomic::AtomicU64},
    version::version_set::VersionSet,
};

use std::{marker::PhantomData, sync::Weak};

pub(crate) struct DbImpl {
    _p: PhantomData<()>,
    //
    version_set: VersionSet,
}

impl WriterEnv for DbImpl {
    //
    fn apply_commit(&self, batch: &Batch) -> Result<()> {
        //
        Ok(())
    }
    //
    fn prepare_commit(&self, batch: &Batch) -> Result<()> {
        //
        Ok(())
    }
}

impl DbImpl {
    //
    //
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
