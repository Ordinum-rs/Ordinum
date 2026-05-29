use crate::{
    Error, Result,
    column_family::cf::ColumnFamilyData,
    db::{
        batch::{Batch, Sealed},
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
    fn apply_commit(&self, batch: &Batch<Sealed>) -> Result<()> {
        //
        Ok(())
    }
    //
    fn prepare_commit(&self, batch: &Batch<Sealed>) -> Result<()> {
        //
        Ok(())
    }
}

impl DbImpl {
    //
    //
    //
    pub(crate) fn write(&self, batch: Batch<Sealed> /* Other params? */) -> Result<()> {
        // Order of operations - process flow

        // validate the batch

        // Does DB assertions and checks

        // self.write_pipeline.commit(batch, /* Pass in writer trait? */)
        // Inside commit
        //      - Enqueue -> Prepare -> Call trait to insert WAL -> Call trait to insert memtable -> try_apply()

        Ok(())
    }
}
