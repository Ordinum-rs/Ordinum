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
}
