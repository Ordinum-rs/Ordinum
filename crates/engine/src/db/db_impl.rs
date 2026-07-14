use crate::{
    Error, Result,
    column_family::cf::ColumnFamilyData,
    db::{
        batch::{Batch, BatchObject, BatchRef, Sealed},
        batch_pool::BatchPool,
        options::DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE,
        write_batch::WBatch,
        write_pipeline::{WritePipeline, WriterEnv},
    },
    sync::{Arc, atomic::AtomicU64},
    version::{SeqNumState, version_set::VersionSet},
};

use std::{marker::PhantomData, sync::Weak};

// -------------------------------------------------------------

pub(crate) struct DbImpl {
    _p: PhantomData<()>,
    //
    pub(super) version_set: VersionSet,
    //
    // Batch pool
    pub(super) batch_pool: BatchPool,
}

impl DbImpl {
    pub(crate) fn new() -> Self {
        Self {
            _p: PhantomData,
            version_set: VersionSet::new(),
            batch_pool: BatchPool::new(),
        }
    }

    pub(crate) fn seq_state(&self) -> Arc<SeqNumState> {
        Arc::clone(&self.seq_state())
    }
}

//

impl WriterEnv for DbImpl {
    //
    fn apply_commit<'env>(&self, batch: &'env BatchRef) -> Result<()> {
        //
        Ok(())
    }
    //
    fn prepare_commit<'env>(&self, batch: &'env BatchRef) -> Result<()> {
        //
        Ok(())
    }
}

impl DbImpl {
    //
    //
    //
}
