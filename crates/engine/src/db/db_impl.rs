use crate::{
    Error, Result,
    column_family::cf::ColumnFamilyData,
    db::{
        batch::{Batch, Sealed},
        options::DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE,
        write_batch::WBatch,
        write_pipeline::{WritePipeline, WriterEnv},
    },
    sync::Arc,
    sync::atomic::AtomicU64,
};

use std::{marker::PhantomData, sync::Weak};

pub(crate) struct SequenceState {
    visible_seq_no: AtomicU64,
    log_seq_no: AtomicU64,
}

impl Default for SequenceState {
    fn default() -> Self {
        Self {
            visible_seq_no: AtomicU64::new(0),
            log_seq_no: AtomicU64::new(0),
        }
    }
}

pub(crate) struct DbImpl {
    _p: PhantomData<()>,
    seq_state: Arc<SequenceState>,
    cf_data: Arc<ColumnFamilyData>,
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
