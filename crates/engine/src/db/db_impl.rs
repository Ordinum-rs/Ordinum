use crate::{
    column_family::cf::ColumnFamilyData,
    db::{
        batch::{Batch, Sealed},
        write_batch::WBatch,
    },
};

use super::write_thread::WriteThread;
use std::{marker::PhantomData, sync::Arc};

pub(crate) struct DbImpl {
    _p: PhantomData<()>,
    write_thread: WriteThread,
    cf_data: Arc<ColumnFamilyData>,
}

impl DbImpl {
    //
    //
    //
    pub(crate) fn write(&self, batch: &Batch<Sealed> /* Other params? */) -> Result<(), ()> {
        // Order of operations - process flow

        // validate the batch

        // Does DB assertions and checks

        // self.write_pipeline.commit(batch, /* Pass in writer trait? */)
        // Inside commit
        //      - Enqueue -> Prepare -> Call trait to insert WAL -> Call trait to insert memtable -> try_apply()

        Ok(())
    }
}
