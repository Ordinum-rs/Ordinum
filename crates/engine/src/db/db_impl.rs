use crate::{column_family::cf::ColumnFamilyData, db::write_batch::WBatch};

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
    pub(crate) fn write(&self, batch: &WBatch /* Other params? */) -> Result<(), ()> {
        // What would i like?
        //

        // let writer = Writer::new(batch);
        //
        // self.write_thread.join(&writer);
        //
        // if writer.is_leader() {
        //
        // // We are leader
        // // Continue with the write
        //
        // }
        //

        Ok(())
    }
}
