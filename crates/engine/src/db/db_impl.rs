use crate::db::write_batch::Batch;

use super::write_thread::WriteThread;
use std::marker::PhantomData;

pub(crate) struct DbImpl {
    _p: PhantomData<()>,
    write_thread: WriteThread,
}

impl DbImpl {
    //
    //
    //
    pub(crate) fn write(&self, batch: &Batch /* Other params? */) -> Result<(), ()> {

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
    }


        Ok(())
    }
}
