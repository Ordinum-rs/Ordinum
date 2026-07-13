#[cfg(test)]
mod tests {
    use crate::db::batch::BatchObjectHandle;
    use crate::{
        db::{
            batch::{BatchObject, BatchRef},
            batch_pool::BatchPool,
            write_pipeline::{WritePipeline, WriterEnv},
        },
        version::SeqNumState,
        wal::SyncQueueSem,
    };
    use crate::{Error, Result};

    use crate::sync::Arc;

    use super::*;

    #[test]
    #[ignore = "API outline until WritePipeline::commit and BatchPool release are implemented"]
    fn correct_api() {
        // The simple correct API for caller acuired batch, accumalating operations and committing the batch
        //

        // SETUP ===================================

        struct EnvStub;
        impl WriterEnv for EnvStub {
            fn apply_commit<'env>(&self, batch: &'env BatchRef) -> Result<()> {
                Ok(())
            }
            fn prepare_commit<'env>(&self, batch: &'env BatchRef) -> Result<()> {
                Ok(())
            }
        }

        let env = Arc::new(EnvStub);

        let seq_state = Arc::new(SeqNumState::default());
        let sync_sem = SyncQueueSem::default();

        let mut wp = WritePipeline::<1, EnvStub>::new_with_size(env, seq_state.clone(), sync_sem);

        let mut pool = Arc::new(BatchPool::new());

        // ============================================

        let batch = pool.acquire();
        let b = BatchObjectHandle::new(Arc::clone(&pool), batch);

        b.put(b"Hello", b"There");

        let sealed_batch = b.seal();

        // We borrow the object because we want ownership to remain in the callers scope, this allows us to return early from pipeline whilst the
        // ptr reference is still queued. If we moved ownership of the Object, then the pipeline would own the NonNullBatchPtr meaning lifetime misery
        // The only problem is we can't return a transitioned state handle from the commit.
        wp.commit(&sealed_batch.inner()).expect("Ahhhhhh");

        let object = sealed_batch.reset();

        // The caller retains ownership of `sealed_batch` throughout the commit.
        // The pipeline only borrows the underlying Batch via its stable heap address.
        //
        // For a synchronous commit, `commit()` does not return until the batch has
        // been fully published (and fsynced if requested). Once it returns, the caller
        // may safely Reset() or Close() the batch.
        //
        // For a future NoSyncWait path, `commit()` may return before the batch has
        // completed publication/fsync. In that case the batch remains InFlight and
        // must not be modified, Reset()'d or Close()'d until completion is observed.
        //
        // While waiting for an earlier batch to complete, the caller may continue
        // doing useful work and build additional batches:
        //
        // batch1.put(...)
        // batch1.commit_no_sync_wait()
        //
        // batch2.put(...)
        // batch2.commit_no_sync_wait()
        //
        // batch3.put(...)
        // batch3.commit_no_sync_wait()
        //
        // batch1.sync_wait()
        // batch2.sync_wait()
        // batch3.sync_wait()
        //
        // This allows WAL fsync and publication latency to overlap with application
        // work, improving throughput. Each in-flight batch remains immutable until
        // its completion has been observed.
    }
}
