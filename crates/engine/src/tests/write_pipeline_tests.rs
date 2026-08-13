#[cfg(test)]
mod tests {
    use crate::db::batch::{Batch, OwnedBatchFactory};
    use crate::db::batch_pool::BatchPoolImpl;
    use crate::{Error, Result};
    use crate::{
        db::{
            batch::{BatchObject, BatchRef},
            batch_pool::BatchPool,
            write_pipeline::{WritePipeline, WriterEnv},
        },
        version::SeqNumState,
        wal::SyncQueueSem,
    };

    use crate::sync::Arc;

    use super::*;

    #[test]
    // #[ignore = "API outline until WritePipeline::commit and BatchPool release are implemented"]
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

        let pool = Arc::new(BatchPoolImpl::<1, 1, 1, 1>::new());

        // ============================================

        // Want stats:
        //
        // TLS MISSES: 3
        // GLOBAL BATCHES REUSED: 1
        // ALLOCATIONS: 2
        // ALLOCATED BYTES: 24
        // BATCHES DROPPED: 0

        let one = pool.acquire_batch(); // Fresh allocation
        let two = pool.acquire_batch(); // Fresh allocation

        one.close(); // Give back to TLS
        two.close(); // Give back to Global

        let three = pool.acquire_batch(); // Acquire from TLS

        let mut b = pool.acquire_batch(); // Miss on TLS - Acquire from Global

        b.put(b"Hello", b"There");

        let sealed_batch = b.seal();

        // We borrow the object because we want ownership to remain in the callers scope, this allows us to return early from pipeline whilst the
        // ptr reference is still queued. If we moved ownership of the Object, then the pipeline would own the NonNull<BatchInner> meaning lifetime misery
        // The only problem is we can't return a transitioned state handle from the commit.
        wp.commit(&sealed_batch)
            .expect("sealed batch commit should succeed");

        let object = sealed_batch.reset();

        let _ = object.close();

        // NOTE: Need to make a snapshot stats to assert on - and it should be printable
        // Check batch pool stats
        pool.print_stats();

        let stats = pool.snapshot_stats();

        assert_eq!(stats.tls_misses, 3);
        assert_eq!(stats.global_batches_reused, 1);
        assert_eq!(stats.allocations, 2);
        assert_eq!(stats.allocated_bytes, 24);
        assert_eq!(stats.batches_dropped, 0);
    }
}
