//
//
//

// DB - Options which are configured and stored on DB::Open()

pub(crate) const DEFAULT_WRITE_PIPELINE_CAPACITY_SIZE: usize = 64;
pub(crate) const DEFAULT_MAX_WRITE_BATCH_BYTES: usize = 1024 * 1024;

pub(crate) struct DBOptions {
    //
    max_system_batch_size: usize,
    //
}
