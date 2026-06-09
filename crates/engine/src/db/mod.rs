use crate::utils::var_int::VarInt;

pub(crate) mod batch_pool;
pub(crate) mod db;
pub(crate) mod db_impl;
pub(crate) mod options;
pub(crate) mod read_path;
pub(crate) mod write_batch;

// Try
pub(crate) mod batch;
pub(crate) mod write_pipeline;

// TODO: Move to CF module
pub(crate) const DEFAULT_CF_ID: u32 = 0;
