use crate::db::batch::BatchRuntimeState;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),

    Shutdown,

    Corruption(String),

    ColumnFamilyNotFound(u32),

    WalError,

    KeyTooLarge {
        size: usize,
        max: usize,
    },

    ValueTooLarge {
        size: usize,
        max: usize,
    },

    RecordTooLarge {
        encoded_size: usize,
        max_batch_size: usize,
    },

    BatchFull {
        record_size: usize,
        current_size: usize,
        max_batch_size: usize,
    },
}
