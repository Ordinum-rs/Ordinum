use crate::db::batch::BatchRuntimeState;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),

    Shutdown,

    Corruption(String),

    ColumnFamilyNotFound(u32),

    WalError,
}
