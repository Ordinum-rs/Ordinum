pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),

    Shutdown,

    InvalidBatch,

    Corruption(String),

    ColumnFamilyNotFound(u32),

    WalError,
}
