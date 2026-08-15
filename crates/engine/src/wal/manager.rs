// WAL Manager
// Central interface that handles WAL Lifecycle operations including: WAL Creation, Recycling, Obsolescence tracking
//
// Link WAL RocksDB: https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-(WAL)
//
// Legacy Format:
//
// +----------+-----------+-----------+----------------+--- ... ---+
// | CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
// +----------+-----------+-----------+----------------+--- ... ---+
// CRC = 32bit hash computed over the payload using CRC
// Size = Length of the payload data
// Type = Type of record
//        (kZeroType, kFullType, kFirstType, kLastType, kMiddleType )
//        The type is used to group a bunch of records together to represent
//        blocks that are larger than kBlockSize
// Payload = Byte stream as long as specified by the payload size
// Log number = 32bit log file number, so that we can distinguish between
// records written by the most recent log writer vs a previous one.
//
// Recylcable format:
//
// +---------+-----------+-----------+----------------+--- ... ---+
// |CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
// +---------+-----------+-----------+----------------+--- ... ---+
// Same as above, with the addition of
// Log number = 32bit log file number, so that we can distinguish between
// records written by the most recent log writer vs a previous one.

// A WAL is created when
// 1.   a new DB is opened,
// 2.   a column family is flushed.

// WAL files are generated with increasing sequence number in the WAL directory.
// In order to reconstruct the state of the database, these files are read in the order of sequence number.
// WAL manager provides the abstraction for reading the WAL files as a single unit.
// Internally, it opens and reads the files using Reader or Writer abstraction.
//
// NOTE: Writes and Reads are handled by a log::Reader and log::Writer

pub(crate) trait WALManager {
    //
    //
    fn init(/* */);
    //
    fn create(/* */);
    //
}
