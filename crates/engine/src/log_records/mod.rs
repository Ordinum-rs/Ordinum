//
// This contains the binary records format for logs used by MANIFEST and WAL
//
//
//

// Link WAL RocksDB: https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-(WAL)
// Link Record Pebble: https://github.com/cockroachdb/pebble/blob/master/record/record.go#L51
//
// Legacy Format:
//
//	+----------+-----------+-----------+--- ... ---+
//	| CRC (4B) | Size (2B) | Type (1B) | Payload   |
//	+----------+-----------+-----------+--- ... ---+

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
//
//  Sync format:
//
//	+----------+-----------+-----------+----------------+------------------+--- ... ---+
//	| CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Sync Offset (8B) | Payload   |
//	+----------+-----------+-----------+----------------+------------------+--- ... ---+
//
// The WAL sync chunk format allows for detection of data corruption in some
// circumstances. The WAL sync format extends the recyclable header with an
// additional offset field. This allows "reading ahead" to be done in order to
// decipher whether an invalid or zeroed chunk was an artifact of corruption or the
// logical end of the log. SyncOffset is a promise that the log should have been
// synced up until the offset. A promised synced offset is needed because cloud
// providers  may write blocks out of order, rendering "read aheads" scanning for
// logNum inaccurate.

use std::{array, mem::MaybeUninit};

const BlockSize: usize = 32 * 1024;
const BlockSizeMask: usize = BlockSize - 1;
const LegacyHeaderSize: usize = 7;
const RecyclableHeaderSize: usize = LegacyHeaderSize + 4;
const WALSyncRecyclableHeaderSize: usize = RecyclableHeaderSize + 8;

const InvalidChunkEncoding: usize = 0;

const FullChunkEncoding: usize = 1;
const FirstChunkEncoding: usize = 2;
const MiddleChunkEncoding: usize = 3;
const LastChunkEncoding: usize = 4;

const RecyclableFullChunkEncoding: usize = 5;
const RecyclableFirstChunkEncoding: usize = 6;
const RecyclableMiddleChunkEncoding: usize = 7;
const RecyclableLastChunkEncoding: usize = 8;

const WALSyncFullChunkEncoding: usize = 9;
const WALSyncFirstChunkEncoding: usize = 10;
const WALSyncMiddleChunkEncoding: usize = 11;
const WALSyncLastChunkEncoding: usize = 12;

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum ChunkPosition {
    InvalidType = 0,
    FullType,
    FirstType,
    MiddleType,
    LastType,
}

// Impl

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum WireFormat {
    InvalidFormat = 0,
    Legacy,
    Recyclable,
    WALSync,
}

// Impl

// ----

#[derive(Debug, Clone, Copy)]
struct HeaderFormat {
    chunk_position: ChunkPosition,
    wire_format: WireFormat,
    header_size: usize,
}

impl HeaderFormat {
    const fn new(
        chunk_position: ChunkPosition,
        wire_format: WireFormat,
        header_size: usize,
    ) -> Self {
        Self {
            chunk_position,
            wire_format,
            header_size,
        }
    }
}

const HEADER_FORMAT_MAP_ARRAY_SIZE: usize = 13;

const INVALID_HEADER_FORMAT: HeaderFormat =
    HeaderFormat::new(ChunkPosition::InvalidType, WireFormat::InvalidFormat, 0);

const fn make_header_format_map_array() -> [HeaderFormat; HEADER_FORMAT_MAP_ARRAY_SIZE] {
    let mut formats: [HeaderFormat; HEADER_FORMAT_MAP_ARRAY_SIZE] =
        [INVALID_HEADER_FORMAT; HEADER_FORMAT_MAP_ARRAY_SIZE];

    // Legacy Mappings

    formats[FullChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::FullType,
        wire_format: WireFormat::Legacy,
        header_size: LegacyHeaderSize,
    };

    formats[FirstChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::FirstType,
        wire_format: WireFormat::Legacy,
        header_size: LegacyHeaderSize,
    };

    formats[MiddleChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::MiddleType,
        wire_format: WireFormat::Legacy,
        header_size: LegacyHeaderSize,
    };

    formats[LastChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::LastType,
        wire_format: WireFormat::Legacy,
        header_size: LegacyHeaderSize,
    };

    // Recyclable Mappings

    formats[RecyclableFullChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::FullType,
        wire_format: WireFormat::Recyclable,
        header_size: RecyclableHeaderSize,
    };

    formats[RecyclableFirstChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::FirstType,
        wire_format: WireFormat::Recyclable,
        header_size: RecyclableHeaderSize,
    };

    formats[RecyclableMiddleChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::MiddleType,
        wire_format: WireFormat::Recyclable,
        header_size: RecyclableHeaderSize,
    };

    formats[RecyclableLastChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::LastType,
        wire_format: WireFormat::Recyclable,
        header_size: RecyclableHeaderSize,
    };

    // WALSync Mappings

    formats[WALSyncFullChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::FullType,
        wire_format: WireFormat::WALSync,
        header_size: WALSyncRecyclableHeaderSize,
    };

    formats[WALSyncFirstChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::FirstType,
        wire_format: WireFormat::WALSync,
        header_size: WALSyncRecyclableHeaderSize,
    };

    formats[WALSyncMiddleChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::MiddleType,
        wire_format: WireFormat::WALSync,
        header_size: WALSyncRecyclableHeaderSize,
    };

    formats[WALSyncLastChunkEncoding] = HeaderFormat {
        chunk_position: ChunkPosition::LastType,
        wire_format: WireFormat::WALSync,
        header_size: WALSyncRecyclableHeaderSize,
    };

    formats
}

const HEADER_FORMAT_MAP: [HeaderFormat; HEADER_FORMAT_MAP_ARRAY_SIZE] =
    unsafe { make_header_format_map_array() };
