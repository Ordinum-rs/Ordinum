//
//
//
// InternalKey is made up of a user key, sequence number, and operation type.
// | user_key (var len) | 8-byte trailer |
// Where:
// (trailer: u64) = (seq_no << 8) | value_type
// +-------------------+-------------------+
// | user key bytes    | 8 byte trailer    |
// +-------------------+-------------------+
//
//
// InternalKey represents the encoding of the internal key used in the db. It is made up of a user key, sequence number, and operation type.
// The functions in this file handle trailer encoding and decoding, as well as bit packing/unpacking of the trailer.
//
// The main structs are InternalKeyRef which represents a borrowed decoded view of an InternalKey
//
//

use std::fmt::Display;

use crate::db::batch::BatchRecordKind;

pub(super) const INLINE_IK_SIZE: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub(crate) enum InternalKeyKind {
    Put = 1,
    Delete = 2,
    Merge = 3, // TODO: Implement Merge Operation into the system
    Max = 255,
}

/// Error returned when a trailer contains a kind byte that is not part of the
/// internal-key encoding.
///
/// Internal keys can be decoded from persisted WAL or table data, so an
/// unknown byte must be treated as malformed input rather than as an
/// unreachable program state. The original byte is retained for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidInternalKeyKind {
    raw: u8,
}

impl InvalidInternalKeyKind {
    pub(crate) fn raw(self) -> u8 {
        self.raw
    }
}

impl Display for InvalidInternalKeyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid internal-key kind: {}", self.raw)?;
        Ok(())
    }
}

impl std::error::Error for InvalidInternalKeyKind {}

impl From<InternalKeyKind> for u64 {
    fn from(op: InternalKeyKind) -> Self {
        u64::from(op as u8)
    }
}

impl TryFrom<u8> for InternalKeyKind {
    type Error = InvalidInternalKeyKind;

    fn try_from(raw: u8) -> std::result::Result<Self, Self::Error> {
        match raw {
            1 => Ok(InternalKeyKind::Put),
            2 => Ok(InternalKeyKind::Delete),
            3 => Ok(InternalKeyKind::Merge),
            255 => Ok(InternalKeyKind::Max),
            raw => Err(InvalidInternalKeyKind { raw }),
        }
    }
}

impl Display for InternalKeyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InternalKeyKind::Put => write!(f, "Put"),
            InternalKeyKind::Delete => write!(f, "Delete"),
            InternalKeyKind::Merge => write!(f, "Merge"),
            InternalKeyKind::Max => write!(f, "Max"),
        }
    }
}

impl TryFrom<BatchRecordKind> for InternalKeyKind {
    type Error = BatchRecordKind;

    fn try_from(kind: BatchRecordKind) -> std::result::Result<Self, Self::Error> {
        match kind {
            BatchRecordKind::Put => Ok(Self::Put),
            BatchRecordKind::Delete => Ok(Self::Delete),
            BatchRecordKind::Merge => Ok(Self::Merge),
            BatchRecordKind::RangeDel => Err(BatchRecordKind::RangeDel),
        }
    }
}

// A Pack function to take the seq_no and operation type and pack them into a trailer u64
#[inline(always)]
fn pack_trailer(seq_no: u64, op: InternalKeyKind) -> u64 {
    debug_assert!(seq_no < (1 << 56)); // Enforce that seq_no is less than 2^56
    (seq_no << 8) | u64::from(op)
}

#[inline(always)]
#[must_use = "trailer bytes should be big endian in order to be compared correctly"]
pub(crate) fn encode_trailer(seq_no: u64, op: InternalKeyKind) -> [u8; 8] {
    pack_trailer(seq_no, op).to_be_bytes()
}

#[inline(always)]
fn unpack_trailer_raw(trailer: u64) -> (u64, u8) {
    (trailer >> 8, (trailer & 0xff) as u8)
}

#[inline(always)]
fn unpack_trailer(
    trailer: u64,
) -> std::result::Result<(u64, InternalKeyKind), InvalidInternalKeyKind> {
    let (seq_no, op) = unpack_trailer_raw(trailer);
    Ok((seq_no, InternalKeyKind::try_from(op)?))
}

#[inline(always)]
fn extract_seq_no(trailer: u64) -> u64 {
    trailer >> 8
}

#[inline(always)]
fn extract_op(trailer: u64) -> std::result::Result<InternalKeyKind, InvalidInternalKeyKind> {
    InternalKeyKind::try_from((trailer & 0xff) as u8)
}

#[inline(always)]
fn extract_op_raw(trailer: u64) -> u8 {
    (trailer & 0xff) as u8
}

// TODO: Finish the internal key logic
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) struct InternalKeyRef<'a> {
    pub(crate) user_key: &'a [u8],
    pub(crate) seq_no: u64,
    pub(crate) op: u8,
    // NOTE: Add Trailer instead for lazy decoding
}

impl<'a> From<&'a [u8]> for InternalKeyRef<'a> {
    fn from(key: &'a [u8]) -> Self {
        debug_assert!(key.len() >= 8, "InternalKey must include trailer");

        let (user_key, trailer_bytes) = key.split_at(key.len() - 8);
        let trailer = u64::from_be_bytes(trailer_bytes.try_into().unwrap());

        let (seq_no, op) = unpack_trailer_raw(trailer);

        Self {
            user_key,
            seq_no,
            op,
        }
    }
}

impl<'a> Display for InternalKeyRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = String::from_utf8_lossy(self.user_key);

        match InternalKeyKind::try_from(self.op) {
            Ok(kind) => write!(f, "{}-{}-{}", key, self.seq_no, kind),
            Err(error) => write!(f, "{}-{}-Invalid({})", key, self.seq_no, error.raw()),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn encode_trailer_works() {
        let trailer_1 = encode_trailer(12345 as u64, InternalKeyKind::Put);
        let trailer_2 = encode_trailer(12346 as u64, InternalKeyKind::Put);

        assert!(
            trailer_2 > trailer_1,
            "trailer_2 should be greater than trailer_1"
        );
    }

    #[test]
    fn point_batch_record_kinds_map_to_internal_key_kinds() {
        assert_eq!(
            InternalKeyKind::try_from(BatchRecordKind::Put),
            Ok(InternalKeyKind::Put)
        );
        assert_eq!(
            InternalKeyKind::try_from(BatchRecordKind::Delete),
            Ok(InternalKeyKind::Delete)
        );
        assert_eq!(
            InternalKeyKind::try_from(BatchRecordKind::Merge),
            Ok(InternalKeyKind::Merge)
        );
    }

    #[test]
    fn range_delete_does_not_map_to_point_internal_key_kind() {
        assert_eq!(
            InternalKeyKind::try_from(BatchRecordKind::RangeDel),
            Err(BatchRecordKind::RangeDel)
        );
    }

    #[test]
    fn invalid_internal_key_kind_preserves_raw_byte() {
        let error = InternalKeyKind::try_from(42).unwrap_err();

        assert_eq!(error.raw(), 42);
        assert_eq!(error.to_string(), "invalid internal-key kind: 42");
    }

    #[test]
    fn internal_key_display_reports_invalid_kind() {
        let key = InternalKeyRef {
            user_key: b"key",
            seq_no: 7,
            op: 42,
        };

        assert_eq!(key.to_string(), "key-7-Invalid(42)");
    }
}
