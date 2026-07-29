//
//
//

const MSB: u8 = 0x80;
const LOW_7_BITS: u32 = 0x7F;
const SHIFT_7_BITS: u32 = 7;

#[derive(Debug)]
pub(crate) struct VarInt {
    buf: [u8; Self::MAX_VARINT],
    len: u8,
}

impl VarInt {
    /// Maximum number of bytes required to encode a `u32` using seven payload
    /// bits per byte.
    pub(crate) const MAX_VARINT: usize = 5;

    pub(crate) fn new(value: u32) -> Self {
        let mut buf = [0u8; Self::MAX_VARINT];
        let mut v = value;
        let mut i = 0;

        while v > 127 {
            buf[i] = (v & LOW_7_BITS) as u8 | MSB;
            v >>= SHIFT_7_BITS;
            i += 1;
        }
        buf[i] = v as u8;

        Self {
            buf,
            len: (i + 1) as u8,
        }
    }

    pub(crate) fn decode(buf: &[u8]) -> (u32, usize) {
        let mut result: u32 = 0;
        let mut shift = 0;
        let mut bytes_read = 0;

        for byte in buf {
            bytes_read += 1;
            result |= ((*byte & 0x7F) as u32) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }

        (result, bytes_read)
    }

    pub(crate) fn size(&self) -> usize {
        self.len as usize
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.buf[..self.size()]
    }
}

// TODO: Make var string next

#[test]
fn want() {
    let value = 3;
    let result = VarInt::new(value);
    assert_eq!(result.as_slice().len(), 1);

    let value_2 = 257;
    let result_2 = VarInt::new(value_2);
    assert_eq!(result_2.as_slice().len(), 2);

    let value_3 = 3000000;
    let result_3 = VarInt::new(value_3);

    assert_eq!(result_3.as_slice().len(), 4);
    assert_eq!(VarInt::decode(result_3.as_slice()), (3000000, 4));
}

#[test]
fn size() {
    let value = 5;
    let varint = VarInt::new(value);

    assert_eq!(varint.size(), 1);

    let big_value = 3000000;
    let varint_big = VarInt::new(big_value);

    assert_eq!(varint_big.size(), 4);
}

#[test]
fn u32_max_uses_five_bytes_and_round_trips() {
    let varint = VarInt::new(u32::MAX);

    assert_eq!(varint.as_slice(), &[0xff, 0xff, 0xff, 0xff, 0x0f]);
    assert_eq!(varint.size(), VarInt::MAX_VARINT);
    assert_eq!(
        VarInt::decode(varint.as_slice()),
        (u32::MAX, VarInt::MAX_VARINT)
    );
}

#[test]
fn varint_size_boundaries_round_trip() {
    let cases = [
        (0, 1),
        (127, 1),
        (128, 2),
        (16_383, 2),
        (16_384, 3),
        (2_097_151, 3),
        (2_097_152, 4),
        (268_435_455, 4),
        (268_435_456, 5),
    ];

    for (value, expected_size) in cases {
        let varint = VarInt::new(value);

        assert_eq!(varint.size(), expected_size);
        assert_eq!(VarInt::decode(varint.as_slice()), (value, expected_size));
    }
}
