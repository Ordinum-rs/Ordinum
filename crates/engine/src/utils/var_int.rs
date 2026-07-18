//
//
//

use std::ops::Deref;

const MSB: u8 = 0x80;
const LOW_7_BITS: u32 = 0x7F;
const SHIFT_7_BITS: u32 = 7;

#[derive(Debug)]
pub(crate) enum VarInt {
    One([u8; 1]),
    Two([u8; 2]),
    Three([u8; 3]),
    Four([u8; 4]),
}

impl VarInt {
    pub(crate) const MAX_VARINT: usize = 4;

    pub(crate) fn new(value: u32) -> Self {
        let mut buf = [0u8; 4];
        let mut v = value;

        let mut i = 0;

        while v > 127 {
            buf[i] = (v & LOW_7_BITS) as u8 | MSB;
            v >>= SHIFT_7_BITS;
            i += 1;
        }
        buf[i] = v as u8;

        match i + 1 {
            1 => Self::One([buf[0]]),
            2 => Self::Two([buf[0], buf[1]]),
            3 => Self::Three([buf[0], buf[1], buf[2]]),
            _ => Self::Four([buf[0], buf[1], buf[2], buf[3]]),
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

    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::One(buf) => buf.as_ref(),
            Self::Two(buf) => buf.as_ref(),
            Self::Three(buf) => buf.as_ref(),
            Self::Four(buf) => buf.as_ref(),
        }
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
