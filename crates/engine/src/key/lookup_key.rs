use super::MAX_KEY_SIZE;
use super::InternalKeyKind;
use super::inner_key::InnerKey;
use super::internal_key::INLINE_IK_SIZE;
use super::internal_key::encode_trailer;

//
//
//
// LookupKey
pub(crate) type LookUpInternalKey = LookUpKey<INLINE_IK_SIZE>;

pub(crate) struct LookUpKey<const N: usize> {
    _inner: InnerKey<N>,
    buffer: Option<Box<[u8]>>,
}

impl<const N: usize> LookUpKey<N> {
    //
    pub(crate) fn new(user_key: &[u8], seq_no: u64, op: InternalKeyKind) -> Self {
        //
        let total = user_key.len() + 8;
        debug_assert!(total <= MAX_KEY_SIZE);

        if total <= N {
            let mut inner = InnerKey::new();
            inner.encode_inline(user_key, seq_no, op);
            return Self {
                _inner: inner,
                buffer: None,
            };
        }
        // Allocate the key
        let mut buffer = Vec::with_capacity(total);
        buffer.extend_from_slice(user_key);
        buffer.extend_from_slice(&encode_trailer(seq_no, op));

        let mut buffer = buffer.into_boxed_slice();

        let mut inner = InnerKey::new();
        inner.set_external(total, buffer.as_mut_ptr());

        Self {
            _inner: inner,
            buffer: Some(buffer),
        }
    }
}

impl<const N: usize> AsRef<[u8]> for LookUpKey<N> {
    fn as_ref(&self) -> &[u8] {
        self._inner.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::super::internal_key::InternalKeyRef;
    use super::*;

    #[test]
    fn lookup_key_size() {
        let user_key = "User".to_string().into_bytes();
        let seq_no = 12345 as u64;
        let op_type = InternalKeyKind::Put;

        let lookup_key: LookUpInternalKey = LookUpKey::new(&user_key, seq_no, op_type);
        assert_eq!(std::mem::size_of::<LookUpKey<INLINE_IK_SIZE>>(), 56);
    }

    #[test]
    fn lookup_key_works() {
        //
        let user_key = "ReallyLongKeyWhichShouldActuallyHeapAllocateBecauseItIsLongerThan200BytesIHopeThatNobodyAbsolutelyAnihiliatesMyDatabaseWithTheseBecauseThatsNotVeryNiceAndItMakesMeHaveToWorkHardOnMemoryAndMyBrainStrugglesWithMemoryAlready".to_string().into_bytes();
        let inline_key = "User".to_string().into_bytes();
        let seq_no = 12345 as u64;
        let op_type = InternalKeyKind::Put;

        let lk: LookUpInternalKey = LookUpKey::new(&user_key, seq_no, op_type);
        let lk_inline: LookUpInternalKey = LookUpKey::new(&inline_key, seq_no, op_type);

        // Test we get back what we want

        let ik = InternalKeyRef::from(lk_inline.as_ref());
        assert_eq!(ik.user_key, inline_key.as_slice());
        assert_eq!(ik.seq_no, seq_no);
        assert_eq!(ik.op, op_type as u8);

        let ik_external = InternalKeyRef::from(lk.as_ref());
        assert_eq!(ik_external.user_key, user_key.as_slice());
        assert_eq!(ik_external.seq_no, seq_no);
        assert_eq!(ik_external.op, op_type as u8);
    }
}
