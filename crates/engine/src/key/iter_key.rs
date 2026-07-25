use super::INITIAL_KEY_BUFFER_CAP;
use super::MAX_KEY_SIZE;
use super::InternalKeyKind;
use super::encode_into;
use super::inner_key::ITER_INLINE;
use super::inner_key::InnerKey;

// Iter Key - owned by an iterator and re-used across iterations

pub(crate) type InternalIterKey = IterKey<ITER_INLINE>;

pub(crate) struct IterKey<const N: usize> {
    _inner: InnerKey<N>,
    heap: Vec<u8>,
}

impl<const N: usize> IterKey<N> {
    pub(crate) fn new() -> Self {
        Self {
            _inner: InnerKey::new(),
            heap: Vec::with_capacity(INITIAL_KEY_BUFFER_CAP),
        }
    }

    pub(crate) fn set(&mut self, user_key: &[u8], seq_no: u64, op: InternalKeyKind) {
        let total = user_key.len() + 8;
        debug_assert!(total <= MAX_KEY_SIZE);

        if total <= N {
            // Can inline
            self._inner.encode_inline(user_key, seq_no, op);
            return;
        }

        // Use heap

        self.heap.clear();
        if self.heap.capacity() < total {
            self.heap.reserve(total - self.heap.capacity());
        }

        // Safety
        //
        // We have reserved enough capacity in the heap to hold the full key + trailer,
        // and set_len(total) ensures the heap's length matches the total size.
        unsafe {
            self.heap.set_len(total);
        }

        self._inner.set_external(total, self.heap.as_mut_ptr());
        encode_into(&mut self.heap[..total], user_key, seq_no, op);
    }

    pub(crate) fn set_from_slice(&mut self, slice: &[u8]) {
        let len = slice.len();

        if len <= N {
            self._inner.encode_inline_from_slice(slice);
            return;
        }

        self.heap.clear();
        if self.heap.capacity() < len {
            self.heap.reserve(len - self.heap.capacity());
        }

        unsafe {
            self.heap.set_len(len);
        }

        self._inner.set_external(len, self.heap.as_mut_ptr());
        self.heap[..len].copy_from_slice(slice);
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        self._inner.as_slice()
    }
}

impl<const N: usize> AsRef<[u8]> for IterKey<N> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::super::internal_key::InternalKeyRef;
    use super::*;

    #[test]
    fn iter_key_set() {
        let mut iter_key = InternalIterKey::new();

        let user_key = b"User";
        let seq_no = 1;
        let op = InternalKeyKind::Put;

        iter_key.set(user_key, seq_no, op);

        let ik = InternalKeyRef::from(iter_key.as_ref());

        assert_eq!(ik.user_key, user_key);
        assert_eq!(ik.seq_no, seq_no);
        assert_eq!(ik.op, op as u8);
    }

    #[test]
    fn iter_key_reuse() {
        let mut iter_key = InternalIterKey::new();

        let user_key = b"Large-User-Key-Which-Should-Cause-A-Heap-Allocation";
        let user_key_2 = b"Another-Large-User-Key-Which-Should-Cause-A-Heap-Allocation";

        //

        iter_key.set(user_key, 1, InternalKeyKind::Put);

        let ptr = iter_key.as_slice().as_ptr();

        iter_key.set(user_key_2, 2, InternalKeyKind::Put);

        let ptr_2 = iter_key.as_slice().as_ptr();

        assert_eq!(ptr, ptr_2);
    }
}
