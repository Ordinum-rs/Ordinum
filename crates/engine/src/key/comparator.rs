//
use std::cmp::Ordering;
use std::sync::Arc;

/// Comparator uses a base lexicographical comparison by first comparing the user key and then the sequence ordering
/// for the correct sequence ordering to work (desc) we need to use big_endian encoding.
/// This must be adhered to for internal key encoding

pub trait Comparator: Send + Sync {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;
    // TODO: Add separator and successor and other signatures we may need
}

pub(crate) struct DefaultComparator {}

pub(crate) type DefaultComparatorArc = Arc<DefaultComparator>;

impl DefaultComparator {
    pub(crate) fn new() -> DefaultComparatorArc {
        Arc::new(DefaultComparator {})
    }
}

impl Comparator for DefaultComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

pub(crate) struct InternalKeyComparator {}

pub(crate) type InternalKeyComparatorArc = Arc<InternalKeyComparator>;

impl InternalKeyComparator {
    pub(crate) fn new() -> InternalKeyComparatorArc {
        Arc::new(InternalKeyComparator {})
    }
}

impl Comparator for InternalKeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let (a_user, a_trailer) = a.split_at(a.len() - 8);
        let (b_user, b_trailer) = b.split_at(b.len() - 8);

        match a_user.cmp(b_user) {
            Ordering::Equal => {
                // reverse ordering for seq/op
                b_trailer.cmp(a_trailer)
            }
            other => other,
        }
    }
}
