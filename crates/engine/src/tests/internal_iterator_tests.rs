#[cfg(test)]
mod tests {

    use crate::arena::allocator::*;
    use crate::arena::arena::*;
    use crate::iterator::internal_iterator::InternalIterator;
    use crate::key::comparator::InternalKeyComparator;
    use crate::key::internal_key::{InternalKeyRef, OperationType};
    use crate::key::lookup_key::{LookUpInternalKey, LookUpKey};
    use crate::memtable::memtable::*;

    #[test]
    fn memtable_internal_iterator() {
        let mem = Memtable::new(
            0,
            ArenaPolicy {
                block_size: 640,
                cap: 640,
            },
            Allocator::System(SystemAllocator::new()),
            InternalKeyComparator::new(),
        );

        let k1 = LookUpInternalKey::new(b"51.1.User1001", 1, OperationType::Put);
        let k2 = LookUpInternalKey::new(b"51.1.User1001", 2, OperationType::Put);
        let k3 = LookUpInternalKey::new(b"51.1.User1001", 3, OperationType::Put);
        let k4 = LookUpInternalKey::new(b"51.1.User1001", 4, OperationType::Delete);
        let k_other = LookUpInternalKey::new(b"51.1.User1002", 5, OperationType::Put);

        mem.insert(k1.as_ref(), b"value_1");
        mem.insert(k2.as_ref(), b"value_2");
        mem.insert(k3.as_ref(), b"value_3");
        mem.insert(k4.as_ref(), b"");
        mem.insert(k_other.as_ref(), b"value_4");

        fn ik(k: &LookUpInternalKey) -> InternalKeyRef<'_> {
            InternalKeyRef::from(k.as_ref())
        }

        // Full ordering
        {
            let mut iter = mem.iter();
            iter.seek_to_first();

            let expected = [&k4, &k3, &k2, &k1, &k_other];

            for k in expected {
                assert!(iter.valid());
                assert_eq!(iter.internal_key().unwrap(), ik(k));
                iter.next();
            }

            assert!(!iter.valid());
        }

        // Seek exact position
        {
            let mut iter = mem.iter();

            iter.seek(k2.as_ref());
            assert!(iter.valid());
            assert_eq!(iter.internal_key().unwrap(), ik(&k2));
        }

        // Seek lands on first ≥ key
        {
            let mut iter = mem.iter();

            // seq=10 is before all User1001 entries → should land on k4
            iter.seek(LookUpInternalKey::new(b"51.1.User1001", 10, OperationType::Max).as_ref());
            assert_eq!(iter.internal_key().unwrap(), ik(&k4));
        }

        // Seek inside version chain
        {
            let mut iter = mem.iter();

            iter.seek(LookUpInternalKey::new(b"51.1.User1001", 3, OperationType::Max).as_ref());
            assert_eq!(iter.internal_key().unwrap(), ik(&k3));
        }

        // Seek past all versions
        {
            let mut iter = mem.iter();

            // lands AFTER all User1001 → should go to next key
            iter.seek(LookUpInternalKey::new(b"51.1.User1001", 0, OperationType::Max).as_ref());

            assert!(iter.valid());
            assert_eq!(iter.internal_key().unwrap(), ik(&k_other));
        }

        // Cross-key seek
        {
            let mut iter = mem.iter();

            iter.seek(LookUpInternalKey::new(b"51.1.User1002", 100, OperationType::Max).as_ref());
            assert_eq!(iter.internal_key().unwrap(), ik(&k_other));
        }

        // Seek to non-existent key
        {
            let mut iter = mem.iter();

            iter.seek(LookUpInternalKey::new(b"51.1.User1001a", 100, OperationType::Max).as_ref());

            assert!(iter.valid());
            assert_eq!(iter.internal_key().unwrap(), ik(&k_other));
        }
    }
}
