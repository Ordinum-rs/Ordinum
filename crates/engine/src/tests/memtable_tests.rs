#[cfg(test)]
mod tests {

    use crate::iterator::internal_iterator::InternalIterator;
    use crate::key::comparator::InternalKeyComparator;
    use crate::key::internal_key::OperationType;
    use crate::key::lookup_key::{LookUpInternalKey, LookUpKey};

    use crate::arena::allocator::*;
    use crate::arena::arena::*;
    use crate::memtable::memtable::*;

    #[test]
    fn memtable_basic_insert_and_get() {
        let mem = Memtable::new(
            0,
            ArenaPolicy {
                block_size: 1024,
                cap: 1024,
            },
            Allocator::System(SystemAllocator::new()),
            InternalKeyComparator::new(),
        );

        // Put a few keys in the memtable

        let k_1: LookUpInternalKey = LookUpKey::new(b"51.1.User1001", 1, OperationType::Put);
        let k_2: LookUpInternalKey = LookUpKey::new(b"51.1.User1001", 2, OperationType::Put);
        let k_3: LookUpInternalKey = LookUpKey::new(b"51.1.User1001", 3, OperationType::Put);
        let k_4: LookUpInternalKey = LookUpKey::new(b"51.1.User1001", 4, OperationType::Delete);
        let k_wrong: LookUpInternalKey = LookUpKey::new(b"51.1.User1002", 5, OperationType::Put);

        mem.insert(k_1.as_ref(), b"value_1");
        mem.insert(k_2.as_ref(), b"value_2");
        mem.insert(k_3.as_ref(), b"value_3");
        mem.insert(k_4.as_ref(), b"");
        mem.insert(k_wrong.as_ref(), b"value_4");

        // Get the value for most recent seq no of 5
        let search_key: LookUpInternalKey = LookUpKey::new(b"51.1.User1001", 8, OperationType::Max);
        let result = mem.get(search_key.as_ref());
        assert!(matches!(result, MemReturn::Deleted));

        // Get the value for snapshot seq no of 3
        let search_key: LookUpInternalKey = LookUpKey::new(b"51.1.User1001", 3, OperationType::Max);
        let result = mem.get(search_key.as_ref());
        assert!(matches!(result, MemReturn::Value(b"value_3")));
    }

    #[test]
    fn memtable_concurrent_writers_same_key() {
        use std::sync::Arc;
        use std::thread;

        const THREADS: u64 = 8;
        const OPS_PER_THREAD: u64 = 100;
        const MAX_SEQ: u64 = (1 << 56) - 1;

        let mem = Arc::new(Memtable::new(
            0,
            ArenaPolicy {
                block_size: 1024 * 64,
                cap: 1024 * 1024,
            },
            Allocator::System(SystemAllocator::new()),
            InternalKeyComparator::new(),
        ));

        thread::scope(|scope| {
            for t in 0..THREADS {
                let mem = mem.clone();

                scope.spawn(move || {
                    for i in 0..OPS_PER_THREAD {
                        let seq = (t * OPS_PER_THREAD) + i + 1;

                        let key = LookUpInternalKey::new(b"51.1.User1001", seq, OperationType::Put);

                        let value = format!("value_{seq}");

                        mem.insert(key.as_ref(), value.as_bytes());
                    }
                });
            }
        });

        //
        // 1. Walk the whole skiplist.
        //    This tells us if nodes vanished or ordering is broken.
        //
        let mut iter = mem.iter();
        iter.seek_to_first();

        let expected_total = THREADS * OPS_PER_THREAD;

        let mut count = 0;
        let mut previous_seq = u64::MAX;

        while iter.valid() {
            let internal = iter.internal_key().unwrap();

            // Must be strictly descending for same user key
            assert!(
                internal.seq_no < previous_seq,
                "skiplist ordering corrupted: {} came after {}",
                internal.seq_no,
                previous_seq,
            );

            previous_seq = internal.seq_no;

            count += 1;

            iter.next();
        }

        // No lost nodes
        assert_eq!(count, expected_total, "lost nodes during concurrent insert",);

        //
        // 2. Latest version must be highest sequence.
        //
        let search_key = LookUpInternalKey::new(b"51.1.User1001", MAX_SEQ, OperationType::Max);

        let result = mem.get(search_key.as_ref());

        let expected_latest = format!("value_{expected_total}");

        assert!(
            matches!(
                result,
                MemReturn::Value(v) if v == expected_latest.as_bytes()
            ),
            "latest version lookup returned wrong value",
        );

        //
        // 3. Snapshot lookup in middle of version chain.
        //
        let snapshot_seq = 250;

        let search_key = LookUpInternalKey::new(b"51.1.User1001", snapshot_seq, OperationType::Max);

        let result = mem.get(search_key.as_ref());

        let expected_snapshot = format!("value_{snapshot_seq}");

        assert!(
            matches!(
                result,
                MemReturn::Value(v) if v == expected_snapshot.as_bytes()
            ),
            "snapshot lookup returned wrong value",
        );
    }

    #[test]
    fn memtable_memory_usage() {

        // Test filling up a memtable and checking chunk usage is working
    }
}
