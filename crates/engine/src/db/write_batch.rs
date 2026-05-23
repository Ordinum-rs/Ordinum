use std::{
    fmt::{self, write},
    ops::Deref,
    ptr,
};

use crate::utils::{self, var_int::VarInt};

//
//
// NOTE: Do we want two queues? One for data commit and one for WAL commit?
//
// Batches use a compact binary representation where all operations are encoded sequentially into a byte slice
// the binary representation is so that batches can form the records of the WAL without any additional changes
// We are free to choose the endianness and for optimisation on x86 architectures we choose little endian here.
// This applies only to the structure of the batch i.e batch count, varint and column_family_id. For the internal key, we defer to the endianness it uses which is
// big endian for seq number comparison
//
// Batch:
// | --------- 12 byte header ----------|--------- Operations ---------|
// | Seq No (8 bytes) | Count (4 bytes) | Operation 1 ... Operation 2...
//
//
// Operation:
// | op_type (1 byte) | cf_if (4 bytes) | key_len (VarInt) | key ... | value_len (VarInt) | value ... |
//
//
// A batch holds a set of operations to be committed atomically as part of the write path.
// Each operation is binary encoded and appended to a contiguous Vec<u8> buffer.
// The buffer begins with a 12-byte header:
//   - 8 bytes: starting sequence number (assigned at commit time)
//   - 4 bytes: operation count
//
// Batches are created both implicitly (e.g. DB::put) and explicitly by users.
// A single DB::put() creates a batch containing one operation, allowing the
// write path to uniformly operate on batches regardless of origin.
//
// Example (Pseudo code):
//
// DB::put("key1", "value1");
//
// // Internally:
//
// fn put(&self, key: &[u8], value: &[u8]) {
//     let mut batch = Batch::new();
//     batch.put(DEFAULT_CF, key, value);
//     self.write(batch);
// }
//
// // Later in the write path:
//
// fn write(&self, mut batch: Batch) {
//     let base_seq = self.seq.fetch_add(batch.count() as u64);
//     batch.set_seq_and_count(base_seq);
//
//     // WAL write
//     wal.write(&batch.data);
//
//     // Apply to memtable
//     let mut seq = base_seq;
//     for rec in batch.iter() {
//         mem.insert(rec.key, seq, rec.kind, rec.value);
//         seq += 1;
//     }
//
//     self.visible_seq.store(seq - 1);
// }
//
//
// Due to the fact that batches are loaded into a writer-queue where they are grouped and then committed, they are cross-threaded so pooling batch
// memory becomes difficult as we must maintain shared ownership of the batch data across threads
// For example: we may apply batch enqueueing it to the commit pipeline and before returning it to the pool for re-use we must ensure no threads are still using it
//
// --------------------------------------------------------------------------------------
// The full outline of the commit pipeline operation is as follows:
//
//	with commitPipeline mutex locked:
//	  assign batch sequence number
//	  write batch to WAL
//	(optionally) add batch to WAL sync list
//	apply batch to memtable (concurrently)
//	wait for earlier batches to apply
//	ratchet read sequence number
//	(optionally) wait for the WAL to sync
//
// As soon as a batch has been written to the WAL, the commitPipeline mutex is
// released allowing another batch to write to the WAL. Each commit operation
// individually applies its batch to the memtable providing concurrency. The
// WAL sync happens concurrently with applying to the memtable
// --------------------------------------------------------------------------------------
//
//
//

const SEQ_NO_OFFSET: usize = 0; // seq starts at byte 0
const BATCH_COUNT_OFFSET: usize = size_of::<u64>(); // count starts at byte 8
const HEADER_SIZE: usize = size_of::<u64>() + size_of::<u32>(); // = 12

#[repr(align(8))]
#[derive(Debug)]
pub(crate) enum BatchOpType {
    Put = 1,
    Delete = 2,
    Merge = 3,
    // XXXX:
}

impl fmt::Display for BatchOpType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Put => {
                write!(f, "Put")
            }
            Self::Delete => {
                write!(f, "Delete")
            }
            Self::Merge => {
                write!(f, "Merge")
            }
        }
    }
}

impl BatchOpType {
    pub(crate) fn into(self) -> u8 {
        match self {
            Self::Put => 1,
            Self::Delete => 2,
            Self::Merge => 3,
        }
    }
}

pub(crate) struct WBatch {
    data: Vec<u8>,
    // content_flags
    // protection_info
    // save_points
    // wal_term_point
    // max_bytes
    max_bytes: usize,
}

// A record in a batch will have an operation type and a column family ID followed by varstring key and value.
//
// Get_op()
// PutCF()
// DeleteCF()
//

//

impl WBatch {
    //
    // As a default, a batch is initialised with 1KB (taken from Pebble - https://github.com/cockroachdb/pebble/blob/a3b8dfe9/batch.go#L38)
    pub(crate) const DEFAULT_BATCH_INIT_SIZE: usize = 1 << 10; // NOTE: This is where we'd like to get to if we pool batches
    //
    pub(crate) const MAX_BATCH_SIZE: usize = 1 << 20;
    //
    pub(crate) const SINGLE_BATCH_INIT_SIZE: usize = 1 << 8; // NOTE: For now we start small (cache line) and grow if needed as we allocate on each batch for now
    //
    //
    //
    /// Batch::new() is used to explicitly create a new batch for multiple operations. If a single operation is needed then rely on regular call to DB instead
    /// as DB will internally create a single operation batch with an optimal buffer size.
    /// Explicit calls to Batch::new() will create a larger initial buffer to account for multiple operations
    ///
    /// Example:
    ///
    /// ```
    /// let batch = Batch::new();
    /// batch.put("key", "");
    /// batch.put("key2", "");
    /// // ...
    ///
    /// batch.write();
    ///
    /// ```
    pub(crate) fn new() -> Self {
        let mut data = Vec::with_capacity(Self::DEFAULT_BATCH_INIT_SIZE);
        data.extend_from_slice(&[0u8; HEADER_SIZE]);
        Self {
            data,
            max_bytes: Self::MAX_BATCH_SIZE,
        }
    }

    pub(crate) fn new_with_capacity(cap: usize) -> Self {
        // NOTE: This, I don't like. Would like to limit big batches and maybe ensure the caller
        // knows that using max batches will encur direct flushable memtables
        assert!(cap <= Self::MAX_BATCH_SIZE);
        let mut data = Vec::with_capacity(cap);
        data.extend_from_slice(&[0u8; HEADER_SIZE]);
        Self {
            data,
            max_bytes: Self::MAX_BATCH_SIZE,
        }
    }

    // Put uses the default column family (DEFAULT_CF)
    pub(crate) fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        // NOTE: What work can we do here before calling put_bytes?
        // value send off to blob file write the pointer bytes?
        self.put_bytes(key.as_ref(), value.as_ref())
    }

    pub(crate) fn put_bytes(&mut self, key: &[u8], value: &[u8]) {
        // Write to batch buffer
        self.data.push(BatchOpType::Put.into());
        self.data.extend_from_slice(&0u32.to_le_bytes());
        self.data
            .extend_from_slice(VarInt::new(key.len() as u32).as_slice());
        self.data.extend_from_slice(key);
        self.data
            .extend_from_slice(VarInt::new(value.len() as u32).as_slice());
        self.data.extend_from_slice(value);

        // Increment count

        let count_slice = &mut self.data[BATCH_COUNT_OFFSET..BATCH_COUNT_OFFSET + 4];

        unsafe {
            utils::write_u32_le_unsafe(
                count_slice.as_mut_ptr(),
                utils::read_u32_le_unsafe(count_slice.as_ptr()) + 1,
            )
        }
    }

    pub(crate) fn batch_count(&self) -> u32 {
        unsafe {
            utils::read_u32_le_unsafe(
                self.data[BATCH_COUNT_OFFSET..BATCH_COUNT_OFFSET + 4].as_ptr(),
            )
        }
    }

    pub(crate) fn batch_size(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    // TOOD: Apply_batch()

    pub(crate) fn apply_batch(&self /*column family resolver, seq_no, flush_scheduler */) {}

    //
    // TODO: Batch Iterator

    // NOTE: Can we defer creation until commit and then build the vec?
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn batch_op_typ() {
        println!("{}", BatchOpType::Put.into());
    }

    #[test]
    fn input_test() {
        let word = "word";
        let mut batch = WBatch::new();

        batch.put(word, "");

        assert_eq!(batch.batch_count(), 1);

        // DB::put(word, value: "");
    }
}
