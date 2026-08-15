// WAL is the main Write Ahead Log process for handling individual records/blocks

pub(super) struct WAL {
    // DirLock ... https://github.com/cockroachdb/pebble/blob/master/internal/base/directory_lock.go
    //
}

// https://github.com/cockroachdb/pebble/blob/master/internal/base/directory_lock.go

// Key points to understand:
//
// 1. Obselete Virtual WAL's:
//      We will need to handle instances where two column families have records in two virtual WAL's where
//      One WAL has been flushed/rotated but we need to archive it because it still refers to active column family state
//      So how do we determine obselete Virtual WAL's?
//
//      A WAL is deleted (or archived if archival is enabled) when all column families have flushed beyond the largest sequence number
//      contained in the WAL, or in other words, all data in the WAL have been persisted to SST files.
//      Archived WALs will be moved to a separate location and purged from disk later on.
//      The actual deletion might be delayed due to replication purposes, see Transaction Log Iterator section below.
//
// 2. Jobs and JobID's:
//      What uses Jobs and how does the concurrency part of the WAL work?
//
// 3. Readers and Writers:
//      We need readers and writers and we need to track in-flight references, so how should we do this and
//      at what level do we declare reader/writers?
//
//      Writer writes to a virtual WAL. A Writer in standalone mode maps to a
//      single record.LogWriter.
//
//      So i imagine we'll have a WAL object/struct be either bound by a LogWriter or add_record() must take a LogWriter which can
//      carry out the writing
//
// 4. Records, lifetime, and memory ownership:
//
//      A logical WAL record is the complete encoded batch, not one operation
//      within that batch. Batch::data already contains the batch header followed
//      by all encoded Put/Delete/Merge/etc. operations. The write pipeline should
//      therefore pass that contiguous byte slice to WriteRecord once. Iterating
//      individual operations is required when applying or recovering a batch,
//      but not when initially writing the batch to the WAL.
//
//      The LogWriter converts the logical record into one or more physical WAL
//      fragments. Each fragment has its own framing/checksum and is copied into
//      WAL-owned block buffers before being written. A logical batch may span
//      several physical blocks without changing the batch's encoded format.
//
//      Pebble's higher-level Writer contract permits some implementations to
//      retain the input byte slice after WriteRecord returns. Its RefCount pins
//      the backing batch allocation until the writer has finished reading it;
//      it does not count individual batch operations and does not permit the
//      writer to mutate the input. Pebble's standalone record.LogWriter instead
//      copies the input into its internal block buffers before returning.
//
//      Our initial Rust API should use the simpler synchronous-consumption
//      contract:
//
//          write_record(record: &[u8], options: SyncOptions) -> Result<LogicalOffset>
//
//      The implementation must finish copying `record` into WAL-owned storage
//      before returning, so the borrow cannot escape and no input reference count
//      is needed. The batch may be reset or returned to its pool only after all
//      other pipeline users have also finished with it.
//
//      If a future asynchronous or failover writer needs to retain the original
//      bytes after returning, `&[u8]` is no longer a sufficient API. It must take
//      an owned record, Arc-backed bytes, or an explicit batch lease that prevents
//      mutation, buffer reallocation, and pool reuse until that lease is dropped.
//
//      Keep these completion points distinct:
//
//      - consumed: the writer no longer reads the caller's batch bytes;
//      - written: the framed WAL blocks have been written/submitted;
//      - durable: the requested fsync has completed.
//
//      A sync waiter normally tracks durability. It does not by itself prove
//      that the caller's input buffer is no longer borrowed unless the writer's
//      ownership contract explicitly couples those two events.
