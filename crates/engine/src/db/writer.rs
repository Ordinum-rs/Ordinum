// Writer is the main implementor of write operations
// It takes a batch of operations from the caller and inserts into the write_thread
// If it becomes the leader it then proceeds to carry out the sequential write of all batches and operations within

/*

caller thread calls db.write(batch)
    │
    └─ becomes Writer — still the caller thread
           │
           └─ joins write_thread queue
                  │
                  ├─ FOLLOWER — blocks, wakes when done, returns
                  │
                  └─ LEADER
                         │
                         ├─ forms write group
                         ├─ assigns sequence numbers
                         ├─ WAL write
                         │
                         └─ iterates write group
                                │
                                └─ for each batch
                                       │
                                       └─ calls batch.apply(cf_resolver, seq_no)
                                              │
                                              └─ batch walks its own bytes
                                                 resolves CF per operation
                                                 calls insert_direct on memtable

 */
