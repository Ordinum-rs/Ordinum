//
//
//
//
// db.write(batch)
//     │
//     ├─ create Writer node on stack
//     ├─ join writer queue
//     │
//     ├─ if FOLLOWER
//     │      block on condvar
//     │      wake up when done
//     │      return
//     │
//     └─ if LEADER
//            form write group
//            assign sequence numbers
//            WAL write
//            apply group to memtables
//            signal followers
//            return
//
// rocksdb/
// ├── db/
// │   ├── write_thread.h          # WriteThread coordination system
// │   ├── write_thread.cc         # WriteThread implementation
// │   ├── write_batch.cc          # WriteBatch internal logic
// │   ├── column_family.h         # Column family management
// │   └── db_impl/
// │       ├── db_impl.h           # DBImpl class definition
// │       └── db_impl_write.cc    # Write implementation methods
// └── include/rocksdb/
//     └── write_batch.h           # Public WriteBatch API
//
//
// Logic:
// db_impl_write.cc  — orchestrates the whole flow on the calling thread
//    │
//    ├── write_thread — just coordination, am I leader or follower?
//    │                  if follower: block here until signalled
//    │                  if leader: return and let caller thread do the work
//    │
//    └── if leader: caller thread continues executing through db_impl_write
//                   accessing self directly for WAL, memtables, CFs
