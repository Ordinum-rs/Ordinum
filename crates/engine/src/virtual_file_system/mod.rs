//                  LSM engine
//                     │
//      ┌──────────────┼───────────────┐
//      │              │               │
//     WAL          SSTables       MANIFEST
//      │              │               │
//      └──────────────┼───────────────┘
//                     │
//                    VFS
//                     │
//         ┌───────────┴───────────┐
//         │                       │
//       File                     FS
//   read/write/sync       create/open/rename/
//                        remove/list/lock/etc.
//         │                       │
//         └───────────┬───────────┘
//                     │
//                   OS
//             std::fs / POSIX
