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

use std::{
    env::temp_dir,
    file, fs,
    io::{Cursor, Seek, SeekFrom, Write},
    os::windows::fs::MetadataExt,
    path::PathBuf,
};

/* NOTE: For implementing VMemFS we can use Cursor: https://doc.rust-lang.org/nightly/std/io/struct.Cursor.html
*  which wraps an in-memory buffer and provides it with a Seek implementation
*  This could be useful anywhere where a Reader/Writer does actual I/O */

// WAL directory layout
//
// Ordinum uses a single WAL directory containing a sequence of numbered
// WAL files. The directory itself is not rotated; individual WAL files are.
//
// Example:
//
// db/
// └── wal/
//     ├── 000017.log
//     ├── 000018.log
//     └── 000019.log   // current active WAL
//
// In standalone mode, one logical WAL maps to one physical WAL file:
//
//     WAL #17 -> 000017.log
//     WAL #18 -> 000018.log
//
// A new WAL is created when the current WAL is rotated, typically alongside
// memtable rotation. Older WALs remain until the data they protect has been
// flushed to persistent SSTables and they are no longer required for recovery.
//
// Once obsolete, a WAL file may either:
//
//     1. be deleted, or
//     2. be retained by the WAL recycler and later renamed/reused as a
//        newly numbered WAL file.
//
// The WAL directory may be located separately from the main database
// directory, but all standalone WAL files reside within the configured
// primary WAL directory.
//

#[test]
fn basic_file_operations() {
    let dir_path = "C:\\Users\\Kristian\\OneDrive\\Desktop\\tmp_ordinum\\test.txt";
    let mut path = PathBuf::from(dir_path);

    match fs::File::create(path) {
        Ok(mut f) => {
            // Write to the file

            f.write(b"Hello").unwrap();
        }
        Err(e) => println!("{e}"),
    }

    //
}

#[test]
fn read_operations() {
    let dir_path = "C:\\Users\\Kristian\\OneDrive\\Desktop\\tmp_ordinum\\test.txt";
    let mut path = PathBuf::from(dir_path);

    println!("{}", path.file_name().unwrap().display());

    if let Ok(mut file) = fs::File::open(path) {
        let size = file.metadata().unwrap().file_size();
        println!("{}", size);

        let seek = file.seek(SeekFrom::Current(0));
        println!("{}", file.stream_position().unwrap());

        let new_seek = file.seek(SeekFrom::Current(2));
        println!("{}", file.stream_position().unwrap());
    };
}
