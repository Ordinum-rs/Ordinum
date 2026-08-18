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
    file,
    fs::{self, Metadata},
    io::{self, Cursor, Error, Read, Seek, SeekFrom, Write},
    os::windows::{
        fs::MetadataExt,
        io::{BorrowedHandle, RawHandle},
    },
    path::PathBuf,
};

#[cfg(unix)]
pub(crate) type NativeHandle<'a> = BorrowedFd<'a>;

#[cfg(windows)]
pub(crate) type NativeHandle<'a> = BorrowedHandle<'a>;

pub(crate) struct FileInfo {
    len: usize,
    is_dir: bool,
    is_file: bool,
    // XXX:
}

/// VfsFile is a file abstraction over IO File operations. This enables different implementations over than OS to act as the
/// file system primarily we can create an in-memory file system.
pub(crate) trait VfsFile: Read + Write + Send {
    //
    fn preallocate(&self, offset: u64, length: u64) -> io::Result<()>;
    fn stat(&self) -> io::Result<FileInfo>;

    fn sync(&self) -> io::Result<()>;

    // Requests that the filesystem begin syncing the file prefix [0, length)
    // toward stable storage.
    //
    // This is primarily a writeback/latency optimisation for large, continuously
    // growing files such as the WAL. By starting writeback of older dirty pages
    // before a real durability barrier is required, a later fsync/sync_data may
    // have less dirty data left to flush, reducing sync latency spikes.
    //
    // This operation must NOT be treated as a durability guarantee unless the
    // implementation explicitly reports that a full synchronous sync occurred.
    // An asynchronous prefix sync may only queue writeback and can still leave
    // data vulnerable to loss on crash.
    //
    // Typical WAL usage:
    //
    //     write -> write -> sync_to(prefix) -> write -> ... -> sync_data()
    //
    // `sync_to` proactively moves data toward storage;
    // `sync_data` provides the actual durability barrier.
    fn sync_to(&self, length: u64) -> io::Result<bool /* Replace bool with FullSync new type */>;

    // Persist all written data.
    fn sync_data(&self) -> io::Result<()>;

    fn prefetch(&self, offset: u64, length: u64) -> io::Result<()>;

    fn raw_file_descriptor_handle<'a>(&'a self) -> Option<NativeHandle<'a>>;
}

pub(crate) type FileHandle = Box<dyn VfsFile>;

pub(crate) trait FileSystem {
    //
    fn create() -> io::Result<FileHandle>;

    // TODO: Finish trait methods
}

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
