#![allow(warnings)]

//
//
//
//
//
mod column_family;
mod db;
pub mod error;
mod iterator;
mod key;
mod memtable;
mod options;
mod range;
mod thread_local_storage;
mod version;
mod wal;

pub mod block;
pub mod tests;
pub mod utils;

// Errors
pub use error::{Error, Result};

pub mod sync {
    #[cfg(feature = "loom")]
    pub use loom::sync::{Arc, Condvar, LockResult, Mutex, MutexGuard};

    #[cfg(feature = "loom")]
    pub mod atomic {
        pub use loom::sync::atomic::*;
    }

    #[cfg(feature = "loom")]
    pub mod cell {
        pub use loom::cell::*;
    }

    #[cfg(not(feature = "loom"))]
    pub use std::sync::{Arc, Condvar, LockResult, Mutex, MutexGuard};

    #[cfg(not(feature = "loom"))]
    pub mod atomic {
        pub use std::sync::atomic::*;
    }

    #[cfg(not(feature = "loom"))]
    pub mod cell {
        pub use std::cell::*;
    }

    pub fn spin_loop() {
        #[cfg(not(feature = "loom"))]
        {
            std::hint::spin_loop();
        }

        #[cfg(feature = "loom")]
        {
            loom::hint::spin_loop();
        }
    }
}
