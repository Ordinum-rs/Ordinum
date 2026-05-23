#![allow(warnings)]

//
//
//
//
//
mod column_family;
mod db;
mod iterator;
mod key;
mod memtable;
mod options;
mod range;
mod thread_ctx;
mod versioning;

pub mod block;
pub mod tests;
pub mod utils;

pub mod sync {
    #[cfg(feature = "loom")]
    pub use loom::sync::{Arc, Condvar, Mutex};

    #[cfg(feature = "loom")]
    pub mod atomic {
        pub use loom::sync::atomic::*;
    }

    #[cfg(not(feature = "loom"))]
    pub use std::sync::{Arc, Condvar, Mutex};

    #[cfg(not(feature = "loom"))]
    pub mod atomic {
        pub use std::sync::atomic::*;
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
