pub(crate) mod scratch;
pub(crate) mod thread_local;
pub(crate) mod thread_local_ptr;

use std::ptr::null_mut;

use crate::sync::Mutex;
use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::Ordering;
use crate::sync::cell::UnsafeCell;
