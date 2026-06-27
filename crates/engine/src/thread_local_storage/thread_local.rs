use std::collections::HashMap;
use std::ptr::NonNull;
use std::ptr::null_mut;

use crate::db::batch_pool::ThreadBatchCache;
use crate::sync::Mutex;
use crate::sync::atomic::AtomicPtr;
use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::Ordering;
use crate::sync::cell::Cell;
use crate::sync::cell::UnsafeCell;

// Thread Matrix

//                  tls_id=0      tls_id=1      tls_id=2
//
// ThreadCtx 1      entries[0]    entries[1]    entries[2]
// ThreadCtx 2      entries[0]    entries[1]    entries[2]
// ThreadCtx 3      entries[0]    entries[1]    entries[2]

// ---- TLS Init ---- //

thread_local! {
    pub(crate) static TLS_THREAD_ROW: ThreadData = ThreadData::default();

    // XXX: Future thread local fields can be separate static entries here ONLY if they are for the thread and not per db instance
}

// ---- Thread Static Meta ---- //

pub(crate) struct ThreadMetaGlobal {
    pub(super) thread_mu: Mutex<()>,

    pub(super) head: UnsafeCell<ThreadData>,

    pub(super) unref_handler_map: UnsafeCell<HashMap<usize, super::thread_local_ptr::UnrefHandler>>,

    pub(super) next_tls_id: AtomicUsize,

    pub(super) tls_id_free_list: UnsafeCell<Vec<usize>>,
}

// TODO: Need safety notes - can we avoid this?
unsafe impl Sync for ThreadMetaGlobal {}
unsafe impl Send for ThreadMetaGlobal {}

impl Default for ThreadMetaGlobal {
    fn default() -> Self {
        Self {
            thread_mu: Mutex::new(()),
            head: UnsafeCell::new(ThreadData::default()),
            unref_handler_map: UnsafeCell::new(HashMap::new()),
            next_tls_id: AtomicUsize::new(0),
            tls_id_free_list: UnsafeCell::new(Vec::new()),
        }
    }
}

impl ThreadMetaGlobal {
    pub(crate) fn new() -> Box<Self> {
        let mut meta = Box::new(Self {
            thread_mu: Mutex::new(()),
            head: UnsafeCell::new(ThreadData::default()),
            unref_handler_map: UnsafeCell::new(HashMap::new()),
            next_tls_id: AtomicUsize::new(0),
            tls_id_free_list: UnsafeCell::new(Vec::new()),
        });

        let head = meta.head.get();

        unsafe {
            (*head).next.set(head);
            (*head).prev.set(head);
        }

        // Empty:
        //   +-----------+
        //   | Sentinel  |
        //   +-----------+
        //    ^         |
        //    |         v
        //   prev     next
        //    |         |
        //    +---------+

        meta
    }
}

pub(crate) fn thread_meta() -> &'static ThreadMetaGlobal {
    #[cfg(not(feature = "loom"))]
    {
        use std::sync::OnceLock;

        static STATIC_META: OnceLock<Box<ThreadMetaGlobal>> = OnceLock::new();
        STATIC_META.get_or_init(ThreadMetaGlobal::new)
    }
    #[cfg(feature = "loom")]
    {
        loom::lazy_static!(
            static ref STATIC_META: StaticMeta = StaticMeta::new();
        ) & STATIC_META
    }
}

// ---- ThreadData ---- //

/*
 * NOTE:
 * We can either store a blank pointer as an entry and let ThreadLocalPtr<T> cast to it and use the index to lookup in the meta hashtable to call the handler
 * OR
 * We define an Entry which stores the pointer and func within
 * OR
 * We define a trait to bind the ThreadLocalPtr<T> to which must implement a handler func
 */

pub(crate) struct ThreadData {
    next: Cell<*mut ThreadData>,
    prev: Cell<*mut ThreadData>,

    // Entries - columns in the thread local matrix, each column can comprise of multiple thread-local-storage sub-systems each with a unique tls_id
    pub(crate) entries: UnsafeCell<Vec<AtomicPtr<()>>>,
    registered: Cell<bool>,
}

impl Default for ThreadData {
    fn default() -> Self {
        Self {
            next: Cell::new(null_mut()),
            prev: Cell::new(null_mut()),
            entries: UnsafeCell::new(Vec::new()),
            registered: Cell::new(false),
        }
    }
}

impl ThreadData {
    // SAFETY:
    //
    // The caller must hold `thread_meta.thread_mu`, which serializes all
    // structural modifications to this thread's TLS row.
    fn entries_mut(&self) -> &mut Vec<AtomicPtr<()>> {
        unsafe { &mut *self.entries.get() }
    }

    pub(super) fn ensure_registered(&self) {
        if self.registered.get() {
            return;
        }

        let meta = thread_meta();

        let _guard = meta.thread_mu.lock().unwrap_or_else(|e| {
            // XXX: In future we may want to handle the poison lock
            panic!("{e}")
        });

        // TODO: Add safety note
        let sentinal = unsafe { &mut *meta.head.get() } as *mut ThreadData;

        let this = self as *const Self as *mut Self;

        let first = unsafe { &*sentinal }.next.get();

        // Before:

        // Sentinel ----> First
        //     ^            |
        //     |            v
        //     <------------

        // After:

        // Sentinel ----> Self ----> First
        //     ^            |          |
        //     |            |          v
        //     <------------<----------

        unsafe {
            // Link self
            (*this).prev.set(sentinal);
            (*this).next.set(first);

            // Correct sentinel next
            (*first).prev.set(this);

            (*sentinal).next.set(this);

            // We don't need to set first.next because it is either the next node OR sentinel which keeps
            // the circular linked list
        }

        // Registered
        self.registered.set(true);
    }

    pub(super) fn drop_row(&self) {
        //
        let meta = thread_meta();

        // We are dropping the row, so only need to unlink from the linked list and walk the entries vec

        let _guard = meta.thread_mu.lock().unwrap_or_else(|e| panic!("{e}"));

        let mut next = self.next.get();
        let mut prev = self.prev.get();

        unsafe {
            if next.is_null() {
                (*prev).next.set(null_mut())
            }

            (*next).prev.set(prev);
            (*prev).next.set(next);
        }

        // Loop the entries, if !null then we need to call the handler and null the entry and handler

        for (idx, e) in self.entries_mut().iter_mut().enumerate() {
            //

            let ptr = e.swap(null_mut(), Ordering::AcqRel);

            if ptr.is_null() {
                continue;
            }

            /* XXX: As a future optimisation we could collect the entry ptr's and id's and handlers? in a vec so we can
            release the lock and call the handlers outside of the lock to reduce lock time */

            let handler = unsafe { &*meta.unref_handler_map.get() };

            if let Some(unref) = handler.get(&idx) {
                unsafe { unref(ptr) }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_access() {
        let meta = thread_meta();

        let td = ThreadData::default();
        let td2 = ThreadData::default();

        let ptr = &td2 as *const ThreadData as *mut ThreadData;
        td.next.set(ptr);
    }

    // TODO: Test link logic
}
