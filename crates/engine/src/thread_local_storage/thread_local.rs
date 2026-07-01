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
    pub(crate) static TLS_THREAD_ROW: ThreadData = ThreadData::new();

    // XXX: Future thread local fields can be separate static entries here ONLY if they are for the thread and not per entry subsystems
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
            head: UnsafeCell::new(ThreadData::new()),
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
            head: UnsafeCell::new(ThreadData::new()),
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

pub(super) struct ThreadData {
    pub(super) next: Cell<*mut ThreadData>,
    pub(super) prev: Cell<*mut ThreadData>,

    // Entries - columns in the thread local matrix, each column can comprise of multiple thread-local-storage sub-systems each with a unique tls_id
    entries: UnsafeCell<Vec<AtomicPtr<()>>>,
    registered: Cell<bool>,
}

impl ThreadData {
    fn new() -> Self {
        Self {
            next: Cell::new(null_mut()),
            prev: Cell::new(null_mut()),
            entries: UnsafeCell::new(Vec::new()),
            registered: Cell::new(false),
        }
    }

    // SAFETY:
    //
    // The caller must hold `thread_meta.thread_mu`, which serializes all
    // structural modifications to this thread's TLS row.
    pub(super) fn entries_mut(&self) -> &mut Vec<AtomicPtr<()>> {
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

        let prev = self.prev.get();
        let next = self.next.get();

        unsafe {
            (*prev).next.set(next);
            (*next).prev.set(prev);
        }

        // Null out self prev/next just for safety so we don't access other threads if we do wrongfully dereference next/prev fields
        self.next.set(null_mut());
        self.prev.set(null_mut());

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

    // SAFETY:
    //
    // This returns the raw pointer currently stored in the calling thread's TLS
    // cell for `tls_id`. The pointer is type-erased and may be null.
    //
    // This function does not prove that the pointer is valid, uniquely borrowed,
    // or safe to dereference. It only ensures that the current thread is
    // registered and that the row has a cell for `tls_id`.
    //
    // The caller is responsible for interpreting the pointer according to the
    // owning ThreadLocalPtr<T>'s protocol.
    pub(super) unsafe fn with_tlp_ptr<F, R>(&self, tls_id: usize, f: F) -> R
    where
        F: FnOnce(&AtomicPtr<()>) -> R,
    {
        self.ensure_registered();

        let entries = self.entries_mut();

        if entries.len() <= tls_id {
            let _guard = thread_meta().thread_mu.lock().unwrap();

            if entries.len() <= tls_id {
                entries.resize_with(tls_id + 1, || AtomicPtr::new(null_mut()));
            }
        }

        let ptr = &entries[tls_id];

        f(ptr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // TODO: Fix this test to use tls closure rather than create ThreadData outside of thread_local!()
    fn link_access() {
        let meta = thread_meta();

        let mut td = ThreadData::new();
        let td2 = ThreadData::new();

        let ptr = &td2 as *const ThreadData as *mut ThreadData;
        td.next.set(ptr);
    }

    #[test]
    fn drop_row() {
        let meta = thread_meta();

        struct Entry {
            switch: Cell<bool>,
        }

        let mock_entry = Box::new(Entry {
            switch: Cell::new(false),
        });

        let me_ptr = Box::into_raw(mock_entry);

        unsafe fn unref(ptr: *mut ()) {
            let entry = ptr.cast::<Entry>();
            unsafe { &*entry }.switch.set(true);
        }

        TLS_THREAD_ROW.with(|data| {
            data.ensure_registered();

            assert!(!data.next.get().is_null());

            // Set the handler and entry

            {
                let _gaurd = meta.thread_mu.lock().unwrap();

                let handler = unsafe { &mut *meta.unref_handler_map.get() };
                let _ = handler.insert(0, unref);

                // Safe to push because we only have 1 entry and we know tls_id is basically 0
                data.entries_mut().push(AtomicPtr::new(me_ptr.cast::<()>()));
            }
        });

        TLS_THREAD_ROW.with(|data| {
            data.drop_row();

            assert!(data.entries_mut()[0].load(Ordering::Relaxed).is_null());
            assert!(data.next.get().is_null());
            assert!(data.next.get().is_null());
        });

        let e = unsafe { Box::from_raw(me_ptr) };
        assert_eq!(e.switch.get(), true);
    }

    #[test]
    fn thread_local_cell_method() {}
}
