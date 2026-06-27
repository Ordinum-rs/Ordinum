use std::{
    marker::PhantomData,
    ops::Index,
    ptr::{NonNull, null_mut},
};

use crate::sync::atomic::AtomicPtr;
use crate::sync::atomic::Ordering;
use crate::thread_local_storage::thread_local::{TLS_THREAD_ROW, thread_meta};

// ---- UnrefHandler Type ---- //

pub(crate) type UnrefHandler = unsafe fn(*mut ());

// ---- ThreadLocalObject Trait ---- //

/// Defines how an object stored within a ThreadLocalPtr TLS entry should be
/// reclaimed when its owning ThreadLocalPtr reclaims the TLS column.
pub(crate) trait ThreadLocalObject: Sized {
    fn handler() -> Option<UnrefHandler> {
        None
    }

    unsafe fn unref_erased(ptr: *mut ()) {
        unsafe {
            Self::unref(ptr.cast::<Self>());
        }
    }

    unsafe fn unref(_: *mut Self) {
        unreachable!("handler() must return Some before unref() can be called");
    }
}

// ---- ThreadLocalPtr ---- //

/// ThreadLocalPtr represents a single column within the global TLS matrix.
///
/// The owner of a ThreadLocalPtr is responsible for allocating and inserting
/// per-thread entry objects. ThreadLocalPtr stores typed pointers to those
/// objects for each thread, and on reclamation walks the column and invokes
/// the registered handler for each entry.
///
/// The entry type determines its own reclamation semantics by implementing
/// `ThreadLocalObject`. ThreadLocalPtr does not own the allocation strategy
/// or lifetime protocol of the stored objects; it only manages their storage
/// and reclamation within the TLS matrix.
pub(crate) struct ThreadLocalPtr<T> {
    tls_id: usize,
    _type: PhantomData<T>,
}

impl<T> ThreadLocalPtr<T> {
    pub(crate) fn new_with_handler(handler: Option<UnrefHandler>) -> Self {
        // Acquire tls_id from meta

        let meta = thread_meta();

        let tls_id = {
            let _guard = meta.thread_mu.lock().unwrap_or_else(|e| panic!("{e}"));

            let tls_id = unsafe { &mut *meta.tls_id_free_list.get() }
                .pop()
                .unwrap_or_else(|| {
                    meta.next_tls_id
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                });

            let handlers = unsafe { &mut *meta.unref_handler_map.get() };

            if let Some(handler) = handler {
                handlers.insert(tls_id, handler);
            } else {
                // Cheap safety measure to ensure we remove stale tls_id handler entries
                // We will always try to remove the handler before pushing tls_id to free_list on drop()
                handlers.remove(&tls_id);
            }

            tls_id
        };

        Self {
            tls_id,
            _type: PhantomData,
        }
    }

    // SAFETY:
    // `entry` must point to a valid instance of `T` that remains valid for the
    // lifetime of this TLS entry, or until it is removed or reclaimed.
    //
    // Each thread may initialize its own TLS entry at most once. Calling `init()`
    // on a non-empty entry is a logic error and will trigger a debug assertion.
    // Existing entries must first be removed through the appropriate lifecycle
    // operation before a new entry may be installed.
    //
    // Allocation of the entry object is the responsibility of the owner of this
    // ThreadLocalPtr. ThreadLocalPtr only stores the pointer within the current
    // thread's TLS row and does not assume ownership of the underlying object.
    //
    // The lifetime and reclamation strategy of the stored object is defined by
    // the entry type's registered `ThreadLocalObject` handler. This may involve
    // dropping a heap allocation, decrementing a reference count, retiring the
    // object through a reclamation scheme, or performing no action, depending on
    // the semantics of `T`.
    pub(crate) fn init(&self, entry: NonNull<T>) {
        let tls_id = self.tls_id;

        TLS_THREAD_ROW.with(|data| {
            data.ensure_registered();

            let entries = unsafe { &mut *data.entries.get() };

            if entries.len() <= tls_id {
                let _guard = thread_meta().thread_mu.lock().unwrap();

                if entries.len() <= tls_id {
                    entries.resize_with(tls_id + 1, || AtomicPtr::new(null_mut()));
                }
            }

            // Init

            debug_assert!(entries[tls_id].load(Ordering::Acquire).is_null());
            entries[tls_id].store(entry.as_ptr().cast(), Ordering::Release);
        })
    }

    pub(crate) fn get(&self) -> Option<NonNull<T>> {
        let tls_id = self.tls_id;

        TLS_THREAD_ROW.with(|data| {
            data.ensure_registered();

            // TODO: Make a method to return &Entries instead of using unsafe
            let entries = unsafe { &mut *data.entries.get() };

            if entries.len() <= tls_id {
                let _guard = thread_meta().thread_mu.lock().unwrap();

                if entries.len() <= tls_id {
                    entries.resize_with(tls_id + 1, || AtomicPtr::new(null_mut()));
                }
            }

            let ptr = entries[tls_id].load(Ordering::Acquire);

            if ptr.is_null() {
                return None;
            } else {
                return Some(unsafe { NonNull::new_unchecked(ptr.cast::<T>()) });
            }
        })
    }

    // SAFETY:
    //
    // Dropping a ThreadLocalPtr reclaims the entire TLS column associated with
    // this instance by walking every registered thread and destroying the
    // thread-local object stored in that column.
    //
    // This operation is inherently a lifecycle transition and therefore relies
    // on a strict ownership invariant:
    //
    // - The owner of this ThreadLocalPtr (e.g. BatchPool, ColumnFamilyData)
    //   is solely responsible for ensuring that no thread can subsequently call
    //   Get/Reset/Swap/CompareAndSwap on this ThreadLocalPtr.
    // - All runtime operations capable of accessing this ThreadLocalPtr must
    //   have quiesced before Drop is entered.
    // - During reclamation, thread_mu serializes registration, thread teardown,
    //   and traversal of the thread list while the column is reclaimed.
    // - The objects stored within the TLS cells remain responsible for their
    //   own concurrent synchronization. Reclaiming the column does not
    //   synchronize access to the objects themselves.
    //
    // Once these invariants hold, it is safe to walk the column, invoke each
    // thread-local object's registered unref handler, clear the entries, and
    // return the TLS ID to the free list.
    fn drop(self) {
        // TODO: Implement safe drop for thread local pointer
    }

    //
    //
}

impl<T: ThreadLocalObject> ThreadLocalPtr<T> {
    pub(crate) fn new() -> Self {
        Self::new_with_handler(T::handler())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Entry {
        thing: usize,
    }

    struct ThreadOwner {
        ptr: ThreadLocalPtr<Entry>,
    }

    impl ThreadLocalObject for Entry {
        fn handler() -> Option<UnrefHandler> {
            Some(Self::unref_erased)
        }

        unsafe fn unref(ptr: *mut Self) {
            let _entry = unsafe { Box::from_raw(ptr) };
        }
    }

    fn setup_thread_owner() -> ThreadOwner {
        ThreadOwner {
            ptr: ThreadLocalPtr::new(),
        }
    }

    fn handler_for(ptr: &ThreadLocalPtr<Entry>) -> UnrefHandler {
        let meta = thread_meta();
        let _guard = meta.thread_mu.lock().unwrap();

        unsafe { &*meta.unref_handler_map.get() }
            .get(&ptr.tls_id)
            .copied()
            .unwrap()
    }

    fn entry_ptr(thing: usize) -> *mut () {
        Box::into_raw(Box::new(Entry { thing })).cast::<()>()
    }

    #[test]
    fn simple_entry() {
        let mut handler_called = false;

        let owner = setup_thread_owner();
        let handler = handler_for(&owner.ptr);
        let ptr = entry_ptr(10);

        unsafe {
            handler(ptr);
            handler_called = true;
        }

        assert_eq!(handler_called, true);
    }

    #[test]
    fn simple_get_on_tlp() {
        let owner = setup_thread_owner();

        let entry = owner.ptr.get();
        assert!(entry.is_none());
    }

    #[test]
    fn get_and_init_tlp() {
        let owner = setup_thread_owner();

        let object_entry = Box::new(Entry { thing: 10 });

        if owner.ptr.get().is_none() {
            owner
                .ptr
                .init(unsafe { NonNull::new_unchecked(Box::into_raw(object_entry)) });
        }

        // Test get

        let object = owner.ptr.get();

        assert!(object.is_some());

        assert_eq!(unsafe { &*object.unwrap().as_ptr() }.thing, 10);
    }
}
