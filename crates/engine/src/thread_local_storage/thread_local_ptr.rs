use std::{
    marker::PhantomData,
    ops::Index,
    ptr::{NonNull, null_mut},
};

use crate::sync::atomic::Ordering;
use crate::thread_local_storage::thread_local::{TLS_THREAD_ROW, thread_meta};
use crate::{sync::atomic::AtomicPtr, thread_local_storage::thread_local::ThreadData};

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

        TLS_THREAD_ROW.with(|data| unsafe {
            data.with_tlp_ptr(tls_id, |ptr| {
                debug_assert!(ptr.load(Ordering::Acquire).is_null());
                ptr.store(entry.as_ptr().cast(), Ordering::Release);
            })
        })
    }

    pub(crate) fn get(&self) -> Option<NonNull<T>> {
        let tls_id = self.tls_id;

        TLS_THREAD_ROW.with(|data| unsafe {
            data.with_tlp_ptr(tls_id, |ptr| {
                let ptr = ptr.load(Ordering::Acquire);

                if ptr.is_null() {
                    return None;
                } else {
                    return Some(unsafe { NonNull::new_unchecked(ptr.cast::<T>()) });
                }
            })
        })
    }

    pub(crate) fn get_or_init(&self, init: impl FnOnce() -> NonNull<T>) -> NonNull<T> {
        let tls_id = self.tls_id;

        // SAFETY:
        //
        // This method returns the raw pointer stored in the calling thread's TLS cell.
        // Unlike `get_or_init_mut`, no Rust reference is created and therefore no
        // aliasing or lifetime guarantees are expressed through the type system.
        //
        // The returned pointer remains valid only while the owning subsystem's
        // lifecycle invariants hold. In particular:
        //
        // - A thread cannot access its own TLS row after thread teardown begins.
        // - The owner of the ThreadLocalPtr must quiesce all accesses before the
        //   ThreadLocalPtr is destroyed and its TLS column reclaimed.
        // - The registered ThreadLocalObject handler defines how the object is
        //   reclaimed once those lifecycle invariants have been established.
        //
        // Consequently, callers remain responsible for ensuring that any
        // dereference of the returned pointer is performed under the appropriate
        // subsystem-specific synchronization and lifetime rules.

        TLS_THREAD_ROW.with(|data| unsafe {
            data.with_tlp_ptr(tls_id, |cell| {
                let ptr = cell.load(Ordering::Acquire);

                if let Some(ptr) = NonNull::new(ptr.cast::<T>()) {
                    return ptr;
                }

                let ptr = init();

                // We want to init only once - if we are not null at this point something is very wrong
                debug_assert!(cell.load(Ordering::Acquire).is_null());
                cell.store(ptr.as_ptr().cast(), Ordering::Release);

                ptr
            })
        })
    }

    pub(crate) fn get_or_init_mut<F, R>(&self, init: impl FnOnce() -> NonNull<T>, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        let tls_id = self.tls_id;

        // SAFETY:
        //
        // Each ThreadLocalPtr access resolves to the calling thread's own TLS row.
        // Therefore only the owning thread can obtain mutable access to the stored
        // object through this API.
        //
        // A temporary `&mut T` is created from the raw pointer stored in the TLS cell.
        // This reference is intentionally scoped to the supplied closure rather than
        // returned to the caller. ThreadLocalPtr does not own the underlying allocation
        // and therefore cannot express its lifetime through Rust's borrow checker. By
        // containing the reference within the closure, it cannot outlive the access or
        // escape beyond the point at which the required invariants are known to hold.
        //
        // The soundness of creating `&mut T` relies on the following invariants:
        //
        // - The owning thread is the only thread that may obtain a mutable reference to
        //   the object through normal ThreadLocalPtr access.
        // - Cross-thread operations (e.g. ReclaimId, thread teardown, or subsystem
        //   walkers) never construct Rust references from the raw pointer. They operate
        //   only on the TLS cell itself or invoke object-specific lifecycle logic.
        // - The owner of the ThreadLocalPtr is responsible for quiescing all accesses
        //   before the TLS row or column may be reclaimed. Consequently, the object
        //   cannot be removed or destroyed while this closure is executing.

        TLS_THREAD_ROW.with(|data| unsafe {
            data.with_tlp_ptr(tls_id, |cell| {
                let ptr = cell.load(Ordering::Acquire);

                if let Some(ptr) = NonNull::new(ptr.cast::<T>()) {
                    return unsafe { f(&mut *ptr.as_ptr()) };
                }

                // We want to init only once - if we are not null at this point something is very wrong
                debug_assert!(ptr.is_null());

                let ptr = init();

                cell.store(ptr.as_ptr().cast(), Ordering::Release);

                unsafe { f(&mut *ptr.as_ptr()) }
            })
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
        let meta = thread_meta();
        let tls_id = self.tls_id;

        let _guard = meta.thread_mu.lock().unwrap_or_else(|e| panic!("{e}"));

        let mut current = meta.head.get();

        // XXX: Im thinking of making a method which takes mutex guard and returns an Impl IntoIterator? ... For now it might be easier to traverse manually

        // TODO: Traverse the linked list and continue the drop method
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

    // Test two threads creating a subsystem - assert entries vec len is 2
    // Test two threads create subsystem, 1 thread drops, another thread creates subsystem, assert tls_id reuse entries vec len is 2
}
