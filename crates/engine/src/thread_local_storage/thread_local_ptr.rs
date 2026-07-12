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
///
/// Entry APIs intentionally accept `NonNull<T>` rather than `Box<T>`. Requiring
/// a `Box<T>` would make heap allocation and `Box::from_raw` reclamation part of
/// this type's contract, but TLS entries may instead use another ownership
/// protocol, such as decrementing a reference count, returning an object to a
/// pool, retiring it through a deferred-reclamation scheme, or performing no
/// reclamation at all. The registered `ThreadLocalObject` handler defines which
/// protocol applies to `T`.
///
/// `NonNull<T>` guarantees only that the pointer is non-null; it does not prove
/// that the pointer is valid, uniquely accessible, or live for long enough.
/// Callers inserting an entry must uphold those properties according to the
/// selected reclamation protocol. They must also ensure that the pointer remains
/// valid until it is removed or its handler is invoked, and that TLS access has
/// quiesced before the row or column can reclaim the entry. Methods that create
/// references from stored pointers therefore form an unsafe implementation
/// boundary even when a subsystem exposes a narrower safe wrapper around them.
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

    pub(super) fn get(&self) -> Option<NonNull<T>> {
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

    pub(super) unsafe fn get_or_init(&self, init: impl FnOnce() -> NonNull<T>) -> NonNull<T> {
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

    /// Initializes the calling thread's entry if necessary and gives `f`
    /// temporary mutable access to it.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    ///
    /// - `init` returns a valid, aligned `NonNull<T>` governed by this
    ///   `ThreadLocalPtr`'s registered reclamation protocol.
    /// - The stored object remains alive and is not removed or reclaimed while
    ///   `f` executes.
    /// - No references or accesses that conflict with the temporary `&mut T`
    ///   exist while `f` executes.
    /// - Access is not re-entered for this same TLS entry while `f` holds the
    ///   mutable reference.
    /// - All accesses are quiesced before the TLS row or column is reclaimed.
    pub(crate) unsafe fn get_or_init_mut<F, R>(&self, init: impl FnOnce() -> NonNull<T>, f: F) -> R
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
                // We get the type erased pointer from the tls matrix cell
                let ptr = cell.load(Ordering::Acquire);

                if let Some(ptr) = NonNull::new(ptr.cast::<T>()) {
                    // Sound because this cell belongs only to the calling
                    // thread's row; other threads may observe or reclaim the
                    // raw pointer only after the owner's external quiescence
                    // protocol has stopped access to this TLP.
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
    //
    // This is an explicit lifecycle operation rather than `Drop` because the
    // caller must first establish quiescence across every thread that could
    // still access this column.
    fn remove_column(&self) {
        let meta = thread_meta();
        let tls_id = self.tls_id;

        let _guard = meta.thread_mu.lock().unwrap_or_else(|e| panic!("{e}"));

        let handler = unsafe { &mut *meta.unref_handler_map.get() }.remove(&tls_id);

        let head = meta.head.get();

        let mut next = unsafe { &mut *head }.next.get();

        while next != head {
            let current = next;
            next = unsafe { &*current }.next.get();

            let entries = unsafe { &mut *current }.entries_mut();

            if entries.len() <= tls_id {
                continue;
            }

            // NOTE: Do we need to use CAS here?
            let ptr = entries[tls_id].swap(null_mut(), Ordering::Acquire);

            if !ptr.is_null() {
                if let Some(handler) = handler {
                    unsafe { handler(ptr) };
                }
            }
        }

        // Add tls_id to the free_list
        unsafe { &mut *meta.tls_id_free_list.get() }.push(tls_id);
    }
}

impl<T> Drop for ThreadLocalPtr<T> {
    fn drop(&mut self) {
        self.remove_column();
    }
}

impl<T: ThreadLocalObject> ThreadLocalPtr<T> {
    pub(crate) fn new() -> Self {
        Self::new_with_handler(T::handler())
    }
}

#[cfg(test)]
mod tests {

    use std::thread;

    use crate::sync::spin_loop;

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

    #[test]
    fn two_entries_vec_len() {
        thread::scope(|t| {
            let tlp = setup_thread_owner();

            tlp.ptr.init(unsafe {
                NonNull::new_unchecked(Box::into_raw(Box::new(Entry { thing: 10 })))
            });

            let tlp_2 = setup_thread_owner();

            tlp_2.ptr.init(unsafe {
                NonNull::new_unchecked(Box::into_raw(Box::new(Entry { thing: 20 })))
            });

            TLS_THREAD_ROW.with(|data| {
                assert_eq!(data.entries_mut().len(), 2);
            })
        });
    }

    #[test]
    fn two_threads_entry_reuse() {
        let ready = std::sync::atomic::AtomicBool::new(false);

        let base_tlp = setup_thread_owner();

        base_tlp
            .ptr
            .init(unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(Entry { thing: 5 }))) });

        base_tlp.ptr.remove_column();

        ready.store(true, Ordering::Release);

        thread::scope(|t| {
            t.spawn(|| {
                while ready.load(Ordering::Acquire) != true {
                    spin_loop();
                }

                let tlp = setup_thread_owner();

                tlp.ptr.init(unsafe {
                    NonNull::new_unchecked(Box::into_raw(Box::new(Entry { thing: 10 })))
                });

                let tlp_2 = setup_thread_owner();

                tlp_2.ptr.init(unsafe {
                    NonNull::new_unchecked(Box::into_raw(Box::new(Entry { thing: 20 })))
                });

                TLS_THREAD_ROW.with(|data| {
                    assert_eq!(data.entries_mut().len(), 2);
                });
            });
        });

        // After the thread drops we should have 2 tls_id's in the free list

        let meta = thread_meta();

        let free_list = unsafe { &*meta.tls_id_free_list.get() };

        assert_eq!(free_list.len(), 2);
    }

    // Need to test competing threads with conflicting actions which the mutex and our safety invariants should protect against
    // TODO: Concurrent Tests

    #[test]
    fn concurrent_get_or_init_same_thread_row_is_stable() {
        // Test that repeated concurrent-style access patterns on a single thread
        // row never allocate more than one entry for the same `tls_id`.
        //
        // What to verify:
        // - `get_or_init()` installs exactly one pointer for the calling thread.
        // - subsequent `get()` / `get_or_init()` calls observe the same pointer.
        // - the row length remains stable once the slot has been created.
        //
        // This is primarily a same-thread invariant test for lazy slot creation
        // and entry installation.
    }

    #[test]
    fn concurrent_threads_isolate_per_thread_entries() {
        // Test that different threads accessing the same `ThreadLocalPtr`
        // install distinct per-thread entries rather than racing on a shared
        // object.
        //
        // What to verify:
        // - each thread gets a unique pointer for the same `tls_id`.
        // - each thread only mutates and reads back its own entry.
        // - the global thread list contains both registered rows while both
        //   threads are alive.
    }

    #[test]
    fn concurrent_registration_and_slot_growth_preserves_indices() {
        // Test concurrent first-touch registration while multiple
        // `ThreadLocalPtr`s are being created and initialized.
        //
        // What to verify:
        // - `ThreadData::ensure_registered()` and `with_tlp_ptr()` do not lose
        //   a row or corrupt the intrusive list.
        // - growing `entries` for larger `tls_id` values preserves previously
        //   initialized indices.
        // - every thread can still read back the value it stored after vector
        //   growth caused by other slots.
    }

    #[test]
    fn concurrent_drop_requires_quiesced_access() {
        // Test the lifecycle boundary around `ThreadLocalPtr::drop()`.
        //
        // What to verify:
        // - with proper external quiescence, dropping the TLP reclaims every
        //   live per-thread entry exactly once.
        // - the handler is invoked once per non-null entry in the reclaimed
        //   column.
        // - the `tls_id` is returned to the free list after reclamation.
        //
        // This should be written as a positive test only after the harness can
        // guarantee that no worker thread is still calling `get()` / `init()` /
        // `get_or_init_mut()` while drop runs.
    }

    #[test]
    fn concurrent_drop_and_access_is_not_permitted() {
        // Document the forbidden case where one thread is still accessing a
        // TLP while another thread reclaims it.
        //
        // What to verify:
        // - this is not a supported behavior to make "work".
        // - if modeled with loom or a dedicated stress harness, the test should
        //   assert the required precondition instead of depending on runtime
        //   behavior after the invariant is broken.
        //
        // This belongs more as an invariant test / documentation test than as a
        // normal unit test.
    }

    #[test]
    fn concurrent_reuse_of_freed_tls_id_clears_stale_state() {
        // Test reuse of a freed `tls_id` after one TLP is dropped and another is
        // created on a different thread.
        //
        // What to verify:
        // - the reused `tls_id` does not retain a stale handler entry.
        // - the new owner observes a clean null slot before initialization.
        // - old per-thread pointers from the reclaimed column are gone before
        //   the new owner installs its entries.
    }

    #[test]
    fn concurrent_handler_invocation_reclaims_each_entry_once() {
        // Test that reclamation of a populated column invokes the registered
        // handler exactly once per thread-local entry.
        //
        // What to verify:
        // - multiple threads can populate the same TLP column independently.
        // - `drop()` swaps each slot to null before invoking the handler.
        // - no entry is leaked and no entry is reclaimed twice.
    }

    #[test]
    fn concurrent_thread_teardown_and_column_reclamation_do_not_conflict() {
        // Test interaction between thread teardown and global column
        // reclamation, since both touch the registered thread rows.
        //
        // What to verify:
        // - `thread_mu` correctly serializes row unlink / teardown with TLP
        //   traversal during drop.
        // - a row disappearing during reclamation does not lead to use-after-free
        //   on the intrusive list.
        // - teardown either reclaims its own entries safely or leaves them in a
        //   state that `drop()` can safely reclaim.
    }
}
