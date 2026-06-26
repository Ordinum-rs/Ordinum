use std::{marker::PhantomData, ops::Index, ptr::null_mut};

use crate::thread_local_storage::thread_local::{TLS_THREAD_ROW, thread_meta};

pub(crate) type UnrefHandler = unsafe fn(*mut ());

pub(crate) trait ThreadLocalObject: Sized {
    fn handler() -> Option<UnrefHandler> {
        None
    }

    unsafe fn unref_erased(ptr: *mut ()) {
        unsafe {
            Self::unref(ptr.cast::<Self>());
        }
    }

    unsafe fn unref(ptr: *mut Self) {}
}

//
//
//
//
pub(crate) struct ThreadLocalPtr<T> {
    tls_id: usize,
    _type: PhantomData<T>,
}

impl<T> ThreadLocalPtr<T> {
    fn new() -> Self {
        Self {
            tls_id: 0,
            _type: PhantomData,
        }
    }

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

    // TODO: Implement get_mut() and reset() and swap()

    // TODO: Test get()
    pub(super) fn get(&self) -> Option<&T> {
        let tls_id = self.tls_id;

        TLS_THREAD_ROW.with(|data| {
            data.ensure_registered();

            let entries = unsafe { &mut *data.entries.get() };

            if entries.len() <= tls_id {
                let _guard = thread_meta().thread_mu.lock().unwrap();

                if entries.len() <= tls_id {
                    entries.resize_with(tls_id + 1, || null_mut());
                }
            }

            let ptr = entries[tls_id];

            if ptr.is_null() {
                return None;
            } else {
                return Some(unsafe { &*ptr.cast::<T>() });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_entry() {
        let meta = thread_meta();

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
                let entry = unsafe { Box::from_raw(ptr) };
                println!("dropping {}", entry.thing);
            }
        }

        let tlo = ThreadOwner {
            ptr: ThreadLocalPtr::new_with_handler(Entry::handler()),
        };

        let _guard = meta.thread_mu.lock().unwrap();

        let handler = unsafe { &*meta.unref_handler_map.get() }
            .get(&tlo.ptr.tls_id)
            .copied()
            .unwrap();

        let entry = Box::new(Entry { thing: 10 });
        let ptr = Box::into_raw(entry).cast::<()>();

        unsafe {
            handler(ptr);
        }
    }
}
