pub(crate) mod registry;
pub(crate) mod scratch;

use crate::thread_local_storage::registry::ThreadCtx;

/*

Thread Context gives us a thread local storage container for different structures

Example:

struct ThreadContext {
    ebr: LocalHandle,
    metrics: Metrics,
    sv: *const SuperVersion, //NOTE: Cached pointer with tagged generated number
    //...
}

 */

// Thread Local Storage Gurantees:
//
// 1. Per-thread isolation must be ensured and enforced. Deliberate and intentional sharing of state between threads must be carefully planned
// and explicit
//
// 2. Single mutable access at a time. Unless explicit lock-free data structures or atomics are used, we must observe exclusivity for mutable access
// and enforce that through api design
//
// 3. References must not escape the scope of mutation or the scope of the thread unless designed for.

use std::cell::RefCell;

// TODO: Should i remove ref cell and wrap the hazard_pointer in UnsafeCell or RefCell so we don't borrow_mut() on whole TLS?
thread_local! {
    pub(crate) static TCTX: ThreadCtx = ThreadCtx::new()
}

pub(crate) fn thread_ctx<F, R>(f: F) -> R
where
    F: FnOnce(&ThreadCtx) -> R,
{
    TCTX.with(|ctx| f(ctx))
}

#[cfg(test)]
mod tests {

    use std::cell::UnsafeCell;

    struct TestThread {
        buffer: UnsafeCell<Vec<u8>>,
    }

    impl TestThread {
        fn new() -> Self {
            Self {
                buffer: UnsafeCell::new(Vec::new()),
            }
        }
    }

    thread_local! {
        static TEST_THREAD: TestThread = TestThread::new()
    }

    #[test]
    fn thread_reuse() {
        // Testing and showing a TLS buffer being overwritten when two variables/callers hold references to the tls buffer
        //

        TEST_THREAD.with(|v| {
            let buff = unsafe { &mut *v.buffer.get() };

            buff.extend_from_slice(b"Hello".as_slice());

            let a = &buff[..];

            let buff2 = unsafe { &mut *v.buffer.get() };

            buff2.clear();
            buff2.extend_from_slice(b"World".as_slice());

            // Testing that holding a reference to a tls buffer/object outside of the scoped mutation or it will be overwritten
            assert_eq!("World".to_string(), String::from_utf8_lossy(a));
        })
    }
}
