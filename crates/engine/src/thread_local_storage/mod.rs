pub(crate) mod registry;
pub(crate) mod scratch;

use std::ptr::null_mut;

use crate::sync::Mutex;
use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::Ordering;
use crate::sync::cell::UnsafeCell;
use crate::thread_local_storage::registry::{DBInstanceCtx, ThreadCtx};

// Static Meta data which is a singleton and hold the meta information and control logic for all threads in the global process

pub(crate) struct StaticMeta {
    thread_mu: Mutex<()>,
    //
    pub(crate) next_tls_id: AtomicUsize,

    // Sentinel node for the global intrusive list of ThreadCtx objects.
    //
    // ThreadLocalPtr operations use this list to traverse every registered
    // thread and access entries[id] for their particular TLS slot.
    head: UnsafeCell<ThreadCtx>,
    //
    // XXX: Later we will want a free list of tls_id or the mechanisms to maintain non-sparse tls_id indexes
}

// TODO: Review this - May be able to wrap head in something which limits the need for unsafe impl
unsafe impl Sync for StaticMeta {}
unsafe impl Send for StaticMeta {}

impl StaticMeta {
    fn new() -> Self {
        Self {
            thread_mu: Mutex::new(()),
            next_tls_id: AtomicUsize::new(0),
            head: UnsafeCell::new(ThreadCtx::new()),
        }
    }
}

pub(crate) fn static_meta() -> &'static StaticMeta {
    #[cfg(not(feature = "loom"))]
    {
        use std::sync::OnceLock;

        static STATIC_META: OnceLock<StaticMeta> = OnceLock::new();
        STATIC_META.get_or_init(StaticMeta::new)
    }
    #[cfg(feature = "loom")]
    {
        loom::lazy_static!(
            static ref STATIC_META: StaticMeta = StaticMeta::new();
        ) & STATIC_META
    }
}

// ---- TLS ---- //

thread_local! {
    static TCTX: ThreadCtx = ThreadCtx::new()
}

// TODO: Check and complete the access functions + test

pub(crate) fn thread_ctx<F, R>(f: F) -> R
where
    F: FnOnce(&ThreadCtx) -> R,
{
    TCTX.with(|ctx| {
        ctx.ensure_registered();
        f(ctx)
    })
}

pub(crate) fn thread_db_instance_ctx<F, R>(db_id: usize, f: F) -> R
where
    F: FnOnce(&DBInstanceCtx) -> R,
{
    TCTX.with(|ctx| {
        ctx.ensure_registered();
        f(ctx.db_instance(db_id))
    })
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
