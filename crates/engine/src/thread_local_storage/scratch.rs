use std::cell::Cell;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

// Want to make a fake reader with a fake structure which writers must change essentially a Mutex into a ZST
// Benchmark many readers hitting pinning and unpinning and some writers changing the "value" meaning using the Mutex
// Need to measure contention and lock acquisition times
//

// Need some global state
//

struct GS {
    cell: Mutex<Vec<LocalHandle>>,
    g_count: AtomicUsize,
}

impl GS {
    fn register(&self) -> LocalHandle {
        Local::register(self)
    }
}

// Only build once
fn build() -> &'static GS {
    static GS: std::sync::OnceLock<GS> = std::sync::OnceLock::new();
    GS.get_or_init(|| GS {
        cell: Mutex::new(Vec::new()),
        g_count: AtomicUsize::new(0),
    })
}

#[derive(Clone)]
struct Local {
    count: Cell<usize>, // Will not be accessed by other threads
}

impl Local {
    fn register(global: &GS) -> LocalHandle {
        let local = LocalHandle {
            local: Box::into_raw(Box::new(Local {
                count: Cell::new(0),
            })),
        };
        global.cell.lock().unwrap().push(local.clone());
        local
    }
}

// NOTE: We need local handle because global must point to the same local object within TLS so local changes are reflected in global states storage
// Implementing
#[derive(Clone)]
struct LocalHandle {
    local: *const Local,
}

unsafe impl Send for LocalHandle {}
unsafe impl Sync for LocalHandle {}

thread_local! {
    static TEST: LocalHandle = build().register();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]

    fn thread_local_test() {
        let gs = build();

        thread::scope(|s| {
            s.spawn(|| {
                println!("Thread 1 - setting local count to 1 and incrementing global");
                let _ = TEST.try_with(|value| {
                    unsafe { (*value.local).count.set(1) };
                });
                let _ = gs
                    .g_count
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
            });
            s.spawn(|| {
                println!("Thread 2 - setting local count to 2 and incrementing global");
                let _ = TEST.try_with(|value| {
                    unsafe { (*value.local).count.set(2) };
                });
                let _ = gs
                    .g_count
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
            });
            s.spawn(|| {
                println!("Thread 3 - setting local count to 3 and incrementing global");
                let _ = TEST.try_with(|value| {
                    unsafe { (*value.local).count.set(3) };
                });
                let _ = gs
                    .g_count
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
            });
        });

        // Loop through global and print local counts and then global count
        for (i, local) in gs.cell.lock().unwrap().iter().enumerate() {
            println!("Thread {} local count: {}", i + 1, unsafe {
                (*local.local).count.get()
            });
        }
        println!(
            "Global count: {}",
            gs.g_count.load(std::sync::atomic::Ordering::Acquire)
        );
    }
}
