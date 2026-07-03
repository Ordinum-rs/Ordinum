// Arena allocator which manages the arenas that each memtable uses
//
// What do we need:
// Arena invariants:
//
// - An arena has a fixed capacity.
// - An arena has exactly one active owner at a time (a memtable).
// - While active:
// - allocations are bump-only
// - memory is never reclaimed
// - An arena is reset only when it has no active owner.
// - Reset invalidates all pointers previously allocated from it.

use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

// Arean Allocator must only allocate one arena at a time and give ownership of that memory to an arena

pub enum Allocator {
    System(SystemAllocator),
    NUMA,
    HugePage,
    MMap,
    Test,
}

impl Allocator {
    pub unsafe fn allocate(&self, size: usize) -> Box<[u8]> {
        match self {
            Allocator::System(allocator) => unsafe { allocator.allocate(size) },
            _ => unimplemented!(),
        }
    }
}

// Default Allocator for allocating chunks to arena
pub struct SystemAllocator {}

impl SystemAllocator {
    pub fn new() -> Self {
        Self {}
    }

    // Default Allocator for allocating chunks to arena
    pub unsafe fn allocate(&self, size: usize) -> Box<[u8]> {
        #[cfg(debug_assertions)]
        {
            // Zeroed memory in debug — safe to inspect fully
            // Fully initialized memory in debug - using 1 here so we can see what exactly got allocated
            vec![1u8; size].into_boxed_slice()
        }

        #[cfg(not(debug_assertions))]
        {
            // Uninitialized memory in release — max performance
            let heap = Box::<[u8]>::new_uninit_slice(size);
            heap.assume_init()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate() {
        let alloc = SystemAllocator::new();
        let chunk = unsafe { alloc.allocate(10) };

        println!("chunk size {:?}", chunk.len());
    }
}
