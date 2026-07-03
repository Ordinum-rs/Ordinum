// Arena runs the management of it's memory allocation given to it by the allocator
//
//

// The Arena will be used like a simple bump allocator for it's memory region
//
// For reference I used:
// https://fitzgen.com/2019/11/01/always-bump-downwards.html
// https://www.williamfedele.com/blog/arena-allocators
//
// This describes a good rust approach to alignment rounding and a recommendation of bumping downward as an optimisation
// RocksDB also uses an arena allocator but allocates from either end - front being aligned and back being unaligned
// For now, we'll use a simple bump allocator that allocates from the front and carefully control alignment of the skip nodes and byte data - if we notice improvements to be made then we can change
// Arena implementation to use a more efficient allocator
//
// Because we'll be allocating T (such as skiplist Nodes) and bytes (already aligned) we need to makes sure that what we write to in the heap is aligned
//

use std::alloc::Layout;
use std::ptr::NonNull;
use std::ptr::*;
use std::sync::{
    Mutex,
    atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crate::arena::allocator::Allocator;

// Constants
const KB: usize = 1024;
const MB: usize = KB;

//
#[derive(Debug)]
pub struct ArenaPolicy {
    pub block_size: usize,
    pub cap: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArenaError {
    AllocationError(usize),
    Overflow,
    ChunkDoubleCheckFailed,
    ArenaFull,
}

#[derive(Debug)]
struct Chunk {
    mem: Box<[u8]>,
    bump: AtomicUsize,
}

impl Chunk {
    fn new(mem: Box<[u8]>) -> Self {
        Self {
            mem,
            bump: AtomicUsize::new(0),
        }
    }

    fn get_bump(&self) -> usize {
        self.bump.load(Ordering::Relaxed)
    }

    fn get_mem(&self) -> &[u8] {
        self.mem.as_ref()
    }

    fn get_mem_ptr(&self) -> *const u8 {
        self.mem.as_ptr()
    }
}

/*
 * NOTE:
 * A future optimisation is that we want a double-ended bump arena where we allocate aligned from the front and un-aligned from the back
 * Also different block/chunk sizes so if we have a Layout which is bigger than 1/4 of a regular block we allocate a irregular block and insert the memory into there
 * and carry on using the regular block
 * We can also add a small block inline optimisation possibly similar to Rocks
 */

/// Arena is responsible for holding blocks of memory and managing memory allocation into those blocks. It will handle alignment and block allocation.
/// Only Memtables will hold an arena.
///
/// Arena makes no attempt ensure pointers are not leaked or that memory being written is correclty aligned.
/// It is the responsibility of the caller to maintain that the data written is the same as the layout provided which arena used to reserve memory for.
///
/// For this reason, no specific Drop implementation is needed. Instead, we rely on memtables to implement Drop to know when an arena can be deallocated.
pub struct Arena {
    current_chunk: AtomicPtr<Chunk>,
    chunks: Mutex<Vec<Box<Chunk>>>,
    allocated_bytes: AtomicUsize,
    memory_used: AtomicUsize,
    // TODO: May want total padding bytes? for later optimization
    allocator: Allocator,
    policy: ArenaPolicy,
}

impl Arena {
    pub fn new(policy: ArenaPolicy, allocator: Allocator) -> Self {
        assert!(
            policy.block_size.is_power_of_two(),
            "Arena block size must be a power of two"
        );

        // assert!(
        //     policy.block_size >= 4 * 1024,
        //     "Arena block size must be at least 4KB"
        // );

        assert!(
            policy.cap >= policy.block_size,
            "Arena capacity must be >= block size"
        );

        assert!(
            policy.cap % policy.block_size == 0,
            "Arena capacity must be a multiple of block size"
        );

        assert!(
            policy.cap <= isize::MAX as usize,
            "Arena capacity exceeds addressable memory range"
        );

        let heap = unsafe { allocator.allocate(policy.block_size) };
        let chunk = Box::new(Chunk::new(heap));

        // Take a raw pointer from the borrow of the box so we don't consume the box and can still store it in the vec
        let chunk_ptr: *mut Chunk = (&*chunk as *const Chunk).cast_mut();

        let block_cap = policy.cap / policy.block_size;
        let mut chunks = Vec::with_capacity(block_cap);
        chunks.push(chunk);

        Self {
            current_chunk: AtomicPtr::new(chunk_ptr),
            chunks: Mutex::new(chunks),
            allocated_bytes: AtomicUsize::new(policy.block_size),
            memory_used: AtomicUsize::new(0),
            allocator,
            policy,
        }
    }

    fn get_chunk(&self) -> &mut Chunk {
        unsafe { &mut *self.current_chunk.load(Ordering::Acquire) }
    }

    #[inline(always)]
    fn alignment_check(&self, bump: usize, layout: Layout) -> Result<(usize, usize), ArenaError> {
        // Get the next required offset based on the layout alignment
        debug_assert!(layout.align().is_power_of_two());
        let aligned = (bump + (layout.align() - 1)) & !(layout.align() - 1);

        let next = aligned
            .checked_add(layout.size())
            .ok_or(ArenaError::Overflow)?;

        if next > self.get_chunk().mem.len() {
            return Err(ArenaError::Overflow);
        }

        Ok((aligned, next))
    }

    // https://algomaster.io/learn/concurrency-interview/compare-and-swap
    // Shows a simple CAS loop where we get value Relaxed - compute the new value we want and try to CAS - if we fail we try again.
    //

    // NOTE: I've made the closure unsafe and it is up to the caller to ensure that the Layout and write to the pointer are correct.
    #[inline(always)]
    pub unsafe fn alloc_raw(&self, layout: Layout) -> NonNull<u8> {
        //

        loop {
            // Get chunk from current_chunk
            let chunk = self.get_chunk();

            // We get relaxed bump here because we will double check if CAS if it fails we try to get bump again in the loop
            let bump = chunk.bump.load(Ordering::Relaxed);

            match self.alignment_check(bump, layout) {
                Err(_) => {
                    // If we fail alignment check we try_new_chunk
                    match self.try_new_chunk(layout) {
                        // Return out
                        Err(e) => panic!("Arena allocation failed: {:?}", e), // TODO: Decide if we can try a backup route for this before panicking
                        Ok(_) => continue,
                    };
                }
                Ok((aligned, next)) => {
                    // If CAS works we can write to the arena heap
                    if chunk
                        .bump
                        .compare_exchange_weak(bump, next, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        // If we are ok then we can write to the arena heap by passing the aligned pointer into closure
                        //

                        let base = chunk.mem.as_mut_ptr();

                        let ptr = unsafe { NonNull::new_unchecked(base.add(aligned)) };

                        // Update meta data
                        self.memory_used.fetch_add(layout.size(), Ordering::Relaxed);

                        return ptr;
                    }

                    // Another thread beat us - we try again
                    std::hint::spin_loop();
                }
            }
        }
    }

    #[inline(always)]
    pub unsafe fn alloc_raw_fallback(&self, layout: Layout) -> Result<NonNull<u8>, ArenaError> {
        //

        loop {
            // Get chunk from current_chunk
            let chunk = self.get_chunk();

            // We get relaxed bump here because we will double check if CAS if it fails we try to get bump again in the loop
            let bump = chunk.bump.load(Ordering::Relaxed);

            match self.alignment_check(bump, layout) {
                Err(_) => {
                    // If we fail alignment check we try_new_chunk
                    match self.try_new_chunk(layout) {
                        // Return out
                        Err(e) => return Err(e),
                        Ok(_) => continue,
                    };
                }
                Ok((aligned, next)) => {
                    // If CAS works we can write to the arena heap
                    if chunk
                        .bump
                        .compare_exchange_weak(bump, next, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                    {
                        // If we are ok then we can write to the arena heap by passing the aligned pointer into closure
                        //

                        let base = chunk.mem.as_mut_ptr();

                        let ptr = unsafe { NonNull::new_unchecked(base.add(aligned)) };

                        // Update meta data
                        self.memory_used.fetch_add(layout.size(), Ordering::Relaxed);

                        return Ok(ptr);
                    }

                    // Another thread beat us - we try again
                    std::hint::spin_loop();
                }
            }
        }
    }

    #[inline(always)]
    fn try_new_chunk(&self, layout: Layout) -> Result<(), ArenaError> {
        // We need to lock and then check we are still ok to mutate the vec and pointer
        let mut lock = self.chunks.lock().unwrap();

        let chunk = self.get_chunk();

        let bump = chunk.bump.load(Ordering::Relaxed);

        // Now we double check we are still good to mutate by checking size and alignment
        if let Ok(_) = self.alignment_check(bump, layout) {
            return Ok(());
        }

        // We failed the size and alignment check meaning we need to allocate a new chunk

        // We need to check that by adding a new chunk we don't exceed the cap
        if self.allocated_bytes.load(Ordering::Relaxed) + self.policy.block_size > self.policy.cap {
            return Err(ArenaError::ArenaFull);
        }

        self.allocated_bytes
            .fetch_add(self.policy.block_size, Ordering::Relaxed);

        // Now we allocate a new chunk of memory from the allocator
        let heap = unsafe { self.allocator.allocate(self.policy.block_size) };
        let new_chunk = Box::new(Chunk::new(heap));
        let chunk_ptr: *mut Chunk = (&*new_chunk as *const Chunk).cast_mut();

        lock.push(new_chunk);

        // Now we need to atomically update the current chunk pointer
        self.current_chunk.store(chunk_ptr, Ordering::Release);

        Ok(())
    }

    #[inline(always)]
    fn blocks_used(&self) -> usize {
        let used = self.allocated_bytes.load(Ordering::Relaxed);
        used / self.policy.block_size
    }

    #[inline(always)]
    fn max_bytes(&self) -> usize {
        self.policy.cap
    }

    #[inline(always)]
    fn number_of_blocks(&self) -> usize {
        self.policy.cap / self.policy.block_size
    }

    #[inline(always)]
    pub fn memory_used(&self) -> usize {
        let used = self.memory_used.load(Ordering::Relaxed);
        used
    }

    #[inline]
    pub fn get_current_init_slice(&self) -> &[u8] {
        let chunk = self.get_chunk();

        let bump = chunk.get_bump();

        unsafe { &*slice_from_raw_parts(chunk.mem.as_ptr(), bump) }
    }

    pub fn print_address(&self) {
        println!(
            "arena current address: {:p}",
            self.current_chunk.load(Ordering::Relaxed)
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::arena::allocator::{Allocator, SystemAllocator};

    use super::*;
    use std::thread::{self};

    #[test]
    fn competing_allocs() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 2000,
                cap: 64000,
            },
            Allocator::System(SystemAllocator::new()),
        );

        let threads = 10;
        let iters = 1000;

        thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|| {
                    for _ in 0..iters {
                        unsafe {
                            let _ = arena.alloc_raw(Layout::new::<u32>());
                        }
                    }
                });
            }
        });

        let expected = threads * iters * std::mem::size_of::<u32>();

        // 1. Total memory used must match
        assert_eq!(arena.memory_used.load(Ordering::Relaxed), expected);

        // 2. No chunk overflow
        let chunks = arena.chunks.lock().unwrap();

        for chunk in chunks.iter() {
            let bump = chunk.bump.load(Ordering::Relaxed);
            assert!(bump <= chunk.mem.len());
        }

        // 3. Cap respected
        assert!(arena.allocated_bytes.load(Ordering::Relaxed) <= arena.max_bytes());

        println!("chunks used: {}", chunks.len());
    }

    #[test]
    fn arena_sizing() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 10,
                cap: 20,
            },
            Allocator::System(SystemAllocator::new()),
        );

        unsafe {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let ptr = arena.alloc_raw(layout);
            std::ptr::write(ptr.as_ptr() as *mut u64, 123);
        }

        let chunk = arena.get_chunk();
        let mem = chunk.get_mem();
        let bump = chunk.get_bump();

        assert_eq!(arena.memory_used(), 8);
        assert!(bump <= mem.len());
        assert!(arena.max_bytes() == 20);
        assert!(arena.number_of_blocks() == 2);
    }

    #[test]
    #[should_panic]
    fn alignment_bitwise() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 10,
                cap: 10,
            },
            Allocator::System(SystemAllocator::new()),
        );

        // First lets alloc a char (1-byte)
        let layout = Layout::new::<u8>();
        unsafe {
            let ptr = arena.alloc_raw(layout);
            ptr.write(2u8);
        }

        let layout = Layout::new::<u32>();

        unsafe {
            arena.alloc_raw(layout);
        }

        // Should get overflow error
        let l3 = Layout::new::<u64>();
        unsafe {
            let _ = arena.alloc_raw(l3);
        }
    }

    #[test]
    fn chunk_change() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 10,
                cap: 20,
            },
            Allocator::System(SystemAllocator::new()),
        );

        // We nede to alloacate a u32 - then allocate a u16 - allocating another u32 should trigger a chunk allocation

        let layout_u32 = Layout::new::<u32>();
        unsafe {
            let ptr = arena.alloc_raw(layout_u32);
            ptr.write(42);
        }
        let layout_u16 = Layout::new::<u16>();
        unsafe {
            let ptr = arena.alloc_raw(layout_u16);
            ptr.write(12);
        }

        // Take copy of current chunk ptr
        let current = arena.current_chunk.load(Ordering::Relaxed);

        let layout_u32_2 = Layout::new::<u32>();
        unsafe {
            let ptr = arena.alloc_raw(layout_u32_2);
            ptr.write(67)
        }

        let check = arena.current_chunk.load(Ordering::Relaxed);
        assert!(current != check);

        assert!(arena.blocks_used() == 2);
        assert!(arena.chunks.lock().unwrap().len() == 2);
    }

    // TODO: Need to write assertions for this
    #[test]
    fn tower_and_bytes() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 20,
                cap: 40,
            },
            Allocator::System(SystemAllocator::new()),
        );

        #[repr(C)]
        struct Node {
            refs_and_height: AtomicUsize,
            key_len: u32, // We lose nothing by making it a u32
            value_len: u32,
            tower: [AtomicPtr<Node>; 0],
        }

        unsafe {
            let ptr = arena.alloc_raw(Layout::new::<Node>());
            ptr.cast::<Node>().write(Node {
                key_len: 1,
                value_len: 2,
                refs_and_height: AtomicUsize::new(3),
                tower: [],
            });
        }

        println!("size of node {:?}", std::mem::size_of::<Node>());

        // Now we try to add a byte or element in the tower array
        unsafe {
            let _ = arena.alloc_raw(Layout::new::<u8>());
        }

        unsafe {
            let _ = arena.alloc_raw(Layout::new::<u8>());
        }

        println!("current chunk {:?}", arena.get_current_init_slice());
        println!("memory used {:?}", arena.memory_used());
    }
}
