// This file is inspired from crossbeams implementation of a skiplist
// (https://github.com/crossbeam-rs/crossbeam)
//
// Copyright 2019 (c) The Crossbeam Project Developers
//
// Licensed under either of the following, at your option:
//
// - Apache License, Version 2.0 (see LICENSE-APACHE or
//   http://www.apache.org/licenses/LICENSE-2.0)
// - MIT License (see LICENSE-MIT or
//   https://opensource.org/licenses/MIT)
//
// Modifications have been made to the original code to use arena backed implementation
//
//
//
// SkipList

// For the skip list node we will be using the flexible array member (FAM) concept from C99.
// I reference this https://john-millikin.com/rust-and-dynamically-sized-thin-pointers for a rust implementation
//
// Header will hold the height of the tower, key len, value len and flags
// Tower [ptr;0] will then server as a marker ptr for the tower atomic pointers
//
// ┌─────────────────────┐
// │ Node header         │
// ├─────────────────────┤
// │ tower[0]            │ level 0
// │ tower[1]            │ level 1
// │ tower[2]            │ level 2
// │ ...                 │ up to height
// ├─────────────────────┤
// │ key bytes           │ key_len
// ├─────────────────────┤
// │ value bytes / ptr   │ val_len or sizeof(ptr)
// └─────────────────────┘

use std::cmp::Ordering as Ord;
use std::marker::PhantomData;
use std::ops::Bound;
use std::ops::Deref;
use std::ops::RangeBounds;
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{alloc::Layout, sync::atomic::AtomicPtr};
use std::{panic, slice};

use crate::key::comparator::{Comparator, DefaultComparator};
use crate::key::internal_key::InternalKeyRef;
use mem::arena::{Arena, ArenaError};

// ------------------------------------------------------

#[derive(Debug)]
pub(crate) enum SkipListError {
    LayoutError(std::alloc::LayoutError),
    Arena(ArenaError),
}

impl From<std::alloc::LayoutError> for SkipListError {
    fn from(err: std::alloc::LayoutError) -> Self {
        SkipListError::LayoutError(err)
    }
}

impl From<mem::arena::ArenaError> for SkipListError {
    fn from(err: mem::arena::ArenaError) -> Self {
        SkipListError::Arena(err)
    }
}

// Max head height for the skip list
// NOTE: Later we may want this configurable
const MAX_HEAD_HEIGHT: usize = 8;

#[repr(C)]
pub(super) struct Header {
    sentinel: NonNull<Node>,
}

impl Header {
    fn new(memory: *mut u8) -> Self {
        // SAFETY: Initializes the header with a sentinel node at the given memory location
        unsafe {
            let header = NonNull::new_unchecked(Node::init_node(
                NonNull::new_unchecked(memory),
                MAX_HEAD_HEIGHT as u16,
                0,
                0,
            ));
            Self { sentinel: header }
        }
    }
}

#[repr(C)]
pub(crate) struct Node {
    // Number of levels of this node
    height: u16,
    //
    key_len: u16,
    //
    value_len: u32,
    //
    // tower is a Flexible Array Member (FAM) - a variable-length array of AtomicPtr<Node> at the end of the struct
    // Because we use Arena allocation, we don't need to track refs on the node or worry about provenance
    pub(crate) tower: [AtomicPtr<Node>; 0],
    //
    // NOTE: Key bytes and value Bytes are stored after the tower in the Arena allocation
}

impl Node {
    //
    //
    fn build_layout(
        height: usize,
        key_len: usize,
        value_len: usize,
    ) -> Result<Layout, SkipListError> {
        // Build the layout for the Node starting with the Self which accounts for the ReprC packed struct and it's fields
        // Height, Key_len, Value_len and Tower (ZST)
        // Then extend the layout beyond that to account for the Height of the tower,
        // The length of the key
        // And the length of the value
        // After Self (Node Struct)

        Ok(Layout::new::<Self>()
            .extend(Layout::array::<AtomicPtr<Node>>(height)?)
            .map_err(SkipListError::LayoutError)?
            .0
            .extend(Layout::array::<u8>(key_len)?)
            .map_err(SkipListError::LayoutError)?
            .0
            .extend(Layout::array::<u8>(value_len)?)
            .map_err(SkipListError::LayoutError)?
            .0)
    }

    #[inline]
    unsafe fn init_node(
        ptr_memory: NonNull<u8>,
        height: u16,
        key_len: u16,
        value_len: u32,
    ) -> *mut Node {
        let node = ptr_memory.as_ptr() as *mut Node;

        // SAFETY: ptr_memory is a NonNull<u8> allocated by Arena::alloc, so it is valid and aligned.
        // We have already been given enough memory to write the Node struct and tower pointers so it is safe to write
        unsafe {
            ptr::write(
                node,
                Node {
                    height,
                    key_len,
                    value_len,
                    tower: [AtomicPtr::new(ptr::null_mut()); 0],
                },
            );
            let tower = Self::tower_ptr(node);

            for i in 0..height as usize {
                tower.add(i).write(AtomicPtr::new(ptr::null_mut()));
            }

            node
        }
    }

    // Pointers to get for the skiplist to handle
    //
    #[inline(always)]
    unsafe fn tower_ptr(node: *mut Node) -> *mut AtomicPtr<Node> {
        // SAFETY: tower is a Flexible Array Member (FAM) at the end of the struct, so adding the offset gives us the tower ptr.
        // We are safe to access the tower ptr because it is the start of the flexible array, and we have already allocated enough memory for it.
        unsafe { (node as *mut u8).add(core::mem::offset_of!(Node, tower)) as *mut AtomicPtr<Node> }
    }

    #[inline(always)]
    unsafe fn node_at_tower_level(node: *mut Node, index: usize) -> *mut AtomicPtr<Node> {
        if !node.is_null() {
            debug_assert!(index < unsafe { (*node).height as usize });
        }
        // SAFETY: We have checked that index is within bounds and that node is not null.
        unsafe { Self::tower_ptr(node).add(index) }
    }

    // SAFETY: We still leave the caller responsible for ensuring that the pointers are compared from a valid arena allocation and not arbitrary pointers.
    #[inline]
    unsafe fn tower_height(node: *mut Node) -> usize {
        // Find the difference between the tower ptr and the key_ptr and then divide by 8
        //
        // SAFETY: Because the two pointers are aligned and are from the same arena allocation we can safely subtract them.
        // Key ptr will always be after the tower ptr in memory, so the difference will be positive and represent the tower height.
        unsafe {
            (Node::key_ptr(node).addr() - Node::tower_ptr(node).addr())
                / std::mem::size_of::<AtomicPtr<Node>>()
        }
    }

    #[inline(always)]
    pub(super) unsafe fn key_ptr(node: *mut Node) -> *mut u8 {
        let key_ptr = unsafe {
            (Self::tower_ptr(node) as *mut u8)
                .add((*node).height as usize * std::mem::size_of::<AtomicPtr<Node>>())
        };
        key_ptr
    }

    #[inline(always)]
    pub(super) unsafe fn value_ptr(node: *mut Node) -> *mut u8 {
        let value_ptr = unsafe { (Self::key_ptr(node) as *mut u8).add((*node).key_len as usize) };
        value_ptr
    }

    unsafe fn alloc(arena: &Arena, height: u16, key_len: u16, value_len: u32) -> *mut Node {
        debug_assert!(height as usize <= MAX_HEAD_HEIGHT);
        if let Ok(layout) =
            Self::build_layout(height as usize, key_len as usize, value_len as usize)
        {
            unsafe {
                let ptr = arena.alloc_raw(layout);
                Self::init_node(ptr, height, key_len, value_len);
                return ptr.as_ptr() as *mut Node;
            };
        } else {
            panic!("Layout failed")
        }
    }

    pub(super) fn get_key_bytes<'a>(node: *mut Node) -> &'a [u8] {
        unsafe { slice::from_raw_parts(Node::key_ptr(node), (*node).key_len as usize) }
    }

    pub(super) fn get_value_bytes<'a>(node: *mut Node) -> &'a [u8] {
        unsafe { slice::from_raw_parts(Node::value_ptr(node), (*node).value_len as usize) }
    }

    pub(super) fn load_next(node: *mut Node, level: usize, ordering: Ordering) -> *mut Node {
        debug_assert!(level < MAX_HEAD_HEIGHT);
        unsafe { (*Self::next(node, level)).load(ordering) }
    }

    pub(super) fn next(node: *mut Node, level: usize) -> *mut AtomicPtr<Node> {
        debug_assert!(level < MAX_HEAD_HEIGHT);
        unsafe { Self::node_at_tower_level(node, level) }
    }
}

// NOTE:
// For the SkipList we want to make sure that certain fields which are concurrently accessed often are given their own cache line
// A great explanation and gathering of sources is in crossbema -> https://github.com/crossbeam-rs/crossbeam/blob/master/crossbeam-utils/src/cache_padded.rs#L150
//
// For now, we will default to aligning to 64 bytes and over time consider using more alignment for different sources

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[repr(align(64))]
struct CachePadded<T> {
    value: T,
}

unsafe impl<T> Send for CachePadded<T> {}
unsafe impl<T> Sync for CachePadded<T> {}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

// We need data for the SkipList such as:
// - Seed for random number generation
// - Entries in the skip list
// - Max level

pub(super) struct Data {
    pub(super) seed: AtomicUsize,
    pub(super) entries: AtomicUsize,
    pub(super) max_level: AtomicUsize,
}

impl Default for Data {
    fn default() -> Self {
        Self {
            seed: AtomicUsize::new(0),
            entries: AtomicUsize::new(0),
            max_level: AtomicUsize::new(MAX_HEAD_HEIGHT),
        }
    }
}

pub(super) struct TraversalCtx {
    // Searched node is Some when we find the node we're searching for - useful for insertions where we can detect duplicates
    pub(super) searched_node: Option<NonNull<Node>>,
    // Predecessors need to be *mut Node because we need to access the node and modify it's next pointers - it's ok to have *mut Node and not AtomicPtr<Node>
    // because we are not changing the node only it's tower pointers
    pub(super) predecessors: [*mut Node; MAX_HEAD_HEIGHT],
    // Successors only need to be *const Node because we only need to modify our own next pointers
    pub(super) successors: [*const Node; MAX_HEAD_HEIGHT],
}

impl TraversalCtx {
    pub(crate) fn new() -> Self {
        Self {
            searched_node: None,
            predecessors: [ptr::null_mut(); MAX_HEAD_HEIGHT],
            successors: [ptr::null(); MAX_HEAD_HEIGHT],
        }
    }
}

impl Default for TraversalCtx {
    fn default() -> Self {
        Self::new()
    }
}

// VictoryDB SkipList is backed by an aligned arena.
// TODO: describe and use diagram

// SkipList
pub(super) struct SkipList {
    pub(super) head: Header,
    data: CachePadded<Data>,
    // We use Arc here because the Comparator is global law for ordering and must be shared across memtables and ssTables
    comparator: Arc<dyn Comparator>,
    // Metrics?
}

/// SkipList is a concurrent lock-free data structure which supports search, scan and insert operations.
/// It operates on nodes backed by an aligned arena. There are no deletions meaning that once a node is inserted it remains in the list until the arena is freed.
/// The structure uses pointer-based locking (AtomicPtr) for concurrent access.
///
/// Nodes form vertical "towers". Higher levels skip over more nodes,
/// allowing logarithmic search time. Traversal starts at the highest
/// level and drops down when the next pointer would overshoot.
///
/// Example layout:
///
/// 3 | HEAD -----> A -----------------------> D
/// 2 | HEAD -----> A -----> B -----> C -----> D
/// 1 | HEAD -----> A -----> B -----> C -----> D -----> E -> F
///       |         |        |        |        |       |    |
///       +---------+--------+--------+--------+-------+----+
///                 A        B        C        D       E    F
///
///
/// /// Example search for key `C`:
///
/// Level 3 : HEAD -----> A
///                       ↓
/// Level 2 :             A -----> B -----> C
///                                         ↓
/// Level 1 :             A        B -----> C   (found)

impl SkipList {
    pub(super) fn new(comparator: Arc<dyn Comparator>, arena: &Arena) -> Self {
        let data = CachePadded {
            value: Data {
                seed: AtomicUsize::new(Self::seed_generator(1)),
                entries: AtomicUsize::new(0),
                max_level: AtomicUsize::new(1),
            },
        };

        let head =
            unsafe { NonNull::new_unchecked(Node::alloc(arena, MAX_HEAD_HEIGHT as u16, 0, 0)) };

        Self {
            head: Header { sentinel: head },
            data,
            comparator,
        }
    }

    pub(super) fn head(&self) -> *mut Node {
        self.head.sentinel.as_ptr()
    }

    // Generates a random seed for the xorshift random number generator
    fn seed_generator(seed: usize) -> usize {
        let mut x: usize = seed;

        x = x.wrapping_add(0x9E3779B97F4A7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
        x = x ^ (x >> 31);

        x
    }

    // Xorshift random number generator - found while reading crossbeams code all credit to those awesome nerds
    // https://github.com/crossbeam-rs/crossbeam/blob/master/crossbeam-skiplist/src/base.rs#L708
    // Original resource:
    // https://en.wikipedia.org/wiki/Xorshift#Initialization
    fn generate_random_level(&self) -> usize {
        //
        let mut starting_num = self.data.seed.load(Ordering::Relaxed);
        starting_num ^= starting_num << 12;
        starting_num ^= starting_num >> 25;
        starting_num ^= starting_num << 27;
        self.data.seed.store(starting_num, Ordering::Relaxed);

        let mut height = std::cmp::min(MAX_HEAD_HEIGHT, starting_num.trailing_zeros() as usize + 1);

        // We may have a height which is way bigger than other nodes in the skip list
        // By looping while height is greater than or equal to 4 we can search to levels below current height and check if the header is null
        // Meaning no other node has reached this height yet. We can the decrement the height by 1 and loop again.
        while height >= 4 {
            let head = Node::next(self.head.sentinel.as_ptr(), height - 2);
            if head.is_null() {
                break;
            }
            height -= 1;
        }

        let mut max_height = self.data.max_level.load(Ordering::Relaxed);
        while height > max_height {
            match self.data.max_level.compare_exchange_weak(
                max_height,
                height,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => max_height = current,
            }
        }

        height
    }

    pub(super) fn search(&self, key: &[u8]) -> TraversalCtx {
        //

        let mut t = TraversalCtx::default();

        // Set t.predecessors to point to the sentinel at each level
        for i in 0..MAX_HEAD_HEIGHT {
            t.predecessors[i] = self.head.sentinel.as_ptr();
        }

        // Load the level to decrement from in while loop
        let mut level = self.data.max_level.load(Ordering::Acquire);

        // We can optimize by skipping levels which have no immediate successors and start straight away at the traversal level
        while level > 0
            && Node::load_next(self.head.sentinel.as_ptr(), level - 1, Ordering::Acquire).is_null()
        {
            level -= 1;
        }

        // We are at a level which we can move right
        // Store the predecessor to keep track and update when we reach the end of the level
        let mut pred = self.head.sentinel.as_ptr();

        while level >= 1 {
            level -= 1;

            // We need to get the current level's node
            let mut curr = Node::load_next(pred, level, Ordering::Acquire);

            // We want to continue moving right until we find a key greater than the search key
            while !curr.is_null() {
                // Need to get the key slice
                let node_key =
                    unsafe { slice::from_raw_parts(Node::key_ptr(curr), (*curr).key_len as usize) };

                match self.comparator.compare(node_key, key) {
                    Ord::Less => {
                        pred = curr;
                        curr = Node::load_next(pred, level, Ordering::Relaxed);
                    }
                    Ord::Equal => {
                        t.searched_node = Some(unsafe { NonNull::new_unchecked(curr) });
                        break;
                    }
                    Ord::Greater => {
                        println!("greater - curr: {:?}", curr);
                        break;
                    }
                    _ => {
                        break;
                    }
                }
            }

            t.predecessors[level] = pred;
            t.successors[level] = curr;
        }
        return t;
    }

    /// Search node returns a raw pointer to the node if found, or null if not found.
    /// It is the caller's responsibility to ensure that the pointer is used correctly and not leaked.
    pub(super) fn search_node(&self, key: &[u8]) -> *mut Node {
        self.search(key).successors[0] as *mut Node
    }

    pub(super) fn load_next(&self, current: *mut Node) -> *mut Node {
        Node::load_next(current, 0, Ordering::Relaxed)
    }

    pub(super) fn first_node(&self) -> *mut Node {
        Node::load_next(self.head(), 0, Ordering::Relaxed)
    }

    /// Inserts a key-value pair into the skip list.
    /// This function is unsafe because it returns a raw pointer to the inserted node and it is the caller's responsibility to ensure that the pointer
    /// is used correctly and not leaked.
    pub(super) unsafe fn insert(&self, key: &[u8], value: &[u8], arena: &Arena) -> *mut Node {
        let mut traversal_ctx = self.search(key);

        if let Some(node) = traversal_ctx.searched_node {
            return node.as_ptr();
        }

        // Build the new node to insert into the searched position
        self.data.entries.fetch_add(1, Ordering::Relaxed);

        let height = self.generate_random_level();
        debug_assert!(height <= MAX_HEAD_HEIGHT);
        debug_assert!(height <= u16::MAX as usize);

        let node_ptr =
            unsafe { Node::alloc(arena, height as u16, key.len() as u16, value.len() as u32) };

        unsafe {
            // Write the key and value into the node
            ptr::copy_nonoverlapping(key.as_ptr(), Node::key_ptr(node_ptr), key.len());
            ptr::copy_nonoverlapping(value.as_ptr(), Node::value_ptr(node_ptr), value.len());
            //
        }

        // Enter into the CAS loop to insert the node at the base level
        // We need to make sure we insert the node successfully before we build the higher levels above to link the rest of the skip list
        loop {
            //
            // We need to take the new node we created and insert the successor at the base level into the node's tower at the base

            let succ = traversal_ctx.successors[0] as *mut Node;
            if !succ.is_null() {
                unsafe {
                    (*Node::next(node_ptr, 0)).store(succ, Ordering::Relaxed);
                }
            }

            unsafe {
                // Now we CAS on the predecessor base pointer to add our new node to it

                let pred = Node::next(traversal_ctx.predecessors[0], 0);

                if (*pred)
                    .compare_exchange(succ, node_ptr, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }

                // We failed to CAS, search again and retry - // TODO: Do we want metrics here to measure contention?
                traversal_ctx = self.search(key);

                if let Some(node) = traversal_ctx.searched_node {
                    return node.as_ptr();
                }
            }
        }

        // Now node has been inserted at base level we need to link the levels above

        'level_loop: for level in 1..height {
            loop {
                // Get the predecessor + successor pointer for the node at the current level in the tower
                let pred = Node::next(traversal_ctx.predecessors[level], level);

                // Link the successor to the new node
                //
                let succ = traversal_ctx.successors[level] as *mut Node;
                if !succ.is_null() {
                    unsafe {
                        (*Node::next(node_ptr, level)).store(succ, Ordering::Relaxed);
                    }
                }

                unsafe {
                    if let Ok(_) = (*pred).compare_exchange(
                        succ,
                        node_ptr,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    ) {
                        break;
                    }
                }

                // If we fail then we must search again?

                traversal_ctx = self.search(key);
            }
        }
        node_ptr
    }

    /// insert_with pre-emptively allocates a node using it's layout into the arena and calls a closure with the node pointer to write directly
    /// into the arena. This allows direct arena allocation without having to pre-allocate objects to pass down.
    pub(crate) unsafe fn insert_with<'a, F>(
        &self,
        key_len: u16,
        value: &[u8],
        arena: &Arena,
        f: F,
    ) -> *mut Node
    where
        F: FnOnce(*mut Node),
    {
        debug_assert!(key_len <= u16::MAX);

        // Build the new node to insert into the searched position
        self.data.entries.fetch_add(1, Ordering::Relaxed);

        let height = self.generate_random_level();
        debug_assert!(height <= MAX_HEAD_HEIGHT);
        debug_assert!(height <= u16::MAX as usize);

        let node_ptr = unsafe { Node::alloc(arena, height as u16, key_len, value.len() as u32) };

        unsafe {
            // Write the key and value into the node
            f(node_ptr);
            ptr::copy_nonoverlapping(value.as_ptr(), Node::value_ptr(node_ptr), value.len());
        }

        // We search down here and optimistically assume the node is not present and allocating is ok to do so
        //
        let key = Node::get_key_bytes(node_ptr);

        let mut traversal_ctx = self.search(key);

        if let Some(node) = traversal_ctx.searched_node {
            return node.as_ptr();
        }
        //
        // Enter into the CAS loop to insert the node at the base level
        // We need to make sure we insert the node successfully before we build the higher levels above to link the rest of the skip list
        loop {
            //
            // We need to take the new node we created and insert the successor at the base level into the node's tower at the base

            let succ = traversal_ctx.successors[0] as *mut Node;
            if !succ.is_null() {
                unsafe {
                    (*Node::next(node_ptr, 0)).store(succ, Ordering::Relaxed);
                }
            }

            unsafe {
                // Now we CAS on the predecessor base pointer to add our new node to it

                let pred = Node::next(traversal_ctx.predecessors[0], 0);

                if (*pred)
                    .compare_exchange(succ, node_ptr, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }

                // We failed to CAS, search again and retry - // TODO: Do we want metrics here to measure contention?
                traversal_ctx = self.search(key);

                if let Some(node) = traversal_ctx.searched_node {
                    return node.as_ptr();
                }
            }
        }

        // Now node has been inserted at base level we need to link the levels above

        'level_loop: for level in 1..height {
            loop {
                // Get the predecessor + successor pointer for the node at the current level in the tower
                let pred = Node::next(traversal_ctx.predecessors[level], level);

                // Link the successor to the new node
                //
                let succ = traversal_ctx.successors[level] as *mut Node;
                if !succ.is_null() {
                    unsafe {
                        (*Node::next(node_ptr, level)).store(succ, Ordering::Relaxed);
                    }
                }

                unsafe {
                    if let Ok(_) = (*pred).compare_exchange(
                        succ,
                        node_ptr,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    ) {
                        break;
                    }
                }

                // If we fail then we must search again?

                traversal_ctx = self.search(key);
            }
        }
        node_ptr
    }

    pub(super) fn iter(&self) -> Iter<'_> {
        let first = Node::load_next(self.head(), 0, Ordering::Relaxed);
        Iter::new(first)
    }

    pub(super) fn seek(&self, key: &[u8]) -> Iter<'_> {
        let ctx = self.search(key);

        if let Some(node) = ctx.searched_node {
            return Iter {
                item: node.as_ptr() as *mut Node,
                _p: PhantomData,
            };
        }

        Iter {
            item: ctx.successors[0] as *mut Node,
            _p: PhantomData,
        }
    }

    pub(super) fn range<'a, R>(&'a self, bound: R) -> RangeIter<'a>
    where
        R: RangeBounds<&'a [u8]>,
    {
        let start = match bound.start_bound() {
            Bound::Excluded(k) => self.seek(*k),
            Bound::Included(k) => self.seek(*k),
            Bound::Unbounded => self.iter(),
        };

        let end_bound = match bound.end_bound() {
            Bound::Included(k) => Bound::Included(*k),
            Bound::Excluded(k) => Bound::Excluded(*k),
            Bound::Unbounded => Bound::Unbounded,
        };

        RangeIter {
            start: start.item,
            end_bound,
        }
    }
}

// Iter is the skiplist low level iterator to implement Rust's Iterator trait - it helps for linear traversal of the skiplist and testing
// Not to be used by DBIterators
pub(super) struct Iter<'a> {
    item: *mut Node,
    _p: PhantomData<&'a ()>,
}

impl<'a> Iter<'a> {
    pub(super) fn new(item: *mut Node) -> Self {
        Self {
            item,
            _p: PhantomData,
        }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = *mut Node;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.item;

        if node.is_null() {
            return None;
        }

        self.item = Node::load_next(node, 0, Ordering::Relaxed);

        Some(node)
    }
}

pub(super) struct RangeIter<'a> {
    start: *mut Node,
    end_bound: Bound<&'a [u8]>,
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = *mut Node;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.start;

        if node.is_null() {
            return None;
        }

        let key = unsafe { Node::get_key_bytes(node) };

        match self.end_bound {
            Bound::Excluded(bound) if key >= bound => return None,
            Bound::Included(bound) if key > bound => return None,
            _ => {}
        }

        // advance iterator
        self.start = unsafe { Node::load_next(node, 0, Ordering::Relaxed) };

        Some(node)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use mem::allocator::*;
    use mem::arena::*;

    mod helpers {}

    //TODO: Add concurrent loom tests

    #[test]
    fn basic_node_layout() {
        //
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 80,
                cap: 160,
            },
            Allocator::System(SystemAllocator::new()),
        );

        // Now we want to alloc a node

        let node = unsafe { Node::alloc(&arena, 1, 1, 0) };
        unsafe {
            ptr::write(Node::key_ptr(node), 24);
        }

        // Validate the key_ptr being 24
        unsafe {
            // If we did Box::from(T) here, we would break the memory ownership - arena owns this memory, so we don't want to hand it to Box because
            // it would try to drop the memory and destruct it invalidating our arena
            // We need to either return a reference slice OR we copy out the bytes into a vec and then into box owned heap
            let s = std::slice::from_raw_parts(
                Node::key_ptr(node) as *const u8,
                (*node).key_len as usize,
            );
            assert_eq!(s, &[24u8; 1]);
            let b = s.to_vec().into_boxed_slice();

            // Now if we drop the box, our arena is still valid
            drop(b); // If we did Box::from(s) it may not immediately cause a panic BUT the premise is that we do not want to take ownership of arena memory
            let s2 = std::slice::from_raw_parts(
                Node::key_ptr(node) as *const u8,
                (*node).key_len as usize,
            );
            assert_eq!(s2, &[24u8; 1]);
        };
    }

    #[test]
    fn level_access() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 80,
                cap: 160,
            },
            Allocator::System(SystemAllocator::new()),
        );
        let skip = SkipList::new(Arc::new(DefaultComparator {}), &arena);

        // Let's get the base level
        let base = Node::next(skip.head.sentinel.as_ptr(), 0);
        assert!(
            unsafe { (*base).load(Ordering::Relaxed).is_null() },
            "base level should be null"
        );

        // Allocate a node at base level
        let node = unsafe { Node::alloc(&arena, 1, 5, 2) };

        unsafe {
            ptr::copy(
                "hello".as_bytes().as_ptr(),
                Node::key_ptr(node),
                (*node).key_len as usize,
            );
        }

        // Write node into the skip list at the base level
        let _ = unsafe {
            (*base).compare_exchange(
                (*base).load(Ordering::Relaxed),
                node,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
        }
        .unwrap();

        // Verify the node was written at base level

        // Check if key is in arena and then check if we can fetch the key from the node
        let key_ptr = unsafe { Node::key_ptr(node) };
        let key = unsafe {
            String::from_utf8_lossy(std::slice::from_raw_parts(
                key_ptr,
                (*node).key_len as usize,
            ))
        };

        assert_eq!(key, "hello");
    }

    #[test]
    fn node_tower_height() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 80,
                cap: 160,
            },
            Allocator::System(SystemAllocator::new()),
        );
        let node = unsafe { Node::alloc(&arena, 1, 5, 2) };

        unsafe {
            assert_eq!(Node::tower_height(node), 1);
        }

        let node2 = unsafe { Node::alloc(&arena, 3, 5, 2) };
        unsafe {
            assert_eq!(Node::tower_height(node2), 3);
        }
    }

    #[test]
    fn basic_search() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 320,
                cap: 640,
            },
            Allocator::System(SystemAllocator::new()),
        );

        let skip = SkipList::new(Arc::new(DefaultComparator {}), &arena);

        // Keys we want:
        // Apple
        // Mango
        // Pear

        unsafe { skip.insert(b"Apple", b"Green", &arena) };
        unsafe { skip.insert(b"Mango", b"Yellow", &arena) };
        unsafe { skip.insert(b"Pear", b"Brown", &arena) };

        let ctx = skip.search(b"Apple");

        assert!(ctx.searched_node.is_some());
        assert_eq!(
            Node::get_key_bytes(ctx.searched_node.unwrap().as_ptr()),
            b"Apple"
        );

        // Search for key that isn't there - should get predecessor
        let ctx = skip.search(b"Orange");

        assert!(ctx.searched_node.is_none());
        assert_eq!(Node::get_key_bytes(ctx.predecessors[0]), b"Mango");
    }

    #[test]
    fn basic_insert() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 320,
                cap: 640,
            },
            Allocator::System(SystemAllocator::new()),
        );

        let skip = SkipList::new(Arc::new(DefaultComparator {}), &arena);

        //
        unsafe { skip.insert(b"Apple", b"Green", &arena) };
        unsafe { skip.insert(b"Mango", b"Yellow", &arena) };
        unsafe { skip.insert(b"Pear", b"Brown", &arena) };

        // Search for Apple should give us Apple
        let result = unsafe { skip.search(b"Apple") };
        assert!(result.searched_node.is_some());
        let node = result.searched_node.unwrap();
        assert_eq!(b"Apple", Node::get_key_bytes(node.as_ptr()));

        // Search for Mango should give us Mango
        let result = unsafe { skip.search(b"Mango") };
        assert!(result.searched_node.is_some());
        let node = result.searched_node.unwrap();
        assert_eq!(b"Mango", Node::get_key_bytes(node.as_ptr()));

        // Search for Pear should give us Pear
        let result = unsafe { skip.search(b"Pear") };
        assert!(result.searched_node.is_some());
        let node = result.searched_node.unwrap();
        assert_eq!(b"Pear", Node::get_key_bytes(node.as_ptr()));

        // Now random key search of Orange should be between Mango and Pear
        let result = unsafe { skip.search(b"Orange") };
        assert!(result.searched_node.is_none());
        let orange_pred = Node::get_key_bytes(result.predecessors[0]);
        let orange_succ = Node::get_key_bytes(result.successors[0] as *mut Node);
        assert_eq!(b"Mango", orange_pred);
        assert_eq!(b"Pear", orange_succ);
    }

    #[test]
    fn direct_insert() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 320,
                cap: 640,
            },
            Allocator::System(SystemAllocator::new()),
        );

        let skip = SkipList::new(Arc::new(DefaultComparator {}), &arena);

        struct tricky_key<'a> {
            key: &'a [u8],
            logic: u16,
        }

        impl tricky_key<'_> {
            fn len(&self) -> u16 {
                self.key.len() as u16 + 2
            }

            fn to_vec(&self) -> Vec<u8> {
                let mut v = Vec::with_capacity(self.key.len() + 2);

                v.extend_from_slice(self.key);
                v.extend_from_slice(&self.logic.to_le_bytes());

                v
            }
        }

        // Insert a tricky key direct to arena without making a separate allocation
        //

        let key_1 = tricky_key {
            key: "Apple".as_bytes(),
            logic: 1,
        };
        let key_2 = tricky_key {
            key: "Mango".as_bytes(),
            logic: 2,
        };
        let key_3 = tricky_key {
            key: "Pear".as_bytes(),
            logic: 3,
        };

        unsafe {
            let _ = skip.insert_with(key_1.len(), b"apple_value", &arena, |n| {
                ptr::copy_nonoverlapping(key_1.key.as_ptr(), Node::key_ptr(n), key_1.key.len());
                ptr::copy_nonoverlapping(
                    key_1.logic.to_le_bytes().as_ptr(),
                    Node::key_ptr(n).add(key_1.key.len()),
                    2,
                )
            });
            let _ = skip.insert_with(key_2.len(), b"mango_value", &arena, |n| {
                ptr::copy_nonoverlapping(key_2.key.as_ptr(), Node::key_ptr(n), key_2.key.len());
                ptr::copy_nonoverlapping(
                    key_2.logic.to_le_bytes().as_ptr(),
                    Node::key_ptr(n).add(key_2.key.len()),
                    2,
                )
            });
            let _ = skip.insert_with(key_3.len(), b"pear_value", &arena, |n| {
                ptr::copy_nonoverlapping(key_3.key.as_ptr(), Node::key_ptr(n), key_3.key.len());
                ptr::copy_nonoverlapping(
                    key_3.logic.to_le_bytes().as_ptr(),
                    Node::key_ptr(n).add(key_3.key.len()),
                    2,
                )
            });
        }

        // Search for Apple should give us Apple

        let apple = key_1.to_vec();
        let result = unsafe { skip.search(&apple) };
        assert!(result.searched_node.is_some());
        let node = result.searched_node.unwrap();
        assert_eq!(&apple, Node::get_key_bytes(node.as_ptr()));

        // Search for Mango should give us Mango
        let mango = key_2.to_vec();
        let result = unsafe { skip.search(&mango) };
        assert!(result.searched_node.is_some());
        let node = result.searched_node.unwrap();
        assert_eq!(&mango, Node::get_key_bytes(node.as_ptr()));
    }

    #[test]
    fn basic_iter() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 320,
                cap: 640,
            },
            Allocator::System(SystemAllocator::new()),
        );

        let skip = SkipList::new(Arc::new(DefaultComparator {}), &arena);

        //
        unsafe { skip.insert(b"Apple", b"Green", &arena) };
        unsafe { skip.insert(b"Mango", b"Yellow", &arena) };
        unsafe { skip.insert(b"Pear", b"Brown", &arena) };

        let mut keys: Vec<&[u8]> = Vec::with_capacity(3);

        keys.push(b"Apple");
        keys.push(b"Mango");
        keys.push(b"Pear");

        for (i, n) in skip.iter().enumerate() {
            assert_eq!(keys[i], Node::get_key_bytes(n));
        }

        let expected_result_from_seek = vec![1, 2];

        for (i, n) in skip.seek(b"Berry").enumerate() {
            assert_eq!(keys[expected_result_from_seek[i]], Node::get_key_bytes(n))
        }

        // Check next method
        let mut iter = skip.iter();
        assert_eq!(Node::get_key_bytes(iter.next().unwrap()), b"Apple");
        assert_eq!(Node::get_key_bytes(iter.next().unwrap()), b"Mango");
        assert_eq!(Node::get_key_bytes(iter.next().unwrap()), b"Pear");
        assert!(iter.next().is_none());
    }

    #[test]
    fn basic_range() {
        let arena = Arena::new(
            ArenaPolicy {
                block_size: 320,
                cap: 640,
            },
            Allocator::System(SystemAllocator::new()),
        );

        let skip = SkipList::new(Arc::new(DefaultComparator {}), &arena);

        //
        unsafe { skip.insert(b"Apple", b"Green", &arena) };
        unsafe { skip.insert(b"Mango", b"Yellow", &arena) };
        unsafe { skip.insert(b"Strawberry", b"Brown", &arena) };

        let mut result = Vec::with_capacity(2);

        result.push(b"Apple");
        result.push(b"Mango");

        // Range check
        for (i, n) in skip
            .range(b"Apple".as_slice()..b"Strawberry".as_slice())
            .enumerate()
        {
            assert_eq!(result[i], (Node::get_key_bytes(n)));
        }
    }
}
