// Memtable

use std::fmt::Display;
use std::marker::PhantomData;
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::atomic::{AtomicU8, AtomicU16};

use crate::db::write_batch::WBatch;
use crate::iterator::internal_iterator::InternalIterator;
use crate::key::comparator::Comparator;
use crate::key::internal_key::{InternalKeyRef, OperationType, encode_trailer};
use crate::memtable::skip_list::{Iter, Node, SkipList};
use mem::allocator::Allocator;
use mem::arena::ArenaSize;
use mem::arena::{Arena, ArenaPolicy};

pub(crate) type MemID = u64;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MemReturn<'a> {
    NotFound,
    Merge,
    Deleted,
    Value(&'a [u8]),
}

#[repr(u8)]
enum MemLifeCycle {
    Active = 1,
    Freezing = 2,
    Frozen = 3,
    Flushing = 4,
    Flushed = 5,
}

impl Display for MemLifeCycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemLifeCycle::Active => write!(f, "Active"),
            MemLifeCycle::Freezing => write!(f, "Freezing"),
            MemLifeCycle::Frozen => write!(f, "Frozen"),
            MemLifeCycle::Flushing => write!(f, "Flushing"),
            MemLifeCycle::Flushed => write!(f, "Flushed"),
        }
    }
}

impl From<u8> for MemLifeCycle {
    fn from(value: u8) -> Self {
        match value {
            1 => MemLifeCycle::Active,
            2 => MemLifeCycle::Freezing,
            3 => MemLifeCycle::Frozen,
            4 => MemLifeCycle::Flushing,
            5 => MemLifeCycle::Flushed,
            _ => panic!("Invalid MemLifeCycle value"),
        }
    }
}

pub(crate) trait MemtableState {
    const NAME: &'static str;
}

#[derive(Debug)]
pub(crate) struct Mutable {}
impl MemtableState for Mutable {
    const NAME: &'static str = "Mutable";
}

#[derive(Debug)]
pub(crate) struct Immutable {}
impl MemtableState for Immutable {
    const NAME: &'static str = "Immutable";
}

#[derive(Debug)]
pub(crate) struct Flushed {}
impl MemtableState for Flushed {
    const NAME: &'static str = "Flushed";
}

impl Display for Flushed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Flushed")
    }
}

// Main Memtable - It is formed as a typestate handle over an Inner which is shared
pub(crate) struct Memtable<S: MemtableState> {
    _state: PhantomData<S>,
    inner: Arc<MemtableInner>,
}

impl<S: MemtableState> Display for Memtable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Memtable<{}> {{ {} }} ", S::NAME, self.inner)
    }
}

impl<S: MemtableState> Clone for Memtable<S> {
    fn clone(&self) -> Self {
        Self {
            _state: PhantomData,
            inner: self.inner.clone(),
        }
    }
}

impl<S: MemtableState> Memtable<S> {
    unsafe fn encode_key(&self, ptr: *mut Node, user_key: &[u8], seq_no: u32, op_type: u32) {
        todo!()
    }
}

// Reader special wrapper for superversion
// Type state helps for paths, but in the superversion read path, all memtables are read only so for the mutable memtable
// it is beneficial to just expose the MemtableInner with a light wrapper over it which is specific for the reader in superversion and allows strict access
// to read-only methods with no control flow access

pub(crate) struct ReadableMemtable {
    inner: Arc<MemtableInner>,
}

impl ReadableMemtable {
    // TODO:
    // Safe readable methods
}

impl Memtable<Mutable> {
    //
    // TODO: This should not be allowed as Mutable memtables should not be able to create more mutable memtables
    // Called should do Memtable::new() and get mutable mem which would happen during a rotation
    pub(crate) fn new(
        id: MemID,
        arena_size: ArenaPolicy,
        allocator: Allocator,
        comp: Arc<dyn Comparator>,
    ) -> Self {
        Self {
            _state: PhantomData,
            inner: Arc::new(MemtableInner::new(id, arena_size, allocator, comp)),
        }
    }

    pub(crate) fn readable_memtable(&self) -> ReadableMemtable {
        ReadableMemtable {
            inner: Arc::clone(&self.inner),
        }
    }

    pub(crate) fn insert(&self, key: &[u8], value: &[u8]) {
        self.inner.insert(key, value)
    }

    // TODO: Do we want the Value(v) to include the key and value?
    pub(crate) fn get(&self, key: &[u8]) -> MemReturn<'_> {
        if let Some((skip_key, v)) = self.inner.first_ge(key) {
            let sk = InternalKeyRef::from(skip_key);
            let lookup = InternalKeyRef::from(key.as_ref());

            if sk.user_key != lookup.user_key {
                return MemReturn::NotFound;
            }

            match sk.op.into() {
                OperationType::Put => MemReturn::Value(v),
                OperationType::Delete => MemReturn::Deleted,
                OperationType::Merge => MemReturn::Merge,
                _ => unreachable!(),
            }
        } else {
            MemReturn::NotFound
        }
    }

    pub(crate) fn iter(&self) -> MemtableIterator<'_> {
        self.inner.iter()
    }
}

pub(super) struct MemtableInner {
    id: MemID,
    highest_seqno: AtomicU64,
    size: AtomicU64,
    requested_rotation: AtomicBool,
    lifecycle: AtomicU8,
    arena: Arena,
    skiplist: SkipList,
}

// SAFETY:
//
// All node memory is arena-backed and never moved.
// Nodes are only reclaimed when the memtable is dropped.
// Forward links are atomic.
// Insert/search operations use proper memory ordering.
// No mutable references to nodes are exposed externally.
//
unsafe impl Send for MemtableInner {}
unsafe impl Sync for MemtableInner {}

impl Display for MemtableInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ lifecycle: {} }}",
            Into::<MemLifeCycle>::into(self.lifecycle.load(Ordering::Relaxed)),
        )
    }
}

impl MemtableInner {
    fn new(
        id: MemID,
        arena_size: ArenaPolicy,
        allocator: Allocator,
        comp: Arc<dyn Comparator>,
    ) -> Self {
        let arena = Arena::new(arena_size, allocator);
        let skiplist = SkipList::new(comp.clone(), &arena);
        Self {
            id,
            highest_seqno: AtomicU64::new(0),
            size: AtomicU64::new(0),
            requested_rotation: AtomicBool::new(false),
            lifecycle: AtomicU8::new(MemLifeCycle::Active as u8),
            arena: arena,
            skiplist,
        }
    }

    fn first_ge(&self, key: &[u8]) -> Option<(&[u8], &[u8])> {
        let node = self.skiplist.search(key).successors[0];
        if !node.is_null() {
            return Some((
                Node::get_key_bytes(node as *mut Node),
                Node::get_value_bytes(node as *mut Node),
            ));
        }
        None
    }

    fn insert(&self, key: &[u8], value: &[u8]) {
        let _ = unsafe { self.skiplist.insert(key, value, &self.arena) };
    }

    // NOTE: If we insert direct we have to make sure that the internal key seq no is greater than the highest seq no so we don't fail on insert and alloc
    // A dead node
    // TODO: We could create a fallback where if we need to we can use the TLS Ephemeral buffer to allocate the internal key and insert
    fn insert_direct(&self, user_key: &[u8], seq_no: u64, op_type: OperationType, value: &[u8]) {
        let user_key_len = user_key.len();

        unsafe {
            self.skiplist
                .insert_with((user_key_len + 8) as u16, value, &self.arena, |node_ptr| {
                    // Insert the user key
                    ptr::copy_nonoverlapping(
                        user_key.as_ptr(),
                        Node::key_ptr(node_ptr),
                        user_key_len,
                    );
                    // Insert the trailer
                    ptr::copy_nonoverlapping(
                        encode_trailer(seq_no, op_type).as_ptr(),
                        Node::key_ptr(node_ptr).add(user_key_len),
                        8,
                    );
                });
        }
    }

    fn iter(&self) -> MemtableIterator<'_> {
        MemtableIterator {
            sl: &self.skiplist,
            item: Node::load_next(self.skiplist.head(), 0, Ordering::Relaxed),
            current: None,
        }
    }

    fn iter_from(&self, key: &[u8]) -> MemtableIterator<'_> {
        MemtableIterator {
            sl: &self.skiplist,
            item: self.skiplist.search_node(key),
            current: None,
        }
    }
}

pub(crate) struct MemtableIterator<'a> {
    sl: &'a SkipList,
    item: *mut Node,
    current: Option<NonNull<Node>>,
}

impl<'a> MemtableIterator<'a> {
    pub(crate) fn internal_key(&self) -> Option<InternalKeyRef<'_>> {
        self.current
            .map(|curr| InternalKeyRef::from(Node::get_key_bytes(curr.as_ptr())))
    }
}

impl<'a> InternalIterator for MemtableIterator<'a> {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn seek_to_first(&mut self) {
        self.item = Node::load_next(self.sl.head(), 0, Ordering::Relaxed);
        self.current = NonNull::new(self.item)
    }

    fn seek(&mut self, key: &[u8]) {
        self.item = self.sl.search_node(key);
        self.current = NonNull::new(self.item)
    }

    fn next(&mut self) {
        if let Some(curr) = self.current {
            self.current = NonNull::new(Node::load_next(curr.as_ptr(), 0, Ordering::Relaxed))
        } else {
            self.current = None;
        }
    }

    fn key(&self) -> &[u8] {
        debug_assert!(self.valid());
        //TODO: We should maybe do a valid check before calling key() does this need to be unsafe?
        Node::get_key_bytes(self.current.unwrap().as_ptr())
    }

    fn value(&self) -> &[u8] {
        //TODO: We should maybe do a valid check before calling value() does this need to be unsafe?
        debug_assert!(self.valid());
        Node::get_value_bytes(self.current.unwrap().as_ptr())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn mem_enum() {
        let mem = MemLifeCycle::Active;
        assert_eq!(mem as u8, 1);
    }
}
