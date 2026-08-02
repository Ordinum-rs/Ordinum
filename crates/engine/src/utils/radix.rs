/*
 *
 *
 * RADIX TREE
 *
 * Search:
 *
 * 1. Begin at the root
 * 2. Traverse until a leaf is found or it is not possible to continue
 * 3. Match if found if we are at a leaf and have used up x.len() elements
 *
 * Search Cases:
 *
 * 1. The Edge's label completely matches part of x --> Edge: N | X: Node
 * 2. An Edge's label and x share a common prefix that is shorter than both labels --> Edge: NODE | X: NORM
 * 3. x is a prefix of the Edge's label --> Edge: NODE | X: NO
 * 4. x has no common prefix --> Edge: NODE | X: GEODE
 *
 *
 * Memory:
 *
 *
 *
 *
 *
 *
 *
 */
use mem::hazard::Pointer;
use std::{array, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

// --- SLAB --- //

struct Slot<T> {
    value: MaybeUninit<T>,
    occupied: bool,
    next_free: Option<NonNull<Slot<T>>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            value: MaybeUninit::uninit(),
            occupied: false,
            next_free: None,
        }
    }
}

struct ChunkSlab<T, const CHUNK_SIZE: usize> {
    chunks: Vec<Box<[Slot<T>; CHUNK_SIZE]>>,
    free: Vec<NonNull<Slot<T>>>,
}

impl<T, const CHUNK_SIZE: usize> ChunkSlab<T, CHUNK_SIZE> {
    //
    fn new() -> Self {
        // Will start off
        // []
        // []
        // Lazily allocate a chunk on first contact
        Self {
            chunks: Vec::new(),
            free: Vec::new(),
        }
    }
    //
    fn allocate_chunk(&mut self) {
        let chunk: Box<[Slot<T>; CHUNK_SIZE]> = Box::new(array::from_fn(|_| Slot::new()));
        chunk.map(|i| self.free.push(NonNull::from(&i)));
    }
}

// -----------------

#[derive(Clone, Copy)]
struct NodePtr {
    ptr: NonNull<()>,
}

#[repr(transparent)]
struct LeafPtr(NodePtr);

#[repr(C)]
struct NodeHeader {
    label: Box<[u8]>,
    children: u16,
    terminal: Option<LeafPtr>,
}

#[repr(C, align(8))]
struct LinearChild<const INDEXES: usize> {
    edges: [u8; INDEXES],
    children: [Option<NodePtr>; INDEXES],
}

impl<const INDEXES: usize> LinearChild<INDEXES> {
    fn new() -> Self {
        Self {
            edges: [0u8; INDEXES],
            children: [None; INDEXES],
        }
    }
}

#[repr(C, align(8))]
struct IndexedChild<const INDEXES: usize, const CHILDREN: usize> {
    edges: [u8; INDEXES],
    children: [Option<NodePtr>; CHILDREN],
}

impl<const INDEXES: usize, const CHILDREN: usize> IndexedChild<INDEXES, CHILDREN> {
    fn new() -> Self {
        Self {
            edges: [0u8; INDEXES],
            children: [None; CHILDREN],
        }
    }
}

#[repr(C, align(8))]
struct DirectChild<const CHILD_LEN: usize> {
    children: [Option<NodePtr>; CHILD_LEN],
}

impl<const CHILD_LEN: usize> DirectChild<CHILD_LEN> {
    fn new() -> Self {
        Self {
            children: [None; CHILD_LEN],
        }
    }
}

#[repr(C, align(8))]
struct Leaf<T> {
    key: Box<[u8]>,
    value: T,
}

#[repr(C)]
struct Node<C> {
    header: NodeHeader,
    children: C,
}

impl<C> Node<C> {
    fn new(children: C) -> Self {
        Self {
            header: NodeHeader {
                label: Box::new([0u8; 0]),
                children: 0,
                terminal: None,
            },
            children,
        }
    }
}

type Node4 = Node<LinearChild<4>>;
impl Default for Node4 {
    fn default() -> Self {
        Node::new(LinearChild::new())
    }
}
type Node16 = Node<LinearChild<16>>;
impl Default for Node16 {
    fn default() -> Self {
        Node::new(LinearChild::new())
    }
}
type Node48 = Node<IndexedChild<48, 256>>;
impl Default for Node48 {
    fn default() -> Self {
        Node::new(IndexedChild::new())
    }
}
type Node256 = Node<DirectChild<256>>;
impl Default for Node256 {
    fn default() -> Self {
        Node::new(DirectChild::new())
    }
}

// Node Kinds

#[repr(u8)]
enum NodeKind {
    Node4,
    Node16,
    Node48,
    Node256,
    Leaf,
}

// Each entry to be wrapped by the slab
struct NodeStore<T> {
    node4: ChunkSlab<Node4, 12>,
    node16: ChunkSlab<Node16, 12>,
    node48: ChunkSlab<Node48, 8>,
    node256: ChunkSlab<Node256, 2>,
    leaf: ChunkSlab<Leaf<T>, 16>,
}

impl<T> NodeStore<T> {
    fn new() -> Self {
        Self {
            node4: ChunkSlab::new(),
            node16: ChunkSlab::new(),
            node48: ChunkSlab::new(),
            node256: ChunkSlab::new(),
            leaf: ChunkSlab::new(),
        }
    }
}

#[test]
fn size_estimates() {
    // TODO: Keep workiing size estimates

    println!("{}", size_of::<Slot<Node4>>());

    let target = 16 * 1024;
    let chunk_est = target / size_of::<Slot<Node4>>();
    println!("{}", chunk_est);
}

#[test]
fn ptr_casting() {
    #[repr(C)]
    struct Header {
        flags: u8,
    }

    #[repr(C)]
    struct Node1 {
        header: Header,
        num: u32,
        // Padding 4 because 4 + 1 = 5 next alignment = 8
    }

    #[repr(C)]
    struct Node2 {
        header: Header,
        num: u64,
        // Padding 7 because 1 + 8 = 9 next alignment = 16
    }

    assert_eq!(size_of::<Header>(), 1);
    assert_eq!(size_of::<Node1>(), 8);
    assert_eq!(size_of::<Node2>(), 16);

    // Fake slab

    struct Slot {
        item: u8,
    }

    let mut slab: Vec<Box<[Slot]>> = Vec::new();
}

#[test]
fn array_map() {
    let slice: [usize; 5] = array::from_fn(|i| i + 1);

    let mut vec: Vec<usize> = Vec::new();

    dbg!(&vec);

    slice.map(|x| vec.push(x));

    dbg!(vec);
}
