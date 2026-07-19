// Skip-list index over operations encoded in a batch.
//
// Index nodes are allocated from the indexed batch's arena, which gives every
// node a stable address until the index is cleared and the arena is reset.
// Nodes do not copy batch keys or values. Instead, they store byte offsets into
// `Batch::data`: `key_offset` and `key_len` identify the user key used during
// traversal, while `record_offset` identifies the complete encoded operation
// returned after a successful lookup.
//
// The tower uses the C99 flexible-array-member pattern. The zero-length
// `tower` field marks the start of `height` inline `*mut IndexNode` links that
// are included in the node's manually calculated arena allocation. Links point
// to other arena-backed index nodes; null represents the end of a level. The
// batch index has exclusive mutation while it is being built, so its links do
// not need the atomic pointers used by the concurrent memtable skip list.
//
// Conceptual allocation for a node with height 3:
//
// ┌──────────────────────────────────┐
// │ IndexNode header                 │
// │  cf_id                           │ column-family ordering domain
// │  record_offset                   │ complete operation in Batch::data
// │  key_offset + key_len            │ user-key bytes in Batch::data
// │  height + reserved padding       │ tower length and alignment
// ├──────────────────────────────────┤
// │ tower[0]: *mut IndexNode         │ level 0 successor
// │ tower[1]: *mut IndexNode         │ level 1 successor
// │ tower[2]: *mut IndexNode         │ level 2 successor
// └──────────────────────────────────┘
//                 │
//                 ├── links point to other nodes in the index arena
//                 └── offsets resolve into the separately owned Batch::data
//
// Unlike a memtable node, no key or value bytes follow the tower. Resetting the
// arena invalidates every node and tower link, so skip-list heads must be
// cleared before arena reuse.
//

use std::ptr::NonNull;

use crate::arena::arena::Arena;

const MAX_HEAD_HEIGHT: usize = 8;

#[repr(C)]
pub(super) struct Header {
    sentinel: NonNull<IndexNode>,
}

impl Header {
    fn new() {
        ()
        // TODO: Finish by using IndexNode::alloc(..)
    }
}

#[repr(C)]
pub(crate) struct IndexNode {
    cf_id: u64,

    /// Offset of the complete encoded operation in Batch::data.
    record_offset: u32,

    /// Offset of the user-key bytes in Batch::data.
    key_offset: u32,

    /// Length of the user key.
    key_len: u32,

    height: u16,
    _reserved: u16,

    tower: [*mut IndexNode; 0],
}

impl IndexNode {
    fn alloc(arena: &Arena, height: u16, record_offset: u32, key_offset: u32, key_len: u32) {

        //
        //
        //
        //
        //
    }
}

pub(crate) struct BatchSkipList {}
