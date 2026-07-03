// For Iteration, we can use a few approaches when allocating.
// 1. Standard Vec on each Merge Iter instance
// 2. Re-usable heap allocation with IterAlloc
// 3. Arena allocation for iterator tree and use vec heap allocation for children and range iters (hybrid approach)
//
// The first option is simple and something to be used purely for testing.
//
// With the other two options, I would like to use feature flags for static compilation, however, we can also use a runtime approach by
// passing in an arena if we want the iterator to use an arena for allocation
//
// DBIter (top-level)
//     ├── MergeIterator
//     │       ├── ChildIter 1
//     │       ├── ChildIter 2
//     │       └── ...

use std::marker::PhantomData;

use crate::arena::arena::Arena;
use crate::iterator::iter_alloc::IterAlloc;

pub(crate) trait IterAllocStrategy {}

pub(crate) struct ArenaIter {
    arena: Arena,
}
impl IterAllocStrategy for ArenaIter {}

pub(crate) struct HeapIter {}
impl IterAllocStrategy for HeapIter {}

pub(crate) struct DBIter<'a, I: IterAllocStrategy> {
    _state: PhantomData<I>,
    iter_alloc: &'a IterAlloc,
}
