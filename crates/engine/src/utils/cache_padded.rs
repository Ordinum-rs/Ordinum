// NOTE:
// For the SkipList we want to make sure that certain fields which are concurrently accessed often are given their own cache line
// A great explanation and gathering of sources is in crossbema -> https://github.com/crossbeam-rs/crossbeam/blob/master/crossbeam-utils/src/cache_padded.rs#L150
//
// For now, we will default to aligning to 64 bytes and over time consider using more alignment for different sources

use std::ops::Deref;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[repr(align(64))]
pub(crate) struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    pub(crate) fn new(t: T) -> Self {
        Self { value: t }
    }
}

unsafe impl<T> Send for CachePadded<T> {}
unsafe impl<T> Sync for CachePadded<T> {}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
