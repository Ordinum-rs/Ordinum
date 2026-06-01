//
//

// From: https://rust-hosted-langs.github.io/book/chapter-interp-tagged-ptrs.html
//
// Many runtimes implement tagged pointers to avoid the space overhead, while partially improving the time overhead of the header type-id lookup.
// In a pointer to any object on the heap, the least most significant bits turn out to always be zero due to word or double-word alignment.
// On a 64 bit platform, a pointer is a 64 bit word. Since objects are at least word-aligned, a pointer is always be a multiple of 8 and the
// 3 least significant bits are always 0. On 32 bit platforms, the 2 least significant bits are always 0.
//
// 64..............48..............32..............16.............xxx
// 0b1111111111111111111111111111111111111111111111111111111111111000
//                                                               / |
//                                                              /  |
//                                                            unused
//
// IMPORTANT!
// When dereferencing a pointer, these bits must always be zero. But we can use them in pointers at rest to store a limited type identifier!
//

use crate::sync::atomic::AtomicUsize;
use std::marker::PhantomData;

// 1. ptr must be aligned to T::ALIGN.
// 2. T::ALIGN must be power of two.
// 3. tag must fit in T::ALIGN - 1.
// 4. TaggedPtr does not protect lifetime.
// 5. It is not complete ABA prevention if the tag wraps.
pub(crate) struct TaggedPtr<T> {
    raw: usize,
    _state: PhantomData<*mut T>,
}

// TODO: Finish implementation

impl<T> TaggedPtr<T> {
    const TAG_MASK: usize = align_of::<T>() - 1;
    const PTR_MASK: usize = !Self::TAG_MASK;
    const ALIGN: usize = std::mem::align_of::<T>();

    #[inline]
    fn assert_aligned(ptr: *mut T) {
        debug_assert_eq!(
            ptr as usize & Self::TAG_MASK,
            0,
            "pointer {:#x} is not aligned to {}",
            ptr as usize,
            std::mem::align_of::<T>(),
        );
    }
}

pub(crate) struct AtomicTaggedPtr<T> {
    raw: AtomicUsize,
    _state: PhantomData<*mut T>,
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;

    use super::*;

    #[test]
    fn tagging_pointer() {
        struct Test {
            x: usize,
        }

        let boxed = Box::new(Test { x: 42 });

        let ptr = NonNull::from(boxed.as_ref());

        let addr = ptr.as_ptr() as usize;

        let align = std::mem::align_of::<Test>();
        let tag_mask = align - 1;
        let ptr_mask = !tag_mask;

        // Alignment guarantees these bits are available for tagging.
        TaggedPtr::assert_aligned(ptr.as_ptr());

        let tag = 5;

        // Tag must fit in available low bits.
        assert!(tag <= tag_mask);

        let tagged = addr | tag;

        let recovered_addr = tagged & ptr_mask;
        let recovered_tag = tagged & tag_mask;

        assert_ne!(tagged, addr);

        assert_eq!(recovered_addr, addr);
        assert_eq!(recovered_tag, tag);

        println!("align          = {}", align);
        println!("tag_mask       = {:#x}", tag_mask);
        println!("ptr_mask       = {:#x}", ptr_mask);
        println!("addr           = {:#x}", addr);
        println!("tagged         = {:#x}", tagged);
        println!("recovered_addr = {:#x}", recovered_addr);
        println!("recovered_tag  = {}", recovered_tag);
    }
}
