use std::{array, marker::PhantomData};

use crate::sync::atomic::AtomicPtr;

pub(crate) struct AtomicTaggedPtr<T>(AtomicPtr<T>);

//
// Types of data <T> will be aligned to the size of the data, often a word or multiple thereof. This may leave a few of the LSB of the pointer unused.
// -> k-alignment/aligned refers to a memory address a where a ≡ 0 (mod k).
// -> (ptr as usize) % alginment == 0 OR BETTER STILL self.addr() & (align - 1) == 0
// -> assert_eq!(ptr.align_offset(alignment), 0); (Used in std from ptr.align_offset())
//
//

#[cfg(target_pointer_width = "64")]
const PTR_WIDTH: usize = 64;
#[cfg(target_pointer_width = "32")]
const PTR_WIDTH: usize = 32;

unsafe trait Taggable<const TAG_BITS: u32>: Sized {
    const VERIFY: () = {
        assert!(align_of::<Self>().trailing_zeros() >= TAG_BITS);
        assert!(TAG_BITS < usize::BITS);
    };

    fn verify() {
        let () = Self::VERIFY;
    }
}

// NOTE: How to ensure alignment is more than 1?
pub(crate) struct TaggedPointer<T: Sized> {
    ptr: *const T,
}

impl<T> TaggedPointer<T> {
    const fn something() -> usize {
        let a = align_of::<T>();
        if a == 8 { a } else { 0 }
    }
}

#[test]
fn full_binary_ptr() {
    let ptr = Box::into_raw(Box::new(0u32));
    let address = ptr as usize;
    let mask: usize = 0x1;
    let width = usize::BITS as usize;

    println!("ptr:  {address:0width$b}");
    println!("mask: {mask:0width$b}");
    println!("tag:  {:0width$b}", address & !mask);

    // SAFETY: `ptr` came from `Box::into_raw` above and is reclaimed once.
    drop(unsafe { Box::from_raw(ptr) });
}

#[test]
fn alignment() {
    let width = usize::BITS as usize;

    let one: u8 = 1;
    let one_ptr = &one as *const u8 as usize;
    let two: u16 = 1;
    let two_ptr = &two as *const u16 as usize;
    let three: u32 = 1;
    let three_ptr = &three as *const u32 as usize;
    let four: u64 = 4;
    let four_ptr = &four as *const u64 as usize;

    println!("one   {one_ptr:0width$b}");
    println!(
        "one   trailing zeroes {}",
        align_of::<u8>().trailing_zeros()
    );
    println!("two   {two_ptr:0width$b}");
    println!(
        "two   trailing zeroes {}",
        align_of::<u16>().trailing_zeros()
    );
    println!("three {three_ptr:0width$b}");
    println!(
        "three trailing zeroes {}",
        align_of::<u32>().trailing_zeros()
    );
    println!("four  {four_ptr:0width$b}");
    println!(
        "four  trailing zeroes {}",
        align_of::<u64>().trailing_zeros()
    );
}

#[test]
fn enforce_alignment() {
    //
    // #[inline(always)]
    // const fn verify_size<T, const EXPECTED_SIZE: usize>() {
    //     struct Foo<T, const EXPECTED_SIZE: usize>(T);
    //     impl<T, const EXPECTED_SIZE: usize> Foo<T, EXPECTED_SIZE> {
    //         const VERIFY: () = if core::mem::size_of::<T>() != EXPECTED_SIZE {
    //             panic!("invalid size");
    //         };
    //     }

    //     Foo::<T, EXPECTED_SIZE>::VERIFY
    // }

    // verify_size::<u8, 10>();
}

#[test]
fn align_of_t() {
    //

    #[repr(align(2))]
    struct Foo {
        num: u8,
    }

    impl Foo {
        fn new() -> Self {
            Self { num: 10 }
        }
    }

    unsafe impl Taggable<1> for Foo {}

    let foo = Foo::new();

    struct Bar<B, const BIT: u32>
    where
        B: Taggable<BIT>,
    {
        _blank: PhantomData<B>,
    }

    impl<B, const BITS: u32> Bar<B, BITS>
    where
        B: Taggable<BITS>,
    {
        fn new() -> Self {
            B::verify();
            Self {
                _blank: PhantomData,
            }
        }
    }

    let bar = Bar::<Foo, 1>::new();

    //
    //
    //
    //
}
