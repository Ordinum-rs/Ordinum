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

#[repr(u32)]
pub(crate) enum TAGGABLE_BITS {
    TAG_16 = 1,
    TAG_32,
    TAG_64,
    TAG_128,
}

impl TAGGABLE_BITS {
    const fn is_taggable(tagging: u32) -> bool {
        match tagging {
            1 => true,
            2 => true,
            3 => true,
            4 => true,
            _ => false,
        }
    }

    const fn from_alignment<T>() -> u32 {
        let trail = align_of::<T>().trailing_zeros();
        match trail {
            1 => TAGGABLE_BITS::TAG_16 as u32,
            2 => TAGGABLE_BITS::TAG_32 as u32,
            3 => TAGGABLE_BITS::TAG_64 as u32,
            4 => TAGGABLE_BITS::TAG_128 as u32,
            _ => unreachable!(),
        }
    }
}

unsafe trait Taggable: Sized {
    const TAG_BITS: u32;
    const VERIFY_ALIGNMENT: () = {
        assert!(TAGGABLE_BITS::is_taggable(Self::TAG_BITS));
        assert!(align_of::<Self>().trailing_zeros() == Self::TAG_BITS);
        assert!(Self::TAG_BITS < usize::BITS);
    };

    fn verify<const REQUIRED_BITS: u32>() {
        let () = Self::VERIFY_ALIGNMENT;

        const {
            assert!(REQUIRED_BITS > 0);
            assert!(REQUIRED_BITS < usize::BITS);
            assert!(Self::TAG_BITS >= REQUIRED_BITS);
        }
    }

    fn available_bits(&self) -> usize {
        Self::TAG_BITS as usize
    }
}

struct TaggedPointerInner<T> {
    ptr: *const T,
}

impl<T> TaggedPointerInner<T> {
    // Standard masking methods and ptr addr mapping
}

pub(crate) struct TaggedPointer<T: Taggable, const TAG_BITS: u32> {
    ptr: TaggedPointerInner<T>,
}

impl<T: Taggable, const TAG_BITS: u32> TaggedPointer<T, TAG_BITS> {
    pub(crate) fn new(object: &T) -> Self {
        // Verify first
        T::verify::<TAG_BITS>();

        Self {
            ptr: TaggedPointerInner {
                ptr: object as *const T,
            },
        }
    }
}

pub(crate) type TaggedPointer1Bit<T> = TaggedPointer<T, 1>;
pub(crate) type TaggedPointer2Bits<T> = TaggedPointer<T, 2>;
pub(crate) type TaggedPointer3Bits<T> = TaggedPointer<T, 3>;
pub(crate) type TaggedPointer4Bits<T> = TaggedPointer<T, 4>;

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
fn align_of_t() {
    //

    #[repr(align(4))]
    struct Foo {
        num: u16,
    }

    println!("{}", align_of::<Foo>());

    impl Foo {
        fn new() -> Self {
            Self { num: 10 }
        }
    }

    // We implement Taggable for Foo which means that we are saying any pointer that is produced for a Foo object
    // is taggable because the pointer will be aligned to the alignment of the object which is of TAB_BITS either (u16, u32, u64)
    unsafe impl Taggable for Foo {
        const TAG_BITS: u32 = TAGGABLE_BITS::from_alignment::<Foo>();
    }

    let foo = Box::new(Foo::new());

    let tagged = TaggedPointer1Bit::<Foo>::new(&foo);

    //
    //
    //
    //
}

#[test]
fn is_taggable() {
    println!("{}", TAGGABLE_BITS::is_taggable(4))
}
