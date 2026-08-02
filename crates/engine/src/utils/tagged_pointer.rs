use crate::sync::atomic::AtomicPtr;

pub(crate) struct AtomicTaggedPtr<T>(AtomicPtr<T>);

// #[cfg(target_pointer_width = "64")]

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
