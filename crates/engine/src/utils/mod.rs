pub(crate) mod var_int;

#[inline]
pub(crate) unsafe fn read_u16_le_unsafe(ptr: *const u8) -> u16 {
    unsafe { std::ptr::read_unaligned(ptr as *const u16).to_le() }
}

#[inline]
pub(crate) unsafe fn write_u16_le_unsafe(b_ptr: *mut u8, value: u16) {
    let bytes = value.to_le_bytes();
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), b_ptr, 2) };
}

#[inline]
pub(crate) unsafe fn read_u32_le_unsafe(ptr: *const u8) -> u32 {
    unsafe { std::ptr::read_unaligned(ptr as *const u32).to_le() }
}

#[inline(always)]
pub(crate) unsafe fn write_u32_le_unsafe(b_ptr: *mut u8, value: u32) {
    let bytes = value.to_le_bytes();
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), b_ptr, 4) };
}

#[inline(always)]
pub(crate) unsafe fn read_u32_be_unsafe(ptr: *const u8) -> u32 {
    unsafe { std::ptr::read_unaligned(ptr as *const u32).to_be() }
}

#[inline(always)]
pub(crate) unsafe fn write_u32_be_unsafe(b_ptr: *mut u8, value: u32) {
    let bytes = value.to_be_bytes();
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), b_ptr, 4) };
}
