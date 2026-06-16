use crate::sync::atomic::AtomicPtr;

pub(crate) struct AtomicTaggedPtr<T>(AtomicPtr<T>);
