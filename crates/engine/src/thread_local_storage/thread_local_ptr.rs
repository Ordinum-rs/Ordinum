use std::marker::PhantomData;

pub(crate) type UnrefHandler = unsafe fn(*mut ());

pub(crate) trait ThreadLocalObject {
    const HANDLER: Option<UnrefHandler> = None;
}

//
//
//
//
pub(crate) struct ThreadLocalPtr<T> {
    tls_id: usize,
    _type: PhantomData<T>,
}

impl<T> ThreadLocalPtr<T> {
    fn new() -> Self {
        Self {
            tls_id: 0,
            _type: PhantomData,
        }
    }

    pub(crate) fn new_with_handler(handler: Option<UnrefHandler>) -> Self {
        // Acquire tls_id from meta
        // Register the handler in meta

        todo!()
    }
}
