//! Access to a MongoDB change stream.
use super::collection::Collection;

use mongoc::bindings;

pub struct ChangeStream<'a> {
    _collection: &'a Collection<'a>,
    inner:       *mut bindings::mongoc_change_stream_t
}

impl<'a> ChangeStream<'a> {
    #[doc(hidden)]
    pub fn new(
        _collection: &'a Collection<'a>,
        inner:      *mut bindings::mongoc_change_stream_t
    ) -> Self {
        Self {
            _collection,
            inner
        }
    }
}

impl<'a> Drop for ChangeStream<'a> {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_change_stream_destroy(self.inner);
        }
    }
}


