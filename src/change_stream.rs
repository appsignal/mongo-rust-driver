//! Access to a MongoDB change stream.

use std::ptr;
use std::iter::Iterator;
use super::collection::Collection;

use mongoc::bindings;
use bson::{Bson,Document};
use super::bsonc::Bsonc;
use super::BsoncError;
use super::Result;


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

    fn error(&self) -> BsoncError {
        assert!(!self.inner.is_null());
        let mut error = BsoncError::empty();
        let mut reply = Bsonc::new();

        unsafe {
            bindings::mongoc_change_stream_error_document(
                self.inner,
                error.mut_inner(),
                reply.mut_inner()
            )
        };
        error
    }
}

impl<'a> Iterator for ChangeStream<'a> {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {

        let mut bson_ptr: *const bindings::bson_t = ptr::null();

        let success = unsafe {
            bindings::mongoc_change_stream_next(
                self.inner,
                &mut bson_ptr
            )
        };

        if success == 1 {
            assert!(!bson_ptr.is_null());

            let bsonc = Bsonc::from_ptr(bson_ptr);
            match bsonc.as_document() {
                Ok(document) => return Some(Ok(document)),
                Err(error)   => return Some(Err(error.into()))
            }
        } else {
            let error = self.error();
            if error.is_empty() {
                None
            } else {
                Some(Err(error.into()))
            }
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


