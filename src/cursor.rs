use std::iter::Iterator;
use std::ptr;

use mongo_c_driver_wrapper::bindings;
use bson::Document;

use super::BsoncError;
use super::bsonc;
use super::client::Client;
use super::collection::Collection;

use super::Result;

pub enum CreatedBy<'a> {
    Collection(&'a Collection<'a>),
    Client(&'a Client<'a>)
}

pub struct Cursor<'a> {
    _created_by: CreatedBy<'a>,
    inner:      *mut bindings::mongoc_cursor_t,
}

impl<'a> Cursor<'a> {
    pub fn new(
        created_by: CreatedBy<'a>,
        inner:      *mut bindings::mongoc_cursor_t
    ) -> Cursor<'a> {
        assert!(!inner.is_null());
        Cursor {
            _created_by: created_by,
            inner:       inner
        }
    }

    pub fn is_alive(&self) -> bool {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_cursor_is_alive(self.inner) == 1
        }
    }

    pub fn more(&self) -> bool {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_cursor_more(self.inner) == 1
        }
    }

    fn error(&self) -> BsoncError {
        assert!(!self.inner.is_null());
        let mut error = BsoncError::empty();
        unsafe {
            bindings::mongoc_cursor_error(
                self.inner,
                error.mut_inner()
            )
        };
        error
    }
}

impl<'a> Iterator for Cursor<'a> {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        assert!(!self.inner.is_null());

        // The C driver writes the document to memory and sets an
        // already existing pointer to it.
        let mut bson_ptr: *const bindings::bson_t = ptr::null();
        let success = unsafe {
            bindings::mongoc_cursor_next(
                self.inner,
                &mut bson_ptr
            )
        };

        if success == 0 {
            let error = self.error();
            if error.is_empty() {
                return None
            } else {
                return Some(Err(error.into()))
            }
        }
        assert!(!bson_ptr.is_null());

        let bsonc    = bsonc::Bsonc::from_ptr(bson_ptr);
        let document = bsonc.as_document();
        match document {
            Ok(document) => Some(Ok(document)),
            Err(error)   => Some(Err(error.into()))
        }
    }
}

impl<'a> Drop for Cursor<'a> {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_cursor_destroy(self.inner);
        }
    }
}

#[cfg(test)]
mod tests {
    use bson;
    use super::super::uri::Uri;
    use super::super::client::ClientPool;
    use super::super::Result;

    #[test]
    fn test_cursor() {
        let uri        = Uri::new("mongodb://localhost:27017/");
        let pool       = ClientPool::new(uri);
        let client     = pool.pop();
        let mut collection = client.get_collection("rust_driver_test", "cursor_items");

        let mut document = bson::Document::new();
        document.insert("key".to_string(), bson::Bson::String("value".to_string()));

        collection.drop().unwrap();
        for _ in 0..10 {
            assert!(collection.insert(&document).is_ok());
        }

        let query  = bson::Document::new();
        let cursor = collection.find(&query).unwrap();

        assert!(cursor.is_alive());

        let documents = cursor.into_iter().collect::<Vec<Result<bson::Document>>>();

        // See if we got 10 results and the iterator then stopped
        assert_eq!(10, documents.len());
    }
}
