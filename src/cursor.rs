use std::iter::Iterator;
use std::ptr;
use std::thread;

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

    fn is_alive(&self) -> bool {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_cursor_is_alive(self.inner) == 1
        }
    }

    fn more(&self) -> bool {
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

        loop {
            if !self.more() {
                return None
            }

            // The C driver writes the document to memory and sets an
            // already existing pointer to it.
            let mut bson_ptr: *const bindings::bson_t = ptr::null();
            let success = unsafe {
                bindings::mongoc_cursor_next(
                    self.inner,
                    &mut bson_ptr
                )
            };

            // Fetch error that might have occurred while getting
            // the next cursor.
            let error = self.error();

            if success == 0 {
                if error.is_empty() {
                    if self.is_alive() {
                        // Since there was no error and the cursor is
                        // alive this must be a tailing cursor and we'll
                        // wait for 500ms before trying again.
                        thread::sleep_ms(500);
                        continue;
                    } else {
                        // No result, no error and cursor not alive anymore
                        // so we must be at the end.
                        return None
                    }
                } else {
                    // There was an error
                    return Some(Err(error.into()))
                }
            }
            assert!(!bson_ptr.is_null());

            // Parse and return bson document.
            let bsonc    = bsonc::Bsonc::from_ptr(bson_ptr);
            let document = bsonc.as_document();
            match document {
                Ok(document) => return Some(Ok(document)),
                Err(error)   => return Some(Err(error.into()))
            }
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
    use std::thread;
    use bson;
    use super::super::flags;
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

    #[test]
    fn test_tailing_cursor() {
        // See: http://api.mongodb.org/c/1.1.8/cursors.html#tailable

        let uri      = Uri::new("mongodb://localhost:27017/");
        let pool     = ClientPool::new(uri);
        let client   = pool.pop();
        let database = client.get_database("rust_test");
        database.get_collection("capped").drop().unwrap_or(());
        database.get_collection("not_capped").drop().unwrap_or(());

        let mut options = bson::Document::new();
        options.insert("capped".to_string(), bson::Bson::Boolean(true));
        options.insert("size".to_string(), bson::Bson::I32(100000));
        let capped_collection = database.create_collection("capped", Some(&options)).unwrap();
        let normal_collection = database.create_collection("not_capped", None).unwrap();

        let mut flags = flags::Flags::new();
        flags.add(flags::QueryFlag::TailableCursor);
        flags.add(flags::QueryFlag::AwaitData);

        // Try to tail on a normal collection
        let failing_cursor = normal_collection.find_with_options(
            &flags,
            0,
            0,
            0,
            &bson::Document::new(),
            None,
            None
        ).unwrap();
        let failing_result = failing_cursor.into_iter().next().unwrap();
        assert!(failing_result.is_err());
        assert_eq!(
            "MongoError (BsoncError: Unable to execute query: error processing query: ns=rust_test.not_capped limit=0 skip=0\nTree: $and\nSort: {}\nProj: {}\n tailable cursor requested on non capped collection)",
            format!("{:?}", failing_result.err().unwrap())
        );

        let mut document = bson::Document::new();
        document.insert("key_1".to_string(), bson::Bson::String("Value 1".to_string()));
        // Insert some documents into the collection
        for _ in 0..5 {
            capped_collection.insert(&document).unwrap();
        }

        // Start a tailing iterator in a thread
        let cloned_pool = pool.clone();
        let guard = thread::spawn(move || {
            let client     = cloned_pool.pop();
            let collection = client.get_collection("rust_test", "capped");

            let cursor = collection.find_with_options(
                &flags,
                0,
                0,
                0,
                &bson::Document::new(),
                None,
                None
            ).unwrap();

            let mut counter = 0usize;
            for result in cursor.into_iter() {
                assert!(result.is_ok());
                counter += 1;
                if counter == 15 {
                    break;
                }
            }
            counter
        });

        // Wait for the thread to boot up
        thread::sleep_ms(200);

        // Insert some more documents into the collection
        for _ in 0..10 {
            capped_collection.insert(&document).unwrap();
        }

        // See if they appeared while iterating the cursor
        // The for loop returns whenever we get more than
        // 15 results.
        assert_eq!(15, guard.join().unwrap());
    }
}
