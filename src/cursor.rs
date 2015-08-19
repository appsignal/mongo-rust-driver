use std::iter::Iterator;
use std::ptr;
use std::thread;

use mongo_c_driver_wrapper::bindings;
use bson::{Bson,Document,oid};

use super::BsoncError;
use super::bsonc;
use super::client::Client;
use super::database::Database;
use super::flags::QueryFlag;
use super::collection::{Collection,TailOptions};
use super::CommandAndFindOptions;

use super::Result;

pub enum CreatedBy<'a> {
    Client(&'a Client<'a>),
    Database(&'a Database<'a>),
    Collection(&'a Collection<'a>)
}

pub struct Cursor<'a> {
    _created_by:       CreatedBy<'a>,
    inner:             *mut bindings::mongoc_cursor_t,
    tailing:           bool,
    tail_wait_time_ms: u32
}

impl<'a> Cursor<'a> {
    pub fn new(
        created_by: CreatedBy<'a>,
        inner:      *mut bindings::mongoc_cursor_t
    ) -> Cursor<'a> {
        assert!(!inner.is_null());
        Cursor {
            _created_by:       created_by,
            inner:             inner,
            tailing:           false,
            tail_wait_time_ms: 0
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
                    if self.tailing && self.is_alive() {
                        // Since there was no error, this is a tailing cursor
                        // and the cursor is alive we'll wait before trying again.
                        thread::sleep_ms(self.tail_wait_time_ms);
                        continue;
                    } else {
                        // No result, no error and cursor not tailing so we must
                        // be at the end.
                        return None
                    }
                } else {
                    // There was an error
                    return Some(Err(error.into()))
                }
            }
            assert!(!bson_ptr.is_null());

            // Parse and return bson document.
            let bsonc = bsonc::Bsonc::from_ptr(bson_ptr);
            match bsonc.as_document() {
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

/// Cursor that will reconnect and resume tailing a collection
/// at the right point if the connection fails.
pub struct TailingCursor<'a> {
    collection:   &'a Collection<'a>,
    query:        Document,
    find_options: CommandAndFindOptions,
    tail_options: TailOptions,
    cursor:       Option<Cursor<'a>>,
    last_seen_id: Option<oid::ObjectId>,
    retry_count:  u32
}

impl<'a> TailingCursor<'a> {
    pub fn new(
        collection:   &'a Collection<'a>,
        query:        Document,
        find_options: CommandAndFindOptions,
        tail_options: TailOptions
    ) -> TailingCursor<'a> {
        // Add flags to make query tailable
        let mut find_options = find_options;
        find_options.query_flags.add(QueryFlag::TailableCursor);
        find_options.query_flags.add(QueryFlag::AwaitData);

        TailingCursor {
            collection:   collection,
            query:        query,
            find_options: find_options,
            tail_options: tail_options,
            cursor:       None,
            last_seen_id: None,
            retry_count:  0
        }
    }
}

impl<'a> Iterator for TailingCursor<'a> {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Start a scope so we're free to set the cursor to None at the end.
            {
                if self.cursor.is_none() {
                    // Add the last seen id to the query if it's present.
                    match self.last_seen_id.take() {
                        Some(id) => {
                            let mut gt_id = Document::new();
                            gt_id.insert("$gt".to_string(), Bson::ObjectId(id));
                            self.query.insert("_id".to_string(), Bson::Document(gt_id));
                        },
                        None => ()
                    };

                    // Set the cursor
                    self.cursor = match self.collection.find(&self.query, Some(&self.find_options)) {
                        Ok(mut c)  => {
                            c.tailing           = true;
                            c.tail_wait_time_ms = self.tail_options.wait_time_ms;
                            Some(c)
                        },
                        Err(e) => return Some(Err(e.into()))
                    };
                }

                let cursor = match self.cursor {
                    Some(ref mut c) => c,
                    None => panic!("It should be impossible to not have a cursor here")
                };

                match cursor.next() {
                    Some(next_result) => {
                        match next_result {
                            Ok(next) => {
                                // This was successfull, so reset retry count and return result.
                                self.retry_count = 0;
                                return Some(Ok(next))
                            },
                            Err(e) => {
                                // Retry if we haven't exceeded the maximum number of retries.
                                if self.retry_count >= self.tail_options.max_retries {
                                    return Some(Err(e.into()))
                                }
                            }
                        }
                    },
                    None => ()
                };
            }

            // We made it to the end, so we weren't able to get the next item from
            // the cursor. We need to reconnect in the next iteration of the loop.
            self.retry_count += 1;
            self.cursor      = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
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

        collection.drop().unwrap_or(());
        for _ in 0..10 {
            assert!(collection.insert(&document, None).is_ok());
        }

        let query  = bson::Document::new();
        let cursor = collection.find(&query, None).unwrap();

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

        // Try to tail on a normal collection
        let failing_cursor = normal_collection.tail(bson::Document::new(), None, None);
        let failing_result = failing_cursor.into_iter().next().unwrap();
        assert!(failing_result.is_err());
        assert_eq!(
            "MongoError (BsoncError: Unable to execute query: error processing query: ns=rust_test.not_capped limit=0 skip=0\nTree: $and\nSort: {}\nProj: {}\n tailable cursor requested on non capped collection)",
            format!("{:?}", failing_result.err().unwrap())
        );

        let mut document = bson::Document::new();
        document.insert("key_1".to_string(), bson::Bson::String("Value 1".to_string()));
        // Insert a first document into the collection
        capped_collection.insert(&document, None).unwrap();

        // Start a tailing iterator in a thread
        let cloned_pool = pool.clone();
        let guard = thread::spawn(move || {
            let client     = cloned_pool.pop();
            let collection = client.get_collection("rust_test", "capped");
            let cursor = collection.tail(bson::Document::new(), None, None);
            let mut counter = 0usize;
            for result in cursor.into_iter() {
                assert!(result.is_ok());
                counter += 1;
                if counter == 25 {
                    break;
                }
            }
            counter
        });

        // Wait for the thread to boot up
        thread::sleep_ms(250);

        // Insert some more documents into the collection
        for _ in 0..25 {
            capped_collection.insert(&document, None).unwrap();
        }

        // See if they appeared while iterating the cursor
        // The for loop returns whenever we get more than
        // 15 results.
        assert_eq!(25, guard.join().unwrap());
    }
}
