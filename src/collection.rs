use std::ptr;
use std::ffi::CStr;
use std::borrow::Cow;

use mongo_c_driver_wrapper::bindings;

use bson::Document;

use super::Result;
use super::{BsoncError,InvalidParamsError};
use super::bsonc::Bsonc;
use super::client::Client;
use super::cursor;
use super::cursor::Cursor;
use super::flags::{Flags,FlagsValue,InsertFlag,QueryFlag,RemoveFlag};
use super::write_concern::WriteConcern;
use super::read_prefs::ReadPrefs;

pub enum CreatedBy<'a> {
    Client(&'a Client<'a>)
}

pub struct Collection<'a> {
    _created_by: CreatedBy<'a>,
    inner:      *mut bindings::mongoc_collection_t
}

impl<'a> Collection<'a> {
    pub fn new(
        client: &'a Client<'a>,
        inner: *mut bindings::mongoc_collection_t
    ) -> Collection<'a> {
        assert!(!inner.is_null());
        Collection {
            _created_by: CreatedBy::Client(client),
            inner:       inner
        }
    }

    pub fn count_with_options(
        &self,
        query_flags: &Flags<QueryFlag>,
        query:       &Document,
        skip:        u32,
        limit:       u32,
        opts:        Option<&Document>,
        read_prefs:  Option<&ReadPrefs>
    ) -> Result<i64> {
        assert!(!self.inner.is_null());

        let mut error = BsoncError::empty();
        let count = unsafe {
            bindings::mongoc_collection_count_with_opts(
                self.inner,
                query_flags.flags(),
                try!(Bsonc::from_document(query)).inner(),
                skip as i64,
                limit as i64,
                match opts {
                    Some(o) => try!(Bsonc::from_document(o)).inner(),
                    None        => ptr::null()
                },
                match read_prefs {
                    Some(prefs) => prefs.inner(),
                    None        => ptr::null()
                },
                error.mut_inner()
            )
        };

        if error.is_empty() {
            Ok(count)
        } else {
            Err(error.into())
        }
    }

    pub fn count(
        &self,
        query: &Document
    ) -> Result<i64> {
        self.count_with_options(
            &Flags::new(),
            query,
            0,
            0,
            None,
            None
        )
    }

    pub fn drop(&mut self) -> Result<()> {
        assert!(!self.inner.is_null());
        let mut error = BsoncError::empty();
        let success = unsafe {
            bindings::mongoc_collection_drop(
                self.inner,
                error.mut_inner()
            )
        };
        if success == 0 {
            assert!(!error.is_empty());
            return Err(error.into())
        }
        Ok(())
    }

    pub fn find_with_options(
        &'a self,
        query_flags: &Flags<QueryFlag>,
        skip:        u32,
        limit:       u32,
        batch_size:  u32,
        query:       &Document,
        fields:      Option<&Document>,
        read_prefs:  Option<&ReadPrefs>
    ) -> Result<Cursor<'a>> {
        assert!(!self.inner.is_null());

        let inner = unsafe {
            bindings::mongoc_collection_find(
                self.inner,
                query_flags.flags(),
                skip,
                limit,
                batch_size,
                try!(Bsonc::from_document(query)).inner(),
                match fields {
                    Some(f) => {
                        try!(Bsonc::from_document(f)).inner()
                    },
                    None => ptr::null()
                },
                match read_prefs {
                    Some(prefs) => prefs.inner(),
                    None        => ptr::null()
                }
            )
        };

        if inner.is_null() {
            return Err(InvalidParamsError.into())
        }

        Ok(Cursor::new(cursor::CreatedBy::Collection(self), inner))
    }

    pub fn find(
        &'a self,
        query: &Document
    ) -> Result<Cursor<'a>> {
        self.find_with_options(
            &Flags::new(),
            0,
            0,
            0,
            &query,
            None,
            None
        )
    }

    pub fn get_name(&self) -> Cow<str> {
        let cstr = unsafe {
            CStr::from_ptr(bindings::mongoc_collection_get_name(self.inner))
        };
        String::from_utf8_lossy(cstr.to_bytes())
    }

    pub fn insert_with_options(
        &'a self,
        insert_flags:  &Flags<InsertFlag>,
        document:      &Document,
        write_concern: &WriteConcern
    ) -> Result<()> {
        assert!(!self.inner.is_null());

        let mut error = BsoncError::empty();
        let success = unsafe {
            bindings::mongoc_collection_insert(
                self.inner,
                insert_flags.flags(),
                try!(Bsonc::from_document(&document)).inner(),
                write_concern.inner(),
                error.mut_inner()
            )
        };

        if success == 1 {
            Ok(())
        } else {
            Err(error.into())
        }
    }

    pub fn insert(&'a self, document: &Document) -> Result<()> {
        self.insert_with_options(
            &Flags::new(),
            document,
            &WriteConcern::new()
        )
    }

    pub fn remove_with_options(
        &self,
        remove_flags:  &Flags<RemoveFlag>,
        selector:      &Document,
        write_concern: &WriteConcern
    ) -> Result<()> {
        assert!(!self.inner.is_null());

        let mut error = BsoncError::empty();
        let success = unsafe {
            bindings::mongoc_collection_remove(
                self.inner,
                remove_flags.flags(),
                try!(Bsonc::from_document(&selector)).inner(),
                write_concern.inner(),
                error.mut_inner()
            )
        };

        if success == 1 {
            Ok(())
        } else {
            Err(error.into())
        }
    }

    pub fn remove(
        &self,
        selector: &Document
    ) -> Result<()> {
        self.remove_with_options(
            &Flags::new(),
            selector,
            &WriteConcern::new()
        )
    }

    pub fn save_with_options(
        &self,
        document:      &Document,
        write_concern: &WriteConcern
    ) -> Result<()> {
        assert!(!self.inner.is_null());

        let mut error = BsoncError::empty();
        let success = unsafe {
            bindings::mongoc_collection_save(
                self.inner,
                try!(Bsonc::from_document(&document)).inner(),
                write_concern.inner(),
                error.mut_inner()
            )
        };

        if success == 1 {
            Ok(())
        } else {
            Err(error.into())
        }
    }

    pub fn save(
        &self,
        document:      &Document,
    ) -> Result<()> {
        self.save_with_options(
            document,
            &WriteConcern::new()
        )
    }
}

impl<'a> Drop for Collection<'a> {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_collection_destroy(self.inner);
        }
    }
}

#[cfg(test)]
mod tests {
    use bson;
    use super::super::uri::Uri;
    use super::super::client::ClientPool;
    use super::super::flags::{Flags};

    #[test]
    fn test_mutation_and_finding() {
        let uri        = Uri::new("mongodb://localhost:27017/");
        let pool       = ClientPool::new(uri);
        let client     = pool.pop();
        let mut collection = client.get_collection("rust_driver_test", "items");
        collection.drop().unwrap_or(());

        assert_eq!("items", collection.get_name().to_mut());

        let mut document = bson::Document::new();
        document.insert("key_1".to_string(), bson::Bson::String("Value 1".to_string()));
        document.insert("key_2".to_string(), bson::Bson::String("Value 2".to_string()));
        assert!(collection.insert(&document).is_ok());

        let mut second_document = bson::Document::new();
        second_document.insert("key_1".to_string(), bson::Bson::String("Value 3".to_string()));
        assert!(collection.insert(&second_document).is_ok());

        let query = bson::Document::new();

        // Count the documents in the collection
        assert_eq!(2, collection.count(&query).unwrap());

        // Find the documents
        assert_eq!(
            collection.find(&document).unwrap().next().unwrap().unwrap().get("key_1").unwrap().to_json(),
            bson::Bson::String("Value 1".to_string()).to_json()
        );
        let mut found_document = collection.find(&second_document).unwrap().next().unwrap().unwrap();
        assert_eq!(
            found_document.get("key_1").unwrap().to_json(),
            bson::Bson::String("Value 3".to_string()).to_json()
        );

        // Update the second document
        found_document.insert("key_1".to_string(), bson::Bson::String("Value 4".to_string()));
        assert!(collection.save(&found_document).is_ok());

        // Reload and check value
        let found_document = collection.find(&found_document).unwrap().next().unwrap().unwrap();
        assert_eq!(
            found_document.get("key_1").unwrap().to_json(),
            bson::Bson::String("Value 4".to_string()).to_json()
        );

        // Remove one
        assert!(collection.remove(&found_document).is_ok());

        // Count again
        assert_eq!(1, collection.count(&query).unwrap());

        // Find the document and see if it has the keys we expect
        {
            let mut cursor = collection.find(&query).unwrap();
            let next_document = cursor.next().unwrap().unwrap();
            assert!(next_document.contains_key("key_1"));
            assert!(next_document.contains_key("key_2"));
        }

        // Find the document with fields set
        let mut fields = bson::Document::new();
        fields.insert("key_1".to_string(), bson::Bson::Boolean(true));
        {
            let mut cursor = collection.find_with_options(
                &Flags::new(),
                0,
                0,
                0,
                &query,
                Some(&fields),
                None
            ).unwrap();
            let next_document = cursor.next().unwrap().unwrap();
            assert!(next_document.contains_key("key_1"));
            assert!(!next_document.contains_key("key_2"));
        }

        // Drop collection
        collection.drop().unwrap();
        assert_eq!(0, collection.count(&query).unwrap());
    }

    #[test]
    fn test_insert_failure() {
        let uri        = Uri::new("mongodb://localhost:27018/"); // There should be no mongo server here
        let pool       = ClientPool::new(uri);
        let client     = pool.pop();
        let collection = client.get_collection("rust_driver_test", "items");
        let document   = bson::Document::new();

        let result = collection.insert(&document);
        assert!(result.is_err());
        assert_eq!(
            "MongoError (BsoncError: Failed to connect to target host: localhost:27018)",
            format!("{:?}", result.err().unwrap())
        );
    }
}
