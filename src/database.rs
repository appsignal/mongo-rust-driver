//! Access to a MongoDB database.

use std::ffi::{CString,CStr};
use std::borrow::Cow;
use std::ptr;

use mongoc::bindings;
use bson::Document;

use super::Result;
use super::CommandAndFindOptions;
use super::{BsoncError,InvalidParamsError};
use super::bsonc::Bsonc;
use super::client::Client;
use super::collection;
use super::collection::Collection;
use super::cursor;
use super::cursor::Cursor;
use super::read_prefs::ReadPrefs;
use flags::FlagsValue;

#[doc(hidden)]
pub enum CreatedBy<'a> {
    BorrowedClient(&'a Client<'a>),
    OwnedClient(Client<'a>)
}

/// Provides access to a MongoDB database.
///
/// A database instance can be created by calling `get_database` or `take_database` on a `Client` instance.
pub struct Database<'a> {
    _created_by: CreatedBy<'a>,
    inner:   *mut bindings::mongoc_database_t
}

impl<'a> Database<'a> {
    #[doc(ignore)]
    pub fn new(
        created_by: CreatedBy<'a>,
        inner: *mut bindings::mongoc_database_t
    ) -> Database<'a> {
        assert!(!inner.is_null());
        Database {
            _created_by: created_by,
            inner: inner
        }
    }

    /// Execute a command on the database.
    /// This is performed lazily and therefore requires calling `next` on the resulting cursor.
    /// Results are returned in batches as per the mongoc driver.
    /// To get the next batch: https://docs.mongodb.com/manual/reference/command/getMore/
    pub fn command(
        &'a self,
        command: Document,
        options: Option<&CommandAndFindOptions>
    ) -> Result<Cursor<'a>> {
        assert!(!self.inner.is_null());

        let default_options = CommandAndFindOptions::default();
        let options         = options.unwrap_or(&default_options);
        let fields_bsonc    = options.fields_bsonc();

        let cursor_ptr = unsafe {
            bindings::mongoc_database_command(
                self.inner,
                options.query_flags.flags(),
                options.skip,
                options.limit,
                options.batch_size,
                try!(Bsonc::from_document(&command)).inner(),
                match fields_bsonc {
                    Some(ref f) => f.inner(),
                    None => ptr::null()
                },
                match options.read_prefs {
                    Some(ref prefs) => prefs.inner(),
                    None => ptr::null()
                }
            )
        };

        if cursor_ptr.is_null() {
            return Err(InvalidParamsError.into())
        }

        Ok(Cursor::new(
            cursor::CreatedBy::Database(self),
            cursor_ptr,
            fields_bsonc
        ))
    }

    /// Simplified version of `command` that returns the first document immediately.
    pub fn command_simple(
        &'a self,
        command: Document,
        read_prefs: Option<&ReadPrefs>
    ) -> Result<Document> {
        assert!(!self.inner.is_null());

        // Bsonc to store the reply
        let mut reply = Bsonc::new();
        // Empty error that might be filled
        let mut error = BsoncError::empty();

        let success = unsafe {
            bindings::mongoc_database_command_simple(
                self.inner,
                try!(Bsonc::from_document(&command)).inner(),
                match read_prefs {
                    Some(ref prefs) => prefs.inner(),
                    None => ptr::null()
                },
                reply.mut_inner(),
                error.mut_inner()
            )
        };

        if success == 1 {
            match reply.as_document_utf8_lossy() {
                Ok(document) => return Ok(document),
                Err(error)   => return Err(error.into())
            }
        } else {
            Err(error.into())
        }
    }

    /// Create a new collection in this database.
    pub fn create_collection<S: Into<Vec<u8>>>(
        &self,
        name:    S,
        options: Option<&Document>
    ) -> Result<Collection> {
        assert!(!self.inner.is_null());

        let mut error = BsoncError::empty();
        let name_cstring = CString::new(name).unwrap();
        let options_bsonc = match options {
            Some(o) => Some(try!(Bsonc::from_document(o))),
            None => None
        };

        let coll = unsafe {
            bindings::mongoc_database_create_collection(
                self.inner,
                name_cstring.as_ptr(),
                match options_bsonc {
                    Some(ref o) => o.inner(),
                    None => ptr::null()
                },
                error.mut_inner()
            )
        };

        if error.is_empty() {
            Ok(Collection::new(collection::CreatedBy::BorrowedDatabase(self), coll))
        } else {
            Err(error.into())
        }
    }

    /// Borrow a collection
    pub fn get_collection<S: Into<Vec<u8>>>(&self, collection: S) -> Collection {
        assert!(!self.inner.is_null());
        let coll = unsafe { self.collection_ptr(collection.into()) };
        Collection::new(collection::CreatedBy::BorrowedDatabase(self), coll)
    }

    /// Take a collection, database is owned by the collection so the collection can easily
    /// be passed around
    pub fn take_collection<S: Into<Vec<u8>>>(self, collection: S) -> Collection<'a> {
        assert!(!self.inner.is_null());
        let coll = unsafe { self.collection_ptr(collection.into()) };
        Collection::new(collection::CreatedBy::OwnedDatabase(self), coll)
    }

    unsafe fn collection_ptr(&self, collection: Vec<u8>) -> *mut bindings::mongoc_collection_t {
        let collection_cstring = CString::new(collection).unwrap();
        bindings::mongoc_database_get_collection(
            self.inner,
            collection_cstring.as_ptr()
        )
    }

    /// Get the name of this database.
    pub fn get_name(&self) -> Cow<str> {
        let cstr = unsafe {
            CStr::from_ptr(bindings::mongoc_database_get_name(self.inner))
        };
        String::from_utf8_lossy(cstr.to_bytes())
    }

    /// Create a new collection in this database.
    pub fn has_collection<S: Into<Vec<u8>>>(
        &self,
        name:    S
    ) -> Result<bool> {
        let mut error = BsoncError::empty();
        let name_cstring = CString::new(name).unwrap();

        let has_collection = unsafe {
            bindings::mongoc_database_has_collection(
                self.inner,
                name_cstring.as_ptr(),
                error.mut_inner())
        };

        if error.is_empty() {
            Ok(match has_collection{
                0 => false,
                _ => true
            })
        } else {
            Err(error.into())
        }
    }
}

impl<'a> Drop for Database<'a> {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_database_destroy(self.inner);
        }
    }
}
