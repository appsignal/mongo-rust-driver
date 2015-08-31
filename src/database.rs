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
use flags::FlagsValue;

pub struct Database<'a> {
    _client: &'a Client<'a>,
    inner:   *mut bindings::mongoc_database_t
}

impl<'a> Database<'a> {
    pub fn new(
        client: &'a Client<'a>,
        inner: *mut bindings::mongoc_database_t
    ) -> Database<'a> {
        assert!(!inner.is_null());
        Database {
            _client: client,
            inner:   inner
        }
    }

    /// Execute a command on the database
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_database_command.html
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

    pub fn create_collection<S: Into<Vec<u8>>>(
        &self,
        name:    S,
        options: Option<&Document>
    ) -> Result<Collection> {
        assert!(!self.inner.is_null());

        let mut error = BsoncError::empty();
        let name_cstring = CString::new(name).unwrap();
        let coll = unsafe {
            bindings::mongoc_database_create_collection(
                self.inner,
                name_cstring.as_ptr(),
                match options {
                    Some(o) => try!(Bsonc::from_document(o)).inner(),
                    None    => ptr::null()
                },
                error.mut_inner()
            )
        };

        if error.is_empty() {
            Ok(Collection::new(collection::CreatedBy::Database(self), coll))
        } else {
            Err(error.into())
        }
    }

    pub fn get_collection<S: Into<Vec<u8>>>(&self, collection: S) -> Collection {
        assert!(!self.inner.is_null());
        let coll = unsafe {
            let collection_cstring = CString::new(collection).unwrap();
            bindings::mongoc_database_get_collection(
                self.inner,
                collection_cstring.as_ptr()
            )
        };
        Collection::new(collection::CreatedBy::Database(self), coll)
    }

    pub fn get_name(&self) -> Cow<str> {
        let cstr = unsafe {
            CStr::from_ptr(bindings::mongoc_database_get_name(self.inner))
        };
        String::from_utf8_lossy(cstr.to_bytes())
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
