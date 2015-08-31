use std::ptr;
use std::ffi::CStr;
use std::borrow::Cow;

use mongoc::bindings;

use bson::Document;

use super::Result;
use super::CommandAndFindOptions;
use super::{BsoncError,InvalidParamsError};
use super::bsonc::Bsonc;
use super::bulk_operation::BulkOperation;
use super::client::Client;
use super::cursor;
use super::cursor::{Cursor,TailingCursor};
use super::database::Database;
use super::flags::{Flags,FlagsValue,InsertFlag,QueryFlag,RemoveFlag};
use super::write_concern::WriteConcern;
use super::read_prefs::ReadPrefs;

pub enum CreatedBy<'a> {
    Client(&'a Client<'a>),
    Database(&'a Database<'a>)
}

pub struct Collection<'a> {
    _created_by: CreatedBy<'a>,
    inner:      *mut bindings::mongoc_collection_t
}

pub struct BulkOperationOptions {
    pub ordered:       bool,
    pub write_concern: WriteConcern
}

impl BulkOperationOptions {
    pub fn default() -> BulkOperationOptions {
        BulkOperationOptions {
            ordered:       false,
            write_concern: WriteConcern::new()
        }
    }
}

pub struct CountOptions {
    pub query_flags: Flags<QueryFlag>,
    pub skip:        u32,
    pub limit:       u32,
    pub opts:        Option<Document>,
    pub read_prefs:  Option<ReadPrefs>
}

impl CountOptions {
    pub fn default() -> CountOptions {
        CountOptions {
            query_flags: Flags::new(),
            skip:        0,
            limit:       0,
            opts:        None,
            read_prefs:  None
        }
    }
}

pub struct InsertOptions {
    pub insert_flags:  Flags<InsertFlag>,
    pub write_concern: WriteConcern
}

impl InsertOptions {
    pub fn default() -> InsertOptions {
        InsertOptions {
            insert_flags:  Flags::new(),
            write_concern: WriteConcern::new()
        }
    }
}

pub struct RemoveOptions {
    pub remove_flags:  Flags<RemoveFlag>,
    pub write_concern: WriteConcern
}

impl RemoveOptions {
    pub fn default() -> RemoveOptions {
        RemoveOptions {
            remove_flags:  Flags::new(),
            write_concern: WriteConcern::new()
        }
    }
}

pub struct TailOptions {
    pub wait_time_ms: u32,
    pub max_retries:  u32
}

impl TailOptions {
    pub fn default() -> TailOptions {
        TailOptions {
            wait_time_ms: 500,
            max_retries:  5
        }
    }
}

impl<'a> Collection<'a> {
    pub fn new(
        created_by: CreatedBy<'a>,
        inner:      *mut bindings::mongoc_collection_t
    ) -> Collection<'a> {
        assert!(!inner.is_null());
        Collection {
            _created_by: created_by,
            inner:       inner
        }
    }

    /// Execute a command on the collection
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_collection_command.html
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
            bindings::mongoc_collection_command(
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
            cursor::CreatedBy::Collection(self),
            cursor_ptr,
            fields_bsonc
        ))
    }

    pub fn count(
        &self,
        query:   &Document,
        options: Option<&CountOptions>
    ) -> Result<i64> {
        assert!(!self.inner.is_null());

        let default_options = CountOptions::default();
        let options         = options.unwrap_or(&default_options);

        let mut error = BsoncError::empty();
        let count = unsafe {
            bindings::mongoc_collection_count_with_opts(
                self.inner,
                options.query_flags.flags(),
                try!(Bsonc::from_document(query)).inner(),
                options.skip as i64,
                options.limit as i64,
                match options.opts {
                    Some(ref o) => try!(Bsonc::from_document(o)).inner(),
                    None        => ptr::null()
                },
                match options.read_prefs {
                    Some(ref prefs) => prefs.inner(),
                    None            => ptr::null()
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

    pub fn create_bulk_operation(
        &'a self,
        options: Option<&BulkOperationOptions>
    ) -> BulkOperation<'a> {
        assert!(!self.inner.is_null());

        let default_options = BulkOperationOptions::default();
        let options         = options.unwrap_or(&default_options);

        let inner = unsafe {
            bindings::mongoc_collection_create_bulk_operation(
                self.inner,
                options.ordered as u8,
                options.write_concern.inner()
            )
        };

        BulkOperation::new(self, inner)
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

    pub fn find(
        &'a self,
        query:   &Document,
        options: Option<&CommandAndFindOptions>
    ) -> Result<Cursor<'a>> {
        assert!(!self.inner.is_null());

        let default_options = CommandAndFindOptions::default();
        let options         = options.unwrap_or(&default_options);
        let fields_bsonc    = options.fields_bsonc();

        let cursor_ptr = unsafe {
            bindings::mongoc_collection_find(
                self.inner,
                options.query_flags.flags(),
                options.skip,
                options.limit,
                options.batch_size,
                try!(Bsonc::from_document(query)).inner(),
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
            cursor::CreatedBy::Collection(self),
            cursor_ptr,
            fields_bsonc
        ))
    }

    pub fn get_name(&self) -> Cow<str> {
        let cstr = unsafe {
            CStr::from_ptr(bindings::mongoc_collection_get_name(self.inner))
        };
        String::from_utf8_lossy(cstr.to_bytes())
    }

    pub fn insert(
        &'a self,
        document: &Document,
        options:  Option<&InsertOptions>
    ) -> Result<()> {
        assert!(!self.inner.is_null());

        let default_options = InsertOptions::default();
        let options         = options.unwrap_or(&default_options);

        let mut error = BsoncError::empty();
        let success = unsafe {
            bindings::mongoc_collection_insert(
                self.inner,
                options.insert_flags.flags(),
                try!(Bsonc::from_document(&document)).inner(),
                options.write_concern.inner(),
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
        selector: &Document,
        options:  Option<&RemoveOptions>
    ) -> Result<()> {
        assert!(!self.inner.is_null());

        let default_options = RemoveOptions::default();
        let options         = options.unwrap_or(&default_options);

        let mut error = BsoncError::empty();
        let success = unsafe {
            bindings::mongoc_collection_remove(
                self.inner,
                options.remove_flags.flags(),
                try!(Bsonc::from_document(&selector)).inner(),
                options.write_concern.inner(),
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
        write_concern: Option<&WriteConcern>
    ) -> Result<()> {
        assert!(!self.inner.is_null());

        let default_write_concern = WriteConcern::new();
        let write_concern         = write_concern.unwrap_or(&default_write_concern);

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

    /// Tails a query
    ///
    /// Takes ownership of query and options because they could be
    /// modified and reused when the connections is disrupted and
    /// we need to restart the query. The query will be placed in a
    /// $query key, so the function can add configuration needed for
    /// proper tailing.
    ///
    /// The query is executed when iterating, so this function doesn't
    /// return a result itself.
    ///
    /// The necessary flags to configure a tailing query will be added
    /// to the configured flags if you choose to supply options.
    pub fn tail(
        &'a self,
        query:        Document,
        find_options: Option<CommandAndFindOptions>,
        tail_options: Option<TailOptions>
    ) -> TailingCursor<'a> {
        let query_with_options = doc! {
            "$query" => query,
            "$natural" => 1
        };

        TailingCursor::new(
            self,
            query_with_options,
            find_options.unwrap_or(CommandAndFindOptions::default()),
            tail_options.unwrap_or(TailOptions::default())
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
