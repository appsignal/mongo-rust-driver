//! Access to a MongoDB collection.
//!
//! `Collection` is the main type used when accessing collections.

use std::ptr;
use std::ffi::CStr;
use std::borrow::Cow;
use std::time::Duration;

use mongoc::bindings;
use bsonc;

use bson::Document;

use super::{Result,BulkOperationResult,BulkOperationError};
use super::CommandAndFindOptions;
use super::{BsoncError,InvalidParamsError};
use super::bsonc::Bsonc;
use super::client::Client;
use super::cursor;
use super::cursor::{Cursor,TailingCursor};
use super::database::Database;
use super::flags::{Flags,FlagsValue,InsertFlag,QueryFlag,RemoveFlag,UpdateFlag};
use super::write_concern::WriteConcern;
use super::read_prefs::ReadPrefs;

#[doc(hidden)]
pub enum CreatedBy<'a> {
    BorrowedClient(&'a Client<'a>),
    OwnedClient(Client<'a>),
    BorrowedDatabase(&'a Database<'a>),
    OwnedDatabase(Database<'a>)
}

/// Provides access to a collection for most CRUD operations, I.e. insert, update, delete, find, etc.
///
/// A collection instance can be created by calling `get_collection` or `take_database` on a `Client` or `Database`
/// instance.
pub struct Collection<'a> {
    _created_by: CreatedBy<'a>,
    inner:      *mut bindings::mongoc_collection_t
}

/// Options to configure an aggregate operation.
pub struct AggregateOptions {
    /// Flags to use
    pub query_flags: Flags<QueryFlag>,
    /// Options for the aggregate
    pub options: Option<Document>,
    /// Read prefs to use
    pub read_prefs:  Option<ReadPrefs>
}

impl AggregateOptions {
    /// Default options that are used if no options are specified
    /// when aggregating.
    pub fn default() -> AggregateOptions {
        AggregateOptions {
            query_flags: Flags::new(),
            options: None,
            read_prefs: None
        }
    }
}

/// Options to configure a bulk operation.
pub struct BulkOperationOptions {
    /// If the operations must be performed in order
    pub ordered:       bool,
    /// `WriteConcern` to use
    pub write_concern: WriteConcern
}

impl BulkOperationOptions {
    /// Default options that are used if no options are specified
    /// when creating a `BulkOperation`.
    pub fn default() -> BulkOperationOptions {
        BulkOperationOptions {
            ordered:       false,
            write_concern: WriteConcern::default()
        }
    }
}

/// Options to configure a find and modify operation.
pub struct FindAndModifyOptions {
    /// Sort order for the query
    pub sort:   Option<Document>,
    /// If the new version of the document should be returned
    pub new:    bool,
    /// The fields to return
    pub fields: Option<Document>
}

impl FindAndModifyOptions {
    /// Default options used if none are provided.
    pub fn default() -> FindAndModifyOptions {
        FindAndModifyOptions {
            sort:   None,
            new:    false,
            fields: None
        }
    }

    fn fields_bsonc(&self) -> Option<bsonc::Bsonc> {
        match self.fields {
            Some(ref f) => Some(bsonc::Bsonc::from_document(f).unwrap()),
            None => None
        }
    }
}

/// Possible find and modify operations.
pub enum FindAndModifyOperation<'a> {
    /// Update the matching documents
    Update(&'a Document),
    /// Upsert the matching documents
    Upsert(&'a Document),
    /// Remove the matching documents
    Remove
}

/// Options to configure a count operation.
pub struct CountOptions {
    /// The query flags to use
    pub query_flags: Flags<QueryFlag>,
    /// Number of results to skip, zero to ignore
    pub skip:        u32,
    /// Limit to the number of results, zero to ignore
    pub limit:       u32,
    /// Optional extra keys to add to the count
    pub opts:        Option<Document>,
    /// Read prefs to use
    pub read_prefs:  Option<ReadPrefs>
}

impl CountOptions {
    /// Default options used if none are provided.
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

/// Options to configure an insert operation.
pub struct InsertOptions {
    /// Flags to use
    pub insert_flags:  Flags<InsertFlag>,
    /// Write concern to use
    pub write_concern: WriteConcern
}

impl InsertOptions {
    /// Default options used if none are provided.
    pub fn default() -> InsertOptions {
        InsertOptions {
            insert_flags:  Flags::new(),
            write_concern: WriteConcern::default()
        }
    }
}

/// Options to configure a remove operation.
pub struct RemoveOptions {
    /// Flags to use
    pub remove_flags:  Flags<RemoveFlag>,
    /// Write concern to use
    pub write_concern: WriteConcern
}

impl RemoveOptions {
    /// Default options used if none are provided.
    pub fn default() -> RemoveOptions {
        RemoveOptions {
            remove_flags:  Flags::new(),
            write_concern: WriteConcern::default()
        }
    }
}

/// Options to configure an update operation.
pub struct UpdateOptions {
    /// Flags to use
    pub update_flags:  Flags<UpdateFlag>,
    /// Write concern to use
    pub write_concern: WriteConcern
}

impl UpdateOptions {
    /// Default options used if none are provided.
    pub fn default() -> UpdateOptions {
        UpdateOptions {
            update_flags:  Flags::new(),
            write_concern: WriteConcern::default()
        }
    }
}

/// Options to configure a tailing query.
pub struct TailOptions {
    /// Duration to wait before checking for new results
    pub wait_duration: Duration,
    /// Maximum number of retries if there is an error
    pub max_retries:   u32
}

impl TailOptions {
    /// Default options used if none are provided.
    pub fn default() -> TailOptions {
        TailOptions {
            wait_duration: Duration::from_millis(500),
            max_retries:  5
        }
    }
}

impl<'a> Collection<'a> {
    #[doc(hidden)]
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

    /// Execute an aggregation query on the collection.
    /// The bson 'pipeline' is not validated, simply passed along as appropriate to the server.
    /// As such, compatibility and errors should be validated in the appropriate server documentation.
    pub fn aggregate(
        &'a self,
        pipeline: &Document,
        options: Option<&AggregateOptions>
    ) -> Result<Cursor<'a>> {
        let default_options = AggregateOptions::default();
        let options         = options.unwrap_or(&default_options);

        let cursor_ptr = unsafe {
            bindings::mongoc_collection_aggregate(
                self.inner,
                options.query_flags.flags(),
                try!(Bsonc::from_document(pipeline)).inner(),
                match options.options {
                    Some(ref o) => {
                        try!(Bsonc::from_document(o)).inner()
                    },
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
            None
        ))
    }

    /// Execute a command on the collection.
    /// This is performed lazily and therefore requires calling `next` on the resulting cursor.
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
            bindings::mongoc_collection_command_simple(
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

    /// Execute a count query on the underlying collection.
    /// The `query` bson is not validated, simply passed along to the server. As such, compatibility and errors should be validated in the appropriate server documentation.
    ///
    /// For more information, see the [query reference](https://docs.mongodb.org/manual/reference/operator/query/) at the MongoDB website.
    pub fn count(
        &self,
        query:   &Document,
        options: Option<&CountOptions>
    ) -> Result<i64> {
        assert!(!self.inner.is_null());

        let default_options = CountOptions::default();
        let options         = options.unwrap_or(&default_options);
        let opts_bsonc      =  match options.opts {
            Some(ref o) => Some(try!(Bsonc::from_document(o))),
            None => None
        };

        let mut error = BsoncError::empty();
        let count = unsafe {
            bindings::mongoc_collection_count_with_opts(
                self.inner,
                options.query_flags.flags(),
                try!(Bsonc::from_document(query)).inner(),
                options.skip as i64,
                options.limit as i64,
                match opts_bsonc {
                    Some(ref o) => o.inner(),
                    None => ptr::null()
                },
                match options.read_prefs {
                    Some(ref prefs) => prefs.inner(),
                    None => ptr::null()
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

    /// Create a bulk operation. After creating call various functions such as `update`,
    /// `insert` and others. When calling `execute` these operations will be executed in
    /// batches.
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

    /// Request that a collection be dropped, including all indexes associated with the collection.
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

    /// Execute a query on the underlying collection.
    /// If no options are necessary, query can simply contain a query such as `{a:1}`.
    /// If you would like to specify options such as a sort order, the query must be placed inside of `{"$query": {}}`
    /// as specified by the server documentation. See the example below for how to properly specify additional options to query.
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

    /// Update and return an object.
    /// This is a thin wrapper around the findAndModify command. Pass in
    /// an operation that either updates, upserts or removes.
    pub fn find_and_modify(
        &'a self,
        query:     &Document,
        operation: FindAndModifyOperation<'a>,
        options:   Option<&FindAndModifyOptions>
    ) -> Result<Document> {
        assert!(!self.inner.is_null());

        let default_options = FindAndModifyOptions::default();
        let options         = options.unwrap_or(&default_options);
        let fields_bsonc    = options.fields_bsonc();

        // Bsonc to store the reply
        let mut reply = Bsonc::new();
        // Empty error that might be filled
        let mut error = BsoncError::empty();

        // Do these before the mongoc call to make sure we keep
        // them around long enough.
        let sort_bsonc = match options.sort {
            Some(ref doc) => {
                Some(try!(Bsonc::from_document(doc)))
            },
            None => None
        };
        let update_bsonc = match operation {
            FindAndModifyOperation::Update(ref doc) | FindAndModifyOperation::Upsert(ref doc) => {
                Some(try!(Bsonc::from_document(doc)))
            },
            FindAndModifyOperation::Remove => None
        };

        let success = unsafe {
            bindings::mongoc_collection_find_and_modify(
                self.inner,
                try!(Bsonc::from_document(&query)).inner(),
                match sort_bsonc {
                    Some(ref s) => s.inner(),
                    None => ptr::null()
                },
                match update_bsonc {
                    Some(ref u) => u.inner(),
                    None => ptr::null()
                },
                match fields_bsonc {
                    Some(ref f) => f.inner(),
                    None => ptr::null()
                },
                match operation {
                    FindAndModifyOperation::Remove => true,
                    _ => false
                } as u8,
                match operation {
                    FindAndModifyOperation::Upsert(_) => true,
                    _ => false
                } as u8,
                options.new as u8,
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

    /// Get the name of the collection.
    pub fn get_name(&self) -> Cow<str> {
        let cstr = unsafe {
            CStr::from_ptr(bindings::mongoc_collection_get_name(self.inner))
        };
        String::from_utf8_lossy(cstr.to_bytes())
    }

    /// Insert document into collection.
    /// If no `_id` element is found in document, then an id will be generated locally and added to the document.
    // TODO: You can retrieve a generated _id from mongoc_collection_get_last_error().
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

    /// Remove documents in the given collection that match selector.
    /// The bson `selector` is not validated, simply passed along as appropriate to the server. As such, compatibility and errors should be validated in the appropriate server documentation.
    ///  If you want to limit deletes to a single document, add the `SingleRemove` flag.
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

    /// Save a document into the collection. If the document has an `_id` field it will be updated.
    /// Otherwise it will be inserted.
    pub fn save(
        &self,
        document:      &Document,
        write_concern: Option<&WriteConcern>
    ) -> Result<()> {
        assert!(!self.inner.is_null());

        let default_write_concern = WriteConcern::default();
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

    /// This function updates documents in collection that match selector.
    /// By default, updates only a single document. Add `MultiUpdate` flag to update multiple documents.
    pub fn update(
        &self,
        selector: &Document,
        update:   &Document,
        options:  Option<&UpdateOptions>
    ) -> Result<()> {
        assert!(!self.inner.is_null());

        let default_options = UpdateOptions::default();
        let options         = options.unwrap_or(&default_options);

        let mut error = BsoncError::empty();
        let success = unsafe {
            bindings::mongoc_collection_update(
                self.inner,
                options.update_flags.flags(),
                try!(Bsonc::from_document(&selector)).inner(),
                try!(Bsonc::from_document(&update)).inner(),
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
        TailingCursor::new(
            self,
            query,
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

/// Provides an abstraction for submitting multiple write operations as a single batch.
///
/// Create a `BulkOperation` by calling `create_bulk_operation` on a `Collection`. After adding all of
/// the write operations using the functions on this struct, `execute` to execute the operation on
/// the server in batches. After executing the bulk operation is consumed and cannot be used anymore.
pub struct BulkOperation<'a> {
    _collection: &'a Collection<'a>,
    inner:       *mut bindings::mongoc_bulk_operation_t
}

impl<'a>BulkOperation<'a> {
    /// Create a new bulk operation, only for internal usage.
    fn new(
        collection: &'a Collection<'a>,
        inner:      *mut bindings::mongoc_bulk_operation_t
    ) -> BulkOperation<'a> {
        assert!(!inner.is_null());
        BulkOperation {
            _collection: collection,
            inner:       inner
        }
    }

    /// Queue an insert of a single document into a bulk operation.
    /// The insert is not performed until `execute` is called.
    pub fn insert(
        &self,
        document: &Document
    ) -> Result<()> {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_bulk_operation_insert(
                self.inner,
                try!(Bsonc::from_document(&document)).inner()
            )
        }
        Ok(())
    }

    /// Queue removal of all documents matching the provided selector into a bulk operation.
    /// The removal is not performed until `execute` is called.
    pub fn remove(
        &self,
        selector: &Document
    ) -> Result<()> {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_bulk_operation_remove(
                self.inner,
                try!(Bsonc::from_document(&selector)).inner()
            )
        }
        Ok(())
    }

    /// Queue removal of a single document into a bulk operation.
    /// The removal is not performed until `execute` is called.
    pub fn remove_one(
        &self,
        selector: &Document
    ) -> Result<()> {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_bulk_operation_remove_one(
                self.inner,
                try!(Bsonc::from_document(&selector)).inner()
            )
        }
        Ok(())
    }

    /// Queue replacement of a single document into a bulk operation.
    /// The replacement is not performed until `execute` is called.
    pub fn replace_one(
        &self,
        selector: &Document,
        document: &Document,
        upsert:   bool
    ) -> Result<()> {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_bulk_operation_replace_one(
                self.inner,
                try!(Bsonc::from_document(&selector)).inner(),
                try!(Bsonc::from_document(&document)).inner(),
                upsert as u8
            )
        }
        Ok(())
    }

    /// Queue update of a single documents into a bulk operation.
    /// The update is not performed until `execute` is called.
    ///
    /// TODO: document must only contain fields whose key starts
    /// with $, these is no error handling for this.
    pub fn update_one(
        &self,
        selector: &Document,
        document: &Document,
        upsert:   bool
    ) -> Result<()> {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_bulk_operation_update_one(
                self.inner,
                try!(Bsonc::from_document(&selector)).inner(),
                try!(Bsonc::from_document(&document)).inner(),
                upsert as u8
            )
        }
        Ok(())
    }

    /// Queue update of multiple documents into a bulk operation.
    /// The update is not performed until `execute` is called.
    ///
    /// TODO: document must only contain fields whose key starts
    /// with $, these is no error handling for this.
    pub fn update(
        &self,
        selector: &Document,
        document: &Document,
        upsert:   bool
    ) -> Result<()> {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_bulk_operation_update(
                self.inner,
                try!(Bsonc::from_document(&selector)).inner(),
                try!(Bsonc::from_document(&document)).inner(),
                upsert as u8
            )
        }
        Ok(())
    }

    /// This function executes all operations queued into this bulk operation.
    /// If ordered was set true, forward progress will be stopped upon the first error.
    ///
    /// This function takes ownership because it is not possible to execute a bulk operation
    /// multiple times.
    ///
    /// Returns a document with an overview of the bulk operation if successfull.
    pub fn execute(self) -> BulkOperationResult<Document> {
        // Bsonc to store the reply
        let mut reply = Bsonc::new();
        // Empty error that might be filled
        let mut error = BsoncError::empty();

        // Execute the operation. This returns a non-zero hint of the peer node on
        // success, otherwise 0 and error is set.
        let return_value = unsafe {
            bindings::mongoc_bulk_operation_execute(
                self.inner,
                reply.mut_inner(),
                error.mut_inner()
            )
        };

        let document = match reply.as_document_utf8_lossy() {
            Ok(document) => document,
            Err(error)   => return Err(BulkOperationError{error: error.into(), reply: doc!{}})
        };

        if return_value != 0 {
            Ok(document)
        } else {
            Err(BulkOperationError{error: error.into(), reply: document})
        }
    }
}

impl<'a> Drop for BulkOperation<'a> {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_bulk_operation_destroy(self.inner);
        }
    }
}
