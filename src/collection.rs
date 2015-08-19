use std::ptr;
use std::ffi::CStr;
use std::borrow::Cow;

use mongo_c_driver_wrapper::bindings;

use bson::{Bson,Document};

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

        let inner = unsafe {
            bindings::mongoc_collection_command(
                self.inner,
                options.query_flags.flags(),
                options.skip,
                options.limit,
                options.batch_size,
                try!(Bsonc::from_document(&command)).inner(),
                match options.fields {
                    Some(ref f) => {
                        try!(Bsonc::from_document(f)).inner()
                    },
                    None => ptr::null()
                },
                match options.read_prefs {
                    Some(ref prefs) => prefs.inner(),
                    None => ptr::null()
                }
            )
        };

        if inner.is_null() {
            return Err(InvalidParamsError.into())
        }

        Ok(Cursor::new(cursor::CreatedBy::Collection(self), inner))
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

        let inner = unsafe {
            bindings::mongoc_collection_find(
                self.inner,
                options.query_flags.flags(),
                options.skip,
                options.limit,
                options.batch_size,
                try!(Bsonc::from_document(query)).inner(),
                match options.fields {
                    Some(ref f) => {
                        try!(Bsonc::from_document(f)).inner()
                    },
                    None => ptr::null()
                },
                match options.read_prefs {
                    Some(ref prefs) => prefs.inner(),
                    None => ptr::null()
                }
            )
        };

        if inner.is_null() {
            return Err(InvalidParamsError.into())
        }

        Ok(Cursor::new(cursor::CreatedBy::Collection(self), inner))
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
        let mut query_with_options = Document::new();
        query_with_options.insert(
            "$query".to_string(),
            Bson::Document(query)
        );
        query_with_options.insert("$natural".to_string(), Bson::I32(1));

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

#[cfg(test)]
mod tests {
    use bson;
    use super::super::uri::Uri;
    use super::super::client::ClientPool;
    use super::super::flags;

    #[test]
    fn test_command() {
        let uri      = Uri::new("mongodb://localhost:27017/");
        let pool     = ClientPool::new(uri);
        let client   = pool.pop();
        let collection = client.get_collection("rust_driver_test", "items");

        let mut command = bson::Document::new();
        command.insert("ping".to_string(), bson::Bson::I32(1));

        let result = collection.command(command, None).unwrap().next().unwrap().unwrap();
        assert!(result.contains_key("ok"));
    }

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
        assert!(collection.insert(&document, None).is_ok());

        let mut second_document = bson::Document::new();
        second_document.insert("key_1".to_string(), bson::Bson::String("Value 3".to_string()));
        assert!(collection.insert(&second_document, None).is_ok());

        let query = bson::Document::new();

        // Count the documents in the collection
        assert_eq!(2, collection.count(&query, None).unwrap());

        // Find the documents
        assert_eq!(
            collection.find(&document, None).unwrap().next().unwrap().unwrap().get("key_1").unwrap().to_json(),
            bson::Bson::String("Value 1".to_string()).to_json()
        );
        let mut found_document = collection.find(&second_document, None).unwrap().next().unwrap().unwrap();
        assert_eq!(
            found_document.get("key_1").unwrap().to_json(),
            bson::Bson::String("Value 3".to_string()).to_json()
        );

        // Update the second document
        found_document.insert("key_1".to_string(), bson::Bson::String("Value 4".to_string()));
        assert!(collection.save(&found_document, None).is_ok());

        // Reload and check value
        let found_document = collection.find(&found_document, None).unwrap().next().unwrap().unwrap();
        assert_eq!(
            found_document.get("key_1").unwrap().to_json(),
            bson::Bson::String("Value 4".to_string()).to_json()
        );

        // Remove one
        assert!(collection.remove(&found_document, None).is_ok());

        // Count again
        assert_eq!(1, collection.count(&query, None).unwrap());

        // Find the document and see if it has the keys we expect
        {
            let mut cursor = collection.find(&query, None).unwrap();
            let next_document = cursor.next().unwrap().unwrap();
            assert!(next_document.contains_key("key_1"));
            assert!(next_document.contains_key("key_2"));
        }

        // Find the document with fields set
        {
            let mut fields = bson::Document::new();
            fields.insert("key_1".to_string(), bson::Bson::Boolean(true));
            let options = super::super::CommandAndFindOptions {
                query_flags: flags::Flags::new(),
                skip:        0,
                limit:       0,
                batch_size:  0,
                fields:      Some(fields),
                read_prefs:  None
            };
            let mut cursor = collection.find(&query, Some(&options)).unwrap();
            let next_document = cursor.next().unwrap().unwrap();
            assert!(next_document.contains_key("key_1"));
            assert!(!next_document.contains_key("key_2"));
        }

        // Drop collection
        collection.drop().unwrap();
        assert_eq!(0, collection.count(&query, None).unwrap());
    }

    #[test]
    fn test_insert_failure() {
        let uri        = Uri::new("mongodb://localhost:27018/"); // There should be no mongo server here
        let pool       = ClientPool::new(uri);
        let client     = pool.pop();
        let collection = client.get_collection("rust_driver_test", "items");
        let document   = bson::Document::new();

        let result = collection.insert(&document, None);
        assert!(result.is_err());
        assert_eq!(
            "MongoError (BsoncError: Failed to connect to target host: localhost:27018)",
            format!("{:?}", result.err().unwrap())
        );
    }
}
