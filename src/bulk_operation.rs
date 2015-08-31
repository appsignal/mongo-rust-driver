use mongoc::bindings;
use bson::Document;

use super::BsoncError;
use super::bsonc::Bsonc;
use super::collection::Collection;

use super::Result;

pub struct BulkOperation<'a> {
    _collection: &'a Collection<'a>,
    inner:       *mut bindings::mongoc_bulk_operation_t
}

impl<'a> BulkOperation<'a> {
    pub fn new(
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
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_bulk_operation_insert.html
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

    /// Queue removal of al documents matching selector into a bulk operation.
    /// The removal is not performed until `execute` is called.
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_bulk_operation_remove.html
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
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_bulk_operation_remove_one.html
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
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_bulk_operation_remove_one.html
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
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_bulk_operation_update_one.html
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
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_bulk_operation_update_one.html
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
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_bulk_operation_execute.html
    pub fn execute(self) -> Result<Document> {
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

        if return_value != 0 {
            match reply.as_document() {
                Ok(document) => return Ok(document),
                Err(error)   => return Err(error.into())
            }
        } else {
            Err(error.into())
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
