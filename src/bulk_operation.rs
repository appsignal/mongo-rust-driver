extern crate libc;
extern crate mongo_c_driver_wrapper;
extern crate bson;

use mongo_c_driver_wrapper::bindings;
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

#[cfg(test)]
mod tests {
    use bson;
    use super::super::uri::Uri;
    use super::super::client::ClientPool;

    #[test]
    fn test_execute_error() {
        let uri            = Uri::new("mongodb://localhost:27017/");
        let pool           = ClientPool::new(uri, None);
        let client         = pool.pop();
        let collection     = client.get_collection("rust_driver_test", "bulk_operation_error");
        let bulk_operation = collection.create_bulk_operation(None);

        let result = bulk_operation.execute();
        assert!(result.is_err());

        let error_message = format!("{:?}", result.err().unwrap());
        assert_eq!(error_message, "MongoError (BsoncError: Cannot do an empty bulk write)");
    }

    #[test]
    fn test_insert_remove_replace_update() {
        let uri            = Uri::new("mongodb://localhost:27017/");
        let pool           = ClientPool::new(uri, None);
        let client         = pool.pop();
        let mut collection = client.get_collection("rust_driver_test", "bulk_operation_insert");
        collection.drop().unwrap_or(());

        // Insert 5 documents
        {
            let bulk_operation = collection.create_bulk_operation(None);

            let mut document = bson::Document::new();
            document.insert("key_1".to_string(), bson::Bson::String("Value 1".to_string()));
            document.insert("key_2".to_string(), bson::Bson::String("Value 2".to_string()));
            for _ in 0..5 {
                bulk_operation.insert(&document).unwrap();
            }

            let result = bulk_operation.execute();
            assert!(result.is_ok());

            assert_eq!(
                result.ok().unwrap().get("nInserted").unwrap().to_json(),
                bson::Bson::I32(5).to_json()
            );
            assert_eq!(5, collection.count(&bson::Document::new(), None).unwrap());
        }

        let query = bson::Document::new();

        let mut update_document = bson::Document::new();
        let mut set = bson::Document::new();
        set.insert("key_1".to_string(), bson::Bson::String("Value update".to_string()));
        update_document.insert("$set".to_string(), bson::Bson::Document(set));

        // Update one
        {
            let bulk_operation = collection.create_bulk_operation(None);
            bulk_operation.update_one(
                &query,
                &update_document,
                false
            ).unwrap();

            let result = bulk_operation.execute();
            println!("{:?}", result);
            assert!(result.is_ok());

            assert_eq!(
                result.ok().unwrap().get("nModified").unwrap().to_json(),
                bson::Bson::I32(1).to_json()
            );

            let first_document = collection.find(&bson::Document::new(), None).unwrap().next().unwrap().unwrap();
            assert_eq!(
                first_document.get("key_1").unwrap().to_json(),
                bson::Bson::String("Value update".to_string()).to_json()
            );
            // Make sure it was updated, it should have other keys
            assert!(first_document.get("key_2").is_some());
        }

        // Update all
        {
            let bulk_operation = collection.create_bulk_operation(None);
            bulk_operation.update(
                &query,
                &update_document,
                false
            ).unwrap();

            let result = bulk_operation.execute();
            println!("{:?}", result);
            assert!(result.is_ok());

            assert_eq!(
                result.ok().unwrap().get("nModified").unwrap().to_json(),
                bson::Bson::I32(4).to_json()
            );

            collection.find(&bson::Document::new(), None).unwrap().next().unwrap().unwrap();
            let second_document = collection.find(&bson::Document::new(), None).unwrap().next().unwrap().unwrap();
            assert_eq!(
                second_document.get("key_1").unwrap().to_json(),
                bson::Bson::String("Value update".to_string()).to_json()
            );
            // Make sure it was updated, it should have other keys
            assert!(second_document.get("key_2").is_some());
        }

        // Replace one
        {
            let mut replace_document = bson::Document::new();
            replace_document.insert("key_1".to_string(), bson::Bson::String("Value replace".to_string()));

            let bulk_operation = collection.create_bulk_operation(None);
            bulk_operation.replace_one(
                &query,
                &replace_document,
                false
            ).unwrap();

            let result = bulk_operation.execute();
            assert!(result.is_ok());

            assert_eq!(
                result.ok().unwrap().get("nModified").unwrap().to_json(),
                bson::Bson::I32(1).to_json()
            );

            let first_document = collection.find(&bson::Document::new(), None).unwrap().next().unwrap().unwrap();
            assert_eq!(
                first_document.get("key_1").unwrap().to_json(),
                bson::Bson::String("Value replace".to_string()).to_json()
            );
            // Make sure it was replaced, it shouldn't have other keys
            assert!(first_document.get("key_2").is_none());
        }

        // Remove one
        {
            let bulk_operation = collection.create_bulk_operation(None);
            bulk_operation.remove_one(&query).unwrap();

            let result = bulk_operation.execute();
            assert!(result.is_ok());

            assert_eq!(
                result.ok().unwrap().get("nRemoved").unwrap().to_json(),
                bson::Bson::I32(1).to_json()
            );
            assert_eq!(4, collection.count(&query, None).unwrap());
        }

        // Remove all remaining documents
        {
            let bulk_operation = collection.create_bulk_operation(None);
            bulk_operation.remove(&query).unwrap();

            let result = bulk_operation.execute();
            assert!(result.is_ok());

            assert_eq!(
                result.ok().unwrap().get("nRemoved").unwrap().to_json(),
                bson::Bson::I32(4).to_json()
            );
            assert_eq!(0, collection.count(&query, None).unwrap());
        }
    }
}
