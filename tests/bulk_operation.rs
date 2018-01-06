use std::env;

use bson;

use mongo_driver::client::{ClientPool,Uri};

#[test]
fn test_execute_error() {
    let uri            = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool           = ClientPool::new(uri, None);
    let client         = pool.pop();
    let collection     = client.get_collection("rust_driver_test", "bulk_operation_error");
    let bulk_operation = collection.create_bulk_operation(None);

    let result = bulk_operation.execute();
    assert!(result.is_err());

    let error_message = format!("{:?}", result.err().unwrap());
    assert_eq!(error_message, "BulkOperationError { error: MongoError (BsoncError: Command/CommandInvalidArg - Cannot do an empty bulk write), reply: OrderedDocument({}) }");
}

#[test]
fn test_basics() {
    let uri            = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool           = ClientPool::new(uri, None);
    let client         = pool.pop();
    let collection     = client.get_collection("rust_driver_test", "bulk_operation_basics");
    let bulk_operation = collection.create_bulk_operation(None);

    let document = doc! {"key_1" => "Value 1"};
    bulk_operation.insert(&document).unwrap();
    assert!(bulk_operation.execute().is_ok());

    let first_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
    assert_eq!(
        first_document.get("key_1").unwrap(),
        &bson::Bson::String("Value 1".to_string())
    );
}

#[test]
fn test_utf8() {
    let uri            = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool           = ClientPool::new(uri, None);
    let client         = pool.pop();
    let collection     = client.get_collection("rust_driver_test", "bulk_operation_utf8");
    let bulk_operation = collection.create_bulk_operation(None);

    let document = doc! {"key_1" => "kācaṃ śaknomyattum; nopahinasti mām."};
    bulk_operation.insert(&document).unwrap();
    assert!(bulk_operation.execute().is_ok());

    let first_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
    assert_eq!(
        first_document.get("key_1").unwrap(),
        &bson::Bson::String("kācaṃ śaknomyattum; nopahinasti mām.".to_string())
    );
}

#[test]
fn test_utf8_invalid() {
    let uri            = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool           = ClientPool::new(uri, None);
    let client         = pool.pop();
    let collection     = client.get_collection("rust_driver_test", "bulk_operation_utf8_invalid");
    let bulk_operation = collection.create_bulk_operation(None);

    let bytes = b"\x80\xae".to_vec();
    let value = unsafe { String::from_utf8_unchecked(bytes) };
    let document = doc! {"key_1" => value};
    bulk_operation.insert(&document).unwrap();
    assert!(bulk_operation.execute().is_ok());

    let first_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
    assert_eq!(
        first_document.get("key_1").unwrap(),
        &bson::Bson::String("𤭢".to_string())
    );
}

#[test]
fn test_insert_remove_replace_update_extended() {
    if env::var("SKIP_EXTENDED_BULK_OPERATION_TESTS") == Ok("true".to_string()) {
        return
    }

    let uri            = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool           = ClientPool::new(uri, None);
    let client         = pool.pop();
    let mut collection = client.get_collection("rust_driver_test", "bulk_operation_extended");
    collection.drop().unwrap_or(());

    // Insert 5 documents
    {
        let bulk_operation = collection.create_bulk_operation(None);

        let document = doc! {
            "key_1" => "Value 1",
            "key_2" => "Value 2"
        };
        for _ in 0..5 {
            bulk_operation.insert(&document).unwrap();
        }

        let result = bulk_operation.execute();
        assert!(result.is_ok());

        assert_eq!(
            result.ok().unwrap().get("nInserted").unwrap(),
            &bson::Bson::I32(5)
        );
        assert_eq!(5, collection.count(&doc!{}, None).unwrap());
    }

    let query = doc!{};

    let update_document = doc! {
        "$set" => {"key_1" => "Value update"}
    };

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
            result.ok().unwrap().get("nModified").unwrap(),
            &bson::Bson::I32(1)
        );

        let first_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
        assert_eq!(
            first_document.get("key_1").unwrap(),
            &bson::Bson::String("Value update".to_string())
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
            result.ok().unwrap().get("nModified").unwrap(),
            &bson::Bson::I32(4)
        );

        collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
        let second_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
        assert_eq!(
            second_document.get("key_1").unwrap(),
            &bson::Bson::String("Value update".to_string())
        );
        // Make sure it was updated, it should have other keys
        assert!(second_document.get("key_2").is_some());
    }

    // Replace one
    {
        let replace_document = doc! { "key_1" => "Value replace" };

        let bulk_operation = collection.create_bulk_operation(None);
        bulk_operation.replace_one(
            &query,
            &replace_document,
            false
        ).unwrap();

        let result = bulk_operation.execute();
        assert!(result.is_ok());

        assert_eq!(
            result.ok().unwrap().get("nModified").unwrap(),
            &bson::Bson::I32(1)
        );

        let first_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
        assert_eq!(
            first_document.get("key_1").unwrap(),
            &bson::Bson::String("Value replace".to_string())
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
            result.ok().unwrap().get("nRemoved").unwrap(),
            &bson::Bson::I32(1)
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
            result.ok().unwrap().get("nRemoved").unwrap(),
            &bson::Bson::I32(4)
        );
        assert_eq!(0, collection.count(&query, None).unwrap());
    }
}
