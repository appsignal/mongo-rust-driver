use bson;

use mongo_driver::uri::Uri;
use mongo_driver::client::ClientPool;

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
            result.ok().unwrap().get("nInserted").unwrap().to_json(),
            bson::Bson::I32(5).to_json()
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
            result.ok().unwrap().get("nModified").unwrap().to_json(),
            bson::Bson::I32(1).to_json()
        );

        let first_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
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

        collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
        let second_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
        assert_eq!(
            second_document.get("key_1").unwrap().to_json(),
            bson::Bson::String("Value update".to_string()).to_json()
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
            result.ok().unwrap().get("nModified").unwrap().to_json(),
            bson::Bson::I32(1).to_json()
        );

        let first_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();
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
