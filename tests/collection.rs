use bson;

use mongo_driver::CommandAndFindOptions;
use mongo_driver::collection::{CountOptions,FindAndModifyOperation};
use mongo_driver::client::{ClientPool,Uri};
use mongo_driver::flags;

#[test]
fn test_aggregate() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = ClientPool::new(uri, None);
    let client   = pool.pop();
    let mut collection = client.get_collection("rust_driver_test", "aggregate");
    collection.drop().unwrap_or(());

    for _ in 0..5 {
        assert!(collection.insert(&doc!{"key" => 1}, None).is_ok());
    }

    let pipeline = doc!{
        "pipeline" => [
            {
                "$group" => {
                    "_id" => "$key",
                    "total" => {"$sum" => "$key"}
                }
            }
        ]
    };

    let total = collection.aggregate(&pipeline, None).unwrap().next().unwrap().unwrap();

    assert_eq!(Ok(5), total.get_i32("total"));
}

#[test]
fn test_command() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = ClientPool::new(uri, None);
    let client   = pool.pop();
    let collection = client.get_collection("rust_driver_test", "items");

    let command = doc! { "ping" => 1 };

    let result = collection.command(command, None).unwrap().next().unwrap().unwrap();
    assert!(result.contains_key("ok"));
}

#[test]
fn test_command_simple() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = ClientPool::new(uri, None);
    let client   = pool.pop();
    let collection = client.get_collection("rust_driver_test", "items");

    let command = doc! { "ping" => 1 };

    let result = collection.command_simple(command, None).unwrap();
    assert!(result.contains_key("ok"));
}

#[test]
fn test_mutation_and_finding() {
    let uri        = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool       = ClientPool::new(uri, None);
    let client     = pool.pop();
    let _ =  client.get_collection("rust_driver_test".to_string(), "items");
    let mut collection = client.get_collection("rust_driver_test", "items");
    collection.drop().unwrap_or(());

    assert_eq!("items", collection.get_name().to_mut());

    let document = doc! {
        "key_1" => "Value 1",
        "key_2" => "kācaṃ śaknomyattum; nopahinasti mām. \u{0}"
    };
    collection.insert(&document, None).expect("Could not insert document");
    {
        let found_document = collection.find(&document, None).unwrap().next().unwrap().unwrap();
        assert_eq!(
            found_document.get("key_1").unwrap(),
            &bson::Bson::String("Value 1".to_string())
        );
        assert_eq!(
            found_document.get("key_2").unwrap(),
            &bson::Bson::String("kācaṃ śaknomyattum; nopahinasti mām. \u{0}".to_string())
        );
    }

    let second_document = doc! {
        "key_1" => "Value 3"
    };
    assert!(collection.insert(&second_document, None).is_ok());

    let query = doc!{};

    // Count the documents in the collection
    assert_eq!(2, collection.count(&query, None).unwrap());

    // Count with options set
    let mut count_options = CountOptions::default();
    count_options.opts = Some(doc!{});
    assert_eq!(2, collection.count(&query, Some(&count_options)).unwrap());

    // Find the documents
    assert_eq!(
        collection.find(&document, None).unwrap().next().unwrap().unwrap().get("key_1").unwrap(),
        &bson::Bson::String("Value 1".to_string())
    );
    let found_document = collection.find(&second_document, None).unwrap().next().unwrap().unwrap();
    assert_eq!(
        found_document.get("key_1").unwrap(),
        &bson::Bson::String("Value 3".to_string())
    );

    // Update the second document
    let update = doc!{"$set" => {"key_1" => "Value 4"}};
    assert!(collection.update(&second_document, &update, None).is_ok());

    // Reload and check value
    let query_after_update = doc! {
        "key_1" => "Value 4"
    };
    let mut found_document = collection.find(&query_after_update, None).unwrap().next().unwrap().unwrap();
    assert_eq!(
        found_document.get("key_1").unwrap(),
        &bson::Bson::String("Value 4".to_string())
    );

    // Save the second document
    found_document.insert("key_1".to_string(), bson::Bson::String("Value 5".to_string()));
    assert!(collection.save(&found_document, None).is_ok());

    // Reload and check value
    let found_document = collection.find(&found_document, None).unwrap().next().unwrap().unwrap();
    assert_eq!(
        found_document.get("key_1").unwrap(),
        &bson::Bson::String("Value 5".to_string())
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
        let options = CommandAndFindOptions {
            query_flags: flags::Flags::new(),
            skip:        0,
            limit:       0,
            batch_size:  0,
            fields:      Some(doc! { "key_1" => true }),
            read_prefs:  None
        };

        // Query a couple of times to make sure the C driver keeps
        // access to the fields bson object.
        for _ in 0..5 {
            collection.find(&query, Some(&options)).unwrap();
        }

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
fn test_find_and_modify() {
    let uri        = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool       = ClientPool::new(uri, None);
    let client     = pool.pop();
    let mut collection = client.get_collection("rust_driver_test", "find_and_modify");
    collection.drop().unwrap_or(());

    // Upsert something, it should now exist
    let query = doc! {
        "key_1" => "Value 1"
    };
    let update = doc! {
        "$set" => {"content" => 1i32}
    };
    let result = collection.find_and_modify(
        &query,
        FindAndModifyOperation::Upsert(&update),
        None
    );
    assert!(result.is_ok());
    assert_eq!(1, collection.count(&query, None).unwrap());
    let found_document = collection.find(&query, None).unwrap().next().unwrap().unwrap();
    assert_eq!(found_document.get_i32("content"), Ok(1));

    // Update this record
    let update2 = doc! {
        "$set" => {"content" => 2i32}
    };
    let result = collection.find_and_modify(
        &query,
        FindAndModifyOperation::Update(&update2),
        None
    );
    assert!(result.is_ok());
    assert_eq!(1, collection.count(&query, None).unwrap());
    let found_document = collection.find(&query, None).unwrap().next().unwrap().unwrap();
    assert_eq!(found_document.get_i32("content"), Ok(2));

    // Remove it
    let result = collection.find_and_modify(
        &query,
        FindAndModifyOperation::Remove,
        None
    );
    assert!(result.is_ok());
    assert_eq!(0, collection.count(&query, None).unwrap());
}

#[test]
fn test_insert_failure() {
    let uri        = Uri::new("mongodb://localhost:27018/?serverSelectionTimeoutMS=1").unwrap(); // There should be no mongo server here
    let pool       = ClientPool::new(uri, None);
    let client     = pool.pop();
    let collection = client.get_collection("rust_driver_test", "items");
    let document   = doc! {};

    let result = collection.insert(&document, None);
    assert!(result.is_err());
    assert!(format!("{:?}", result.err().unwrap()).contains("No suitable servers found"));
}
