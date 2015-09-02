use bson;

use mongo_driver::CommandAndFindOptions;
use mongo_driver::uri::Uri;
use mongo_driver::client::ClientPool;
use mongo_driver::flags;

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
        "key_2" => "Value 2"
    };
    assert!(collection.insert(&document, None).is_ok());

    let second_document = doc! {
        "key_1" => "Value 3"
    };
    assert!(collection.insert(&second_document, None).is_ok());

    let query = doc!{};

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
fn test_insert_failure() {
    let uri        = Uri::new("mongodb://localhost:27018/").unwrap(); // There should be no mongo server here
    let pool       = ClientPool::new(uri, None);
    let client     = pool.pop();
    let collection = client.get_collection("rust_driver_test", "items");
    let document   = doc! {};

    let result = collection.insert(&document, None);
    assert!(result.is_err());
    assert_eq!(
        "MongoError (BsoncError: Failed to connect to target host: localhost:27018)",
        format!("{:?}", result.err().unwrap())
    );
}
