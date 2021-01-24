extern crate bson;
extern crate mongo_driver;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bson::doc;

use mongo_driver::client::{ClientPool,Uri};
use mongo_driver::Result;

#[test]
fn test_cursor() {
    let uri        = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool       = ClientPool::new(uri, None);
    let client     = pool.pop();
    let mut collection = client.get_collection("rust_driver_test", "cursor_items");

    let document = doc! { "key": "value" };

    collection.drop().unwrap_or(());
    for _ in 0..10 {
        assert!(collection.insert(&document, None).is_ok());
    }

    let query  = doc! {};
    let cursor = collection.find(&query, None).unwrap();

    let documents = cursor.into_iter().collect::<Vec<Result<bson::Document>>>();

    // See if we got 10 results and the iterator then stopped
    assert_eq!(10, documents.len());
}

#[test]
fn test_tailing_cursor() {
    // See: http://mongoc.org/libmongoc/current/cursors.html#tailable

    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = Arc::new(ClientPool::new(uri, None));
    let client   = pool.pop();
    let database = client.get_database("rust_test");
    database.get_collection("capped").drop().unwrap_or(());
    database.get_collection("not_capped").drop().unwrap_or(());

    let options = doc! {
        "capped": true,
        "size": 100000
    };
    let capped_collection = database.create_collection("capped", Some(&options)).unwrap();
    let normal_collection = database.create_collection("not_capped", None).unwrap();

    // Try to tail on a normal collection
    let failing_cursor = normal_collection.tail(doc!{}, None, None);
    let failing_result = failing_cursor.into_iter().next().expect("Nothing in iterator");
    assert!(failing_result.is_err());

    let document = doc! { "key_1": "Value 1" };
    // Insert a first document into the collection
    capped_collection.insert(&document, None).unwrap();

    // Start a tailing iterator in a thread
    let cloned_pool = pool.clone();
    let guard = thread::spawn(move || {
        let client     = cloned_pool.pop();
        let collection = client.get_collection("rust_test", "capped");
        let cursor = collection.tail(doc!{}, None, None);
        let mut counter = 0usize;
        for result in cursor.into_iter() {
            assert!(result.is_ok());
            counter += 1;
            if counter == 25 {
                break;
            }
        }
        counter
    });

    // Wait for the thread to boot up
    thread::sleep(Duration::from_secs(1));

    // Insert some more documents into the collection
    for _ in 0..25 {
        capped_collection.insert(&document, None).unwrap();
    }

    // See if they appeared while iterating the cursor
    // The for loop returns whenever we get more than
    // 15 results.
    assert_eq!(25, guard.join().expect("Thread failed"));
}

#[cfg_attr(target_os = "windows", ignore)]
#[test]
fn test_batch_cursor() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = Arc::new(ClientPool::new(uri, None));
    let client   = pool.pop();
    let database = client.get_database("rust_test");

    const TEST_COLLECTION_NAME: &str = "test_batch_cursor";
    const NUM_TO_TEST: i32 = 10000;

    let mut collection = database.get_collection(TEST_COLLECTION_NAME);
    if database.has_collection(TEST_COLLECTION_NAME).unwrap() {
        collection.drop().unwrap();  // if prev test failed the old collection may still exist
    }

    // add test rows.  need many to exercise the batches
    {
        let bulk_operation = collection.create_bulk_operation(None);

        for i in 0..NUM_TO_TEST {
            bulk_operation.insert(&doc!{"key": i}).unwrap();
        }

        let result = bulk_operation.execute();
        assert!(result.is_ok());

        assert_eq!(
            result.ok().unwrap().get("nInserted").unwrap(), // why is this an i32?
            &bson::Bson::Int32(NUM_TO_TEST)
        );
        assert_eq!(NUM_TO_TEST as i64, collection.count(&doc!{}, None).unwrap());
    }

    {
        let cur = database.command_batch(doc!{"find":TEST_COLLECTION_NAME},None);
        let mut count = 0;
        for doc in cur.unwrap() {
            count += 1;
            println!("doc: {:?}", doc );
        }
        assert_eq!(count,NUM_TO_TEST);
    }

    collection.drop().unwrap();
}
