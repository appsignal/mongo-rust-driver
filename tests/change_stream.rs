//use bson;

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use mongo_driver::client::{ClientPool,Uri};

#[test]
fn test_change_stream() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = Arc::new(ClientPool::new(uri, None));
    let client   = pool.pop();
    let collection = client.get_collection("rust_driver_test", "change_stream");

    let cloned_pool = pool.clone();
    let guard = thread::spawn(move || {
        let client     = cloned_pool.pop();
        let collection = client.get_collection("rust_driver_test", "change_stream");
        let stream = collection.watch(&doc!{}, &doc!{}, Some(1000)).unwrap();

        let mut counter = 10;
        for x in stream {
            let c = x.unwrap().get_document("fullDocument").unwrap().get_i32("c").unwrap();
            if c == counter {
                counter += 1;
            }
            if counter == 25 {
                break;
            }
        };
        counter
    });

    thread::sleep(Duration::from_millis(100));

    for i in 0..25 {
        collection.insert(&doc! {"c": i}, None).unwrap();
    }

    assert_eq!(25, guard.join().unwrap());
}

