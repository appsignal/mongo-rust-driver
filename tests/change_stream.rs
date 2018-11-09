//use bson;

use std::sync::Arc;
use std::thread;

use mongo_driver::client::{ClientPool,Uri};
use mongo_driver::Result;

#[test]
fn test_change_stream() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = Arc::new(ClientPool::new(uri, None));
    let client   = pool.pop();
    let mut collection = client.get_collection("rust_driver_test", "change_stream");

    let stream = collection.watch(&doc!{}, &doc!{"maxAwaitTimeMS": 10_000}).unwrap();
    let next = stream.into_iter().next().unwrap().unwrap();
    assert_eq!(true, false);
}

