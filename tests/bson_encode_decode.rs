use chrono::prelude::*;

use mongo_driver::client::{ClientPool,Uri};

use bson::oid::ObjectId;
use bson::spec::BinarySubtype;

// Sanity check to make sure the bson implementation
// properly encodes and decodes when passing through
// the database.

#[test]
fn test_bson_encode_decode() {
    let uri    = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool   = ClientPool::new(uri, None);
    let client = pool.pop();
    let mut collection = client.get_collection("rust_driver_test", "bson");
    collection.drop().unwrap_or(());

    let datetime = Utc.ymd(2014, 7, 8).and_hms(9, 10, 11);
    let document = doc! {
        "_id" => (ObjectId::new().unwrap()),
        "floating_point" => 10.0,
        "string" => "a value",
        "array" => [10, 20, 30],
        "doc" => {"key" => 1},
        "bool" => true,
        "i32" => 1i32,
        "i64" => 1i64,
        "datetime" => datetime,
        "binary_generic" => (BinarySubtype::Generic, vec![0, 1, 2, 3, 4])
    };
    assert!(collection.insert(&document, None).is_ok());

    let found_document = collection.find(&doc!{}, None).unwrap().next().unwrap().unwrap();

    assert_eq!(document, found_document);
}
