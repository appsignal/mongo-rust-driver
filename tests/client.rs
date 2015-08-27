use std::path::PathBuf;
use std::thread;

use mongo_driver::uri::Uri;
use mongo_driver::client::{ClientPool,SslOptions};

#[test]
fn test_new_pool_and_pop_client() {
    let uri = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool = ClientPool::new(uri, None);

    // Pop a client and get a database and collection
    let client = pool.pop();
    pool.pop();

    let database = client.get_database("rust_test");
    assert_eq!("rust_test", database.get_name().to_mut());

    let collection = client.get_collection("rust_test", "items");
    assert_eq!("items", collection.get_name().to_mut());
}

#[test]
fn test_new_pool_and_pop_client_in_threads() {
    let uri = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool = ClientPool::new(uri, None);

    let pool1 = pool.clone();
    let guard1 = thread::spawn(move || {
        let client = pool1.pop();
        client.get_collection("test", "items");
    });

    let pool2 = pool.clone();
    let guard2 = thread::spawn(move || {
        let client = pool2.pop();
        client.get_collection("test", "items");
    });

    guard1.join().unwrap();
    guard2.join().unwrap();
}

#[test]
fn test_get_server_status() {
    let uri = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool = ClientPool::new(uri, None);
    let client = pool.pop();

    let status = client.get_server_status(None).unwrap();

    assert!(status.contains_key("host"));
    assert!(status.contains_key("version"));
}

#[test]
fn test_new_pool_with_ssl_options() {
    // We currently have no way to test full operations
    let uri = Uri::new("mongodb://localhost:27017/").unwrap();
    let ssl_options = SslOptions::new(
        Some(PathBuf::from("./README.md")),
        Some("password".to_string()),
        Some(PathBuf::from("./README.md")),
        Some(PathBuf::from("./README.md")),
        Some(PathBuf::from("./README.md")),
        false
    );
    assert!(ssl_options.is_ok());
    ClientPool::new(uri, Some(ssl_options.unwrap()));
}

#[test]
fn test_ssl_options_nonexistent_file() {
    assert!(SslOptions::new(
        Some(PathBuf::from("/tmp/aaaaa.aa")),
        Some("password".to_string()),
        Some(PathBuf::from("/tmp/aaaaa.aa")),
        Some(PathBuf::from("/tmp/aaaaa.aa")),
        Some(PathBuf::from("/tmp/aaaaa.aa")),
        false
    ).is_err());
}
