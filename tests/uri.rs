extern crate mongo_driver;
use mongo_driver::client::Uri;

#[test]
fn test_new_uri() {
    let uri = Uri::new("mongodb://localhost:27017/").unwrap();
    assert_eq!("mongodb://localhost:27017/", uri.as_str());
}

#[test]
fn test_new_invalid_uri() {
    assert!(Uri::new("@:/mongo::").is_none());
}

#[test]
fn test_get_database_empty() {
    let uri = Uri::new("mongodb://localhost:27017/").unwrap();
    assert!(uri.get_database().is_none());
}

#[test]
fn test_get_database() {
    let uri = Uri::new("mongodb://localhost:27017/db").unwrap();
    assert_eq!("db", uri.get_database().unwrap());
}

#[test]
fn test_equality() {
    let uri1 = Uri::new("mongodb://localhost:27017/").unwrap();
    let uri2 = Uri::new("mongodb://localhost:27018/").unwrap();

    assert_eq!(uri1, uri1.clone());
    assert!(uri1 == uri1.clone());
    assert!(uri1 != uri2);
}
