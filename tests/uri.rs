use mongo_driver::uri::Uri;

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
