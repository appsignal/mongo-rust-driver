use mongo_driver::client::{ClientPool,Uri};

#[test]
fn test_command() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = ClientPool::new(uri, None);
    let client   = pool.pop();
    let database = client.get_database("rust_test");

    let command = doc! { "ping" => 1 };

    let result = database.command(command, None).unwrap().next().unwrap().unwrap();
    assert!(result.contains_key("ok"));
}

#[test]
fn test_command_simple() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = ClientPool::new(uri, None);
    let client   = pool.pop();
    let database = client.get_database("rust_test");

    let command = doc! { "ping" => 1 };

    let result = database.command_simple(command, None).unwrap();
    assert!(result.contains_key("ok"));
}

#[test]
fn test_get_collection_and_name() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = ClientPool::new(uri, None);
    let client   = pool.pop();
    let database = client.get_database("rust_test");

    assert_eq!("rust_test", database.get_name().to_mut());

    let collection = database.get_collection("items");
    assert_eq!("items", collection.get_name().to_mut());
}

#[test]
fn test_create_collection() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = ClientPool::new(uri, None);
    let client   = pool.pop();
    let database = client.get_database("rust_test");
    database.get_collection("created_collection").drop().unwrap_or(());

    let collection = database.create_collection(
        "created_collection",
        None
    ).unwrap();

    assert_eq!("created_collection", collection.get_name().to_mut());
}

#[test]
fn test_has_collection() {
    let uri      = Uri::new("mongodb://localhost:27017/").unwrap();
    let pool     = ClientPool::new(uri, None);
    let client   = pool.pop();
    let database = client.get_database("rust_test");
    database.get_collection("created_collection").drop().unwrap_or(());

    let collection = database.create_collection(
        "created_collection",
        None
    ).unwrap();

    assert_eq!("created_collection", collection.get_name().to_mut());
    assert!(database.has_collection("created_collection").unwrap());
}