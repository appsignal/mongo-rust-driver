use std::env;

pub fn mongodb_test_connection_string() -> &'static str {
    match env::var("MONGODB_CONNECTION_STRING") {
        Ok(value) => Box::leak(value.into_boxed_str()),
        Err(_) => "mongodb://localhost:27017",
    }
}
