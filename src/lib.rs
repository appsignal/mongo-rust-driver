extern crate libc;
extern crate mongo_c_driver_wrapper;
extern crate bson;

use std::result;
use std::sync::{Once,ONCE_INIT};

use mongo_c_driver_wrapper::bindings;

pub mod bsonc;
pub mod bulk_operation;
pub mod client;
pub mod collection;
pub mod cursor;
pub mod database;
pub mod error;
pub mod flags;
pub mod read_prefs;
pub mod uri;
pub mod write_concern;

pub use error::{MongoError,BsoncError,InvalidParamsError};

pub type Result<T> = result::Result<T, MongoError>;

static MONGOC_INIT: Once = ONCE_INIT;

/// Init mongo driver, needs to be called once before doing
/// anything else.
fn init() {
    MONGOC_INIT.call_once(|| {
        unsafe { bindings::mongoc_init(); }
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_init() {
        super::init();
        super::init();
    }
}
