#![feature(scoped)]

extern crate libc;
extern crate mongo_c_driver_wrapper;
extern crate bson;

use std::result;

use mongo_c_driver_wrapper::bindings;

pub mod bsonc;
pub mod client;
pub mod collection;
pub mod cursor;
pub mod error;
pub mod flags;
pub mod read_prefs;
pub mod uri;
pub mod write_concern;

pub use error::{MongoError,BsoncError,InvalidParamsError};

pub type Result<T> = result::Result<T, MongoError>;

static mut INITIALIZED: bool = false;

/// Init mongo driver, needs to be called once before doing
/// anything else.
pub fn init() {
  unsafe {
      bindings::mongoc_init();
      INITIALIZED = true;
  }
}

/// Clean up mongo driver's resources
pub fn cleanup() {
    unsafe {
        bindings::mongoc_cleanup();
        INITIALIZED = false;
    }
}

pub fn is_initialized() -> bool {
    unsafe { INITIALIZED }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_init_and_cleanup() {
        super::init();
        assert!(super::is_initialized());

        super::cleanup();
        assert!(!super::is_initialized());
    }
}
