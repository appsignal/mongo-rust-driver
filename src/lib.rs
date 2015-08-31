#![feature(convert)]

extern crate libc;
extern crate mongoc_sys as mongoc;

#[macro_use]
extern crate bson;

use std::result;
use std::sync::{Once,ONCE_INIT};

use mongoc::bindings;

pub mod bulk_operation;
pub mod client;
pub mod collection;
pub mod cursor;
pub mod database;
pub mod flags;
pub mod read_prefs;
pub mod uri;
pub mod write_concern;

mod bsonc;
mod error;

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

pub struct CommandAndFindOptions {
    pub query_flags: flags::Flags<flags::QueryFlag>,
    pub skip:        u32,
    pub limit:       u32,
    pub batch_size:  u32,
    pub fields:      Option<bson::Document>,
    pub read_prefs:  Option<read_prefs::ReadPrefs>
}

impl CommandAndFindOptions {
    pub fn default() -> CommandAndFindOptions {
        CommandAndFindOptions {
            query_flags: flags::Flags::new(),
            skip:        0,
            limit:       0,
            batch_size:  0,
            fields:      None,
            read_prefs:  None
        }
    }

    pub fn with_fields(fields: bson::Document) -> CommandAndFindOptions {
        CommandAndFindOptions {
            query_flags: flags::Flags::new(),
            skip:        0,
            limit:       0,
            batch_size:  0,
            fields:      Some(fields),
            read_prefs:  None
        }
    }

    fn fields_bsonc(&self) -> Option<bsonc::Bsonc> {
        match self.fields {
            Some(ref f) => Some(bsonc::Bsonc::from_document(f).unwrap()),
            None => None
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_init() {
        super::init();
        super::init();
    }
}
