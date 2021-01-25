//! Flags to configure various MongoDB operations.

use crate::mongoc::bindings;

use std::collections::BTreeSet;

/// Structure to hold flags for various flag types
pub struct Flags<T> {
    flags: BTreeSet<T>
}

impl <T> Flags<T> where T: Ord {
    /// Creare a new empty flags instance
    pub fn new() -> Flags<T> {
        Flags {
            flags: BTreeSet::new()
        }
    }

    /// Add a flag to this instance
    pub fn add(&mut self, flag: T) {
        self.flags.insert(flag);
    }
}

/// To provide the combined value of all flags.
pub trait FlagsValue {
    fn flags(&self) -> u32;
}

/// Flags for insert operations
/// See: http://mongoc.org/libmongoc/current/mongoc_insert_flags_t.html
#[derive(Eq,PartialEq,Ord,PartialOrd)]
pub enum InsertFlag {
    ContinueOnError,
    NoValidate
}

const INSERT_FLAG_NO_VALIDATE: u32 = 1 | 31; // MONGOC_INSERT_NO_VALIDATE defined in macro

impl FlagsValue for Flags<InsertFlag> {
    fn flags(&self) -> u32 {
        if self.flags.is_empty() {
            bindings::MONGOC_INSERT_NONE
        } else {
            self.flags.iter().fold(0, { |flags, flag|
                flags | match flag {
                    &InsertFlag::ContinueOnError => bindings::MONGOC_INSERT_CONTINUE_ON_ERROR,
                    &InsertFlag::NoValidate      => INSERT_FLAG_NO_VALIDATE
                }
            })
        }
    }
}

/// Flags for query operations
/// See: http://mongoc.org/libmongoc/current/mongoc_query_flags_t.html
#[derive(Eq,PartialEq,Ord,PartialOrd)]
pub enum QueryFlag {
    TailableCursor,
    SlaveOk,
    OplogReplay,
    NoCursorTimeout,
    AwaitData,
    Exhaust,
    Partial
}

impl FlagsValue for Flags<QueryFlag> {
    fn flags(&self) -> u32 {
        if self.flags.is_empty() {
            bindings::MONGOC_QUERY_NONE
        } else {
            self.flags.iter().fold(0, { |flags, flag|
                flags | match flag {
                    &QueryFlag::TailableCursor  => bindings::MONGOC_QUERY_TAILABLE_CURSOR,
                    &QueryFlag::SlaveOk         => bindings::MONGOC_QUERY_SLAVE_OK,
                    &QueryFlag::OplogReplay     => bindings::MONGOC_QUERY_OPLOG_REPLAY,
                    &QueryFlag::NoCursorTimeout => bindings::MONGOC_QUERY_NO_CURSOR_TIMEOUT,
                    &QueryFlag::AwaitData       => bindings::MONGOC_QUERY_AWAIT_DATA,
                    &QueryFlag::Exhaust         => bindings::MONGOC_QUERY_EXHAUST,
                    &QueryFlag::Partial         => bindings::MONGOC_QUERY_PARTIAL
                }
            })
        }
    }
}

/// Flags for deletion operations
/// See: http://mongoc.org/libmongoc/current/mongoc_remove_flags_t.html
#[derive(Eq,PartialEq,Ord,PartialOrd)]
pub enum RemoveFlag {
    SingleRemove
}

impl FlagsValue for Flags<RemoveFlag> {
    fn flags(&self) -> u32 {
        if self.flags.is_empty() {
            bindings::MONGOC_REMOVE_NONE
        } else {
            bindings::MONGOC_REMOVE_SINGLE_REMOVE
        }
    }
}

/// Flags for update operations
/// See: http://mongoc.org/libmongoc/current/mongoc_update_flags_t.html
#[derive(Eq,PartialEq,Ord,PartialOrd)]
pub enum UpdateFlag {
    Upsert,
    MultiUpdate
}

impl FlagsValue for Flags<UpdateFlag> {
    fn flags(&self) -> u32 {
        if self.flags.is_empty() {
            bindings::MONGOC_UPDATE_NONE
        } else {
            self.flags.iter().fold(0, { |flags, flag|
                flags | match flag {
                    &UpdateFlag::Upsert      => bindings::MONGOC_UPDATE_UPSERT,
                    &UpdateFlag::MultiUpdate => bindings::MONGOC_UPDATE_MULTI_UPDATE
                }
            })
        }
    }
}
