use mongo_c_driver_wrapper::bindings;

pub struct Flags<T> {
    flags: Vec<T>
}

impl <T> Flags<T> {
    pub fn new() -> Flags<T> {
        Flags {
            flags: Vec::new()
        }
    }

    pub fn add(&mut self, flag: T) {
        self.flags.push(flag);
    }
}

pub trait FlagsValue {
    fn flags(&self) -> u32;
}

/// Flags for insert operations
/// See: http://api.mongodb.org/c/current/mongoc_insert_flags_t.html
pub enum InsertFlag {
    ContinueOnError,
    NoValidate
}

impl FlagsValue for Flags<InsertFlag> {
    fn flags(&self) -> u32 {
        if self.flags.is_empty() {
            bindings::MONGOC_INSERT_NONE
        } else {
            self.flags.iter().fold(0, { |flags, flag|
                flags | match flag {
                    &InsertFlag::ContinueOnError => bindings::MONGOC_INSERT_CONTINUE_ON_ERROR,
                    &InsertFlag::NoValidate      => 1 | 31  // MONGOC_INSERT_NO_VALIDATE defined in macro
                }
            })
        }
    }
}

/// Flags for query operations
/// See: http://api.mongodb.org/c/current/mongoc_query_flags_t.html
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
/// See: http://api.mongodb.org/c/1.1.8/mongoc_remove_flags_t.html
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

#[cfg(test)]
mod tests {
    use super::FlagsValue;

    #[test]
    pub fn test_insert_flags() {
        let mut flags = super::Flags::new();
        assert_eq!(0, flags.flags());

        flags.add(super::InsertFlag::ContinueOnError);
        assert_eq!(1, flags.flags());

        flags.add(super::InsertFlag::NoValidate);
        assert_eq!(31, flags.flags());
    }

    #[test]
    pub fn test_query_flags() {
        let mut flags = super::Flags::new();
        assert_eq!(0, flags.flags());

        flags.add(super::QueryFlag::TailableCursor);
        assert_eq!(2, flags.flags());

        flags.add(super::QueryFlag::Partial);
        assert_eq!(130, flags.flags());
    }
}
