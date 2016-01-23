//! Abstraction on top of the MongoDB connection write concern.

use mongoc::bindings;

/// Possible write concern levels, only default is supported at the moment.
pub enum WriteConcernLevel {
    /// By default, writes block awaiting acknowledgment from MongoDB. Acknowledged write concern allows clients to catch network, duplicate key, and other errors.
    Default,

    // We'd like to support the following write concerns too at some point, pull request welcome:

    // With this write concern, MongoDB does not acknowledge the receipt of write operation. Unacknowledged is similar to errors ignored; however, mongoc attempts to receive and handle network errors when possible.
    // WriteUnacknowledged,
    // Block until a write has been propagated to a majority of the nodes in the replica set.
    // Majority,
    // Block until a write has been propagated to at least n nodes in the replica set.
    // AtLeastNumberOfNodes(u32),
    // Block until the node receiving the write has committed the journal.
    // Journal
}

/// This tells the driver what level of acknowledgment to await from the server.
/// The default, `Default`, is right for the great majority of applications.
pub struct WriteConcern {
    inner: *mut bindings::mongoc_write_concern_t
}

impl WriteConcern {
    /// Get the default write concern
    pub fn default() -> WriteConcern {
        Self::new(WriteConcernLevel::Default)
    }

    /// Create a new write concern
    pub fn new(_: WriteConcernLevel) -> WriteConcern {
        let inner = unsafe { bindings::mongoc_write_concern_new() };
        assert!(!inner.is_null());
        WriteConcern { inner: inner }
    }

    #[doc(hidden)]
    pub fn inner(&self) -> *const bindings::mongoc_write_concern_t {
        assert!(!self.inner.is_null());
        self.inner
    }
}

impl Drop for WriteConcern {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_write_concern_destroy(self.inner);
        }
    }
}
