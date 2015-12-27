//! Abstraction on top of the MongoDB connection read prefences.

use mongoc::bindings;

/// Describes how reads should be dispatched.
pub enum ReadMode {
    Primary,
    Secondary,
    PrimaryPreferred,
    SecondaryPreferred,
    Nearest
}

fn read_mode_value(read_mode: &ReadMode) -> bindings::mongoc_read_mode_t {
    match read_mode {
        &ReadMode::Primary            => bindings::MONGOC_READ_PRIMARY,
        &ReadMode::Secondary          => bindings::MONGOC_READ_SECONDARY,
        &ReadMode::PrimaryPreferred   => bindings::MONGOC_READ_PRIMARY_PREFERRED,
        &ReadMode::SecondaryPreferred => bindings::MONGOC_READ_SECONDARY_PREFERRED,
        &ReadMode::Nearest            => bindings::MONGOC_READ_NEAREST
    }
}

/// Provides an abstraction on top of the MongoDB connection read prefences.
///
/// It allows for hinting to the driver which nodes in a replica set should be accessed first.
/// Generally, it makes the most sense to stick with the global default, `Primary`. All of the other modes come with caveats that won't be covered in great detail here.
pub struct ReadPrefs {
    inner: *mut bindings::mongoc_read_prefs_t
}

impl ReadPrefs {
    /// Create a new empty read prefs.
    pub fn new(read_mode: &ReadMode) -> ReadPrefs {
        let read_mode_value = read_mode_value(read_mode);
        let inner = unsafe { bindings::mongoc_read_prefs_new(read_mode_value) };
        assert!(!inner.is_null());
        ReadPrefs { inner: inner }
    }

    /// Get a new instance of the default read pref.
    pub fn default() -> ReadPrefs{
        ReadPrefs::new(&ReadMode::Primary)
    }

    #[doc(hidden)]
    pub fn inner(&self) -> *const bindings::mongoc_read_prefs_t {
        assert!(!self.inner.is_null());
        self.inner
    }

    #[doc(hidden)]
    pub fn mut_inner(&self) -> *mut bindings::mongoc_read_prefs_t {
        assert!(!self.inner.is_null());
        self.inner as *mut bindings::mongoc_read_prefs_t
    }
}

impl Drop for ReadPrefs {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_read_prefs_destroy(self.inner);
        }
    }
}
