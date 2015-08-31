use mongoc::bindings;

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

pub struct ReadPrefs {
    inner: *mut bindings::mongoc_read_prefs_t
}

impl ReadPrefs {
    pub fn new(read_mode: &ReadMode) -> ReadPrefs {
        let read_mode_value = read_mode_value(read_mode);
        let inner = unsafe { bindings::mongoc_read_prefs_new(read_mode_value) };
        assert!(!inner.is_null());
        ReadPrefs { inner: inner }
    }

    pub fn default() -> ReadPrefs{
        ReadPrefs::new(&ReadMode::Primary)
    }

    pub fn inner(&self) -> *const bindings::mongoc_read_prefs_t {
        assert!(!self.inner.is_null());
        self.inner
    }

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
