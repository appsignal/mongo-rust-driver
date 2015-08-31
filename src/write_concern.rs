use mongoc::bindings;

pub struct WriteConcern {
    inner: *mut bindings::mongoc_write_concern_t
}

impl WriteConcern {
    pub fn new() -> WriteConcern {
        let inner = unsafe { bindings::mongoc_write_concern_new() };
        assert!(!inner.is_null());
        WriteConcern { inner: inner }
    }

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
