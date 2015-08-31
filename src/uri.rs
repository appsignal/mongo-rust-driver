use std::borrow::Cow;
use std::ffi::{CStr,CString};
use std::fmt;

use mongoc::bindings;

/// Abstraction on top of MongoDB connection URI format.
/// See: http://api.mongodb.org/c/current/mongoc_uri_t.html

pub struct Uri {
    inner: *mut bindings::mongoc_uri_t
}

impl Uri {
    /// Parses a string containing a MongoDB style URI connection string.
    ///
    /// Returns None if the uri is not in the correct format, there is no
    /// further information available if this is not the case.
    ///
    /// See: http://api.mongodb.org/c/current/mongoc_uri_new.html
    pub fn new<T: Into<Vec<u8>>>(uri_string: T) -> Option<Uri> {
        let uri_cstring = CString::new(uri_string).unwrap();
        let uri = unsafe { bindings::mongoc_uri_new(uri_cstring.as_ptr()) };
        if uri.is_null() {
            None
        } else {
            Some(Uri { inner: uri })
        }
    }

    pub unsafe fn inner(&self) -> *const bindings::mongoc_uri_t {
        assert!(!self.inner.is_null());
        self.inner
    }

    pub fn as_str<'a>(&'a self) -> Cow<'a, str> {
        assert!(!self.inner.is_null());
        unsafe {
            let cstr = CStr::from_ptr(
                bindings::mongoc_uri_get_string(self.inner)
            );
            String::from_utf8_lossy(cstr.to_bytes())
        }
    }

    pub fn get_database<'a>(&'a self) -> Option<Cow<'a, str>> {
        assert!(!self.inner.is_null());
        unsafe {
            let ptr = bindings::mongoc_uri_get_database(self.inner);
            if ptr.is_null() {
                None
            } else {
                let cstr = CStr::from_ptr(ptr);
                Some(String::from_utf8_lossy(cstr.to_bytes()))
            }
        }
    }

    // TODO add various methods that are available on uri
}

impl fmt::Debug for Uri {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Clone for Uri {
    fn clone(&self) -> Uri {
        Uri::new(self.as_str().into_owned()).unwrap()
    }
}

impl Drop for Uri {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_uri_destroy(self.inner);
        }
    }
}
