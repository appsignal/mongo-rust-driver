use std::ffi::CStr;
use std::ptr;
use std::fmt;
use std::slice;
use libc::c_void;

use crate::mongoc::bindings;
use bson;

use super::Result;

pub struct Bsonc {
    inner: *mut bindings::bson_t,
    destroy_inner_on_drop: bool
}

impl Bsonc {
    pub fn new() -> Bsonc {
        Bsonc::from_ptr(unsafe { bindings::bson_new() })
    }

    /// Create a bsonc from a raw pointer. Does not run cleanup
    /// logic on drop in this case.
    pub fn from_ptr(inner: *const bindings::bson_t) -> Bsonc {
        assert!(!inner.is_null());
        Bsonc {
            inner: inner as *mut bindings::bson_t,
            destroy_inner_on_drop: false
        }
    }

    pub fn from_document(document: &bson::Document) -> Result<Bsonc> {
        let mut buffer = Vec::new();
        document.to_writer(&mut buffer)?;

        let inner = unsafe {
            bindings::bson_new_from_data(
                buffer[..].as_ptr(),
                buffer.len() as libc::c_ulong
            )
        };

        // Inner will be null if there was an error converting the data.
        // We're assuming the bson crate works and therefore assert here.
        // See: http://mongoc.org/libbson/current/bson_new_from_data.html
        assert!(!inner.is_null());

        Ok(Bsonc{
            inner: inner,
            destroy_inner_on_drop: true
        })
    }

    /// Decode a bson from the C side to a document
    pub fn as_document(&self) -> Result<bson::Document> {
        assert!(!self.inner.is_null());

        // This pointer should not be modified or freed
        // See: http://mongoc.org/libbson/current/bson_get_data.html
        let data_ptr = unsafe { bindings::bson_get_data(self.inner) };
        assert!(!data_ptr.is_null());

        let data_len = unsafe {
            let bson = *self.inner;
            bson.len
        } as usize;

        let mut slice = unsafe {
            slice::from_raw_parts(data_ptr, data_len)
        };

        Ok(bson::Document::from_reader_utf8_lossy(&mut slice)?)
    }

    pub fn as_json(&self) -> String {
        assert!(!self.inner.is_null());
        let json_ptr = unsafe { bindings::bson_as_json(self.inner, ptr::null_mut()) };
        assert!(!json_ptr.is_null());
        let json_cstr = unsafe { CStr::from_ptr(json_ptr) };
        let out = String::from_utf8_lossy(json_cstr.to_bytes()).into_owned();
        unsafe { bindings::bson_free(json_ptr as *mut c_void); }
        out
    }

    pub fn inner(&self) -> *const bindings::bson_t {
        assert!(!self.inner.is_null());
        self.inner
    }

    pub fn mut_inner(&mut self) -> *mut bindings::bson_t {
        assert!(!self.inner.is_null());
        self.inner as *mut bindings::bson_t
    }
}

impl fmt::Debug for Bsonc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Bsonc: {}", self.as_json())
    }
}

impl Drop for Bsonc {
    fn drop(&mut self) {
        if self.destroy_inner_on_drop {
            unsafe {
                bindings::bson_destroy(self.inner);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_bsonc_from_and_as_document() {
        let document = doc! { "key": "value" };
        let bsonc = super::Bsonc::from_document(&document).unwrap();

        let decoded = bsonc.as_document().unwrap();
        assert_eq!(decoded.get_str("key").unwrap(), "value");
    }

    #[test]
    fn test_bsonc_from_and_as_document_invalid_utf8() {
        let bytes = b"\x80\xae".to_vec();
        let value = unsafe { String::from_utf8_unchecked(bytes) };
        let document = doc! { "key": value };
        let bsonc = super::Bsonc::from_document(&document).unwrap();

        let decoded = bsonc.as_document().unwrap();
        assert_eq!(decoded.get_str("key").unwrap(), "��");
    }

    #[test]
    fn test_bsonc_as_json() {
        let document = doc! { "key": "value" };
        let bsonc = super::Bsonc::from_document(&document).unwrap();
        assert_eq!("{ \"key\" : \"value\" }".to_owned(), bsonc.as_json());
    }
}
