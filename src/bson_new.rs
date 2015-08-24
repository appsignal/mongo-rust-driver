use std::fmt;
use std::ffi::{CStr,CString};
use std::mem;
use std::ptr;
use std::str;

use libc::types::common::c95::c_void;

use mongo_c_driver_wrapper::bindings;

#[derive(Debug,PartialEq)]
pub enum Value {
    Document(Document),
    I32(i32),
    I64(i64),
    ObjectId(ObjectId),
    String(String)
}

#[derive(Debug,PartialEq)]
pub enum FieldAccessError {
    NotPresent,
    UnexpectedType
}

pub type FieldAccessResult<T> = Result<T, FieldAccessError>;

pub struct ObjectId {
    oid: bindings::bson_oid_t
}

impl ObjectId {
    pub fn new() -> ObjectId {
        let mut oid: bindings::bson_oid_t = unsafe { mem::uninitialized() };
        unsafe { bindings::bson_oid_init(&mut oid, ptr::null_mut()); }
        ObjectId { oid: oid }
    }

    fn from_oid(oid: bindings::bson_oid_t) -> ObjectId {
        ObjectId { oid: oid }
    }

    pub fn from_str(id: &str) -> ObjectId {
        let ptr = CString::new(id).unwrap().as_ptr();
        let mut oid: bindings::bson_oid_t = unsafe { mem::uninitialized() };
        unsafe { bindings::bson_oid_init_from_string(&mut oid, ptr); }
        ObjectId { oid: oid }
    }

    pub fn to_string(&self) -> String {
        let buffer: *mut i8 = unsafe { mem::uninitialized() };
        unsafe {
            bindings::bson_oid_to_string(&self.oid, buffer);
            let cstr = CStr::from_ptr(buffer);
            str::from_utf8_unchecked(cstr.to_bytes()).to_string()
        }
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ObjectId: {}", self.to_string())
    }
}

impl PartialEq for ObjectId {
    fn eq(&self, other: &ObjectId) -> bool {
        unsafe {
            bindings::bson_oid_equal(
                &self.oid,
                &other.oid
            ) == 1
        }
    }
}

pub struct Document {
    inner: *mut bindings::bson_t
}

impl Document {
    pub fn new() -> Document {
        let inner = unsafe { bindings::bson_new() };
        Document { inner: inner }
    }

    fn from_ptr(inner: *const bindings::bson_t) -> Document {
        assert!(!inner.is_null());
        Document { inner: inner as *mut bindings::bson_t }
    }

    fn from_data(data: *const u8, len: u32) -> Document {
        let inner = unsafe { bindings::bson_new_from_data(data, len as bindings::size_t) };
        Document { inner: inner as *mut bindings::bson_t }
    }

    pub fn insert<T: Into<Value>>(&mut self, key: &str, value: T) {
        assert!(!self.inner.is_null());

        let key_ptr = CString::new(key).unwrap().as_ptr();

        let success = match value.into() {
            Value::Document(v) => unsafe { bindings::bson_append_document(self.inner, key_ptr, -1, v.inner) },
            Value::I32(v) => unsafe { bindings::bson_append_int32(self.inner, key_ptr, -1, v) },
            Value::I64(v) => unsafe { bindings::bson_append_int64(self.inner, key_ptr, -1, v) },
            Value::ObjectId(v) => unsafe { bindings::bson_append_oid(self.inner, key_ptr, -1, &v.oid) },
            Value::String(v) => unsafe {
                let cstring = CString::new(v).unwrap();
                let ptr  = cstring.as_ptr();
                bindings::bson_append_utf8(self.inner, key_ptr, -1, ptr, -1)
            }
        };

        // If this not true the bson should be discarded. We'll have
        // to see if this can happen when we enforce the types and
        // mutability properly.
        assert!(success == 1);
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        assert!(!self.inner.is_null());

        // We need to create an iterator to be able to find the field we want
        // See: http://api.mongodb.org/libbson/current/parsing.html

        let wanted_key_cstr = CString::new(key).unwrap();

        unsafe {
            // Create and initialize the iterator
            let mut iter: bindings::bson_iter_t = mem::uninitialized();
            assert!(bindings::bson_iter_init(&mut iter, self.inner) == 1);

            // Set the iterator to the position of the field we're looking for
            // or return None.
            if bindings::bson_iter_find(&mut iter, wanted_key_cstr.as_ptr()) != 1 {
                return None
            }

            let bson_type = bindings::bson_iter_type(&iter);
            match bson_type {
                bindings::BSON_TYPE_DOCUMENT => {
                    let mut len: u32 = mem::uninitialized();
                    let mut buffer: *const u8 = mem::uninitialized();
                    bindings::bson_iter_document(&iter, &mut len, &mut buffer);
                    return Some(Value::Document(Document::from_data(buffer, len)))
                },
                bindings::BSON_TYPE_INT32 => {
                    return Some(Value::I32(bindings::bson_iter_int32(&iter)))
                },
                bindings::BSON_TYPE_INT64 => {
                    return Some(Value::I64(bindings::bson_iter_int64(&iter)))
                },
                bindings::BSON_TYPE_OID => {
                    let oid = *bindings::bson_iter_oid(&iter);
                    return Some(Value::ObjectId(ObjectId::from_oid(oid)))
                },
                bindings::BSON_TYPE_UTF8 => {
                    let ptr = bindings::bson_iter_utf8(&iter, ptr::null_mut());
                    let cstr = CStr::from_ptr(ptr);
                    let string = str::from_utf8_unchecked(cstr.to_bytes()).to_string();
                    return Some(Value::String(string))
                },
                _ => panic!("Type not supported yet")
            }
        }
    }

    pub fn as_document(&self, key: &str) -> FieldAccessResult<Document> {
        match self.get(key) {
            Some(Value::Document(v)) => Ok(v),
            Some(_) => Err(FieldAccessError::UnexpectedType),
            None => Err(FieldAccessError::NotPresent)
        }
    }

    pub fn as_i32(&self, key: &str) -> FieldAccessResult<i32> {
        match self.get(key) {
            Some(Value::I32(v)) => Ok(v),
            Some(_) => Err(FieldAccessError::UnexpectedType),
            None => Err(FieldAccessError::NotPresent)
        }
    }

    pub fn as_i64(&self, key: &str) -> FieldAccessResult<i64> {
        match self.get(key) {
            Some(Value::I64(v)) => Ok(v),
            Some(_) => Err(FieldAccessError::UnexpectedType),
            None => Err(FieldAccessError::NotPresent)
        }
    }

    pub fn as_object_id(&self, key: &str) -> FieldAccessResult<ObjectId> {
        match self.get(key) {
            Some(Value::ObjectId(v)) => Ok(v),
            Some(_) => Err(FieldAccessError::UnexpectedType),
            None => Err(FieldAccessError::NotPresent)
        }
    }

    pub fn as_string(&self, key: &str) -> FieldAccessResult<String> {
        match self.get(key) {
            Some(Value::String(v)) => Ok(v),
            Some(_) => Err(FieldAccessError::UnexpectedType),
            None => Err(FieldAccessError::NotPresent)
        }
    }

    pub fn to_json(&self) -> String {
        assert!(!self.inner.is_null());
        let json_ptr = unsafe { bindings::bson_as_json(self.inner, ptr::null_mut()) };
        assert!(!json_ptr.is_null());
        let json_cstr = unsafe { CStr::from_ptr(json_ptr) };
        let out = unsafe { str::from_utf8_unchecked(json_cstr.to_bytes()) };
        unsafe { bindings::bson_free(json_ptr as *mut c_void); }
        out.to_string()
    }
}

impl Clone for Document {
    fn clone(&self) -> Document {
        assert!(!self.inner.is_null());
        let copied_inner = unsafe {
            bindings::bson_copy(self.inner)
        };
        Document { inner: copied_inner }
    }
}

impl fmt::Debug for Document {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Document: {}", self.to_json())
    }
}

impl Drop for Document {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::bson_destroy(self.inner);
        }
    }
}

impl PartialEq for Document {
    fn eq(&self, other: &Document) -> bool {
        assert!(!self.inner.is_null());
        assert!(!other.inner.is_null());
        unsafe {
            bindings::bson_compare(
                self.inner,
                other.inner
            ) == 0
        }
    }
}

impl From<Document> for Value {
    fn from(v: Document) -> Value {
        Value::Document(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Value {
        Value::I32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Value {
        Value::I64(v)
    }
}

impl From<ObjectId> for Value {
    fn from(v: ObjectId) -> Value {
        Value::ObjectId(v)
    }
}

impl From<&'static str> for Value {
    fn from(v: &str) -> Value {
        Value::String(v.to_string())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Value {
        Value::String(v)
    }
}

#[cfg(test)]
mod tests {
    use super::{Document,ObjectId,Value};

    #[test]
    fn test_object_id() {
        let id = ObjectId::new();

        // Check something is in the bytes array
        assert!(id.oid.bytes[0] > 0);
        assert!(id.to_string().len() > 10);

        // Make sure the next id is not the same
        assert!(id != ObjectId::new());
    }

    #[test]
    fn test_object_from_and_to_string() {
        let id = "55dc546717ed939a14478a51";
        let oid = ObjectId::from_str(id);

        assert_eq!(id.to_string(), oid.to_string());
    }

    #[test]
    fn test_object_id_equality() {
        let id = "55dc546717ed939a14478a51";

        let oid1 = ObjectId::from_str(id);
        let oid2 = ObjectId::from_str(id);
        let oid3 = ObjectId::new();

        assert!(oid1 == oid2);
        assert!(oid1 != oid3);
    }

    #[test]
    fn test_document_get() {
        // Create a document with some fields
        let mut doc = Document::new();
        doc.insert("field1", 1i32);
        doc.insert("field2", 2i32);
        doc.insert("field3", 3i32);
        doc.insert("field4", 4i32);
        doc.insert("field5", 5i32);

        // Find one in the middle
        assert_eq!(Some(Value::I32(3i32)), doc.get("field3"));

        // Try to find one that does not exist
        assert_eq!(None, doc.get("something"));
    }

    #[test]
    fn test_document_clone_and_equality() {
        let mut doc1 = Document::new();
        doc1.insert("some_key", 10);

        let mut doc2 = Document::new();
        doc2.insert("some_key", 10);

        let doc3 = doc2.clone();

        let mut doc4 = Document::new();
        doc4.insert("some_key", 20);

        assert!(doc1 == doc2);
        assert!(doc1 == doc3);
        assert!(doc1 != doc4);
    }

    #[test]
    fn test_document() {
        let mut doc = Document::new();
        let mut embedded = Document::new();
        embedded.insert("an_int", 10i32);
        doc.insert("embedded", embedded.clone());

        assert_eq!(Some(Value::Document(embedded.clone())), doc.get("embedded"));
        assert_eq!(Ok(embedded), doc.as_document("embedded"));
    }

    #[test]
    fn test_document_i32() {
        let mut doc = Document::new();
        doc.insert("i32", 10i32);
        assert_eq!(Some(Value::I32(10i32)), doc.get("i32"));
        assert_eq!(Ok(10i32), doc.as_i32("i32"));
    }

    #[test]
    fn test_document_i64() {
        let mut doc = Document::new();
        doc.insert("i64", 10i64);
        assert_eq!(Some(Value::I64(10i64)), doc.get("i64"));
        assert_eq!(Ok(10i64), doc.as_i64("i64"));
    }

    #[test]
    fn test_document_object_id() {
        let id = "55dc546717ed939a14478a51";

        let mut doc = Document::new();
        doc.insert("_id", ObjectId::from_str(id));
        assert_eq!(Some(Value::ObjectId(ObjectId::from_str(id))), doc.get("_id"));
        assert_eq!(Ok(ObjectId::from_str(id)), doc.as_object_id("_id"));
    }

    #[test]
    fn test_document_string() {
        let mut doc = Document::new();
        doc.insert("str", "this is a str: Iñtërnâtiônàlizætiøn");
        doc.insert("string", "this is a string: Iñtërnâtiônàlizætiøn".to_string());
        assert_eq!(Some(Value::String("this is a str: Iñtërnâtiônàlizætiøn".to_string())), doc.get("str"));
        assert_eq!(Some(Value::String("this is a string: Iñtërnâtiônàlizætiøn".to_string())), doc.get("string"));
        assert_eq!(Ok("this is a string: Iñtërnâtiônàlizætiøn".to_string()), doc.as_string("string"));
    }

    #[test]
    fn test_document_to_json() {
        let mut doc = Document::new();
        doc.insert("str", "this is a str: Iñtërnâtiônàlizætiøn");
        assert_eq!("{ \"str\" : \"this is a str: Iñtërnâtiônàlizætiøn\" }".to_string(), doc.to_json());
    }
}
