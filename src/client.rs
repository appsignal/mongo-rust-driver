use std::fmt;
use std::ffi::CString;

use mongo_c_driver_wrapper::bindings;

use super::uri::Uri;
use super::collection::Collection;

// TODO: We're using a sort of poor man's Arc here
// with this root bool, there must be a better way.
pub struct ClientPool {
    root_instance: bool,
    uri:           Uri,
    inner:         *mut bindings::mongoc_client_pool_t
}

impl ClientPool {
    /// Create a new ClientPool
    /// See: http://api.mongodb.org/c/current/mongoc_client_pool_t.html
    pub fn new(uri: Uri) -> ClientPool {
        let pool = unsafe {
            let pool_ptr = bindings::mongoc_client_pool_new(uri.inner());
            assert!(!pool_ptr.is_null());
            pool_ptr
        };
        ClientPool {
            root_instance: true,
            uri:           uri, // Become owner of uri so it doesn't go out of scope
            inner:         pool
        }
    }

    /// Retrieve a client from the client pool, possibly blocking until one is available.
    /// See: http://api.mongodb.org/c/current/mongoc_client_pool_pop.html
    pub fn pop(&self) -> Client {
        assert!(!self.inner.is_null());
        let client = unsafe { bindings::mongoc_client_pool_pop(self.inner) };
        Client{
            client_pool: self,
            inner:       client
        }
    }

    /// Return a client back to the client pool, called from drop of client.
    /// See: http://api.mongodb.org/c/current/mongoc_client_pool_push.html
    unsafe fn push(&self, mongo_client: *mut bindings::mongoc_client_t) {
        assert!(!self.inner.is_null());
        bindings::mongoc_client_pool_push(
            self.inner,
            mongo_client
        );
    }
}

unsafe impl Send for ClientPool { }
unsafe impl Sync for ClientPool { }

impl fmt::Debug for ClientPool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClientPool for {}", self.uri.as_str())
    }
}

impl Clone for ClientPool {
    fn clone(&self) -> ClientPool {
        assert!(!self.inner.is_null());
        ClientPool {
            root_instance: false,
            uri:           self.uri.clone(),
            inner:         self.inner.clone()
        }
    }
}

impl Drop for ClientPool {
    fn drop(&mut self) {
        if self.root_instance {
            assert!(!self.inner.is_null());
            unsafe {
                bindings::mongoc_client_pool_destroy(self.inner);
            }
        }
    }
}

pub struct Client<'a> {
    client_pool: &'a ClientPool,
    inner:       *mut bindings::mongoc_client_t
}

impl<'a> Client<'a> {
    pub fn get_collection<S: Into<Vec<u8>>>(&'a self, db: S, collection: S) -> Collection<'a> {
        assert!(!self.inner.is_null());
        let mut coll;
        unsafe {
            let db_cstring         = CString::new(db).unwrap();
            let collection_cstring = CString::new(collection).unwrap();

            coll = bindings::mongoc_client_get_collection(
                self.inner,
                db_cstring.as_ptr(),
                collection_cstring.as_ptr()
            );
        }
        Collection::new(self, coll)
    }
}

impl<'a> Drop for Client<'a> {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            self.client_pool.push(self.inner);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use super::super::uri::Uri;
    use super::super::client::ClientPool;

    #[test]
    fn test_new_pool_and_pop_client() {
        super::super::init();

        let uri = Uri::new("mongodb://localhost:27017/");
        let pool = ClientPool::new(uri);

        // Pop a client and insert a couple of times
        for _ in 0..10 {
            let client = pool.pop();
            pool.pop();
            client.get_collection("rust_test", "items");
        }
    }

    #[test]
    fn test_new_pool_and_pop_client_in_threads() {
        super::super::init();

        let uri = Uri::new("mongodb://localhost:27017/");
        let pool = ClientPool::new(uri);

        let pool1 = pool.clone();
        let guard1 = thread::scoped(move || {
            let client = pool1.pop();
            client.get_collection("test", "items");
        });

        let pool2 = pool.clone();
        let guard2 = thread::scoped(move || {
            let client = pool2.pop();
            client.get_collection("test", "items");
        });

        guard1.join();
        guard2.join();
    }
}
