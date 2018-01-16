//! Client to access a MongoDB node, replica set or sharded cluster.
//!
//! Get started by creating a `ClientPool` you can use to pop a `Client`.

use std::borrow::Cow;
use std::fmt;
use std::ffi::{CStr,CString};
use std::path::PathBuf;
use std::mem;
use std::ptr;
use std::io;
use std::fs::File;

use mongoc::bindings;

use bson::Document;

use super::Result;
use super::BsoncError;
use super::bsonc::Bsonc;
use super::collection;
use super::collection::Collection;
use super::database;
use super::database::Database;
use super::read_prefs::ReadPrefs;

/// Pool that allows usage of clients out of a single pool from multiple threads.
///
/// Use the pool to pop a client and do operations. The client will be automatically added
/// back to the pool when it goes out of scope.
///
/// This client pool cannot be cloned, but it can be use from different threads by using an `Arc`.
/// Clients cannot be shared between threads, pop a client from the pool for very single thread
/// where you need a connection.
pub struct ClientPool {
    // Uri and SslOptions need to be present for the lifetime of this pool otherwise the C driver
    // loses access to resources it needs.
    uri:          Uri,
    _ssl_options: Option<SslOptions>,
    inner:         *mut bindings::mongoc_client_pool_t
}

impl ClientPool {
    /// Create a new ClientPool with that can provide clients pointing to the specified uri.
    /// The pool will connect via SSL if you add `?ssl=true` to the uri. You can optionally pass
    /// in SSL options to configure SSL certificate usage and so on.
    pub fn new(uri: Uri, ssl_options: Option<SslOptions>) -> ClientPool {
        super::init();
        let pool = unsafe {
            let pool_ptr = bindings::mongoc_client_pool_new(uri.inner());
            assert!(!pool_ptr.is_null());
            pool_ptr
        };
        match ssl_options {
            Some(ref options) => {
                unsafe {
                    bindings::mongoc_client_pool_set_ssl_opts(
                        pool,
                        options.inner()
                    );
                }
            },
            None => ()
        };
        ClientPool {
            uri:          uri,
            _ssl_options: ssl_options,
            inner:        pool
        }
    }

    /// Get a reference to this pool's Uri.
    pub fn get_uri(&self) -> &Uri {
        &self.uri
    }

    /// Retrieve a client from the client pool, possibly blocking until one is available.
    pub fn pop(&self) -> Client {
        assert!(!self.inner.is_null());
        let client = unsafe { bindings::mongoc_client_pool_pop(self.inner) };
        Client{
            client_pool: self,
            inner:       client
        }
    }

    /// Return a client back to the client pool, called from drop of client.
    unsafe fn push(&self, mongo_client: *mut bindings::mongoc_client_t) {
        assert!(!self.inner.is_null());
        assert!(!mongo_client.is_null());
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

impl Drop for ClientPool {
    fn drop(&mut self) {
        assert!(!self.inner.is_null());
        unsafe {
            bindings::mongoc_client_pool_destroy(self.inner);
        }
    }
}

/// Optional SSL configuration for a `ClientPool`.
pub struct SslOptions {
    inner:                bindings::mongoc_ssl_opt_t,
    // We need to store everything so both memory sticks around
    // for the C driver and we can clone this struct.
    pem_file:              Option<PathBuf>,
    _pem_file_cstring:     Option<CString>,
    pem_password:          Option<String>,
    _pem_password_cstring: Option<CString>,
    ca_file:               Option<PathBuf>,
    _ca_file_cstring:      Option<CString>,
    ca_dir:                Option<PathBuf>,
    _ca_dir_cstring:       Option<CString>,
    crl_file:              Option<PathBuf>,
    _crl_file_cstring:     Option<CString>,
    weak_cert_validation: bool
}

impl SslOptions {
    /// Create a new ssl options instance that can be used to configured
    /// a `ClientPool`.
    pub fn new(
        pem_file:             Option<PathBuf>,
        pem_password:         Option<String>,
        ca_file:              Option<PathBuf>,
        ca_dir:               Option<PathBuf>,
        crl_file:             Option<PathBuf>,
        weak_cert_validation: bool
    ) -> io::Result<SslOptions> {
        let pem_file_cstring     = try!(Self::cstring_from_path(&pem_file));
        let pem_password_cstring = Self::cstring_from_string(&pem_password);
        let ca_file_cstring      = try!(Self::cstring_from_path(&ca_file));
        let ca_dir_cstring       = try!(Self::cstring_from_path(&ca_dir));
        let crl_file_cstring     = try!(Self::cstring_from_path(&crl_file));

        let ssl_options = bindings::mongoc_ssl_opt_t {
            pem_file: match pem_file_cstring {
                Some(ref f) => f.as_ptr(),
                None => ptr::null()
            },
            pem_pwd: match pem_password_cstring {
                Some(ref password) => password.as_ptr(),
                None => ptr::null()
            },
            ca_file: match ca_file_cstring {
                Some(ref f) => f.as_ptr(),
                None => ptr::null()
            },
            ca_dir: match ca_dir_cstring {
                Some(ref f) => f.as_ptr(),
                None => ptr::null()
            },
            crl_file: match crl_file_cstring {
                Some(ref f) => f.as_ptr(),
                None => ptr::null()
            },
            weak_cert_validation: weak_cert_validation as u8,
            padding: unsafe { mem::zeroed() }
        };

        Ok(SslOptions {
            inner:                 ssl_options,
            pem_file:              pem_file,
            _pem_file_cstring:     pem_file_cstring,
            pem_password:          pem_password,
            _pem_password_cstring: pem_password_cstring,
            ca_file:               ca_file,
            _ca_file_cstring:      ca_file_cstring,
            ca_dir:                ca_dir,
            _ca_dir_cstring:       ca_dir_cstring,
            crl_file:              crl_file,
            _crl_file_cstring:     crl_file_cstring,
            weak_cert_validation:  weak_cert_validation
        })
    }

    fn cstring_from_path(path: &Option<PathBuf>) -> io::Result<Option<CString>> {
        match path {
            &Some(ref p) => {
                try!(File::open(p.as_path()));
                Ok(Some(CString::new(p.to_string_lossy().into_owned()).unwrap()))
            },
            &None => Ok(None)
        }
    }

    fn cstring_from_string(path: &Option<String>) -> Option<CString> {
        match path {
            &Some(ref p) => Some(CString::new(p.clone()).unwrap()),
            &None => None
        }
    }

    fn inner(&self) -> *const bindings::mongoc_ssl_opt_t {
        &self.inner
    }
}

impl Clone for SslOptions {
    fn clone(&self) -> SslOptions {
        SslOptions::new(
            self.pem_file.clone(),
            self.pem_password.clone(),
            self.ca_file.clone(),
            self.ca_dir.clone(),
            self.crl_file.clone(),
            self.weak_cert_validation
        ).unwrap()
    }
}

/// Client that provides access to a MongoDB MongoDB node, replica-set, or sharded-cluster.
///
/// It maintains management of underlying sockets and routing to individual nodes based on
/// `ReadPrefs` or `WriteConcern`. Clients cannot be shared between threads, pop a new one from
/// a `ClientPool` in every thread that needs a connection instead.
pub struct Client<'a> {
    client_pool: &'a ClientPool,
    inner:       *mut bindings::mongoc_client_t
}

impl<'a> Client<'a> {
    /// Borrow a collection
    pub fn get_collection<DBT: Into<Vec<u8>>, CT: Into<Vec<u8>>>(&'a self, db: DBT, collection: CT) -> Collection<'a> {
        assert!(!self.inner.is_null());
        let coll = unsafe { self.collection_ptr(db.into(), collection.into()) };
        Collection::new(collection::CreatedBy::BorrowedClient(self), coll)
    }

    /// Take a collection, client is owned by the collection so the collection can easily
    /// be passed around
    pub fn take_collection<DBT: Into<Vec<u8>>, CT: Into<Vec<u8>>>(self, db: DBT, collection: CT) -> Collection<'a> {
        assert!(!self.inner.is_null());
        let coll = unsafe { self.collection_ptr(db.into(), collection.into()) };
        Collection::new(collection::CreatedBy::OwnedClient(self), coll)
    }

    unsafe fn collection_ptr(&self, db: Vec<u8>, collection: Vec<u8>) -> *mut bindings::mongoc_collection_t {
        let db_cstring         = CString::new(db).unwrap();
        let collection_cstring = CString::new(collection).unwrap();
        bindings::mongoc_client_get_collection(
            self.inner,
            db_cstring.as_ptr(),
            collection_cstring.as_ptr()
        )
    }

    /// Borrow a database
    pub fn get_database<S: Into<Vec<u8>>>(&'a self, db: S) -> Database<'a> {
        assert!(!self.inner.is_null());
        let coll = unsafe { self.database_ptr(db.into()) };
        Database::new(database::CreatedBy::BorrowedClient(self), coll)
    }

    /// Take a database, client is owned by the database so the database can easily
    /// be passed around
    pub fn take_database<S: Into<Vec<u8>>>(self, db: S) -> Database<'a> {
        assert!(!self.inner.is_null());
        let coll = unsafe { self.database_ptr(db.into()) };
        Database::new(database::CreatedBy::OwnedClient(self), coll)
    }

    unsafe fn database_ptr(&self, db: Vec<u8>) -> *mut bindings::mongoc_database_t {
        let db_cstring = CString::new(db).unwrap();
        bindings::mongoc_client_get_database(
            self.inner,
            db_cstring.as_ptr()
        )
    }

    /// Queries the server for the current server status, returns a document with this information.
    pub fn get_server_status(&self, read_prefs: Option<ReadPrefs>) -> Result<Document> {
        assert!(!self.inner.is_null());

        // Bsonc to store the reply
        let mut reply = Bsonc::new();
        // Empty error that might be filled
        let mut error = BsoncError::empty();

        let success = unsafe {
            bindings::mongoc_client_get_server_status(
                self.inner,
                match read_prefs {
                    Some(ref prefs) => prefs.mut_inner(),
                    None => ptr::null_mut()
                },
                reply.mut_inner(),
                error.mut_inner()
            )
        };

        if success == 1 {
            match reply.as_document_utf8_lossy() {
                Ok(document) => return Ok(document),
                Err(error)   => return Err(error.into())
            }
        } else {
            Err(error.into())
        }
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

/// Abstraction on top of MongoDB connection URI format.
pub struct Uri {
    inner: *mut bindings::mongoc_uri_t
}

impl Uri {
    /// Parses a string containing a MongoDB style URI connection string.
    ///
    /// Returns None if the uri is not in the correct format, there is no
    /// further information available if this is not the case.
    pub fn new<T: Into<Vec<u8>>>(uri_string: T) -> Option<Uri> {
        let uri_cstring = CString::new(uri_string).unwrap();
        let uri = unsafe { bindings::mongoc_uri_new(uri_cstring.as_ptr()) };
        if uri.is_null() {
            None
        } else {
            Some(Uri { inner: uri })
        }
    }

    unsafe fn inner(&self) -> *const bindings::mongoc_uri_t {
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

impl PartialEq for Uri {
    fn eq(&self, other: &Uri) -> bool {
        self.as_str() == other.as_str()
    }
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

unsafe impl Send for Uri { }
unsafe impl Sync for Uri { }
