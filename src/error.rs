use std::error;
use std::fmt;
use std::borrow::Cow;
use std::ffi::CStr;

use bson::{DecoderError,EncoderError,ValueAccessError};

use mongoc::bindings;

pub enum MongoError {
    Bsonc(BsoncError),
    Decoder(DecoderError),
    Encoder(EncoderError),
    ValueAccessError(ValueAccessError),
    InvalidParams(InvalidParamsError)
}

impl fmt::Display for MongoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MongoError::Bsonc(ref err) => write!(f, "{}", err),
            MongoError::Encoder(ref err) => write!(f, "{}", err),
            MongoError::Decoder(ref err) => write!(f, "{}", err),
            MongoError::ValueAccessError(ref err) => write!(f, "{}", err),
            MongoError::InvalidParams(ref err) => write!(f, "{}", err)
        }
    }
}

impl fmt::Debug for MongoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MongoError::Bsonc(ref err) => write!(f, "MongoError ({:?})", err),
            MongoError::Decoder(ref err) => write!(f, "MongoError ({:?})", err),
            MongoError::Encoder(ref err) => write!(f, "MongoError ({:?})", err),
            MongoError::ValueAccessError(ref err) => write!(f, "MongoError ({:?})", err),
            MongoError::InvalidParams(ref err) => write!(f, "MongoError ({:?})", err)
        }
    }
}

impl error::Error for MongoError {
    fn description(&self) -> &str {
        match *self {
            MongoError::Bsonc(ref err) => err.description(),
            MongoError::Decoder(ref err) => err.description(),
            MongoError::Encoder(ref err) => err.description(),
            MongoError::ValueAccessError(ref err) => err.description(),
            MongoError::InvalidParams(ref err) => err.description()
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            MongoError::Bsonc(ref err) => Some(err),
            MongoError::Decoder(ref err) => Some(err),
            MongoError::Encoder(ref err) => Some(err),
            MongoError::ValueAccessError(ref err) => Some(err),
            MongoError::InvalidParams(ref err) => Some(err)
        }
    }
}

impl From<DecoderError> for MongoError {
    fn from(error: DecoderError) -> MongoError {
        MongoError::Decoder(error)
    }
}

impl From<EncoderError> for MongoError {
    fn from(error: EncoderError) -> MongoError {
        MongoError::Encoder(error)
    }
}

impl From<ValueAccessError> for MongoError {
    fn from(error: ValueAccessError) -> MongoError {
        MongoError::ValueAccessError(error)
    }
}

pub struct BsoncError {
    inner: bindings::bson_error_t,
}

#[derive(Debug,PartialEq)]
pub enum MongoErrorDomain {
    Blank,
    Client,
    Stream,
    Protocol,
    Cursor,
    Query,
    Insert,
    Sasl,
    Bson,
    Matcher,
    Namespace,
    Command,
    Collection,
    Gridfs,
    Scram,
    Unknown
}

#[derive(Debug,PartialEq)]
pub enum MongoErrorCode {
    Blank,
    StreamInvalidType,
    StreamInvalidState,
    StreamNameResolution,
    StreamSocket,
    StreamConnect,
    StreamNotEstablished,
    ClientNotReady,
    ClientTooBig,
    ClientTooSmall,
    ClientGetnonce,
    ClientAuthenticate,
    ClientNoAcceptablePeer,
    ClientInExhaust,
    ProtocolInvalidReply,
    ProtocolBadWireVersion,
    CursorInvalidCursor,
    QueryFailure,
    BsonInvalid,
    MatcherInvalid,
    NamespaceInvalid,
    NamespaceInvalidFilterType,
    CommandInvalidArg,
    CollectionInsertFailed,
    CollectionUpdateFailed,
    CollectionDeleteFailed,
    CollectionDoesNotExist,
    GridfsInvalidFilename,
    ScramNotDone,
    ScramProtocolError,
    QueryCommandNotFound,
    QueryNotTailable,
    Unknown
}

impl BsoncError {
    pub fn empty() -> BsoncError {
        BsoncError {
            inner: bindings::bson_error_t {
                domain:  0,
                code:    0,
                message: [0; 504]
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.domain == 0 && self.inner.code == 0
    }

    pub fn domain(&self) -> MongoErrorDomain {
        match self.inner.domain {
            0                                 => MongoErrorDomain::Blank,
            bindings::MONGOC_ERROR_CLIENT     => MongoErrorDomain::Client,
            bindings::MONGOC_ERROR_STREAM     => MongoErrorDomain::Stream,
            bindings::MONGOC_ERROR_PROTOCOL   => MongoErrorDomain::Protocol,
            bindings::MONGOC_ERROR_CURSOR     => MongoErrorDomain::Cursor,
            bindings::MONGOC_ERROR_QUERY      => MongoErrorDomain::Query,
            bindings::MONGOC_ERROR_INSERT     => MongoErrorDomain::Insert,
            bindings::MONGOC_ERROR_SASL       => MongoErrorDomain::Sasl,
            bindings::MONGOC_ERROR_BSON       => MongoErrorDomain::Bson,
            bindings::MONGOC_ERROR_MATCHER    => MongoErrorDomain::Matcher,
            bindings::MONGOC_ERROR_NAMESPACE  => MongoErrorDomain::Namespace,
            bindings::MONGOC_ERROR_COMMAND    => MongoErrorDomain::Command,
            bindings::MONGOC_ERROR_COLLECTION => MongoErrorDomain::Collection,
            bindings::MONGOC_ERROR_GRIDFS     => MongoErrorDomain::Gridfs,
            bindings::MONGOC_ERROR_SCRAM      => MongoErrorDomain::Scram,
            _                                 => MongoErrorDomain::Unknown
        }
    }

    pub fn code(&self) -> MongoErrorCode {
        match self.inner.code {
            0                                                    => MongoErrorCode::Blank,
            bindings::MONGOC_ERROR_STREAM_INVALID_TYPE           => MongoErrorCode::StreamInvalidType,
            bindings::MONGOC_ERROR_STREAM_INVALID_STATE          => MongoErrorCode::StreamInvalidState,
            bindings::MONGOC_ERROR_STREAM_NAME_RESOLUTION        => MongoErrorCode::StreamNameResolution,
            bindings::MONGOC_ERROR_STREAM_SOCKET                 => MongoErrorCode::StreamSocket,
            bindings::MONGOC_ERROR_STREAM_CONNECT                => MongoErrorCode::StreamConnect,
            bindings::MONGOC_ERROR_STREAM_NOT_ESTABLISHED        => MongoErrorCode::StreamNotEstablished,
            bindings::MONGOC_ERROR_CLIENT_NOT_READY              => MongoErrorCode::ClientNotReady,
            bindings::MONGOC_ERROR_CLIENT_TOO_BIG                => MongoErrorCode::ClientTooBig,
            bindings::MONGOC_ERROR_CLIENT_TOO_SMALL              => MongoErrorCode::ClientTooSmall,
            bindings::MONGOC_ERROR_CLIENT_GETNONCE               => MongoErrorCode::ClientGetnonce,
            bindings::MONGOC_ERROR_CLIENT_AUTHENTICATE           => MongoErrorCode::ClientAuthenticate,
            bindings::MONGOC_ERROR_CLIENT_NO_ACCEPTABLE_PEER     => MongoErrorCode::ClientNoAcceptablePeer,
            bindings::MONGOC_ERROR_CLIENT_IN_EXHAUST             => MongoErrorCode::ClientInExhaust,
            bindings::MONGOC_ERROR_PROTOCOL_INVALID_REPLY        => MongoErrorCode::ProtocolInvalidReply,
            bindings::MONGOC_ERROR_PROTOCOL_BAD_WIRE_VERSION     => MongoErrorCode::ProtocolBadWireVersion,
            bindings::MONGOC_ERROR_CURSOR_INVALID_CURSOR         => MongoErrorCode::CursorInvalidCursor,
            bindings::MONGOC_ERROR_QUERY_FAILURE                 => MongoErrorCode::QueryFailure,
            bindings::MONGOC_ERROR_BSON_INVALID                  => MongoErrorCode::BsonInvalid,
            bindings::MONGOC_ERROR_MATCHER_INVALID               => MongoErrorCode::MatcherInvalid,
            bindings::MONGOC_ERROR_NAMESPACE_INVALID             => MongoErrorCode::NamespaceInvalid,
            bindings::MONGOC_ERROR_NAMESPACE_INVALID_FILTER_TYPE => MongoErrorCode::NamespaceInvalidFilterType,
            bindings::MONGOC_ERROR_COMMAND_INVALID_ARG           => MongoErrorCode::CommandInvalidArg,
            bindings::MONGOC_ERROR_COLLECTION_INSERT_FAILED      => MongoErrorCode::CollectionInsertFailed,
            bindings::MONGOC_ERROR_COLLECTION_UPDATE_FAILED      => MongoErrorCode::CollectionUpdateFailed,
            bindings::MONGOC_ERROR_COLLECTION_DELETE_FAILED      => MongoErrorCode::CollectionDeleteFailed,
            bindings::MONGOC_ERROR_COLLECTION_DOES_NOT_EXIST     => MongoErrorCode::CollectionDoesNotExist,
            bindings::MONGOC_ERROR_GRIDFS_INVALID_FILENAME       => MongoErrorCode::GridfsInvalidFilename,
            bindings::MONGOC_ERROR_SCRAM_NOT_DONE                => MongoErrorCode::ScramNotDone,
            bindings::MONGOC_ERROR_SCRAM_PROTOCOL_ERROR          => MongoErrorCode::ScramProtocolError,
            bindings::MONGOC_ERROR_QUERY_COMMAND_NOT_FOUND       => MongoErrorCode::QueryCommandNotFound,
            bindings::MONGOC_ERROR_QUERY_NOT_TAILABLE            => MongoErrorCode::QueryNotTailable,
            _                                                    => MongoErrorCode::Unknown
        }
    }

    pub fn get_message(&self) -> Cow<str> {
        let cstr = unsafe { CStr::from_ptr(&self.inner.message as *const i8) };
        String::from_utf8_lossy(cstr.to_bytes())
    }

    pub fn mut_inner(&mut self) -> &mut bindings::bson_error_t {
        &mut self.inner
    }
}

impl fmt::Debug for BsoncError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BsoncError: {}", &self.get_message())
    }
}

impl fmt::Display for BsoncError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.get_message())
    }
}

impl error::Error for BsoncError {
    fn description(&self) -> &str {
        "Error reported by the underlying Mongo C driver"
    }
}

impl From<BsoncError> for MongoError {
    fn from(error: BsoncError) -> MongoError {
        MongoError::Bsonc(error)
    }
}

pub struct InvalidParamsError;

impl fmt::Debug for InvalidParamsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InvalidParamsError: Invalid params supplied")
    }
}

impl fmt::Display for InvalidParamsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid params supplied")
    }
}

impl error::Error for InvalidParamsError {
    fn description(&self) -> &str {
        "Invalid params reported by the underlying Mongo C driver, no more information is available"
    }
}

impl From<InvalidParamsError> for MongoError {
    fn from(error: InvalidParamsError) -> MongoError {
        MongoError::InvalidParams(error)
    }
}

#[cfg(test)]
mod tests {
    use super::{BsoncError,MongoErrorDomain,MongoErrorCode};

    #[test]
    fn test_bson_error_empty() {
        let mut error = BsoncError::empty();
        assert!(error.is_empty());
        error.mut_inner().code = 1;
        assert!(!error.is_empty());
        error.mut_inner().domain = 1;
        error.mut_inner().code = 0;
        assert!(!error.is_empty());
    }

    #[test]
    fn test_bson_error_domain() {
        let mut error = BsoncError::empty();
        assert_eq!(MongoErrorDomain::Blank, error.domain());
        error.mut_inner().domain = 1;
        assert_eq!(MongoErrorDomain::Client, error.domain());
    }

    #[test]
    fn test_bson_error_code() {
        let mut error = BsoncError::empty();
        assert_eq!(MongoErrorCode::Blank, error.code());
        error.mut_inner().code = 1;
        assert_eq!(MongoErrorCode::StreamInvalidType, error.code());
    }
}
