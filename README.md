# Mongo Rust Driver

Mongo Rust driver built on top of the [Mongo C driver](https://github.com/mongodb/mongo-c-driver).
This driver is a thin wrapper around the production-ready C driver that provides a safe and ergonomic Rust interface which handles all the gnarly usage details of the C driver for you.

Bson encoding and decoding is handled by the [bson crate](https://github.com/zonyitoo/bson-rs), the bindings are based on generated bindings by [bindgen](https://github.com/crabtw/rust-bindgen).

The API should still be considered experimental, but I'm not expecting changes at the moment.

[Documentation](https://docs.rs/mongo_driver/)

## Compatibility

The driver currently only builds on Unix, tested on Mac Os X and Linux so far. It's compatible with MongoDB 2.6 up to 3.4 and has full replica set and SSL support.

## Installation

If you have any trouble installing the crate (linking openssl can be
tricky) please check out the [installation instructions for the C driver](http://mongoc.org/libmongoc/current/installing.html).

To build on Mac install OpenSSL 1.1 and cmake:

```
brew install openssl@1.1
brew install cmake
```

Export these env vars the before you make a clean build:

```
export LDFLAGS="-L/usr/local/opt/openssl@1.1/lib"
export CPPFLAGS="-I/usr/local/opt/openssl@1.1/include"
export PKG_CONFIG_PATH="/usr/local/opt/openssl@1.1/lib/pkgconfig"
```

## Logging

All internal logging by mongoc is redirected to the macros in the [log
crate](http://doc.rust-lang.org/log/log/index.html). See the `log` docs
to configure output in your application.

## SSL test

There is a test included to connect to a replica set over SSL. To skip
this test:

```
SKIP_SSL_CONNECTION_TESTS=true cargo test
```

To run this tests fill these environment variables with something appropiate to
connect to a replica set:

```
MONGO_RUST_DRIVER_SSL_URI
MONGO_RUST_DRIVER_SSL_PEM_FILE
MONGO_RUST_DRIVER_SSL_CA_FILE
```

## Examples

See the tests directory for examples of how to use the driver.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Contributions are very welcome, only the functionality we use has been wrapped so far. Please write a test for any behavior you add.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
