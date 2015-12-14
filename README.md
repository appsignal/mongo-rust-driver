# Mongo Rust Driver

## About

Mongo Rust driver built on top of the [Mongo C driver](https://github.com/mongodb/mongo-c-driver).
This drivers aims to be a thin wrapper around the production-ready C driver, while providing a safe
and ergonomic Rust interface that handles all the gnarly usage details of the C driver for you.

Bson encoding and decoding is handled by the [bson crate](https://github.com/zonyitoo/bson-rs), the bindings
are based on generated bindings by [bindgen](https://github.com/crabtw/rust-bindgen).

The API should still be considered experimental, but I'm not expecting changes at the moment.

## Compatibility

The driver currently only builds on Unix, tested on Mac Os X and Linux so far.

## Logging

All internal logging by mongoc is redirected to the macros in the [log
crate](http://doc.rust-lang.org/log/log/index.html). See the `log` docs
to configure output in your application.

## Examples

See the tests directory for examples of how to use the driver.

## Contributing

Contributions are very welcome, only the functionality we use has been wrapped so far. Please write a test for any behavior you add.
