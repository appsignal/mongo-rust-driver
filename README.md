# Mongo Rust Driver

## About

Mongo Rust driver built on top of the [Mongo C driver](https://github.com/mongodb/mongo-c-driver).
This drivers aims to be a thin wrapper around the production-ready C driver, while providing a safe
and ergonomic Rust interface that handles all the gnarly usage details of the C driver for you.

Bson encoding and decoding is handled by the [bson crate](https://github.com/zonyitoo/bson-rs), the bindings
are generated using [bindgen](https://github.com/crabtw/rust-bindgen).

The API is experimental, it might change at any time.

## Compatibility

The driver currently only builds on Unix, only tested on Mac Os X so far.

## Contributing

Contributions are very welcome, only the parts of the C driver we need have been wrapped so far. Please
write a test for any behavior you add.
