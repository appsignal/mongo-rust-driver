#![feature(path_ext)]

extern crate bindgen;

use std::env;
use std::fs::PathExt;
use std::path::Path;
use std::process::Command;

static VERSION: &'static str = "1.1.10"; // Should be the same as the version in the manifest

fn main() {
    let out_dir_var = env::var("OUT_DIR").unwrap();
    let out_dir = format!("{}/{}", out_dir_var, VERSION);
    let current_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let driver_src_path = format!("mongo-c-driver-{}", VERSION);

    let libmongoc_path = Path::new(&out_dir).join("lib/libmongoc-1.0.a");
    if !libmongoc_path.exists() {
        // Download and extract driver archive
        let url = format!(
            "https://github.com/mongodb/mongo-c-driver/releases/download/{}/mongo-c-driver-{}.tar.gz",
            VERSION,
            VERSION
        );
        assert!(Command::new("curl").arg("-O") // Save to disk
                                    .arg("-L") // Follow redirects
                                    .arg(url)
                                    .status()
                                    .unwrap()
                                    .success());

        let archive_name = format!(
            "mongo-c-driver-{}.tar.gz",
            VERSION
        );
        assert!(Command::new("tar").arg("xzf")
                                   .arg(&archive_name)
                                   .status()
                                   .unwrap()
                                   .success());

        // Configure and install
        let mut command = Command::new("sh");
        command.arg("configure");
        command.arg("--enable-ssl=yes");
        command.arg("--enable-sasl=no");
        command.arg("--enable-static=yes");
        command.arg("--enable-shared=no");
        command.arg("--enable-shm-counters=no");
        command.arg("--with-libbson=bundled");
        command.arg("--with-pic=yes");
        command.arg(format!("--prefix={}", &out_dir));
        command.current_dir(&driver_src_path);

        // Enable debug symbols if configured for this profile
        if env::var("DEBUG") == Ok("true".to_string()) {
            command.arg("--enable-debug-symbols=yes");
        }

        // Use target that Cargo sets
        if let Ok(target) = env::var("TARGET") {
            command.arg(format!("--build={}", target));
        }

        assert!(command.status().unwrap().success());
        assert!(Command::new("make").
                         current_dir(&driver_src_path).
                         status().
                         unwrap().
                         success());
        assert!(Command::new("make").
                         arg("install").
                         current_dir(&driver_src_path).
                         status().
                         unwrap().
                         success());

        // Generate bindings
        let bindings_rs_path = Path::new(&current_dir).join("src/bindings.rs");
        let mongo_h_path     = Path::new(&current_dir).join(&driver_src_path).join("src/mongoc/mongoc.h");
        let bson_path        = Path::new(&current_dir).join(&driver_src_path).join("src/libbson/src/bson");
        let mongo_h_path_arg = mongo_h_path.to_str().unwrap();
        let bson_path_arg    = bson_path.to_str().unwrap();

        let mut builder = bindgen::builder();
        builder.emit_builtins();
        builder.header(mongo_h_path_arg);
        builder.clang_arg("-I".to_owned());
        builder.clang_arg(bson_path_arg);

        // Add clang include dir if it's detected by bindgen.
        if let Some(path) = bindgen::get_include_dir() {
            builder.clang_arg("-I".to_owned());
            builder.clang_arg(path);
        }

        // Add clang include dir from env var, use as a last resort
        // if include cannot be detected normally.
        if let Ok(path) = env::var("CLANG_INCLUDE_DIR") {
            builder.clang_arg("-I".to_owned());
            builder.clang_arg(path);
        }

        let binding = builder.generate().unwrap();
        binding.write_to_file(&bindings_rs_path).unwrap();
    }

    // Output to Cargo
    println!("cargo:root={}", &out_dir);
    println!("cargo:libdir={}/lib", &out_dir);
    println!("cargo:include={}/include", &out_dir);
    println!("cargo:rustc-link-search={}/lib", &out_dir);
    println!("cargo:rustc-link-lib=static=bson-1.0");
    println!("cargo:rustc-link-lib=static=mongoc-1.0");

    // Link openssl dynamically
    // TODO see if we can make this compatible with openssl-sys
    println!("cargo:rustc-link-lib=dylib=ssl");
    println!("cargo:rustc-link-lib=dylib=crypto");
}
