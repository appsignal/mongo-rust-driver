#![feature(path_ext)]

extern crate bindgen;
extern crate pkg_config;

use std::env;
use std::fs::PathExt;
use std::path::Path;
use std::process::Command;

static VERSION: &'static str = "1.1.8"; // Should be the same as the version in the manifest

fn main() {
    let out_dir     = env::var("OUT_DIR").unwrap();
    let current_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    
    let driver_path = format!(
        "mongo-c-driver-{}",
        VERSION
    );

    let libmongoc_path = Path::new(&out_dir).join("lib/libmongoc-1.0.a");
    if !libmongoc_path.exists() { // TODO: This should check if we're at the right version
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
        assert!(Command::new("sh").arg("configure")
                          .arg("--enable-ssl=yes")
                          .arg("--enable-sasl=no")
                          .arg("--enable-static=yes")
                          .arg("--enable-shared=no")
                          .arg("--with-libbson=bundled")
                          .arg(format!("--prefix={}", &out_dir))
                          .current_dir(&driver_path)
                          .status()
                          .unwrap()
                          .success());
        assert!(Command::new("make").current_dir(&driver_path).status().unwrap().success());
        assert!(Command::new("make").arg("install").current_dir(&driver_path).status().unwrap().success());

        // Generate bindings
        let bindings_rs_path = Path::new(&current_dir).join("src/bindings.rs");
        let mongo_h_path     = Path::new(&current_dir).join(&driver_path).join("src/mongoc/mongoc.h");
        let bson_path        = Path::new(&current_dir).join(&driver_path).join("src/libbson/src/bson");

        // Add include CLANG_INCLUDE_DIR so that several issue in searching
        // clang include dir
        bindgen::builder()
            .emit_builtins()
            .header(mongo_h_path.to_str().unwrap())
            .clang_arg(format!("-I{}", bson_path.to_str().unwrap()))
            .clang_arg(format!("-I{}", env::var("CLANG_INCLUDE_DIR")
                               .unwrap_or(String::from("/usr/lib/clang/3.6.1/include"))))
            .generate()
            .unwrap()
            .write_to_file(&bindings_rs_path)
            .unwrap();
    }

    // Output to Cargo
    println!("cargo:root={}", &out_dir);
    println!("cargo:libdir={}/lib", &out_dir);
    println!("cargo:include={}/include", &out_dir);
    println!("cargo:rustc-link-search={}/lib", &out_dir);
    println!("cargo:rustc-link-lib=static=bson-1.0");
    println!("cargo:rustc-link-lib=static=mongoc-1.0");

    for link_path in pkg_config::find_library("openssl").unwrap().link_paths.iter(){
        println!("cargo:rustc-link-search=framework={}", &link_path.display());
    }
}
