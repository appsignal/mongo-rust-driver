extern crate pkg_config;

use std::env;
use std::path::Path;
use std::process::Command;

fn main() {
    let mongoc_version = env!("CARGO_PKG_VERSION")
        .split('-')
        .next()
        .expect("Crate version is not valid");


    let out_dir_var = env::var("OUT_DIR").expect("No out dir");
    let out_dir = Path::new(&out_dir_var);
    let driver_src_path = out_dir.join(format!("mongo-c-driver-{}", mongoc_version));

    let libmongoc_path = out_dir.join("usr/local/lib/libmongoc-static-1.0.a");
    if !libmongoc_path.exists() {
        // Download and extract driver archive
        let url = format!(
            "https://github.com/mongodb/mongo-c-driver/releases/download/{}/mongo-c-driver-{}.tar.gz",
            mongoc_version,
            mongoc_version
        );
        assert!(
            Command::new("curl").arg("-O") // Save to disk
                .current_dir(out_dir)
                .arg("-L") // Follow redirects
                .arg(url)
                .status()
                .expect("Could not run curl")
                .success()
        );

        let archive_name = format!("mongo-c-driver-{}.tar.gz", mongoc_version);
        assert!(
            Command::new("tar")
                .current_dir(out_dir)
                .arg("xzf")
                .arg(&archive_name)
                .status()
                .expect("Could not run tar")
                .success()
        );

        // Set up cmake command
        let mut cmake = Command::new("cmake");
        cmake.current_dir(&driver_src_path);

        let pkg = pkg_config::Config::new();
        pkg.probe("zlib").expect("Cannot find zlib");
        #[cfg(target_os = "linux")] pkg.probe("icu-i18n").expect("Cannot find icu");
        match pkg.probe("snappy") {
            Ok(_) => {
                cmake.arg("-DENABLE_SNAPPY=ON");
            },
            Err(e) => {
                println!("Snappy not found: {}", e);
                cmake.arg("-DENABLE_SNAPPY=OFF");
            }
        }

        cmake.arg("-DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF");
        cmake.arg("-DENABLE_SSL=OPENSSL");
        cmake.arg("-DENABLE_SASL=OFF");
        cmake.arg("-DENABLE_STATIC=ON");
        cmake.arg("-DENABLE_BSON=ON");
        cmake.arg("-DENABLE_ENABLE_EXAMPLES=OFF");
        cmake.arg("-DENABLE_TESTS=OFF");
        cmake.arg("-DENABLE_SHM_COUNTERS=OFF");
        cmake.arg("-DWITH_PIC=ON");

        // Run in current dir
        cmake.arg(".");

        // Run cmake command
        assert!(cmake.status().expect("Could not run cmake").success());

        // Set up make install command
        let mut make = Command::new("make");
        make.current_dir(&driver_src_path);
        make.arg(format!("DESTDIR={}", out_dir.to_string_lossy()));
        make.arg("install");

        // Run make command
        assert!(make.status().expect("Could not run make install").success());
    }

    // Output to Cargo
    println!("cargo:rustc-link-search=native={}/usr/local/lib", &out_dir.to_string_lossy());
    println!("cargo:rustc-link-lib=static=bson-static-1.0");
    println!("cargo:rustc-link-lib=static=mongoc-static-1.0");
    println!("cargo:rustc-link-lib=resolv");
}
