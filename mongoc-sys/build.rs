extern crate pkg_config;

use std::env;
use std::path::Path;
use std::process::Command;

fn main() {
    let mongoc_version = env!("CARGO_PKG_VERSION")
        .split('-')
        .next()
        .expect("Crate version is not valid");

    if pkg_config::Config::new()
        .atleast_version(mongoc_version)
        .statik(true)
        .probe("libmongoc-1.0")
        .is_err()
    {
        let out_dir_var = env::var("OUT_DIR").expect("No out dir");
        let out_dir = format!("{}/{}", out_dir_var, mongoc_version);
        let driver_src_path = format!("mongo-c-driver-{}", mongoc_version);

        let libmongoc_path = Path::new(&out_dir).join("lib/libmongoc-1.0.a");
        if !libmongoc_path.exists() {
            // Download and extract driver archive
            let url = format!(
                "https://github.com/mongodb/mongo-c-driver/releases/download/{}/mongo-c-driver-{}.tar.gz",
                mongoc_version,
                mongoc_version
            );
            Command::new("curl").arg("-O") // Save to disk
                .arg("-L") // Follow redirects
                .arg(url)
                .status()
                .expect("Could not run curl");

            let archive_name = format!("mongo-c-driver-{}.tar.gz", mongoc_version);
            Command::new("tar")
                .arg("xzf")
                .arg(&archive_name)
                .status()
                .expect("Could not unarchive tar");

            // Configure
            let mut command = Command::new("cmake");
            command.arg("");
            command.arg("-DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF");
            command.arg("-DENABLE_STATIC=ON");
            command.arg("-DENABLE_BSON=ON");
            command.arg("-DENABLE_SSL=openssl");
            command.arg(format!("-DCMAKE_INSTALL_PREFIX:PATH={}", &out_dir));
            command.current_dir(&driver_src_path);

            // Enable debug symbols if configured for this profile
            if env::var("DEBUG") == Ok("true".to_string()) {
                command.arg("--enable-debug-symbols=yes");
            }

            // Use target that Cargo sets
            if let Ok(target) = env::var("TARGET") {
                command.arg(format!("--build={}", target));
            }

            command.status().expect("cmake failed");
            Command::new("make")
                .current_dir(&driver_src_path)
                .status()
                .expect("Make failed");
            Command::new("make")
                .arg("install")
                .current_dir(&driver_src_path)
                .status()
                .expect("make install failed");
        }

        // Output to Cargo
        println!("cargo:rustc-link-search=native={}/lib", &out_dir);
        println!("cargo:rustc-link-lib=static=bson-1.0");
        println!("cargo:rustc-link-lib=static=mongoc-1.0");
    }
}
