extern crate pkg_config;
#[cfg(target_env = "msvc")]
extern crate vcpkg;

use std::env;
use std::path::Path;
use std::process::Command;


#[cfg(not(target_env = "msvc"))]
fn lin(mongoc_version: &str) {
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
                assert!(
                    Command::new("curl").arg("-O") // Save to disk
                        .arg("-L") // Follow redirects
                        .arg(url)
                        .status()
                        .expect("Could not run curl")
                        .success()
                );

                let archive_name = format!("mongo-c-driver-{}.tar.gz", mongoc_version);
                assert!(
                    Command::new("tar")
                        .arg("xzf")
                        .arg(&archive_name)
                        .status()
                        .expect("Could not run tar")
                        .success()
                );

                // Configure and install
                let mut command = Command::new("sh");
                command.arg("configure");
                command.arg("--enable-ssl=openssl");
                command.arg("--enable-sasl=no");
                command.arg("--enable-static=yes");
                command.arg("--enable-shared=no");
                command.arg("--enable-shm-counters=no");
                command.arg("--with-libbson=bundled");
                command.arg("--with-pic=yes");
                command.arg("--with-snappy=no");
                command.arg("--with-zlib=no");
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

                assert!(command.status().expect("Could not run configure").success());
                assert!(
                    Command::new("make")
                        .current_dir(&driver_src_path)
                        .env("CFLAGS", "-DMONGOC_TRACE")
                        .status()
                        .expect("Could not run make")
                        .success()
                );
                assert!(
                    Command::new("make")
                        .arg("install")
                        .current_dir(&driver_src_path)
                        .status()
                        .expect("Could not run make install")
                        .success()
                );
            }

            // Output to Cargo
            println!("cargo:rustc-link-search=native={}/lib", &out_dir);
            println!("cargo:rustc-link-lib=static=bson-1.0");
            println!("cargo:rustc-link-lib=static=mongoc-1.0");
        }
}

#[cfg(target_env = "msvc")]
fn win(mongoc_version: &str) {
    use vcpkg;

    let mongo_lib = "mongoc-1.0";
    let bson_lib = "bson-1.0";

    if vcpkg::Config::new()
        .emit_includes(true)
        .probe("mongo-c-driver")
        .is_ok()
    {
        // Output to Cargo
        println!("cargo:rustc-link-lib={}", bson_lib);
        println!("cargo:rustc-link-lib={}", mongo_lib);
    } else {
        if let Ok(bson_dir_lib) = env::var("MONGO_LIB") {
            if let Ok(mongo_dir_lib) = env::var("BSON_LIB") {
                println!("cargo:rustc-link-search=native={}", bson_dir_lib);
                println!("cargo:rustc-link-lib=dylib={}", bson_lib);
                println!("cargo:rustc-link-search=native={}", mongo_dir_lib);
                println!("cargo:rustc-link-lib=dylib={}", mongo_lib);

            } else {
                panic!("please define BSON_LIB to {}.lib, \n for example set BSON_LIB=C:\\vcpkg\\packages\\libbson_x64-windows", bson_lib);
            }
        } else {
            panic!("please define MONGO_LIB to {}.lib, \n for example set MONGO_LIB=C:\\vcpkg\\packages\\mongo-c-driver_x64-windows\\lib", mongo_lib);
        }
    }
}

fn main() {
    let mongoc_version = env!("CARGO_PKG_VERSION")
        .split('-')
        .next()
        .expect("Crate version is not valid");

    #[cfg(target_env = "msvc")]
    win(mongoc_version);
    #[cfg(not(target_env = "msvc"))]
    lib(mongoc_version);
}
