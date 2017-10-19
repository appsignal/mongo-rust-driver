extern crate semver;

use std::env;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;

use semver::Version;

static VERSION: &'static str = "1.8.0"; // Should be the same major version as in the manifest

fn main() {
    let mut local_exist = false;
    if let Ok(output) = Command::new("pkg-config")
        .arg("--modversion")
        .arg("libmongoc-1.0")
        .output()
    {
        if output.status.success() {
            if let Ok(ref stdout) = String::from_utf8(output.stdout) {
                if let Ok(local_version) = Version::from_str(stdout) {
                    let except_version = Version::from_str(VERSION).unwrap();
                    if local_version.major == except_version.major &&
                        local_version.minor == except_version.minor
                    {
                        local_exist = true;
                    }
                }
            }
        }
    }

    if !local_exist {
        let out_dir_var = env::var("OUT_DIR").expect("No out dir");
        let out_dir = format!("{}/{}", out_dir_var, VERSION);
        let driver_src_path = format!("mongo-c-driver-{}", VERSION);

        let libmongoc_path = Path::new(&out_dir).join("lib/libmongoc-1.0.a");
        if !libmongoc_path.exists() {
            // Download and extract driver archive
            let url = format!(
                "https://github.com/mongodb/mongo-c-driver/releases/download/{}/mongo-c-driver-{}.tar.gz",
                VERSION,
                VERSION
            );
            assert!(
                Command::new("curl").arg("-O") // Save to disk
                                    .arg("-L") // Follow redirects
                                    .arg(url)
                                    .status()
                                    .expect("Could not run curl")
                                    .success()
            );

            let archive_name = format!("mongo-c-driver-{}.tar.gz", VERSION);
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
