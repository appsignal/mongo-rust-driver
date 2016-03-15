use std::env;
use std::path::Path;
use std::process::Command;

static VERSION: &'static str = "1.3.4"; // Should be the same as the version in the manifest

fn main() {
    let out_dir_var = env::var("OUT_DIR").unwrap();
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
                         env("CFLAGS", "-DMONGOC_TRACE").
                         status().
                         unwrap().
                         success());
        assert!(Command::new("make").
                         arg("install").
                         current_dir(&driver_src_path).
                         status().
                         unwrap().
                         success());
    }

    // Output to Cargo
    println!("cargo:rustc-link-search=native={}/lib", &out_dir);
    println!("cargo:rustc-link-lib=static=bson-1.0");
    println!("cargo:rustc-link-lib=static=mongoc-1.0");
}
