fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    #[cfg(feature = "napi-bindings")]
    {
        extern crate napi_build;
        napi_build::setup();
    }
}
