fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    let proto = "../../proto/chronicle/v1/ingest.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&[proto], &["../../proto"])?;
    Ok(())
}
