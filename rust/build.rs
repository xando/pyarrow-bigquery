// Compile the vendored BigQuery Storage v1 protos. Only the Read service is
// used at runtime; we build the client side and skip server generation.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("proto");

    let protos: &[&str] = &[
        "google/cloud/bigquery/storage/v1/storage.proto",
        "google/cloud/bigquery/storage/v1/stream.proto",
        "google/cloud/bigquery/storage/v1/arrow.proto",
        "google/cloud/bigquery/storage/v1/avro.proto",
        "google/cloud/bigquery/storage/v1/table.proto",
        "google/cloud/bigquery/storage/v1/annotations.proto",
        "google/cloud/bigquery/storage/v1/protobuf.proto",
    ];

    let inputs: Vec<PathBuf> = protos.iter().map(|p| proto_root.join(p)).collect();

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&inputs, &[proto_root.clone()])?;

    for p in &inputs {
        println!("cargo:rerun-if-changed={}", p.display());
    }
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
