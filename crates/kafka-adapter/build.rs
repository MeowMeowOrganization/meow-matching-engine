fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc must be available");

    let proto_root = "proto";
    let proto_files = [
        "proto/meow/exchange/order/v1alpha1/order.proto",
        "proto/meow/exchange/matching/v1alpha1/commands.proto",
        "proto/meow/exchange/matching/v1alpha1/events.proto",
        "proto/meow/exchange/matching/v1alpha1/results.proto",
    ];

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);

    config
        .compile_protos(&proto_files, &[proto_root])
        .expect("matching protobuf contracts must compile");

    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={proto_file}");
    }
}
