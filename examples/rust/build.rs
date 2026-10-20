fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&["../../rust-node/proto/node_api.proto"], &["../../rust-node/proto/"])
        .unwrap();
}
