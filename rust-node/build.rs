fn main() {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());

    // Internal node API
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/node_api.proto"], &["proto/"])
        .unwrap();

    // Tron Network Protos
    let tron_proto_files = &[
        "proto/tron/api/api.proto",
        "proto/tron/core/Tron.proto",
        "proto/tron/core/Discover.proto",
        "proto/tron/core/contract/balance_contract.proto",
        "proto/tron/core/contract/smart_contract.proto",
        "proto/core/Connect.proto",
    ];

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        // prost_types::Any does not implement Serialize/Deserialize
        .field_attribute("protocol.Transaction.Contract.parameter", "#[serde(skip)]")
        .compile_protos(tron_proto_files, &["proto/tron/", "proto/"])
        .unwrap();
}
