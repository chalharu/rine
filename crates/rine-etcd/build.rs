fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("bundled protoc");

    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(&protoc);

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_with_config(
            config,
            &["proto/mvccpb/kv.proto", "proto/etcdserverpb/rpc.proto"],
            &["proto"],
        )
        .expect("proto compile");
}
