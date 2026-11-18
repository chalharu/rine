fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("bundled protoc");
    std::env::set_var("PROTOC", &protoc);
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/mvccpb/kv.proto", "proto/etcdserverpb/rpc.proto"],
            &["proto"],
        )
        .expect("proto compile");
}
