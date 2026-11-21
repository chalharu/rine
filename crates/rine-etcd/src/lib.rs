//! etcd-compatible API server with pluggable backend.
//!
//! Supported services: KV (Range/Put/DeleteRange/Txn/Compact), Watch,
//! Lease, Maintenance::Status, Maintenance::Alarm (GET only), Cluster::MemberList,
//! grpc.health.v1.
//! Maintenance::Snapshot is registered but intentionally unsupported
//! (UNIMPLEMENTED with an explanatory message): rine never stores bbolt
//! images, so backups are backend-native.
//! Every client listener also serves plain HTTP on the same port:
//! `/health`, `/version`, `/metrics` (Prometheus text exposition).

pub mod backend;
pub mod cluster;
pub mod error;
pub mod http;
pub mod kv;
pub mod lease;
pub mod memory;
pub mod server;
pub mod watch;

pub mod mvccpb {
    tonic::include_proto!("mvccpb");
}

pub mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");
}

pub use backend::{Backend, BackendFactory, Event, KeyValue, LeaseId, Revision};
pub use error::{Error, Result};
pub use memory::{MemoryBackend, MemoryFactory};
