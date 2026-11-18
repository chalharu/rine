//! etcd-compatible API server with pluggable backend.
//!
//! kine-inspired scope: KV (Range/Put/DeleteRange/Txn/Compact), Watch,
//! Lease, Maintenance::Status, Cluster::MemberList.

pub mod backend;
pub mod cluster;
pub mod error;
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

pub use backend::{Backend, Event, KeyValue, LeaseId, Revision};
pub use error::{Error, Result};
pub use memory::MemoryBackend;
