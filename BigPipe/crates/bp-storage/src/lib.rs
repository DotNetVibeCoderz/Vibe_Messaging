//! BigPipe storage engine.
//!
//! - [`partition::Partition`]: per-partition log made of local segments, tiered segments and
//!   diskless extents, owned by a single shard.
//! - [`read`]: executes read plans outside the shard (files, object storage, memory).
//! - [`objstore::ObjectStorage`]: S3 / GCS / Azure Blob / MinIO / filesystem with an LRU cache.
//! - [`diskless`]: diskless object format and reference counting.
//! - [`compact`]: log compaction for `cleanup.policy=compact` topics.
//!
//! Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.

pub mod compact;
pub mod config;
pub mod diskless;
pub mod objstore;
pub mod partition;
pub mod read;

pub use config::{Durability, StorageMode, TopicConfig};
pub use objstore::ObjectStorage;
pub use partition::{AppendResult, CompactionStats, Partition, PartitionInfo};
pub use read::{Chunk, ReadPlan};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("object store: {0}")]
    Object(#[from] object_store::Error),
    #[error("object store path: {0}")]
    ObjectPath(#[from] object_store::path::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] bp_protocol::ProtocolError),
    #[error("offset {requested} out of range [{log_start}, {high_watermark}]")]
    OffsetOutOfRange { requested: i64, log_start: i64, high_watermark: i64 },
    #[error("producer state rejected batch (kafka error {0})")]
    Producer(i16),
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("configuration: {0}")]
    Config(String),
}

impl StorageError {
    /// Kafka error code for this failure.
    pub fn kafka_code(&self) -> i16 {
        use bp_protocol::messages::error;
        match self {
            StorageError::OffsetOutOfRange { .. } => error::OFFSET_OUT_OF_RANGE,
            StorageError::Producer(c) => *c,
            StorageError::Protocol(_) => error::CORRUPT_MESSAGE,
            _ => error::UNKNOWN_SERVER_ERROR,
        }
    }
}
