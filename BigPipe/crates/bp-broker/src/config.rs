use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Node configuration (`bigpipe.yaml`). Every field has a dev-friendly default so
/// `bigpiped --mode dev` works with no file at all.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeConfig {
    pub node_id: i32,
    pub cluster_id: String,
    pub data_dir: PathBuf,
    /// Kafka wire protocol listener.
    pub kafka_addr: String,
    /// Host/port returned to clients in Metadata responses.
    pub advertised_host: String,
    pub advertised_port: i32,
    /// REST/SSE gateway listener.
    pub http_addr: String,
    /// Admin REST API listener (used by the .NET control plane and `bpctl`).
    pub admin_addr: String,
    /// Prometheus/OpenMetrics listener.
    pub metrics_addr: String,
    pub rack: Option<String>,
    /// Number of shards (one OS thread each). 0 = number of CPU cores.
    pub shards: usize,
    /// Object storage URL for tiered and diskless data. Empty = `<data_dir>/objects`.
    pub object_store_url: String,
    pub object_cache_bytes: usize,
    pub diskless_linger_ms: u64,
    pub diskless_max_file_bytes: usize,
    pub auto_create_topics: bool,
    pub default_partitions: u32,
    pub default_storage_mode: String,
    pub group_initial_rebalance_delay_ms: u64,
    pub group_min_session_timeout_ms: i32,
    pub group_max_session_timeout_ms: i32,
    /// Optional API key required on the admin API (`Authorization: Bearer <key>`).
    pub admin_api_key: Option<String>,
    pub max_request_bytes: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            cluster_id: "bigpipe-dev".into(),
            data_dir: PathBuf::from("./data"),
            kafka_addr: "0.0.0.0:9092".into(),
            advertised_host: "localhost".into(),
            advertised_port: 9092,
            http_addr: "0.0.0.0:8082".into(),
            admin_addr: "0.0.0.0:9644".into(),
            metrics_addr: "0.0.0.0:9645".into(),
            rack: None,
            shards: 0,
            object_store_url: String::new(),
            object_cache_bytes: 256 * 1024 * 1024,
            diskless_linger_ms: 100,
            diskless_max_file_bytes: 8 * 1024 * 1024,
            auto_create_topics: true,
            default_partitions: 3,
            default_storage_mode: "local".into(),
            group_initial_rebalance_delay_ms: 300,
            group_min_session_timeout_ms: 1_000,
            group_max_session_timeout_ms: 300_000,
            admin_api_key: None,
            max_request_bytes: 100 * 1024 * 1024,
        }
    }
}

impl NodeConfig {
    pub fn shard_count(&self) -> usize {
        if self.shards > 0 {
            self.shards
        } else {
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
        }
    }

    pub fn object_store_url(&self) -> String {
        if self.object_store_url.is_empty() {
            self.data_dir.join("objects").to_string_lossy().into_owned()
        } else {
            self.object_store_url.clone()
        }
    }
}
