//! BigPipe broker core.
//!
//! Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.

pub mod agent;
pub mod broker;
pub mod config;
pub mod coordinator;
pub mod flow;
pub mod kafka;
pub mod metrics;
pub mod shard;
pub mod share;

pub use broker::{Broker, BrokerError, TopicMeta};
pub use config::NodeConfig;

/// Adapter so bpql expressions can evaluate decoded records.
pub struct RecordView<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub record: &'a bp_protocol::records::Record,
}

impl bp_expr::RecordContext for RecordView<'_> {
    fn key(&self) -> Option<&[u8]> {
        self.record.key.as_deref()
    }
    fn value(&self) -> Option<&[u8]> {
        self.record.value.as_deref()
    }
    fn header(&self, name: &str) -> Option<&[u8]> {
        self.record.header(name)
    }
    fn topic(&self) -> &str {
        self.topic
    }
    fn partition(&self) -> i32 {
        self.partition
    }
    fn offset(&self) -> i64 {
        self.record.offset
    }
    fn timestamp(&self) -> i64 {
        self.record.timestamp
    }
}
