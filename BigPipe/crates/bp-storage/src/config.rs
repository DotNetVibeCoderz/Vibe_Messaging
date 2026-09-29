use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Where new appends for a topic go. Storage is a per-topic policy, not a cluster decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum StorageMode {
    /// Segment log on local disk.
    #[default]
    Local,
    /// Local hot segments, sealed segments uploaded to object storage.
    Tiered,
    /// Straight to object storage through the stateless agent batcher.
    Diskless,
}

impl StorageMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "local" => Some(Self::Local),
            "tiered" => Some(Self::Tiered),
            "diskless" => Some(Self::Diskless),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Tiered => "tiered",
            Self::Diskless => "diskless",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    /// Acknowledge once written to the OS page cache (Kafka's default). Sealed segments are
    /// fsynced in the background; `flush.ms` optionally adds periodic fsyncs of the active one.
    #[default]
    Flush,
    /// fsync every append group before acknowledging.
    Fsync,
}

/// Resolved per-topic configuration. Built from the raw `key=value` map stored with the topic.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicConfig {
    pub mode: StorageMode,
    pub segment_bytes: u64,
    pub segment_ms: i64,
    /// -1 = infinite.
    pub retention_ms: i64,
    /// -1 = infinite.
    pub retention_bytes: i64,
    /// Tiered: how long sealed segments stay on local disk after upload.
    pub local_retention_ms: i64,
    pub durability: Durability,
    pub flush_ms: i64,
    pub max_message_bytes: usize,
    pub cache_bytes: usize,
    /// Server-side schema validation: none | lenient | strict (JSON syntax check in this release).
    pub schema_validation: String,
    pub cleanup_policy: String,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            mode: StorageMode::Local,
            segment_bytes: 256 * 1024 * 1024,
            segment_ms: 7 * 24 * 3600 * 1000,
            retention_ms: 7 * 24 * 3600 * 1000,
            retention_bytes: -1,
            local_retention_ms: 3600 * 1000,
            durability: Durability::Flush,
            // -1: leave active-segment flushing to the OS, like Kafka's default.
            flush_ms: -1,
            max_message_bytes: 8 * 1024 * 1024,
            cache_bytes: 4 * 1024 * 1024,
            schema_validation: "none".into(),
            cleanup_policy: "delete".into(),
        }
    }
}

/// Keys BigPipe understands, with their defaults, for DescribeConfigs and the admin API.
pub const KNOWN_KEYS: &[&str] = &[
    "bigpipe.storage.mode",
    "segment.bytes",
    "segment.ms",
    "retention.ms",
    "retention.bytes",
    "local.retention.ms",
    "bigpipe.durability",
    "flush.ms",
    "max.message.bytes",
    "bigpipe.cache.bytes",
    "bigpipe.schema.validation",
    "cleanup.policy",
];

#[derive(Debug, thiserror::Error)]
#[error("invalid value `{value}` for `{key}`")]
pub struct ConfigError {
    pub key: String,
    pub value: String,
}

impl TopicConfig {
    pub fn from_map(raw: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        let mut c = Self::default();
        for (k, v) in raw {
            let bad = || ConfigError { key: k.clone(), value: v.clone() };
            let int = || v.trim().parse::<i64>().map_err(|_| bad());
            match k.as_str() {
                "bigpipe.storage.mode" => c.mode = StorageMode::parse(v).ok_or_else(bad)?,
                "segment.bytes" => c.segment_bytes = int()?.max(1024) as u64,
                "segment.ms" => c.segment_ms = int()?.max(1000),
                "retention.ms" => c.retention_ms = int()?,
                "retention.bytes" => c.retention_bytes = int()?,
                "local.retention.ms" => c.local_retention_ms = int()?,
                "bigpipe.durability" => {
                    c.durability = match v.as_str() {
                        "flush" | "raft_quorum" => Durability::Flush,
                        "fsync" | "raft_quorum_fsync" => Durability::Fsync,
                        _ => return Err(bad()),
                    }
                }
                "flush.ms" => c.flush_ms = int()?,
                "max.message.bytes" => c.max_message_bytes = int()?.max(1024) as usize,
                "bigpipe.cache.bytes" => c.cache_bytes = int()?.max(0) as usize,
                "bigpipe.schema.validation" => {
                    if !matches!(v.as_str(), "none" | "lenient" | "strict") {
                        return Err(bad());
                    }
                    c.schema_validation = v.clone();
                }
                "cleanup.policy" => c.cleanup_policy = v.clone(),
                // Unknown keys are kept verbatim (Kafka tooling sets many we do not act on).
                _ => {}
            }
        }
        Ok(c)
    }

    /// Effective value of a key as a string (for DescribeConfigs / admin API).
    pub fn describe(&self, key: &str) -> Option<String> {
        Some(match key {
            "bigpipe.storage.mode" => self.mode.as_str().to_string(),
            "segment.bytes" => self.segment_bytes.to_string(),
            "segment.ms" => self.segment_ms.to_string(),
            "retention.ms" => self.retention_ms.to_string(),
            "retention.bytes" => self.retention_bytes.to_string(),
            "local.retention.ms" => self.local_retention_ms.to_string(),
            "bigpipe.durability" => match self.durability {
                Durability::Flush => "flush".into(),
                Durability::Fsync => "fsync".into(),
            },
            "flush.ms" => self.flush_ms.to_string(),
            "max.message.bytes" => self.max_message_bytes.to_string(),
            "bigpipe.cache.bytes" => self.cache_bytes.to_string(),
            "bigpipe.schema.validation" => self.schema_validation.clone(),
            "cleanup.policy" => self.cleanup_policy.clone(),
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_keys() {
        let mut m = BTreeMap::new();
        m.insert("bigpipe.storage.mode".into(), "diskless".into());
        m.insert("retention.ms".into(), "-1".into());
        m.insert("unknown.key".into(), "x".into());
        let c = TopicConfig::from_map(&m).unwrap();
        assert_eq!(c.mode, StorageMode::Diskless);
        assert_eq!(c.retention_ms, -1);
        m.insert("segment.bytes".into(), "abc".into());
        assert!(TopicConfig::from_map(&m).is_err());
    }
}
