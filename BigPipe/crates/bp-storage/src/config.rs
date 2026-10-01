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
    /// `delete`, `compact`, or both (`compact,delete`).
    pub cleanup_policy: String,
    /// Compaction starts once this share of a partition's sealed bytes has not been cleaned yet.
    pub min_cleanable_dirty_ratio: f64,
    /// Records younger than this are never compacted away.
    pub min_compaction_lag_ms: i64,
    /// How long a tombstone (null value) survives compaction, so consumers can see the delete.
    pub delete_retention_ms: i64,
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
            min_cleanable_dirty_ratio: 0.5,
            min_compaction_lag_ms: 0,
            delete_retention_ms: 24 * 3600 * 1000,
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
    "min.cleanable.dirty.ratio",
    "min.compaction.lag.ms",
    "delete.retention.ms",
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
                "cleanup.policy" => {
                    let mut parts: Vec<&str> = v.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
                    parts.sort_unstable();
                    parts.dedup();
                    if parts.is_empty() || parts.iter().any(|p| !matches!(*p, "compact" | "delete")) {
                        return Err(bad());
                    }
                    c.cleanup_policy = parts.join(",");
                }
                "min.cleanable.dirty.ratio" => {
                    let r = v.trim().parse::<f64>().map_err(|_| bad())?;
                    if !(0.0..=1.0).contains(&r) {
                        return Err(bad());
                    }
                    c.min_cleanable_dirty_ratio = r;
                }
                "min.compaction.lag.ms" => c.min_compaction_lag_ms = int()?.max(0),
                "delete.retention.ms" => c.delete_retention_ms = int()?.max(0),
                // Unknown keys are kept verbatim (Kafka tooling sets many we do not act on).
                _ => {}
            }
        }
        if c.compact() && c.mode != StorageMode::Local {
            // Compaction rewrites segments on local disk; tiered and diskless data is immutable.
            return Err(ConfigError {
                key: "cleanup.policy".into(),
                value: format!("{} (compaction needs bigpipe.storage.mode=local, not {})", c.cleanup_policy, c.mode.as_str()),
            });
        }
        Ok(c)
    }

    /// True when the topic keeps only the latest record per key.
    pub fn compact(&self) -> bool {
        self.cleanup_policy.split(',').any(|p| p == "compact")
    }

    /// True when time and size retention delete old segments.
    pub fn delete(&self) -> bool {
        self.cleanup_policy.split(',').any(|p| p == "delete")
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
            "min.cleanable.dirty.ratio" => self.min_cleanable_dirty_ratio.to_string(),
            "min.compaction.lag.ms" => self.min_compaction_lag_ms.to_string(),
            "delete.retention.ms" => self.delete_retention_ms.to_string(),
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

    #[test]
    fn cleanup_policy() {
        let mut m = BTreeMap::new();
        m.insert("cleanup.policy".into(), "delete, compact".into());
        let c = TopicConfig::from_map(&m).unwrap();
        assert_eq!(c.cleanup_policy, "compact,delete");
        assert!(c.compact() && c.delete());
        m.insert("cleanup.policy".into(), "compact".into());
        let c = TopicConfig::from_map(&m).unwrap();
        assert!(c.compact() && !c.delete());
        m.insert("cleanup.policy".into(), "shred".into());
        assert!(TopicConfig::from_map(&m).is_err());
        m.insert("cleanup.policy".into(), "compact".into());
        m.insert("bigpipe.storage.mode".into(), "diskless".into());
        let e = TopicConfig::from_map(&m).unwrap_err();
        assert!(e.value.contains("needs bigpipe.storage.mode=local"), "{e}");
    }
}
