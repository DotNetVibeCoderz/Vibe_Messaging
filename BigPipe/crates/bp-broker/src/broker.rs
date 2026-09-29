//! The node: topic metadata, routing to shards / the diskless agent, and the high-level
//! produce / fetch / admin operations shared by the Kafka server and the HTTP gateway.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use bp_protocol::messages::error;
use bp_protocol::records::{self, Record};
use bp_storage::diskless::FileRefs;
use bp_storage::partition::{PartitionInfo, now_ms};
use bp_storage::{AppendResult, ObjectStorage, StorageError, StorageMode, TopicConfig};
use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::agent::{AgentConfig, AgentHandle, spawn_agent};
use crate::config::NodeConfig;
use crate::coordinator::{Coordinator, GroupConfig};
use crate::metrics::Metrics;
use crate::shard::{ShardCmd, ShardHandle, TpKey, shard_for, spawn_shards};

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("unknown topic or partition `{0}`")]
    UnknownTopic(String),
    #[error("topic `{0}` already exists")]
    TopicExists(String),
    #[error("invalid topic name `{0}`: use 1-249 characters from [a-zA-Z0-9._-]")]
    InvalidTopic(String),
    #[error("invalid partition count: {0}")]
    InvalidPartitions(String),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("invalid record: {0}")]
    InvalidRecord(String),
    #[error("record batch larger than max.message.bytes ({0} bytes)")]
    TooLarge(usize),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl BrokerError {
    pub fn kafka_code(&self) -> i16 {
        match self {
            BrokerError::UnknownTopic(_) => error::UNKNOWN_TOPIC_OR_PARTITION,
            BrokerError::TopicExists(_) => error::TOPIC_ALREADY_EXISTS,
            BrokerError::InvalidTopic(_) => error::INVALID_TOPIC_EXCEPTION,
            BrokerError::InvalidPartitions(_) => error::INVALID_PARTITIONS,
            BrokerError::InvalidConfig(_) => error::INVALID_CONFIG,
            BrokerError::InvalidRecord(_) => error::INVALID_RECORD,
            BrokerError::TooLarge(_) => error::MESSAGE_TOO_LARGE,
            BrokerError::Storage(e) => e.kafka_code(),
        }
    }

    /// HTTP status for the REST APIs.
    pub fn http_status(&self) -> u16 {
        match self {
            BrokerError::UnknownTopic(_) => 404,
            BrokerError::TopicExists(_) => 409,
            BrokerError::Storage(StorageError::OffsetOutOfRange { .. }) => 416,
            BrokerError::Storage(StorageError::Producer(_)) => 409,
            BrokerError::Storage(_) => 500,
            BrokerError::TooLarge(_) => 413,
            _ => 400,
        }
    }
}

pub struct TopicMeta {
    pub name: Arc<str>,
    pub partitions: i32,
    pub raw_config: BTreeMap<String, String>,
    pub config: Arc<TopicConfig>,
    pub created_ms: i64,
    pub hw: Vec<watch::Receiver<i64>>,
}

impl TopicMeta {
    pub fn is_internal(&self) -> bool {
        self.name.starts_with("__")
    }
}

#[derive(Serialize, Deserialize)]
struct PersistedTopic {
    name: String,
    partitions: i32,
    config: BTreeMap<String, String>,
    created_ms: i64,
}

pub struct FetchData {
    pub chunks: Vec<Bytes>,
    pub high_watermark: i64,
    pub log_start: i64,
}

impl FetchData {
    pub fn bytes(&self) -> usize {
        self.chunks.iter().map(Bytes::len).sum()
    }
}

pub struct Broker {
    pub cfg: NodeConfig,
    topics: RwLock<BTreeMap<String, Arc<TopicMeta>>>,
    shards: Vec<ShardHandle>,
    agent: AgentHandle,
    pub store: Arc<ObjectStorage>,
    pub coordinator: Arc<Coordinator>,
    pub metrics: Arc<Metrics>,
    refs: Arc<FileRefs>,
    next_producer_id: AtomicI64,
    round_robin: std::sync::atomic::AtomicU32,
    pub shares: crate::share::ShareGroups,
    pub flows: crate::flow::Flows,
    admin_lock: tokio::sync::Mutex<()>,
    pub started: Instant,
}

pub fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

impl Broker {
    pub async fn start(cfg: NodeConfig) -> anyhow::Result<Arc<Self>> {
        std::fs::create_dir_all(cfg.data_dir.join("meta"))?;
        std::fs::create_dir_all(cfg.data_dir.join("topics"))?;
        let metrics = Arc::new(Metrics::default());
        let store = Arc::new(ObjectStorage::open(&cfg.object_store_url(), cfg.object_cache_bytes)?);
        let refs = Arc::new(FileRefs::default());
        let shards = spawn_shards(cfg.shard_count(), store.clone(), refs.clone(), metrics.clone());
        let agent = spawn_agent(
            AgentConfig {
                node_id: cfg.node_id,
                linger: Duration::from_millis(cfg.diskless_linger_ms),
                max_file_bytes: cfg.diskless_max_file_bytes,
            },
            shards.clone(),
            store.clone(),
            refs.clone(),
            metrics.clone(),
        );
        let coordinator = Coordinator::open(
            &cfg.data_dir,
            GroupConfig {
                initial_delay: Duration::from_millis(cfg.group_initial_rebalance_delay_ms),
                min_session_ms: cfg.group_min_session_timeout_ms,
                max_session_ms: cfg.group_max_session_timeout_ms,
            },
            metrics.clone(),
        )?;
        let pid_seed: i64 = std::fs::read_to_string(cfg.data_dir.join("meta").join("producer_id"))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(1000);

        let broker = Arc::new(Self {
            cfg,
            topics: RwLock::new(BTreeMap::new()),
            shards,
            agent,
            store,
            coordinator,
            metrics,
            refs,
            next_producer_id: AtomicI64::new(pid_seed + 1000),
            round_robin: std::sync::atomic::AtomicU32::new(0),
            shares: crate::share::ShareGroups::default(),
            flows: crate::flow::Flows::default(),
            admin_lock: tokio::sync::Mutex::new(()),
            started: Instant::now(),
        });
        broker.persist_producer_id();

        let persisted: Vec<PersistedTopic> = std::fs::read(broker.topics_file())
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        for t in persisted {
            let meta = broker.open_topic(&t.name, t.partitions, t.config, t.created_ms).await?;
            broker.topics.write().insert(t.name, meta);
        }
        crate::flow::Flows::restore(&broker);
        tracing::info!(
            topics = broker.topics.read().len(),
            shards = broker.shards.len(),
            object_store = broker.store.url(),
            "broker started"
        );

        // Garbage-collect diskless objects no partition references (e.g. crash between PUT and commit).
        let weak = Arc::downgrade(&broker);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(600));
            loop {
                t.tick().await;
                let Some(b) = weak.upgrade() else { return };
                b.gc_diskless_orphans().await;
            }
        });
        Ok(broker)
    }

    fn topics_file(&self) -> PathBuf {
        self.cfg.data_dir.join("meta").join("topics.json")
    }

    fn persist_topics(&self) -> std::io::Result<()> {
        let list: Vec<PersistedTopic> = self
            .topics
            .read()
            .values()
            .map(|t| PersistedTopic {
                name: t.name.to_string(),
                partitions: t.partitions,
                config: t.raw_config.clone(),
                created_ms: t.created_ms,
            })
            .collect();
        let tmp = self.topics_file().with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&list)?)?;
        std::fs::rename(tmp, self.topics_file())
    }

    fn persist_producer_id(&self) {
        let v = self.next_producer_id.load(Relaxed);
        let _ = std::fs::write(self.cfg.data_dir.join("meta").join("producer_id"), v.to_string());
    }

    fn partition_dir(&self, topic: &str, p: i32) -> PathBuf {
        self.cfg.data_dir.join("topics").join(topic).join(p.to_string())
    }

    fn shard(&self, topic: &str, p: i32) -> &ShardHandle {
        &self.shards[shard_for(topic, p, self.shards.len())]
    }

    async fn open_partition(&self, name: &Arc<str>, p: i32, cfg: &Arc<TopicConfig>) -> Result<watch::Receiver<i64>, BrokerError> {
        let dir = self.partition_dir(name, p);
        let key: TpKey = (name.clone(), p);
        let cfg = cfg.clone();
        Ok(self.shard(name, p).call(|reply| ShardCmd::Open { key, dir, cfg, reply }).await?)
    }

    async fn open_topic(
        &self,
        name: &str,
        partitions: i32,
        raw: BTreeMap<String, String>,
        created_ms: i64,
    ) -> Result<Arc<TopicMeta>, BrokerError> {
        let config = Arc::new(TopicConfig::from_map(&raw).map_err(|e| BrokerError::InvalidConfig(e.to_string()))?);
        let name: Arc<str> = Arc::from(name);
        let mut hw = Vec::with_capacity(partitions as usize);
        for p in 0..partitions {
            hw.push(self.open_partition(&name, p, &config).await?);
        }
        Ok(Arc::new(TopicMeta { name, partitions, raw_config: raw, config, created_ms, hw }))
    }

    // ------------------------------------------------------------------ topics ----------------

    pub fn topic(&self, name: &str) -> Option<Arc<TopicMeta>> {
        self.topics.read().get(name).cloned()
    }

    pub fn topics(&self) -> Vec<Arc<TopicMeta>> {
        self.topics.read().values().cloned().collect()
    }

    pub fn partitions_of(&self, name: &str) -> i32 {
        self.topic(name).map(|t| t.partitions).unwrap_or(0)
    }

    pub async fn create_topic(
        &self,
        name: &str,
        partitions: i32,
        mut config: BTreeMap<String, String>,
    ) -> Result<Arc<TopicMeta>, BrokerError> {
        if !valid_topic_name(name) {
            return Err(BrokerError::InvalidTopic(name.to_string()));
        }
        let partitions = if partitions <= 0 { self.cfg.default_partitions as i32 } else { partitions };
        if partitions > 10_000 {
            return Err(BrokerError::InvalidPartitions(format!("{partitions} exceeds the 10000 limit")));
        }
        config
            .entry("bigpipe.storage.mode".into())
            .or_insert_with(|| self.cfg.default_storage_mode.clone());
        TopicConfig::from_map(&config).map_err(|e| BrokerError::InvalidConfig(e.to_string()))?;
        let _g = self.admin_lock.lock().await;
        if self.topics.read().contains_key(name) {
            return Err(BrokerError::TopicExists(name.to_string()));
        }
        let meta = self.open_topic(name, partitions, config, now_ms()).await?;
        self.topics.write().insert(name.to_string(), meta.clone());
        self.persist_topics().map_err(StorageError::from)?;
        tracing::info!(topic = name, partitions, mode = meta.config.mode.as_str(), "topic created");
        Ok(meta)
    }

    /// Returns the topic, creating it when auto-creation is enabled.
    pub async fn ensure_topic(&self, name: &str) -> Result<Arc<TopicMeta>, BrokerError> {
        if let Some(t) = self.topic(name) {
            return Ok(t);
        }
        if !self.cfg.auto_create_topics {
            return Err(BrokerError::UnknownTopic(name.to_string()));
        }
        match self.create_topic(name, 0, BTreeMap::new()).await {
            Ok(t) => Ok(t),
            Err(BrokerError::TopicExists(_)) => self.topic(name).ok_or_else(|| BrokerError::UnknownTopic(name.into())),
            Err(e) => Err(e),
        }
    }

    pub async fn delete_topic(&self, name: &str) -> Result<(), BrokerError> {
        let _g = self.admin_lock.lock().await;
        let meta = self.topics.write().remove(name).ok_or_else(|| BrokerError::UnknownTopic(name.to_string()))?;
        self.persist_topics().map_err(StorageError::from)?;
        for p in 0..meta.partitions {
            let key: TpKey = (meta.name.clone(), p);
            self.shard(name, p).call(|reply| ShardCmd::Delete { key, reply }).await?;
        }
        let _ = std::fs::remove_dir_all(self.cfg.data_dir.join("topics").join(name));
        tracing::info!(topic = name, "topic deleted");
        Ok(())
    }

    /// Applies config changes (`None` removes a key). Changing `bigpipe.storage.mode`
    /// migrates the topic online: new data goes to the new mode, existing offsets stay put.
    pub async fn alter_topic_config(
        &self,
        name: &str,
        changes: BTreeMap<String, Option<String>>,
    ) -> Result<Arc<TopicMeta>, BrokerError> {
        let _g = self.admin_lock.lock().await;
        let old = self.topic(name).ok_or_else(|| BrokerError::UnknownTopic(name.to_string()))?;
        let mut raw = old.raw_config.clone();
        for (k, v) in changes {
            match v {
                Some(v) => raw.insert(k, v),
                None => raw.remove(&k),
            };
        }
        let config = Arc::new(TopicConfig::from_map(&raw).map_err(|e| BrokerError::InvalidConfig(e.to_string()))?);
        for p in 0..old.partitions {
            self.shard(name, p).send(ShardCmd::SetConfig { key: (old.name.clone(), p), cfg: config.clone() });
        }
        let meta = Arc::new(TopicMeta {
            name: old.name.clone(),
            partitions: old.partitions,
            raw_config: raw,
            config: config.clone(),
            created_ms: old.created_ms,
            hw: old.hw.clone(),
        });
        self.topics.write().insert(name.to_string(), meta.clone());
        self.persist_topics().map_err(StorageError::from)?;
        if old.config.mode != config.mode {
            tracing::info!(topic = name, from = old.config.mode.as_str(), to = config.mode.as_str(), "storage mode migrated");
        }
        Ok(meta)
    }

    pub async fn add_partitions(&self, name: &str, count: i32) -> Result<Arc<TopicMeta>, BrokerError> {
        let _g = self.admin_lock.lock().await;
        let old = self.topic(name).ok_or_else(|| BrokerError::UnknownTopic(name.to_string()))?;
        if count <= old.partitions {
            return Err(BrokerError::InvalidPartitions(format!(
                "topic has {} partitions; the new count must be larger",
                old.partitions
            )));
        }
        let mut hw = old.hw.clone();
        for p in old.partitions..count {
            hw.push(self.open_partition(&old.name, p, &old.config).await?);
        }
        let meta = Arc::new(TopicMeta {
            name: old.name.clone(),
            partitions: count,
            raw_config: old.raw_config.clone(),
            config: old.config.clone(),
            created_ms: old.created_ms,
            hw,
        });
        self.topics.write().insert(name.to_string(), meta.clone());
        self.persist_topics().map_err(StorageError::from)?;
        Ok(meta)
    }

    // ------------------------------------------------------------------ data path -------------

    /// Appends a produce payload (one or more record batches) to a partition.
    pub async fn produce(&self, topic: &TopicMeta, partition: i32, raw: BytesMut) -> Result<AppendResult, BrokerError> {
        if partition < 0 || partition >= topic.partitions {
            return Err(BrokerError::UnknownTopic(format!("{}-{partition}", topic.name)));
        }
        if raw.len() > topic.config.max_message_bytes {
            return Err(BrokerError::TooLarge(topic.config.max_message_bytes));
        }
        let started = Instant::now();
        let info = records::validate_batches(&raw).map_err(|e| BrokerError::InvalidRecord(e.to_string()))?;
        if topic.config.schema_validation == "strict" {
            validate_json_values(&raw)?;
        }
        let key: TpKey = (topic.name.clone(), partition);
        let len = raw.len() as u64;
        let r = if topic.config.mode == StorageMode::Diskless {
            self.agent.append(key, raw, info).await?
        } else {
            self.shard(&topic.name, partition).call(|reply| ShardCmd::Append { key, raw, info, reply }).await?
        };
        self.metrics.produce_latency.observe(started.elapsed());
        self.metrics.produce_records_total.fetch_add(info.record_count as u64, Relaxed);
        self.metrics.produce_bytes_total.fetch_add(len, Relaxed);
        Ok(r)
    }

    /// Chooses a partition: explicit, else murmur2(key) like Kafka, else round-robin.
    pub fn choose_partition(&self, topic: &TopicMeta, explicit: Option<i32>, key: Option<&[u8]>) -> i32 {
        match (explicit, key) {
            (Some(p), _) => p,
            (None, Some(k)) => records::partition_for_key(k, topic.partitions),
            (None, None) => (self.round_robin.fetch_add(1, Relaxed) % topic.partitions.max(1) as u32) as i32,
        }
    }

    /// Encodes and appends records, grouping them into one batch per partition.
    /// Returns (partition, offset) for every record, in input order.
    pub async fn produce_records(
        &self,
        topic: &TopicMeta,
        items: Vec<(Option<i32>, records::NewRecord)>,
        compression: records::Compression,
    ) -> Result<Vec<(i32, i64)>, BrokerError> {
        let now = now_ms();
        let mut builders: BTreeMap<i32, (records::BatchBuilder, Vec<usize>)> = BTreeMap::new();
        let n = items.len();
        for (i, (explicit, rec)) in items.into_iter().enumerate() {
            let p = self.choose_partition(topic, explicit, rec.key.as_deref());
            if p < 0 || p >= topic.partitions {
                return Err(BrokerError::UnknownTopic(format!("{}-{p}", topic.name)));
            }
            let e = builders.entry(p).or_insert_with(|| (records::BatchBuilder::new(compression), Vec::new()));
            e.0.push(&rec, now);
            e.1.push(i);
        }
        let mut out = vec![(0, 0); n];
        let futs = builders.into_iter().map(|(p, (b, idx))| async move {
            let raw = b.build().map_err(|e| BrokerError::InvalidRecord(e.to_string()))?;
            let r = self.produce(topic, p, raw).await?;
            Ok::<_, BrokerError>((p, r.base_offset, idx))
        });
        for r in futures::future::join_all(futs).await {
            let (p, base, idx) = r?;
            for (k, i) in idx.into_iter().enumerate() {
                out[i] = (p, base + k as i64);
            }
        }
        Ok(out)
    }

    /// Reads up to ~`max_bytes` of whole batches starting at `offset`.
    pub async fn read(&self, topic: &TopicMeta, partition: i32, offset: i64, max_bytes: usize) -> Result<FetchData, BrokerError> {
        if partition < 0 || partition >= topic.partitions {
            return Err(BrokerError::UnknownTopic(format!("{}-{partition}", topic.name)));
        }
        let started = Instant::now();
        let key: TpKey = (topic.name.clone(), partition);
        let plan = self.shard(&topic.name, partition).call(|reply| ShardCmd::Plan { key, offset, max_bytes, reply }).await?;
        let chunks = bp_storage::read::execute(&plan, &self.store, max_bytes).await?;
        let data = FetchData { chunks, high_watermark: plan.high_watermark, log_start: plan.log_start };
        self.metrics.fetch_latency.observe(started.elapsed());
        self.metrics.fetch_bytes_total.fetch_add(data.bytes() as u64, Relaxed);
        Ok(data)
    }

    /// Reads and decodes records (HTTP gateway / SDKs). Returns records and the next offset.
    pub async fn read_records(
        &self,
        topic: &TopicMeta,
        partition: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<(Vec<Record>, i64, i64), BrokerError> {
        let data = self.read(topic, partition, offset, max_bytes).await?;
        let mut out = Vec::new();
        for c in &data.chunks {
            out.extend(records::decode_records(c, offset).map_err(|e| BrokerError::InvalidRecord(e.to_string()))?);
        }
        let next = out.last().map(|r| r.offset + 1).unwrap_or(offset);
        // Batches of control records (transaction markers) advance the position too.
        let next = data
            .chunks
            .iter()
            .flat_map(|c| records::BatchIter::new(c).map(|(_, h)| h.last_offset() + 1).collect::<Vec<_>>())
            .max()
            .map_or(next, |n| n.max(next));
        self.metrics.fetch_records_total.fetch_add(out.len() as u64, Relaxed);
        Ok((out, next, data.high_watermark))
    }

    pub fn high_watermark(&self, topic: &TopicMeta, partition: i32) -> i64 {
        topic.hw.get(partition as usize).map(|r| *r.borrow()).unwrap_or(0)
    }

    /// Waits until any of the partitions' high watermark changes, or the deadline passes.
    pub async fn wait_for_data(receivers: &mut [watch::Receiver<i64>], deadline: Instant) {
        if receivers.is_empty() {
            tokio::time::sleep_until(deadline.into()).await;
            return;
        }
        let waits = receivers.iter_mut().map(|r| Box::pin(r.changed()));
        let _ = tokio::time::timeout_at(deadline.into(), futures::future::select_all(waits)).await;
    }

    /// Kafka ListOffsets: -1 latest, -2 earliest, otherwise first offset with timestamp >= ts.
    pub async fn list_offset(&self, topic: &TopicMeta, partition: i32, ts: i64) -> Result<(i64, i64), BrokerError> {
        let info = self.partition_info(topic, partition).await?;
        Ok(match ts {
            -1 => (info.high_watermark, -1),
            -2 => (info.log_start_offset, -1),
            ts => {
                let key: TpKey = (topic.name.clone(), partition);
                match self.shard(&topic.name, partition).call(|reply| ShardCmd::OffsetForTime { key, ts, reply }).await? {
                    Some((o, t)) => (o, t),
                    None => (info.high_watermark, -1),
                }
            }
        })
    }

    pub async fn partition_info(&self, topic: &TopicMeta, partition: i32) -> Result<PartitionInfo, BrokerError> {
        if partition < 0 || partition >= topic.partitions {
            return Err(BrokerError::UnknownTopic(format!("{}-{partition}", topic.name)));
        }
        let key: TpKey = (topic.name.clone(), partition);
        Ok(self.shard(&topic.name, partition).call(|reply| ShardCmd::Info { key, reply }).await?)
    }

    pub fn allocate_producer_id(&self) -> i64 {
        let id = self.next_producer_id.fetch_add(1, Relaxed);
        if id % 100 == 0 {
            self.persist_producer_id();
        }
        id
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn diskless_files_referenced(&self) -> usize {
        self.refs.len()
    }

    async fn gc_diskless_orphans(&self) {
        let Ok(objs) = self.store.list("diskless").await else { return };
        let cutoff = now_ms() - 10 * 60 * 1000;
        for o in objs {
            if o.last_modified_ms < cutoff && !self.refs.is_referenced(&o.key) {
                tracing::info!(key = %o.key, "deleting orphaned diskless object");
                let _ = self.store.delete(&o.key).await;
            }
        }
    }

    pub async fn flush_all(&self) {
        for s in &self.shards {
            let (tx, rx) = tokio::sync::oneshot::channel();
            s.send(ShardCmd::Flush { reply: tx });
            let _ = rx.await;
        }
    }

    pub async fn shutdown(&self) {
        self.persist_producer_id();
        for s in &self.shards {
            let (tx, rx) = tokio::sync::oneshot::channel();
            s.send(ShardCmd::Shutdown { reply: tx });
            let _ = rx.await;
        }
    }
}

/// `bigpipe.schema.validation=strict`: every record value must be valid JSON.
fn validate_json_values(raw: &[u8]) -> Result<(), BrokerError> {
    for (pos, _) in records::BatchIter::new(raw) {
        for r in records::decode_batch(&raw[pos..]).map_err(|e| BrokerError::InvalidRecord(e.to_string()))? {
            if let Some(v) = &r.value {
                serde_json::from_slice::<serde::de::IgnoredAny>(v)
                    .map_err(|e| BrokerError::InvalidRecord(format!("value is not valid JSON: {e}")))?;
            }
        }
    }
    Ok(())
}
