//! Thread-per-core shards.
//!
//! Each shard is one OS thread running a single-threaded tokio runtime and exclusively owns a
//! set of partitions. Everything else talks to a shard through its command channel, so the
//! write path has no locks. Commands are drained in groups; fsync topics are synced once per
//! group before replies go out (group commit).
//!
//! On Linux production builds the design calls for glommio + io_uring; this portable runtime
//! uses the same ownership model on tokio so it also runs on Windows and macOS.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bp_protocol::records::BatchSetInfo;
use bp_storage::diskless::FileRefs;
use bp_storage::partition::{Maintenance, PartitionInfo};
use bp_storage::{AppendResult, ObjectStorage, Partition, ReadPlan, StorageError, TopicConfig};
use bytes::{Bytes, BytesMut};
use tokio::sync::{mpsc, oneshot, watch};

use crate::metrics::Metrics;

pub type TpKey = (Arc<str>, i32);

type Reply<T> = oneshot::Sender<Result<T, StorageError>>;

pub struct DisklessCommit {
    pub key: TpKey,
    pub file: Arc<str>,
    pub pos: u64,
    pub raw: Bytes,
    pub info: BatchSetInfo,
    pub reply: Reply<AppendResult>,
}

pub enum ShardCmd {
    Open { key: TpKey, dir: PathBuf, cfg: Arc<TopicConfig>, reply: Reply<watch::Receiver<i64>> },
    Delete { key: TpKey, reply: Reply<()> },
    SetConfig { key: TpKey, cfg: Arc<TopicConfig> },
    Append { key: TpKey, raw: BytesMut, info: BatchSetInfo, reply: Reply<AppendResult> },
    CommitDiskless(Vec<DisklessCommit>),
    Plan { key: TpKey, offset: i64, max_bytes: usize, reply: Reply<ReadPlan> },
    OffsetForTime { key: TpKey, ts: i64, reply: Reply<Option<(i64, i64)>> },
    Info { key: TpKey, reply: Reply<PartitionInfo> },
    Uploaded { key: TpKey, base: i64, object_key: String },
    Flush { reply: oneshot::Sender<()> },
    Shutdown { reply: oneshot::Sender<()> },
}

#[derive(Clone)]
pub struct ShardHandle {
    pub id: usize,
    tx: mpsc::UnboundedSender<ShardCmd>,
}

impl ShardHandle {
    pub fn send(&self, cmd: ShardCmd) {
        // A closed shard only happens during shutdown; callers then observe a dropped reply.
        let _ = self.tx.send(cmd);
    }

    pub async fn call<T>(&self, make: impl FnOnce(Reply<T>) -> ShardCmd) -> Result<T, StorageError> {
        let (tx, rx) = oneshot::channel();
        self.send(make(tx));
        rx.await.map_err(|_| StorageError::Io(std::io::Error::other("shard stopped")))?
    }
}

/// Stable shard assignment for a partition (FNV-1a over topic name + partition).
pub fn shard_for(topic: &str, partition: i32, shards: usize) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in topic.bytes().chain(partition.to_be_bytes()) {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    (h % shards as u64) as usize
}

struct Owned {
    log: Partition,
    hw: watch::Sender<i64>,
}

struct Shard {
    id: usize,
    self_tx: mpsc::UnboundedSender<ShardCmd>,
    parts: HashMap<TpKey, Owned>,
    store: Arc<ObjectStorage>,
    refs: Arc<FileRefs>,
    metrics: Arc<Metrics>,
    uploading: std::collections::HashSet<(TpKey, i64)>,
}

pub fn spawn_shards(n: usize, store: Arc<ObjectStorage>, refs: Arc<FileRefs>, metrics: Arc<Metrics>) -> Vec<ShardHandle> {
    (0..n)
        .map(|id| {
            let (tx, rx) = mpsc::unbounded_channel();
            let self_tx = tx.clone();
            let store = store.clone();
            let refs = refs.clone();
            let metrics = metrics.clone();
            std::thread::Builder::new()
                .name(format!("bp-shard-{id}"))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("shard runtime");
                    let local = tokio::task::LocalSet::new();
                    let shard = Shard {
                        id,
                        self_tx,
                        parts: HashMap::new(),
                        store,
                        refs,
                        metrics,
                        uploading: Default::default(),
                    };
                    local.block_on(&rt, shard.run(rx));
                })
                .expect("spawn shard thread");
            ShardHandle { id, tx }
        })
        .collect()
}

impl Shard {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<ShardCmd>) {
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut group: Vec<ShardCmd> = Vec::with_capacity(256);
        loop {
            tokio::select! {
                n = rx.recv_many(&mut group, 256) => {
                    if n == 0 {
                        return;
                    }
                    if self.process(&mut group) {
                        return;
                    }
                }
                _ = tick.tick() => self.maintenance(),
            }
        }
    }

    /// Processes one drained group of commands. Returns true on shutdown.
    fn process(&mut self, group: &mut Vec<ShardCmd>) -> bool {
        let mut pending: Vec<(Reply<AppendResult>, Result<AppendResult, StorageError>, TpKey)> = Vec::new();
        let mut shutdown = None;
        for cmd in group.drain(..) {
            match cmd {
                ShardCmd::Append { key, raw, info, reply } => {
                    let r = match self.parts.get_mut(&key) {
                        Some(o) => {
                            let r = o.log.append_local(raw, &info);
                            if r.is_ok() {
                                o.hw.send_replace(o.log.high_watermark());
                            }
                            r
                        }
                        None => Err(unknown(&key)),
                    };
                    pending.push((reply, r, key));
                }
                ShardCmd::CommitDiskless(items) => {
                    for c in items {
                        let r = match self.parts.get_mut(&c.key) {
                            Some(o) => {
                                let r = o.log.commit_diskless(c.file.clone(), c.pos, c.raw, &c.info);
                                if matches!(&r, Ok(a) if !a.duplicate) {
                                    o.hw.send_replace(o.log.high_watermark());
                                } else {
                                    // Not referenced after all (duplicate or rejected).
                                    self.release_file(&c.file);
                                }
                                r
                            }
                            None => {
                                self.release_file(&c.file);
                                Err(unknown(&c.key))
                            }
                        };
                        let _ = c.reply.send(r);
                    }
                }
                ShardCmd::Open { key, dir, cfg, reply } => {
                    let r = match self.parts.get(&key) {
                        Some(o) => Ok(o.hw.subscribe()),
                        None => Partition::open(dir, key.0.clone(), key.1, cfg).map(|log| {
                            for f in log.diskless_files() {
                                self.refs.acquire(f, 1);
                            }
                            let (hw, rx) = watch::channel(log.high_watermark());
                            self.parts.insert(key, Owned { log, hw });
                            rx
                        }),
                    };
                    let _ = reply.send(r);
                }
                ShardCmd::Delete { key, reply } => {
                    let r = match self.parts.remove(&key) {
                        Some(o) => o.log.destroy().map(|m| self.apply_maintenance(&key, m)),
                        None => Ok(()),
                    };
                    let _ = reply.send(r);
                }
                ShardCmd::SetConfig { key, cfg } => {
                    if let Some(o) = self.parts.get_mut(&key) {
                        o.log.set_config(cfg);
                    }
                }
                ShardCmd::Plan { key, offset, max_bytes, reply } => {
                    let r = match self.parts.get(&key) {
                        Some(o) => o.log.read_plan(offset, max_bytes),
                        None => Err(unknown(&key)),
                    };
                    let _ = reply.send(r);
                }
                ShardCmd::OffsetForTime { key, ts, reply } => {
                    let r = match self.parts.get(&key) {
                        Some(o) => o.log.offset_for_timestamp(ts),
                        None => Err(unknown(&key)),
                    };
                    let _ = reply.send(r);
                }
                ShardCmd::Info { key, reply } => {
                    let r = match self.parts.get(&key) {
                        Some(o) => Ok(o.log.info()),
                        None => Err(unknown(&key)),
                    };
                    let _ = reply.send(r);
                }
                ShardCmd::Uploaded { key, base, object_key } => {
                    self.uploading.remove(&(key.clone(), base));
                    if object_key.is_empty() {
                        continue;
                    }
                    if let Some(o) = self.parts.get_mut(&key) {
                        if let Err(e) = o.log.mark_uploaded(base, object_key) {
                            tracing::warn!(shard = self.id, topic = %key.0, partition = key.1, error = %e, "mark uploaded failed");
                        }
                    }
                }
                ShardCmd::Flush { reply } => {
                    for o in self.parts.values_mut() {
                        let _ = o.log.flush();
                    }
                    let _ = reply.send(());
                }
                ShardCmd::Shutdown { reply } => {
                    for o in self.parts.values_mut() {
                        let _ = o.log.flush();
                    }
                    shutdown = Some(reply);
                }
            }
        }
        // Group commit: one fsync per partition for everything appended in this group.
        if !pending.is_empty() {
            let mut synced = std::collections::HashSet::new();
            for (_, r, key) in &mut pending {
                if r.is_err() || synced.contains(key) {
                    continue;
                }
                if let Some(o) = self.parts.get_mut(key) {
                    if o.log.needs_sync() {
                        if let Err(e) = o.log.flush() {
                            *r = Err(e);
                        }
                    }
                }
                synced.insert(key.clone());
            }
            for (reply, r, _) in pending {
                let _ = reply.send(r);
            }
        }
        if let Some(reply) = shutdown {
            let _ = reply.send(());
            return true;
        }
        false
    }

    fn maintenance(&mut self) {
        let now = bp_storage::partition::now_ms();
        let keys: Vec<TpKey> = self.parts.keys().cloned().collect();
        for key in keys {
            let Some(o) = self.parts.get_mut(&key) else { continue };
            match o.log.maintenance(now) {
                Ok(m) => self.apply_maintenance(&key, m),
                Err(e) => tracing::warn!(shard = self.id, topic = %key.0, partition = key.1, error = %e, "maintenance failed"),
            }
        }
    }

    fn apply_maintenance(&mut self, key: &TpKey, m: Maintenance) {
        for (base, path, object_key) in m.uploads {
            if !self.uploading.insert((key.clone(), base)) {
                continue;
            }
            let store = self.store.clone();
            let tx = self.self_tx.clone();
            let key = key.clone();
            let metrics = self.metrics.clone();
            tokio::task::spawn_local(async move {
                let data = match tokio::fs::read(&path).await {
                    Ok(d) => Bytes::from(d),
                    Err(e) => {
                        tracing::warn!(path = %path.display(), error = %e, "tiered read failed");
                        return;
                    }
                };
                match store.put(&object_key, data).await {
                    Ok(()) => {
                        metrics.tiered_uploads_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let _ = tx.send(ShardCmd::Uploaded { key, base, object_key });
                    }
                    Err(e) => {
                        tracing::warn!(key = %object_key, error = %e, "tiered upload failed; will retry");
                        // Empty key = failed: clears the in-flight marker so the next tick retries.
                        let _ = tx.send(ShardCmd::Uploaded { key, base, object_key: String::new() });
                    }
                }
            });
        }
        for k in m.delete_objects {
            let store = self.store.clone();
            tokio::task::spawn_local(async move {
                if let Err(e) = store.delete(&k).await {
                    tracing::warn!(key = %k, error = %e, "object delete failed");
                }
            });
        }
        for f in m.released_files {
            self.release_file(&f);
        }
    }

    fn release_file(&self, f: &Arc<str>) {
        if self.refs.release(f) {
            let store = self.store.clone();
            let k = f.to_string();
            tokio::task::spawn_local(async move {
                let _ = store.delete(&k).await;
            });
        }
    }
}

fn unknown(key: &TpKey) -> StorageError {
    StorageError::Corrupt(format!("partition {}-{} is not hosted on this node", key.0, key.1))
}
