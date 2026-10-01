//! Diskless agent (WarpStream-style write path).
//!
//! Produce requests for `diskless` topics are buffered here. Batches from many partitions and
//! topics are packed into one object, which is PUT to object storage when either the linger
//! window closes or the buffer reaches `diskless_max_file_bytes`. Only after the PUT succeeds
//! are offsets assigned (by each partition's owning shard) and producers acknowledged.
//! Uploads run concurrently, but commits happen strictly in file order so idempotent
//! producers never observe reordering.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use bp_protocol::records::BatchSetInfo;
use bp_storage::diskless::{FileBuilder, FileRefs, new_file_key};
use bp_storage::{AppendResult, ObjectStorage, StorageError};
use bytes::BytesMut;
use tokio::sync::{mpsc, oneshot};

use crate::metrics::Metrics;
use crate::shard::{DisklessCommit, ShardCmd, ShardHandle, TpKey, shard_for};

pub struct AgentRequest {
    pub key: TpKey,
    pub raw: BytesMut,
    pub info: BatchSetInfo,
    pub reply: oneshot::Sender<Result<AppendResult, StorageError>>,
}

#[derive(Clone)]
pub struct AgentHandle {
    tx: mpsc::UnboundedSender<AgentRequest>,
}

impl AgentHandle {
    pub async fn append(&self, key: TpKey, raw: BytesMut, info: BatchSetInfo) -> Result<AppendResult, StorageError> {
        let rx = self.enqueue(key, raw, info)?;
        rx.await.map_err(|_| StorageError::Io(std::io::Error::other("diskless agent dropped request")))?
    }

    /// Queues the append now (in call order) and returns the receiver for its result.
    pub fn enqueue(
        &self,
        key: TpKey,
        raw: BytesMut,
        info: BatchSetInfo,
    ) -> Result<oneshot::Receiver<Result<AppendResult, StorageError>>, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(AgentRequest { key, raw, info, reply })
            .map_err(|_| StorageError::Io(std::io::Error::other("diskless agent stopped")))?;
        Ok(rx)
    }
}

pub struct AgentConfig {
    pub node_id: i32,
    pub linger: Duration,
    pub max_file_bytes: usize,
}

pub fn spawn_agent(
    cfg: AgentConfig,
    shards: Vec<ShardHandle>,
    store: Arc<ObjectStorage>,
    refs: Arc<FileRefs>,
    metrics: Arc<Metrics>,
) -> AgentHandle {
    let (tx, mut rx) = mpsc::unbounded_channel::<AgentRequest>();
    tokio::spawn(async move {
        let mut buffer: Vec<AgentRequest> = Vec::new();
        let mut bytes = 0usize;
        let mut deadline: Option<Instant> = None;
        // Commit ordering: each flush waits for the previous flush's commit.
        let (first_tx, mut prev_done) = oneshot::channel::<()>();
        let _ = first_tx.send(());
        loop {
            let timeout = deadline.map(|d| d.saturating_duration_since(Instant::now()));
            let next = match timeout {
                Some(t) => tokio::time::timeout(t, rx.recv()).await.ok(),
                None => Some(rx.recv().await),
            };
            match next {
                Some(Some(req)) => {
                    bytes += req.raw.len();
                    buffer.push(req);
                    deadline.get_or_insert_with(|| Instant::now() + cfg.linger);
                    if bytes < cfg.max_file_bytes {
                        continue;
                    }
                }
                Some(None) if buffer.is_empty() => return,
                _ => {}
            }
            if buffer.is_empty() {
                deadline = None;
                continue;
            }
            let batch = std::mem::take(&mut buffer);
            bytes = 0;
            deadline = None;
            let (done_tx, done_rx) = oneshot::channel();
            let wait_prev = std::mem::replace(&mut prev_done, done_rx);
            tokio::spawn(flush(
                batch,
                cfg.node_id,
                shards.clone(),
                store.clone(),
                refs.clone(),
                metrics.clone(),
                wait_prev,
                done_tx,
            ));
        }
    });
    AgentHandle { tx }
}

#[allow(clippy::too_many_arguments)]
async fn flush(
    batch: Vec<AgentRequest>,
    node_id: i32,
    shards: Vec<ShardHandle>,
    store: Arc<ObjectStorage>,
    refs: Arc<FileRefs>,
    metrics: Arc<Metrics>,
    wait_prev: oneshot::Receiver<()>,
    done: oneshot::Sender<()>,
) {
    let mut fb = FileBuilder::new();
    let mut placed = Vec::with_capacity(batch.len());
    for req in &batch {
        placed.push(fb.push(&req.raw));
    }
    let file = fb.finish();
    let key: Arc<str> = Arc::from(new_file_key(node_id));
    let started = Instant::now();
    let put = store.put(&key, file.clone()).await;
    metrics.diskless_put_latency.observe(started.elapsed());
    let _ = wait_prev.await;
    match put {
        Ok(()) => {
            metrics.diskless_files_total.fetch_add(1, Relaxed);
            metrics.diskless_bytes_total.fetch_add(file.len() as u64, Relaxed);
            // Read-your-write: tailing consumers are served without a GET.
            store.cache_insert(&key, file.clone());
            refs.acquire(&key, batch.len());
            let mut per_shard: Vec<Vec<DisklessCommit>> = (0..shards.len()).map(|_| Vec::new()).collect();
            for (req, (pos, len)) in batch.into_iter().zip(placed) {
                let s = shard_for(&req.key.0, req.key.1, shards.len());
                per_shard[s].push(DisklessCommit {
                    key: req.key,
                    file: key.clone(),
                    pos,
                    raw: file.slice(pos as usize..pos as usize + len as usize),
                    info: req.info,
                    reply: req.reply,
                });
            }
            for (s, items) in per_shard.into_iter().enumerate() {
                if !items.is_empty() {
                    shards[s].send(ShardCmd::CommitDiskless(items));
                }
            }
        }
        Err(e) => {
            tracing::error!(key = %key, error = %e, "diskless PUT failed");
            let msg = e.to_string();
            for req in batch {
                let _ = req.reply.send(Err(StorageError::Io(std::io::Error::other(msg.clone()))));
            }
        }
    }
    let _ = done.send(());
}
