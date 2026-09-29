//! One partition's log. Owned by exactly one shard (never shared, never locked).
//!
//! A partition is a sequence of offset *extents*. Each extent lives in one place:
//! - a local segment file (`local` mode, and the hot part of `tiered`),
//! - a segment uploaded to object storage (`tiered` after local retention),
//! - a slice of a shared diskless object written by the agent (`diskless`).
//!
//! Switching a topic's storage mode only changes where *new* extents go, so online migration
//! never rewrites offsets.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bp_protocol::records::{self, BatchHeader, BatchIter, BatchSetInfo, BATCH_HEADER_LEN};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::config::{Durability, StorageMode, TopicConfig};
use crate::read::{Chunk, ReadPlan};
use crate::StorageError;

const INDEX_INTERVAL_BYTES: u64 = 4096;
const INDEX_ENTRY_LEN: usize = 24;

#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    offset: i64,
    pos: u64,
    /// Max timestamp of all batches up to and including this one (monotonic).
    ts: i64,
}

#[derive(Debug)]
struct Segment {
    base: i64,
    /// Offset after the last batch (== base when empty).
    next: i64,
    size: u64,
    max_ts: i64,
    index: Vec<IndexEntry>,
    bytes_since_index: u64,
    local: bool,
    object_key: Option<String>,
    sealed: bool,
    created_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct TierSidecar {
    object_key: String,
    size: u64,
    next: i64,
    max_ts: i64,
}

/// A slice of a diskless object belonging to this partition.
#[derive(Debug, Clone)]
pub struct Extent {
    pub base: i64,
    pub next: i64,
    pub file: Arc<str>,
    pub pos: u64,
    pub len: u32,
    pub max_ts: i64,
}

#[derive(Debug, Clone, Copy)]
struct ProducerState {
    epoch: i16,
    first_seq: i32,
    last_seq: i32,
    base_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerCheck {
    Append,
    /// Retried batch already written; answer with the original base offset.
    Duplicate(i64),
    /// Kafka error code.
    Reject(i16),
}

#[derive(Debug, Clone, Copy)]
pub struct AppendResult {
    pub base_offset: i64,
    pub last_offset: i64,
    pub log_start: i64,
    pub duplicate: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PartitionInfo {
    pub partition: i32,
    pub log_start_offset: i64,
    pub high_watermark: i64,
    pub local_bytes: u64,
    pub remote_bytes: u64,
    pub diskless_bytes: u64,
    pub segments: usize,
    pub remote_segments: usize,
    pub diskless_extents: usize,
    pub cache_bytes: usize,
    pub records_in: u64,
    pub bytes_in: u64,
}

/// Work the shard must do asynchronously after [`Partition::maintenance`].
#[derive(Debug, Default)]
pub struct Maintenance {
    /// (segment base, local file path, object key)
    pub uploads: Vec<(i64, PathBuf, String)>,
    /// Object keys whose data is fully expired (tiered segments).
    pub delete_objects: Vec<String>,
    /// Diskless files that lost one reference each.
    pub released_files: Vec<Arc<str>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PartitionState {
    /// Lower bound for the next offset (survives retention deleting every extent).
    next_offset_floor: i64,
}

pub struct Partition {
    pub topic: Arc<str>,
    pub id: i32,
    dir: PathBuf,
    cfg: Arc<TopicConfig>,
    segments: BTreeMap<i64, Segment>,
    writer: Option<File>,
    diskless: BTreeMap<i64, Extent>,
    diskless_log: Option<File>,
    diskless_logged: usize,
    next_offset: i64,
    state: PartitionState,
    cache: VecDeque<(i64, i64, Bytes)>,
    cache_bytes: usize,
    producers: HashMap<i64, ProducerState>,
    dirty: bool,
    last_flush_ms: i64,
    records_in: u64,
    bytes_in: u64,
}

/// One process-wide thread that fsyncs files handed over by shards (rolled segments and
/// periodic flushes), keeping disk stalls off the shard event loops.
fn background_sync(f: File) {
    use std::sync::OnceLock;
    use std::sync::mpsc::{Sender, channel};
    static FLUSHER: OnceLock<parking_lot::Mutex<Sender<File>>> = OnceLock::new();
    let tx = FLUSHER.get_or_init(|| {
        let (tx, rx) = channel::<File>();
        std::thread::Builder::new()
            .name("bp-flusher".into())
            .spawn(move || {
                for f in rx {
                    if let Err(e) = f.sync_data() {
                        tracing::warn!(error = %e, "background fsync failed");
                    }
                }
            })
            .expect("spawn flusher");
        parking_lot::Mutex::new(tx)
    });
    let _ = tx.lock().send(f);
}

fn seg_name(base: i64, ext: &str) -> String {
    format!("{base:020}.{ext}")
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

impl Partition {
    /// Opens (or creates) a partition directory and recovers its state.
    pub fn open(dir: PathBuf, topic: Arc<str>, id: i32, cfg: Arc<TopicConfig>) -> Result<Self, StorageError> {
        fs::create_dir_all(&dir)?;
        let state: PartitionState = fs::read(dir.join("partition.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();

        let mut bases: Vec<i64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if let Some(stem) = name.strip_suffix(".log").or_else(|| name.strip_suffix(".idx")) {
                if let Ok(b) = stem.parse::<i64>() {
                    if !bases.contains(&b) {
                        bases.push(b);
                    }
                }
            }
        }
        bases.sort_unstable();

        let mut segments = BTreeMap::new();
        let last_local = bases.iter().rev().find(|b| dir.join(seg_name(**b, "log")).exists()).copied();
        for &base in &bases {
            let log = dir.join(seg_name(base, "log"));
            let idx = dir.join(seg_name(base, "idx"));
            let tier: Option<TierSidecar> =
                fs::read(dir.join(seg_name(base, "tier"))).ok().and_then(|b| serde_json::from_slice(&b).ok());
            let local = log.exists();
            let is_active = Some(base) == last_local;
            let created_ms = fs::metadata(if local { &log } else { &idx })
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or_else(now_ms);
            let seg = if !is_active && idx.exists() {
                let mut s = read_index_file(&idx, base)?;
                s.local = local;
                s.object_key = tier.map(|t| t.object_key);
                s.sealed = true;
                s.created_ms = created_ms;
                s
            } else if local {
                let mut s = scan_segment(&log, base)?;
                s.object_key = tier.map(|t| t.object_key);
                s.sealed = !is_active;
                s.created_ms = created_ms;
                if s.sealed {
                    write_index_file(&idx, &s)?;
                }
                s
            } else {
                continue;
            };
            segments.insert(base, seg);
        }

        let (diskless, diskless_logged) = read_diskless_index(&dir.join("diskless.idx"))?;

        let mut next_offset = state.next_offset_floor;
        if let Some(s) = segments.values().next_back() {
            next_offset = next_offset.max(s.next);
        }
        if let Some(e) = diskless.values().next_back() {
            next_offset = next_offset.max(e.next);
        }

        let writer = match segments.values().next_back() {
            Some(s) if s.local && !s.sealed => {
                Some(OpenOptions::new().append(true).open(dir.join(seg_name(s.base, "log")))?)
            }
            _ => None,
        };

        Ok(Self {
            topic,
            id,
            dir,
            cfg,
            segments,
            writer,
            diskless,
            diskless_log: None,
            diskless_logged,
            next_offset,
            state,
            cache: VecDeque::new(),
            cache_bytes: 0,
            producers: HashMap::new(),
            dirty: false,
            last_flush_ms: now_ms(),
            records_in: 0,
            bytes_in: 0,
        })
    }

    pub fn config(&self) -> &Arc<TopicConfig> {
        &self.cfg
    }

    pub fn set_config(&mut self, cfg: Arc<TopicConfig>) {
        self.cfg = cfg;
    }

    pub fn mode(&self) -> StorageMode {
        self.cfg.mode
    }

    pub fn high_watermark(&self) -> i64 {
        self.next_offset
    }

    pub fn log_start(&self) -> i64 {
        let seg = self.segments.values().find(|s| s.next > s.base).map(|s| s.base);
        let ext = self.diskless.values().next().map(|e| e.base);
        match (seg, ext) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) | (None, Some(a)) => a,
            (None, None) => self.next_offset,
        }
    }

    pub fn info(&self) -> PartitionInfo {
        let mut local_bytes = 0;
        let mut remote_bytes = 0;
        let mut remote_segments = 0;
        for s in self.segments.values() {
            if s.local {
                local_bytes += s.size;
            }
            if s.object_key.is_some() {
                remote_bytes += s.size;
                remote_segments += 1;
            }
        }
        PartitionInfo {
            partition: self.id,
            log_start_offset: self.log_start(),
            high_watermark: self.next_offset,
            local_bytes,
            remote_bytes,
            diskless_bytes: self.diskless.values().map(|e| e.len as u64).sum(),
            segments: self.segments.len(),
            remote_segments,
            diskless_extents: self.diskless.len(),
            cache_bytes: self.cache_bytes,
            records_in: self.records_in,
            bytes_in: self.bytes_in,
        }
    }

    // -----------------------------------------------------------------------------------------
    // Idempotent producer bookkeeping
    // -----------------------------------------------------------------------------------------

    pub fn check_producer(&self, info: &BatchSetInfo) -> ProducerCheck {
        if info.producer_id < 0 || info.base_sequence < 0 {
            return ProducerCheck::Append;
        }
        match self.producers.get(&info.producer_id) {
            None => ProducerCheck::Append,
            Some(p) if info.producer_epoch < p.epoch => ProducerCheck::Reject(bp_protocol::messages::error::INVALID_PRODUCER_EPOCH),
            Some(p) if info.producer_epoch > p.epoch => ProducerCheck::Append,
            Some(p) => {
                if info.base_sequence == p.first_seq && info.last_sequence == p.last_seq {
                    ProducerCheck::Duplicate(p.base_offset)
                } else if info.base_sequence == p.last_seq.wrapping_add(1) {
                    ProducerCheck::Append
                } else if info.base_sequence <= p.last_seq {
                    ProducerCheck::Reject(bp_protocol::messages::error::DUPLICATE_SEQUENCE_NUMBER)
                } else {
                    ProducerCheck::Reject(bp_protocol::messages::error::OUT_OF_ORDER_SEQUENCE_NUMBER)
                }
            }
        }
    }

    fn record_producer(&mut self, info: &BatchSetInfo, base_offset: i64) {
        if info.producer_id >= 0 && info.base_sequence >= 0 {
            self.producers.insert(
                info.producer_id,
                ProducerState {
                    epoch: info.producer_epoch,
                    first_seq: info.base_sequence,
                    last_seq: info.last_sequence,
                    base_offset,
                },
            );
        }
    }

    // -----------------------------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------------------------

    /// Appends validated batches to the local segment log. Offsets are patched in place.
    pub fn append_local(&mut self, mut raw: BytesMut, info: &BatchSetInfo) -> Result<AppendResult, StorageError> {
        match self.check_producer(info) {
            ProducerCheck::Append => {}
            ProducerCheck::Duplicate(base) => {
                return Ok(AppendResult {
                    base_offset: base,
                    last_offset: base + info.offset_span - 1,
                    log_start: self.log_start(),
                    duplicate: true,
                });
            }
            ProducerCheck::Reject(code) => return Err(StorageError::Producer(code)),
        }
        self.ensure_writable()?;
        let base = self.next_offset;
        let last = records::assign_offsets(&mut raw, base);
        let bytes = raw.freeze();

        let writer = self.writer.as_mut().expect("writable segment");
        writer.write_all(&bytes)?;
        let seg = self.segments.values_mut().next_back().expect("active segment");
        let pos = seg.size;
        for (at, h) in BatchIter::new(&bytes) {
            let batch_pos = pos + at as u64;
            seg.max_ts = seg.max_ts.max(h.max_timestamp());
            if seg.index.is_empty() || seg.bytes_since_index >= INDEX_INTERVAL_BYTES {
                seg.index.push(IndexEntry { offset: h.base_offset(), pos: batch_pos, ts: seg.max_ts });
                seg.bytes_since_index = 0;
            }
            seg.bytes_since_index += h.total_size() as u64;
        }
        seg.size += bytes.len() as u64;
        seg.next = last + 1;
        let roll = seg.size >= self.cfg.segment_bytes;

        self.next_offset = last + 1;
        self.records_in += info.record_count as u64;
        self.bytes_in += bytes.len() as u64;
        self.record_producer(info, base);
        self.cache_push(base, last + 1, bytes);
        // Fsync topics are synced by the shard once per drained command group (group commit),
        // see `needs_sync`.
        self.dirty = true;
        if roll {
            self.roll()?;
        }
        Ok(AppendResult { base_offset: base, last_offset: last, log_start: self.log_start(), duplicate: false })
    }

    /// Makes sure the last local segment continues exactly at `next_offset`.
    fn ensure_writable(&mut self) -> Result<(), StorageError> {
        let ok = match (self.segments.values().next_back(), &self.writer) {
            (Some(s), Some(_)) => s.local && !s.sealed && s.next == self.next_offset,
            _ => false,
        };
        if !ok {
            self.roll()?;
        }
        Ok(())
    }

    /// Seals the active segment (if any) and starts a new one at `next_offset`.
    pub fn roll(&mut self) -> Result<(), StorageError> {
        if let Some(w) = self.writer.take() {
            background_sync(w);
        }
        let next = self.next_offset;
        if let Some(s) = self.segments.values_mut().next_back() {
            if !s.sealed {
                if s.next == s.base && s.base == next {
                    // Empty active segment already positioned correctly.
                    self.writer = Some(OpenOptions::new().append(true).open(self.dir.join(seg_name(s.base, "log")))?);
                    return Ok(());
                }
                s.sealed = true;
                write_index_file(&self.dir.join(seg_name(s.base, "idx")), s)?;
                if s.next == s.base {
                    // Never-written segment: drop it.
                    let base = s.base;
                    let _ = fs::remove_file(self.dir.join(seg_name(base, "log")));
                    let _ = fs::remove_file(self.dir.join(seg_name(base, "idx")));
                    self.segments.remove(&base);
                }
            }
        }
        let path = self.dir.join(seg_name(next, "log"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        self.segments.insert(
            next,
            Segment {
                base: next,
                next,
                size: 0,
                max_ts: -1,
                index: Vec::new(),
                bytes_since_index: 0,
                local: true,
                object_key: None,
                sealed: false,
                created_ms: now_ms(),
            },
        );
        self.writer = Some(file);
        Ok(())
    }

    /// Registers batches the diskless agent has durably written to object storage.
    /// `raw` is the unpatched payload (shared with the agent's file buffer).
    pub fn commit_diskless(
        &mut self,
        file: Arc<str>,
        pos: u64,
        raw: Bytes,
        info: &BatchSetInfo,
    ) -> Result<AppendResult, StorageError> {
        match self.check_producer(info) {
            ProducerCheck::Append => {}
            ProducerCheck::Duplicate(base) => {
                return Ok(AppendResult {
                    base_offset: base,
                    last_offset: base + info.offset_span - 1,
                    log_start: self.log_start(),
                    duplicate: true,
                });
            }
            ProducerCheck::Reject(code) => return Err(StorageError::Producer(code)),
        }
        // Any active local segment must not receive data past this extent.
        if let Some(s) = self.segments.values_mut().next_back() {
            if !s.sealed && s.next > s.base {
                self.roll_seal_only()?;
            }
        }
        let base = self.next_offset;
        let next = base + info.offset_span;
        let ext = Extent { base, next, file, pos, len: raw.len() as u32, max_ts: info.max_timestamp };
        self.append_diskless_log(&ext)?;
        self.diskless.insert(base, ext);
        self.next_offset = next;
        self.records_in += info.record_count as u64;
        self.bytes_in += raw.len() as u64;
        self.record_producer(info, base);
        if self.cfg.cache_bytes > 0 {
            let mut patched = BytesMut::from(&raw[..]);
            records::assign_offsets(&mut patched, base);
            self.cache_push(base, next, patched.freeze());
        }
        Ok(AppendResult { base_offset: base, last_offset: next - 1, log_start: self.log_start(), duplicate: false })
    }

    fn roll_seal_only(&mut self) -> Result<(), StorageError> {
        if let Some(w) = self.writer.take() {
            background_sync(w);
        }
        if let Some(s) = self.segments.values_mut().next_back() {
            if !s.sealed {
                s.sealed = true;
                write_index_file(&self.dir.join(seg_name(s.base, "idx")), s)?;
            }
        }
        Ok(())
    }

    fn append_diskless_log(&mut self, e: &Extent) -> Result<(), StorageError> {
        if self.diskless_log.is_none() {
            self.diskless_log =
                Some(OpenOptions::new().create(true).append(true).open(self.dir.join("diskless.idx"))?);
        }
        let mut buf = Vec::with_capacity(48 + e.file.len());
        encode_extent(&mut buf, e);
        let f = self.diskless_log.as_mut().unwrap();
        f.write_all(&buf)?;
        // The offset assignment is the commit point for diskless data: make it durable.
        f.sync_data()?;
        self.diskless_logged += 1;
        Ok(())
    }

    fn cache_push(&mut self, base: i64, next: i64, bytes: Bytes) {
        let cap = self.cfg.cache_bytes;
        if cap == 0 || bytes.len() > cap {
            self.cache.clear();
            self.cache_bytes = 0;
            return;
        }
        // Cache must stay contiguous in offsets.
        if let Some((_, last_next, _)) = self.cache.back() {
            if *last_next != base {
                self.cache.clear();
                self.cache_bytes = 0;
            }
        }
        self.cache_bytes += bytes.len();
        self.cache.push_back((base, next, bytes));
        while self.cache_bytes > cap {
            if let Some((_, _, b)) = self.cache.pop_front() {
                self.cache_bytes -= b.len();
            }
        }
    }

    /// True when this partition has unsynced writes and its topic requires fsync before ack.
    pub fn needs_sync(&self) -> bool {
        self.dirty && self.cfg.durability == Durability::Fsync
    }

    /// Periodic flush for `durability=flush` topics: hands the fsync to the background
    /// flusher thread so the shard never blocks on disk.
    pub fn flush_background(&mut self) {
        if self.dirty {
            if let Some(w) = &self.writer {
                if let Ok(f) = w.try_clone() {
                    background_sync(f);
                }
            }
            self.dirty = false;
        }
        self.last_flush_ms = now_ms();
    }

    /// Synchronous fsync (fsync-durability group commit and shutdown).
    pub fn flush(&mut self) -> Result<(), StorageError> {
        if self.dirty {
            if let Some(w) = &self.writer {
                w.sync_data()?;
            }
            self.dirty = false;
        }
        self.last_flush_ms = now_ms();
        Ok(())
    }

    // -----------------------------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------------------------

    /// Plans a fetch: which bytes to read from memory, local files or object storage.
    /// The I/O itself happens outside the shard (see [`crate::read`]).
    pub fn read_plan(&self, offset: i64, max_bytes: usize) -> Result<ReadPlan, StorageError> {
        let hw = self.next_offset;
        let log_start = self.log_start();
        let mut plan = ReadPlan { chunks: Vec::new(), fetch_offset: offset, high_watermark: hw, log_start };
        if offset == hw {
            return Ok(plan);
        }
        if offset > hw || offset < log_start {
            return Err(StorageError::OffsetOutOfRange { requested: offset, log_start, high_watermark: hw });
        }

        // Tailing consumers: serve from the in-memory batch cache.
        if let Some((front, _, _)) = self.cache.front() {
            if offset >= *front {
                let mut planned = 0;
                for (_, next, b) in &self.cache {
                    if *next <= offset {
                        continue;
                    }
                    plan.chunks.push(Chunk::Mem(b.clone()));
                    planned += b.len();
                    if planned >= max_bytes {
                        break;
                    }
                }
                return Ok(plan);
            }
        }

        let mut cursor = offset;
        let mut planned: usize = 0;
        while cursor < hw && (planned < max_bytes || plan.chunks.is_empty()) {
            let seg = self.segments.range(..=cursor).next_back().map(|(_, s)| s).filter(|s| cursor < s.next);
            let ext = self.diskless.range(..=cursor).next_back().map(|(_, e)| e).filter(|e| cursor < e.next);
            if let Some(s) = seg {
                let start = s.index.iter().rev().find(|e| e.offset <= cursor).map(|e| e.pos).unwrap_or(0);
                let want = (max_bytes - planned.min(max_bytes)).max(1) as u64 + INDEX_INTERVAL_BYTES;
                let len = (s.size - start).min(want);
                if s.local {
                    plan.chunks.push(Chunk::File { path: self.dir.join(seg_name(s.base, "log")), pos: start, len });
                } else if let Some(key) = &s.object_key {
                    plan.chunks.push(Chunk::Object { key: key.clone(), pos: start, len, patch_base: None });
                } else {
                    return Err(StorageError::Corrupt(format!("segment {} has no data location", s.base)));
                }
                planned += len as usize;
                if start + len < s.size {
                    break; // budget exhausted inside this segment
                }
                cursor = s.next;
            } else if let Some(e) = ext {
                plan.chunks.push(Chunk::Object {
                    key: e.file.to_string(),
                    pos: e.pos,
                    len: e.len as u64,
                    patch_base: Some(e.base),
                });
                planned += e.len as usize;
                cursor = e.next;
            } else {
                // Gap (expired extent between live ones): skip to the next available extent.
                let next_seg = self.segments.range(cursor..).find(|(_, s)| s.next > s.base).map(|(b, _)| *b);
                let next_ext = self.diskless.range(cursor..).next().map(|(b, _)| *b);
                match next_seg.into_iter().chain(next_ext).min() {
                    Some(n) if n > cursor => cursor = n,
                    _ => break,
                }
            }
        }
        Ok(plan)
    }

    /// Offset of the first batch whose max timestamp is >= `ts` (ListOffsets by time).
    pub fn offset_for_timestamp(&self, ts: i64) -> Result<Option<(i64, i64)>, StorageError> {
        let mut best: Option<(i64, i64)> = None;
        for s in self.segments.values() {
            if s.max_ts < ts || s.next == s.base {
                continue;
            }
            // Narrow down with the sparse index, then scan batch headers when the file is local.
            let from = s.index.iter().rev().find(|e| e.ts < ts).map(|e| e.pos).unwrap_or(0);
            let found = if s.local {
                scan_for_timestamp(&self.dir.join(seg_name(s.base, "log")), from, ts)?
            } else {
                s.index.iter().find(|e| e.ts >= ts).map(|e| (e.offset, e.ts))
            };
            if let Some(f) = found {
                best = Some(f);
                break;
            }
        }
        for e in self.diskless.values() {
            if e.max_ts >= ts {
                if best.is_none_or(|(o, _)| e.base < o) {
                    best = Some((e.base, e.max_ts));
                }
                break;
            }
        }
        Ok(best)
    }

    // -----------------------------------------------------------------------------------------
    // Maintenance: flush, time-based roll, retention, tiering
    // -----------------------------------------------------------------------------------------

    pub fn maintenance(&mut self, now: i64) -> Result<Maintenance, StorageError> {
        let mut out = Maintenance::default();
        if self.dirty && self.cfg.flush_ms >= 0 && now - self.last_flush_ms >= self.cfg.flush_ms {
            self.flush_background();
        }
        let roll_due = self
            .segments
            .values()
            .next_back()
            .is_some_and(|s| !s.sealed && s.next > s.base && now - s.created_ms >= self.cfg.segment_ms);
        if roll_due {
            self.roll()?;
        }

        let cfg = self.cfg.clone();
        let expired = |max_ts: i64, created: i64| {
            cfg.retention_ms >= 0 && now - if max_ts > 0 { max_ts } else { created } > cfg.retention_ms
        };

        // Time-based retention on sealed segments.
        let doomed: Vec<i64> =
            self.segments.values().filter(|s| s.sealed && expired(s.max_ts, s.created_ms)).map(|s| s.base).collect();
        let mut removed_any = !doomed.is_empty();
        for base in doomed {
            self.drop_segment(base, &mut out);
        }
        // Size-based retention (local + remote bytes of sealed segments).
        if cfg.retention_bytes >= 0 {
            let mut total: u64 = self.segments.values().map(|s| s.size).sum();
            while total > cfg.retention_bytes as u64 {
                let Some(base) = self.segments.values().find(|s| s.sealed).map(|s| s.base) else { break };
                total -= self.segments[&base].size;
                self.drop_segment(base, &mut out);
                removed_any = true;
            }
        }
        // Diskless extents.
        let doomed: Vec<i64> =
            self.diskless.values().filter(|e| expired(e.max_ts, now)).map(|e| e.base).collect();
        if !doomed.is_empty() {
            removed_any = true;
            for base in doomed {
                if let Some(e) = self.diskless.remove(&base) {
                    out.released_files.push(e.file);
                }
            }
            if self.diskless_logged > 2 * self.diskless.len() + 64 {
                self.rewrite_diskless_log()?;
            }
        }
        if removed_any {
            self.state.next_offset_floor = self.next_offset;
            fs::write(self.dir.join("partition.json"), serde_json::to_vec(&self.state)?)?;
            // Cached batches may now precede log start; that is fine (plan checks log_start first).
        }

        // Tiering: upload sealed segments, then drop local copies after local retention.
        let tiered = cfg.mode == StorageMode::Tiered;
        for s in self.segments.values_mut() {
            if !s.sealed || !s.local {
                continue;
            }
            match &s.object_key {
                None if tiered => out.uploads.push((
                    s.base,
                    self.dir.join(seg_name(s.base, "log")),
                    format!("tiered/{}/{}/{}", self.topic, self.id, seg_name(s.base, "log")),
                )),
                Some(_) if now - s.max_ts.max(s.created_ms) > cfg.local_retention_ms => {
                    fs::remove_file(self.dir.join(seg_name(s.base, "log")))?;
                    s.local = false;
                }
                _ => {}
            }
        }
        Ok(out)
    }

    fn drop_segment(&mut self, base: i64, out: &mut Maintenance) {
        if let Some(s) = self.segments.remove(&base) {
            for ext in ["log", "idx", "tier"] {
                let _ = fs::remove_file(self.dir.join(seg_name(base, ext)));
            }
            if let Some(k) = s.object_key {
                out.delete_objects.push(k);
            }
        }
    }

    /// Records that a sealed segment is now in object storage.
    pub fn mark_uploaded(&mut self, base: i64, key: String) -> Result<(), StorageError> {
        if let Some(s) = self.segments.get_mut(&base) {
            let side = TierSidecar { object_key: key.clone(), size: s.size, next: s.next, max_ts: s.max_ts };
            fs::write(self.dir.join(seg_name(base, "tier")), serde_json::to_vec(&side)?)?;
            s.object_key = Some(key);
        }
        Ok(())
    }

    fn rewrite_diskless_log(&mut self) -> Result<(), StorageError> {
        self.diskless_log = None;
        let tmp = self.dir.join("diskless.idx.tmp");
        let mut buf = Vec::new();
        for e in self.diskless.values() {
            encode_extent(&mut buf, e);
        }
        fs::write(&tmp, &buf)?;
        fs::rename(&tmp, self.dir.join("diskless.idx"))?;
        self.diskless_logged = self.diskless.len();
        Ok(())
    }

    /// Every diskless file referenced by this partition (for reference counting on startup).
    pub fn diskless_files(&self) -> impl Iterator<Item = &Arc<str>> {
        self.diskless.values().map(|e| &e.file)
    }

    /// Deletes the partition from disk. Returns object keys to delete and diskless files released.
    pub fn destroy(mut self) -> Result<Maintenance, StorageError> {
        let mut out = Maintenance::default();
        self.writer = None;
        self.diskless_log = None;
        for s in self.segments.values() {
            if let Some(k) = &s.object_key {
                out.delete_objects.push(k.clone());
            }
        }
        out.released_files.extend(self.diskless.values().map(|e| e.file.clone()));
        fs::remove_dir_all(&self.dir)?;
        Ok(out)
    }
}

// ---------------------------------------------------------------------------------------------
// On-disk helpers
// ---------------------------------------------------------------------------------------------

/// Scans a segment file, validating every batch; truncates a torn tail left by a crash.
fn scan_segment(path: &Path, base: i64) -> Result<Segment, StorageError> {
    let mut f = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = f.metadata()?.len();
    let mut r = BufReader::with_capacity(1 << 20, &mut f);
    let mut seg = Segment {
        base,
        next: base,
        size: 0,
        max_ts: -1,
        index: Vec::new(),
        bytes_since_index: 0,
        local: true,
        object_key: None,
        sealed: false,
        created_ms: now_ms(),
    };
    let mut pos: u64 = 0;
    let mut buf = Vec::new();
    loop {
        let mut head = [0u8; BATCH_HEADER_LEN];
        if r.read_exact(&mut head).is_err() {
            break;
        }
        let Ok(h) = BatchHeader::parse(&head) else { break };
        let total = h.total_size();
        buf.clear();
        buf.extend_from_slice(&head);
        buf.resize(total, 0);
        if r.read_exact(&mut buf[BATCH_HEADER_LEN..]).is_err() {
            break;
        }
        let h = BatchHeader::parse(&buf)?;
        if h.validate_crc().is_err() {
            break;
        }
        seg.max_ts = seg.max_ts.max(h.max_timestamp());
        if seg.index.is_empty() || seg.bytes_since_index >= INDEX_INTERVAL_BYTES {
            seg.index.push(IndexEntry { offset: h.base_offset(), pos, ts: seg.max_ts });
            seg.bytes_since_index = 0;
        }
        seg.bytes_since_index += total as u64;
        seg.next = h.last_offset() + 1;
        pos += total as u64;
    }
    drop(r);
    if pos < file_len {
        tracing::warn!(path = %path.display(), valid = pos, len = file_len, "truncating torn segment tail");
        f.set_len(pos)?;
    }
    seg.size = pos;
    Ok(seg)
}

fn write_index_file(path: &Path, s: &Segment) -> Result<(), StorageError> {
    let mut buf = Vec::with_capacity(24 + s.index.len() * INDEX_ENTRY_LEN);
    buf.extend_from_slice(&s.next.to_be_bytes());
    buf.extend_from_slice(&s.size.to_be_bytes());
    buf.extend_from_slice(&s.max_ts.to_be_bytes());
    for e in &s.index {
        buf.extend_from_slice(&e.offset.to_be_bytes());
        buf.extend_from_slice(&e.pos.to_be_bytes());
        buf.extend_from_slice(&e.ts.to_be_bytes());
    }
    let tmp = path.with_extension("idx.tmp");
    fs::write(&tmp, &buf)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn read_index_file(path: &Path, base: i64) -> Result<Segment, StorageError> {
    let b = fs::read(path)?;
    if b.len() < 24 || (b.len() - 24) % INDEX_ENTRY_LEN != 0 {
        return Err(StorageError::Corrupt(format!("bad index file {}", path.display())));
    }
    let i64_at = |at: usize| i64::from_be_bytes(b[at..at + 8].try_into().unwrap());
    let mut index = Vec::with_capacity((b.len() - 24) / INDEX_ENTRY_LEN);
    let mut at = 24;
    while at < b.len() {
        index.push(IndexEntry { offset: i64_at(at), pos: i64_at(at + 8) as u64, ts: i64_at(at + 16) });
        at += INDEX_ENTRY_LEN;
    }
    Ok(Segment {
        base,
        next: i64_at(0),
        size: i64_at(8) as u64,
        max_ts: i64_at(16),
        index,
        bytes_since_index: 0,
        local: true,
        object_key: None,
        sealed: true,
        created_ms: now_ms(),
    })
}

fn scan_for_timestamp(path: &Path, from: u64, ts: i64) -> Result<Option<(i64, i64)>, StorageError> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(from))?;
    let mut r = BufReader::new(f);
    let mut head = [0u8; BATCH_HEADER_LEN];
    loop {
        if r.read_exact(&mut head).is_err() {
            return Ok(None);
        }
        let h = BatchHeader::parse(&head)?;
        if h.max_timestamp() >= ts {
            return Ok(Some((h.base_offset(), h.max_timestamp())));
        }
        r.seek_relative((h.total_size() - BATCH_HEADER_LEN) as i64)?;
    }
}

fn encode_extent(buf: &mut Vec<u8>, e: &Extent) {
    buf.extend_from_slice(&e.base.to_be_bytes());
    buf.extend_from_slice(&e.next.to_be_bytes());
    buf.extend_from_slice(&e.pos.to_be_bytes());
    buf.extend_from_slice(&e.len.to_be_bytes());
    buf.extend_from_slice(&e.max_ts.to_be_bytes());
    buf.extend_from_slice(&(e.file.len() as u16).to_be_bytes());
    buf.extend_from_slice(e.file.as_bytes());
}

fn read_diskless_index(path: &Path) -> Result<(BTreeMap<i64, Extent>, usize), StorageError> {
    let mut map = BTreeMap::new();
    let Ok(b) = fs::read(path) else { return Ok((map, 0)) };
    let mut at = 0;
    let mut count = 0;
    let mut files: HashMap<String, Arc<str>> = HashMap::new();
    while at + 38 <= b.len() {
        let i64_at = |p: usize| i64::from_be_bytes(b[p..p + 8].try_into().unwrap());
        let base = i64_at(at);
        let next = i64_at(at + 8);
        let pos = i64_at(at + 16) as u64;
        let len = u32::from_be_bytes(b[at + 24..at + 28].try_into().unwrap());
        let max_ts = i64_at(at + 28);
        let klen = u16::from_be_bytes([b[at + 36], b[at + 37]]) as usize;
        if at + 38 + klen > b.len() {
            break; // torn tail
        }
        let key = String::from_utf8_lossy(&b[at + 38..at + 38 + klen]).into_owned();
        let file = files.entry(key.clone()).or_insert_with(|| Arc::from(key.as_str())).clone();
        map.insert(base, Extent { base, next, file, pos, len, max_ts });
        at += 38 + klen;
        count += 1;
    }
    Ok((map, count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bp_protocol::records::{BatchBuilder, Compression, NewRecord};

    fn batch(n: usize, ts: i64) -> (BytesMut, BatchSetInfo) {
        let mut b = BatchBuilder::new(Compression::None);
        for i in 0..n {
            b.push(
                &NewRecord {
                    key: None,
                    value: Some(Bytes::from(format!("value-{i}"))),
                    headers: vec![],
                    timestamp: Some(ts + i as i64),
                },
                0,
            );
        }
        let raw = b.build().unwrap();
        let info = records::validate_batches(&raw).unwrap();
        (raw, info)
    }

    fn open(dir: &Path, cfg: TopicConfig) -> Partition {
        Partition::open(dir.join("t-0"), Arc::from("t"), 0, Arc::new(cfg)).unwrap()
    }

    #[test]
    fn append_recover_and_plan() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = TopicConfig { segment_bytes: 300, cache_bytes: 0, ..Default::default() };
        {
            let mut p = open(dir.path(), cfg.clone());
            for i in 0..10 {
                let (raw, info) = batch(3, 1000 + i * 10);
                let r = p.append_local(raw, &info).unwrap();
                assert_eq!(r.base_offset, i * 3);
            }
            assert_eq!(p.high_watermark(), 30);
            assert!(p.info().segments > 1, "segment.bytes=300 must roll");
            p.flush().unwrap();
        }
        let p = open(dir.path(), cfg);
        assert_eq!(p.high_watermark(), 30);
        assert_eq!(p.log_start(), 0);
        let plan = p.read_plan(7, 1 << 20).unwrap();
        assert!(!plan.chunks.is_empty());
        assert!(matches!(p.read_plan(31, 100), Err(StorageError::OffsetOutOfRange { .. })));
        assert_eq!(p.offset_for_timestamp(1045).unwrap().map(|x| x.0), Some(15));
    }

    #[test]
    fn torn_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = TopicConfig::default();
        {
            let mut p = open(dir.path(), cfg.clone());
            let (raw, info) = batch(2, 1);
            p.append_local(raw, &info).unwrap();
            p.flush().unwrap();
        }
        let seg = dir.path().join("t-0").join(seg_name(0, "log"));
        let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&[0u8; 30]).unwrap();
        drop(f);
        let mut p = open(dir.path(), cfg);
        assert_eq!(p.high_watermark(), 2);
        let (raw, info) = batch(1, 5);
        assert_eq!(p.append_local(raw, &info).unwrap().base_offset, 2);
    }

    #[test]
    fn idempotent_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let mut p = open(dir.path(), TopicConfig::default());
        let mk = |seq: i32| {
            let mut b = BatchBuilder::new(Compression::None).idempotent(7, 0, seq);
            b.push(&NewRecord { value: Some(Bytes::from_static(b"x")), ..Default::default() }, 0);
            let raw = b.build().unwrap();
            let info = records::validate_batches(&raw).unwrap();
            (raw, info)
        };
        let (r, i) = mk(0);
        assert_eq!(p.append_local(r, &i).unwrap().base_offset, 0);
        let (r, i) = mk(0);
        let dup = p.append_local(r, &i).unwrap();
        assert!(dup.duplicate);
        assert_eq!(dup.base_offset, 0);
        let (r, i) = mk(5);
        assert!(matches!(p.append_local(r, &i), Err(StorageError::Producer(45))));
        let (r, i) = mk(1);
        assert_eq!(p.append_local(r, &i).unwrap().base_offset, 1);
    }

    #[test]
    fn mixed_local_and_diskless_extents() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = TopicConfig { cache_bytes: 0, ..Default::default() };
        let mut p = open(dir.path(), cfg.clone());
        let (raw, info) = batch(2, 1);
        p.append_local(raw, &info).unwrap();
        let (raw, info) = batch(3, 2);
        let r = p.commit_diskless(Arc::from("diskless/f1.bpd"), 8, raw.freeze(), &info).unwrap();
        assert_eq!(r.base_offset, 2);
        let (raw, info) = batch(1, 3);
        assert_eq!(p.append_local(raw, &info).unwrap().base_offset, 5);
        drop(p);
        let p = open(dir.path(), cfg);
        assert_eq!(p.high_watermark(), 6);
        let plan = p.read_plan(0, 1 << 20).unwrap();
        assert_eq!(plan.chunks.len(), 3);
        assert!(matches!(&plan.chunks[1], Chunk::Object { patch_base: Some(2), .. }));
        let plan = p.read_plan(3, 1 << 20).unwrap();
        assert!(matches!(&plan.chunks[0], Chunk::Object { .. }));
    }

    #[test]
    fn retention_removes_old_segments() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = TopicConfig { segment_bytes: 100, retention_ms: 1000, ..Default::default() };
        let mut p = open(dir.path(), cfg);
        let now = now_ms();
        for _ in 0..5 {
            let (raw, info) = batch(2, now - 10_000);
            p.append_local(raw, &info).unwrap();
        }
        let (raw, info) = batch(2, now);
        p.append_local(raw, &info).unwrap();
        p.maintenance(now).unwrap();
        // The segment holding offsets 8..12 also holds fresh data, so it survives.
        assert_eq!(p.log_start(), 8);
        assert_eq!(p.high_watermark(), 12);
        assert!(matches!(p.read_plan(0, 100), Err(StorageError::OffsetOutOfRange { .. })));
    }
}
