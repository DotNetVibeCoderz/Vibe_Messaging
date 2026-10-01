//! Log compaction (`cleanup.policy=compact`): keep only the latest record for every key.
//!
//! The shard hands out a [`CompactionJob`] describing sealed local segments; [`run`] does the
//! heavy work on a blocking thread so the shard never stalls:
//!
//! 1. Build an offset map: a 128-bit hash of every key to the offset of its latest record.
//! 2. Rewrite each segment, keeping a record only if it is that latest offset. Tombstones
//!    (null values) are kept until they are older than `delete.retention.ms`, so consumers
//!    get a chance to see the delete. Batches keep their offsets ([`records::rebuild_batch`]).
//!
//! Output goes to `<base>-<generation>.log.cleaned` / `.idx.cleaned`. The shard renames them into place
//! in [`crate::Partition::apply_compaction`] only if the segment did not change meanwhile; a
//! crash before that leaves `.cleaned` files that recovery deletes.

use std::collections::HashMap;
use std::fs::{self, File};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use bp_protocol::records::{self, BATCH_HEADER_LEN, BatchHeader, Rebuilt};

use crate::StorageError;
use crate::partition::{CompactionStats, INDEX_INTERVAL_BYTES, IndexEntry, Segment, seg_file, write_index_file};

/// One sealed segment as the shard saw it when the job was created.
#[derive(Debug, Clone)]
pub struct SegmentSnapshot {
    pub base: i64,
    pub generation: u32,
    pub size: u64,
    pub next: i64,
}

#[derive(Debug, Clone)]
pub struct CompactionJob {
    pub dir: PathBuf,
    pub segments: Vec<SegmentSnapshot>,
    /// Tombstones with a timestamp below this are removed.
    pub tombstone_horizon_ms: i64,
}

#[derive(Debug)]
pub struct CleanedSegment {
    pub snapshot: SegmentSnapshot,
    /// Generation of the `.cleaned` output; `None` when no record survived.
    pub new_generation: Option<u32>,
}

#[derive(Debug)]
pub struct CompactionResult {
    pub segments: Vec<CleanedSegment>,
    pub stats: CompactionStats,
}

/// Collision-resistant 128-bit key fingerprint (two independent SipHash runs).
fn fingerprint(key: &[u8]) -> u128 {
    let mut a = DefaultHasher::new();
    key.hash(&mut a);
    let mut b = DefaultHasher::new();
    0x9e37_79b9_7f4a_7c15u64.hash(&mut b);
    key.hash(&mut b);
    ((a.finish() as u128) << 64) | b.finish() as u128
}

/// Calls `f` with every complete batch in a segment file.
fn for_each_batch(path: &Path, mut f: impl FnMut(&[u8]) -> Result<(), StorageError>) -> Result<(), StorageError> {
    let mut r = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut buf = Vec::new();
    loop {
        let mut head = [0u8; BATCH_HEADER_LEN];
        if r.read_exact(&mut head).is_err() {
            return Ok(());
        }
        let total = BatchHeader::parse(&head)?.total_size();
        buf.clear();
        buf.extend_from_slice(&head);
        buf.resize(total, 0);
        if r.read_exact(&mut buf[BATCH_HEADER_LEN..]).is_err() {
            return Ok(()); // sealed segments are complete; tolerate a short read anyway
        }
        f(&buf)?;
    }
}

pub(crate) fn cleaned_path(dir: &Path, base: i64, generation: u32, ext: &str) -> PathBuf {
    dir.join(format!("{}.cleaned", seg_file(base, generation, ext)))
}

/// Runs a compaction job. Blocking: call it from a blocking thread.
pub fn run(job: &CompactionJob) -> Result<CompactionResult, StorageError> {
    let started = Instant::now();
    // Pass 1: latest offset per key.
    let mut latest: HashMap<u128, i64> = HashMap::new();
    for s in &job.segments {
        let path = job.dir.join(seg_file(s.base, s.generation, "log"));
        for_each_batch(&path, |batch| {
            for rec in records::decode_batch(batch)? {
                if let Some(k) = &rec.key {
                    latest.insert(fingerprint(k), rec.offset);
                }
            }
            Ok(())
        })?;
    }

    // Pass 2: rewrite every segment.
    let mut stats = CompactionStats { segments: job.segments.len(), ..Default::default() };
    let mut out = Vec::with_capacity(job.segments.len());
    for s in &job.segments {
        let new_generation = s.generation + 1;
        let log_tmp = cleaned_path(&job.dir, s.base, new_generation, "log");
        let mut w = BufWriter::with_capacity(1 << 20, File::create(&log_tmp)?);
        let mut seg = Segment::empty(s.base);
        let mut before = 0u64;
        let mut after = 0u64;
        let write = |seg: &mut Segment, w: &mut BufWriter<File>, bytes: &[u8]| -> Result<(), StorageError> {
            let h = BatchHeader::parse(bytes)?;
            seg.max_ts = seg.max_ts.max(h.max_timestamp());
            if seg.index.is_empty() || seg.bytes_since_index >= INDEX_INTERVAL_BYTES {
                seg.index.push(IndexEntry { offset: h.base_offset(), pos: seg.size, ts: seg.max_ts });
                seg.bytes_since_index = 0;
            }
            seg.bytes_since_index += bytes.len() as u64;
            seg.size += bytes.len() as u64;
            w.write_all(bytes)?;
            Ok(())
        };
        let path = job.dir.join(seg_file(s.base, s.generation, "log"));
        for_each_batch(&path, |batch| {
            let h = BatchHeader::parse(batch)?;
            if !h.is_control() {
                before += h.record_count().max(0) as u64;
            }
            let rebuilt = records::rebuild_batch(batch, |rec| match &rec.key {
                // Kafka rejects null keys on compacted topics; data written before the policy
                // changed is simply kept.
                None => true,
                Some(k) => {
                    latest.get(&fingerprint(k)) == Some(&rec.offset)
                        && !(rec.value.is_none() && rec.timestamp < job.tombstone_horizon_ms)
                }
            })?;
            match rebuilt {
                Rebuilt::Unchanged => {
                    if !h.is_control() {
                        after += h.record_count().max(0) as u64;
                    }
                    write(&mut seg, &mut w, batch)
                }
                Rebuilt::Empty => Ok(()),
                Rebuilt::New(b) => {
                    after += BatchHeader::parse(&b)?.record_count().max(0) as u64;
                    write(&mut seg, &mut w, &b)
                }
            }
        })?;
        w.flush()?;
        let file = w.into_inner().map_err(|e| e.into_error())?;
        file.sync_data()?;
        drop(file);
        stats.records_before += before;
        stats.records_after += after;
        stats.bytes_before += s.size;
        if seg.size == 0 {
            let _ = fs::remove_file(&log_tmp);
            out.push(CleanedSegment { snapshot: s.clone(), new_generation: None });
            continue;
        }
        stats.bytes_after += seg.size;
        // The offset range covered by the segment never shrinks.
        seg.next = s.next;
        write_index_file(&cleaned_path(&job.dir, s.base, new_generation, "idx"), &seg)?;
        out.push(CleanedSegment { snapshot: s.clone(), new_generation: Some(new_generation) });
    }
    stats.duration_ms = started.elapsed().as_millis() as u64;
    Ok(CompactionResult { segments: out, stats })
}

/// Removes the `.cleaned` outputs of a job whose result is being discarded.
pub(crate) fn discard(dir: &Path, cleaned: &CleanedSegment) {
    if let Some(g) = cleaned.new_generation {
        let _ = fs::remove_file(cleaned_path(dir, cleaned.snapshot.base, g, "log"));
        let _ = fs::remove_file(cleaned_path(dir, cleaned.snapshot.base, g, "idx"));
    }
}
