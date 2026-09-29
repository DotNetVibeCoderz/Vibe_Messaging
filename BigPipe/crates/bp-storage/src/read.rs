//! Executes a [`ReadPlan`] outside the owning shard: local file reads run on the blocking pool,
//! object reads go through the node's object cache. The result is trimmed to whole batches
//! within `[fetch_offset, high_watermark)`.

use std::path::PathBuf;

use bp_protocol::records::{self, BatchHeader, BATCH_HEADER_LEN};
use bytes::{Bytes, BytesMut};

use crate::objstore::ObjectStorage;
use crate::StorageError;

#[derive(Debug, Clone)]
pub enum Chunk {
    /// Already-patched batches from the shard's batch cache.
    Mem(Bytes),
    /// Byte range of a local segment file (starts at a batch boundary).
    File { path: PathBuf, pos: u64, len: u64 },
    /// Byte range of an object. `patch_base` is set for diskless extents whose batches still
    /// carry unassigned offsets.
    Object { key: String, pos: u64, len: u64, patch_base: Option<i64> },
}

#[derive(Debug, Clone)]
pub struct ReadPlan {
    pub chunks: Vec<Chunk>,
    pub fetch_offset: i64,
    pub high_watermark: i64,
    pub log_start: i64,
}

/// Reads the planned chunks and returns whole batches, at most ~`max_bytes` (but always at
/// least one batch when data exists, as Kafka requires).
pub async fn execute(plan: &ReadPlan, store: &ObjectStorage, max_bytes: usize) -> Result<Vec<Bytes>, StorageError> {
    let mut out = Vec::new();
    let mut total = 0usize;
    for chunk in &plan.chunks {
        if total >= max_bytes && !out.is_empty() {
            break;
        }
        let data = match chunk {
            Chunk::Mem(b) => b.clone(),
            Chunk::File { path, pos, len } => read_file_range(path.clone(), *pos, *len).await?,
            Chunk::Object { key, pos, len, patch_base } => {
                let b = store.get_range(key, *pos..*pos + *len).await?;
                match patch_base {
                    Some(base) => {
                        let mut m = BytesMut::from(&b[..]);
                        records::assign_offsets(&mut m, *base);
                        m.freeze()
                    }
                    None => b,
                }
            }
        };
        let mut data = data;
        // A single batch larger than the planned range: extend the read to the full batch.
        if let (Chunk::File { path, pos, .. }, Some(need)) = (chunk, first_batch_shortfall(&data)) {
            data = read_file_range(path.clone(), *pos, need as u64).await?;
        }
        if let (Chunk::Object { key, pos, patch_base: None, .. }, Some(need)) = (chunk, first_batch_shortfall(&data)) {
            data = store.get_range(key, *pos..*pos + need as u64).await?;
        }
        let trimmed = trim(&data, plan.fetch_offset, plan.high_watermark, max_bytes.saturating_sub(total), out.is_empty());
        if let Some(t) = trimmed {
            total += t.len();
            out.push(t);
        }
    }
    Ok(out)
}

/// When the buffer starts with a batch that is not fully contained, returns the needed length.
fn first_batch_shortfall(data: &[u8]) -> Option<usize> {
    if data.len() < BATCH_HEADER_LEN {
        return None;
    }
    let h = BatchHeader::parse(data).ok()?;
    (h.total_size() > data.len()).then_some(h.total_size())
}

/// Keeps complete batches overlapping `[from, hw)`, bounded by `budget` bytes
/// (the first batch is always kept when `must_take_one`).
fn trim(data: &Bytes, from: i64, hw: i64, budget: usize, must_take_one: bool) -> Option<Bytes> {
    let mut start = None;
    let mut end = 0;
    let mut pos = 0;
    while pos + BATCH_HEADER_LEN <= data.len() {
        let Ok(h) = BatchHeader::parse(&data[pos..]) else { break };
        let size = h.total_size();
        if pos + size > data.len() || h.base_offset() >= hw {
            break;
        }
        if h.last_offset() >= from {
            let s = *start.get_or_insert(pos);
            let taken = pos + size - s;
            if taken > budget && !(must_take_one && pos == s) {
                break;
            }
            end = pos + size;
        }
        pos += size;
    }
    start.filter(|s| end > *s).map(|s| data.slice(s..end))
}

async fn read_file_range(path: PathBuf, pos: u64, len: u64) -> Result<Bytes, StorageError> {
    tokio::task::spawn_blocking(move || -> Result<Bytes, StorageError> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(&path)?;
        f.seek(SeekFrom::Start(pos))?;
        let mut buf = Vec::with_capacity(len as usize);
        f.take(len).read_to_end(&mut buf)?;
        Ok(Bytes::from(buf))
    })
    .await
    .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use bp_protocol::records::{BatchBuilder, Compression, NewRecord};

    fn stored(base: i64, n: usize) -> BytesMut {
        let mut b = BatchBuilder::new(Compression::None);
        for _ in 0..n {
            b.push(&NewRecord { value: Some(Bytes::from_static(b"abcdefgh")), ..Default::default() }, 0);
        }
        let mut raw = b.build().unwrap();
        records::assign_offsets(&mut raw, base);
        raw
    }

    #[test]
    fn trim_bounds() {
        let mut all = stored(0, 2);
        all.extend_from_slice(&stored(2, 2));
        all.extend_from_slice(&stored(4, 2));
        let one = stored(0, 2).len();
        let data = all.freeze();
        // skip first batch (offsets 0-1), stop at hw=4
        let t = trim(&data, 2, 4, usize::MAX, true).unwrap();
        assert_eq!(t.len(), one);
        // budget smaller than one batch still returns one batch
        let t = trim(&data, 0, 10, 10, true).unwrap();
        assert_eq!(t.len(), one);
        // partial trailing batch is dropped
        let t = trim(&data.slice(..data.len() - 5), 0, 10, usize::MAX, true).unwrap();
        assert_eq!(t.len(), one * 2);
        assert!(trim(&data, 6, 10, usize::MAX, true).is_none());
    }
}
