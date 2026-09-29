//! Diskless object format and file reference counting.
//!
//! One diskless object packs batches from many partitions and topics (WarpStream-style):
//! `BPDL` magic + version byte, then raw record batches back to back. Offsets are assigned
//! by the partition owner at commit time, so batches inside the object keep base offset 0 and
//! are patched on read.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;

pub const MAGIC: &[u8; 5] = b"BPDL\x01";

pub struct FileBuilder {
    buf: BytesMut,
}

impl Default for FileBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FileBuilder {
    pub fn new() -> Self {
        let mut buf = BytesMut::with_capacity(1 << 20);
        buf.extend_from_slice(MAGIC);
        Self { buf }
    }

    /// Appends a payload and returns its (position, length) inside the object.
    pub fn push(&mut self, payload: &[u8]) -> (u64, u32) {
        let pos = self.buf.len() as u64;
        self.buf.extend_from_slice(payload);
        (pos, payload.len() as u32)
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.len() <= MAGIC.len()
    }

    pub fn finish(self) -> Bytes {
        self.buf.freeze()
    }
}

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique, time-ordered object key: `diskless/<ms>-<node>-<seq>.bpd`.
pub fn new_file_key(node_id: i32) -> String {
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("diskless/{:013}-{node_id}-{seq:06}.bpd", crate::partition::now_ms())
}

/// Counts how many partition extents reference each diskless object; an object is deleted
/// once its last extent expires.
#[derive(Default)]
pub struct FileRefs {
    refs: Mutex<HashMap<Arc<str>, usize>>,
}

impl FileRefs {
    pub fn acquire(&self, key: &Arc<str>, n: usize) {
        *self.refs.lock().entry(key.clone()).or_insert(0) += n;
    }

    /// Returns true when the object is no longer referenced.
    pub fn release(&self, key: &Arc<str>) -> bool {
        let mut m = self.refs.lock();
        match m.get_mut(key) {
            Some(c) if *c > 1 => {
                *c -= 1;
                false
            }
            Some(_) => {
                m.remove(key);
                true
            }
            None => false,
        }
    }

    pub fn is_referenced(&self, key: &str) -> bool {
        self.refs.lock().contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.refs.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
