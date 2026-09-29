//! Object storage abstraction (S3, GCS, Azure Blob, MinIO, local filesystem, in-memory)
//! with a byte-bounded LRU cache so each diskless file is fetched once per node.

use std::collections::HashMap;
use std::ops::Range;
use std::path::Path as FsPath;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parking_lot::Mutex;

use crate::StorageError;

#[derive(Debug, Default)]
pub struct ObjectStats {
    pub puts: AtomicU64,
    pub gets: AtomicU64,
    pub deletes: AtomicU64,
    pub cache_hits: AtomicU64,
    pub bytes_put: AtomicU64,
    pub bytes_get: AtomicU64,
    pub put_micros_total: AtomicU64,
    pub errors: AtomicU64,
}

pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    url: String,
    cache: Mutex<LruBytes>,
    pub stats: ObjectStats,
}

impl std::fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStorage").field("url", &self.url).finish()
    }
}

#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub last_modified_ms: i64,
}

impl ObjectStorage {
    /// `url` may be `s3://bucket/prefix`, `gs://…`, `az://…`, `memory://`, `file:///abs/path`
    /// or a plain directory path. Credentials come from the usual environment variables
    /// (`AWS_ACCESS_KEY_ID`, `AWS_ENDPOINT` for MinIO, `GOOGLE_SERVICE_ACCOUNT`, `AZURE_STORAGE_*`).
    pub fn open(url: &str, cache_bytes: usize) -> Result<Self, StorageError> {
        let is_remote = ["s3://", "s3a://", "gs://", "az://", "abfs://", "abfss://", "azure://", "memory://", "https://"]
            .iter()
            .any(|p| url.starts_with(p));
        let (store, prefix): (Arc<dyn ObjectStore>, Path) = if is_remote {
            let parsed = url::Url::parse(url).map_err(|e| StorageError::Config(format!("object store url: {e}")))?;
            let opts: Vec<(String, String)> = std::env::vars().map(|(k, v)| (k.to_ascii_lowercase(), v)).collect();
            let (s, p) = object_store::parse_url_opts(&parsed, opts)?;
            (Arc::from(s), p)
        } else {
            let dir = url.strip_prefix("file://").unwrap_or(url);
            // file:///C:/x on Windows yields "/C:/x".
            let dir = if cfg!(windows) && dir.starts_with('/') && dir.as_bytes().get(2) == Some(&b':') {
                &dir[1..]
            } else {
                dir
            };
            std::fs::create_dir_all(dir)?;
            let fs = object_store::local::LocalFileSystem::new_with_prefix(FsPath::new(dir))?.with_automatic_cleanup(true);
            (Arc::new(fs), Path::default())
        };
        Ok(Self {
            store,
            prefix,
            url: url.to_string(),
            cache: Mutex::new(LruBytes::new(cache_bytes)),
            stats: ObjectStats::default(),
        })
    }

    pub fn in_memory() -> Self {
        Self::open("memory://", 64 * 1024 * 1024).expect("in-memory store")
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn path(&self, key: &str) -> Path {
        if self.prefix.as_ref().is_empty() {
            Path::from(key)
        } else {
            Path::from(format!("{}/{}", self.prefix, key))
        }
    }

    pub async fn put(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        let started = Instant::now();
        let len = data.len() as u64;
        let r = self.store.put(&self.path(key), PutPayload::from(data)).await;
        self.stats.puts.fetch_add(1, Ordering::Relaxed);
        self.stats.put_micros_total.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        match r {
            Ok(_) => {
                self.stats.bytes_put.fetch_add(len, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                Err(e.into())
            }
        }
    }

    /// Whole-object read through the cache.
    pub async fn get(&self, key: &str) -> Result<Bytes, StorageError> {
        if let Some(b) = self.cache.lock().get(key) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(b);
        }
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        let b = match self.store.get(&self.path(key)).await {
            Ok(r) => r.bytes().await?,
            Err(e) => {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Err(e.into());
            }
        };
        self.stats.bytes_get.fetch_add(b.len() as u64, Ordering::Relaxed);
        self.cache.lock().insert(key.to_string(), b.clone());
        Ok(b)
    }

    /// Ranged read, served from the cache when the whole object is cached.
    pub async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        if let Some(b) = self.cache.lock().get(key) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            let end = (range.end as usize).min(b.len());
            let start = (range.start as usize).min(end);
            return Ok(b.slice(start..end));
        }
        self.stats.gets.fetch_add(1, Ordering::Relaxed);
        let b = self.store.get_range(&self.path(key), range).await.inspect_err(|_| {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
        })?;
        self.stats.bytes_get.fetch_add(b.len() as u64, Ordering::Relaxed);
        Ok(b)
    }

    pub fn cache_insert(&self, key: &str, data: Bytes) {
        self.cache.lock().insert(key.to_string(), data);
    }

    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.cache.lock().remove(key);
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn list(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, StorageError> {
        let p = self.path(key_prefix);
        let strip = if self.prefix.as_ref().is_empty() { String::new() } else { format!("{}/", self.prefix) };
        let metas: Vec<_> = self.store.list(Some(&p)).try_collect().await?;
        Ok(metas
            .into_iter()
            .map(|m| ObjectInfo {
                key: m.location.as_ref().strip_prefix(&strip).unwrap_or(m.location.as_ref()).to_string(),
                size: m.size,
                last_modified_ms: m.last_modified.timestamp_millis(),
            })
            .collect())
    }

    pub fn cached_bytes(&self) -> usize {
        self.cache.lock().used
    }
}

/// Byte-bounded LRU keyed by object key.
struct LruBytes {
    cap: usize,
    used: usize,
    tick: u64,
    map: HashMap<String, (Bytes, u64)>,
}

impl LruBytes {
    fn new(cap: usize) -> Self {
        Self { cap, used: 0, tick: 0, map: HashMap::new() }
    }

    fn get(&mut self, k: &str) -> Option<Bytes> {
        self.tick += 1;
        let t = self.tick;
        self.map.get_mut(k).map(|(b, used)| {
            *used = t;
            b.clone()
        })
    }

    fn insert(&mut self, k: String, b: Bytes) {
        if b.len() > self.cap {
            return;
        }
        self.tick += 1;
        self.used += b.len();
        if let Some((old, _)) = self.map.insert(k, (b, self.tick)) {
            self.used -= old.len();
        }
        while self.used > self.cap {
            let Some(victim) = self.map.iter().min_by_key(|(_, (_, t))| *t).map(|(k, _)| k.clone()) else { break };
            if let Some((old, _)) = self.map.remove(&victim) {
                self.used -= old.len();
            }
        }
    }

    fn remove(&mut self, k: &str) {
        if let Some((old, _)) = self.map.remove(k) {
            self.used -= old.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_oldest() {
        let mut l = LruBytes::new(10);
        l.insert("a".into(), Bytes::from_static(b"12345"));
        l.insert("b".into(), Bytes::from_static(b"12345"));
        assert!(l.get("a").is_some());
        l.insert("c".into(), Bytes::from_static(b"12345"));
        assert!(l.get("b").is_none());
        assert!(l.get("a").is_some());
        assert_eq!(l.used, 10);
    }

    #[tokio::test]
    async fn local_fs_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let s = ObjectStorage::open(dir.path().to_str().unwrap(), 1024).unwrap();
        s.put("diskless/a.bpd", Bytes::from_static(b"hello world")).await.unwrap();
        assert_eq!(&s.get_range("diskless/a.bpd", 6..11).await.unwrap()[..], b"world");
        assert_eq!(&s.get("diskless/a.bpd").await.unwrap()[..], b"hello world");
        let listed = s.list("diskless").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].key, "diskless/a.bpd");
        s.delete("diskless/a.bpd").await.unwrap();
        assert!(s.list("diskless").await.unwrap().is_empty());
    }
}
