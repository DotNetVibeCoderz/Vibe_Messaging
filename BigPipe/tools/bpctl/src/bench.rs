//! `bpctl bench`: a native Kafka-protocol load generator built on `bp-protocol`.
//!
//! Each worker owns one TCP connection and keeps exactly one request in flight
//! (closed loop), so `--concurrency` equals outstanding requests. Reports throughput and
//! produce latency percentiles.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use bp_protocol::records::{BatchBuilder, BatchIter, Compression, NewRecord};
use bp_protocol::{Decoder, Encoder};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct ProduceBench {
    pub bootstrap: String,
    pub topic: String,
    pub partitions: i32,
    pub records: u64,
    pub record_size: usize,
    pub batch_records: usize,
    pub concurrency: usize,
    pub compression: Compression,
}

struct Conn {
    s: TcpStream,
    corr: i32,
}

impl Conn {
    async fn open(addr: &str) -> anyhow::Result<Self> {
        let s = TcpStream::connect(addr).await.with_context(|| format!("connecting to {addr}"))?;
        s.set_nodelay(true)?;
        Ok(Self { s, corr: 0 })
    }

    async fn call(&mut self, api_key: i16, version: i16, body: impl FnOnce(&mut Encoder)) -> anyhow::Result<Decoder> {
        self.corr += 1;
        let mut e = Encoder::new();
        e.i16(api_key);
        e.i16(version);
        e.i32(self.corr);
        e.string("bpctl-bench");
        body(&mut e);
        for chunk in e.finish_frame() {
            self.s.write_all(&chunk).await?;
        }
        let mut len = [0u8; 4];
        self.s.read_exact(&mut len).await?;
        let mut buf = BytesMut::zeroed(i32::from_be_bytes(len) as usize);
        self.s.read_exact(&mut buf).await?;
        let mut d = Decoder::new(buf);
        let corr = d.i32()?;
        if corr != self.corr {
            bail!("correlation mismatch: sent {}, got {corr}", self.corr);
        }
        Ok(d)
    }
}

fn percentile(sorted: &[u64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i] as f64 / 1000.0
}

pub async fn produce(cfg: ProduceBench) -> anyhow::Result<()> {
    let payload = Bytes::from(vec![b'x'; cfg.record_size]);
    let batches_total = cfg.records.div_ceil(cfg.batch_records as u64);
    let next_batch = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut workers = Vec::new();
    for w in 0..cfg.concurrency {
        let next_batch = next_batch.clone();
        let bytes_sent = bytes_sent.clone();
        let payload = payload.clone();
        let topic = cfg.topic.clone();
        let addr = cfg.bootstrap.clone();
        let (partitions, batch_records, compression) = (cfg.partitions, cfg.batch_records, cfg.compression);
        workers.push(tokio::spawn(async move {
            let mut conn = Conn::open(&addr).await?;
            let mut lat = Vec::new();
            loop {
                let b = next_batch.fetch_add(1, Relaxed);
                if b >= batches_total {
                    break;
                }
                let p = ((b + w as u64) % partitions as u64) as i32;
                let mut builder = BatchBuilder::new(compression);
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as i64;
                for _ in 0..batch_records {
                    builder.push(&NewRecord { key: None, value: Some(payload.clone()), headers: vec![], timestamp: None }, now);
                }
                let raw = builder.build()?.freeze();
                let t0 = Instant::now();
                let mut d = conn
                    .call(0, 3, |e| {
                        e.nullable_string(None);
                        e.i16(-1); // acks=all
                        e.i32(30_000);
                        e.array_len(1);
                        e.string(&topic);
                        e.array_len(1);
                        e.i32(p);
                        e.records(Some(std::slice::from_ref(&raw)));
                    })
                    .await?;
                lat.push(t0.elapsed().as_micros() as u64);
                // responses[0].partitions[0].error_code
                d.i32()?;
                d.string()?;
                d.i32()?;
                d.i32()?;
                let code = d.i16()?;
                if code != 0 {
                    bail!("produce error code {code}");
                }
                bytes_sent.fetch_add((batch_records * payload.len()) as u64, Relaxed);
            }
            Ok::<_, anyhow::Error>(lat)
        }));
    }
    let mut lat = Vec::new();
    for w in workers {
        lat.extend(w.await??);
    }
    let secs = started.elapsed().as_secs_f64();
    lat.sort_unstable();
    let mb = bytes_sent.load(Relaxed) as f64 / (1024.0 * 1024.0);
    println!("produce  records={} size={}B batch={} concurrency={} compression={:?}", cfg.records, cfg.record_size, cfg.batch_records, cfg.concurrency, cfg.compression);
    println!("         {:.0} records/s, {:.1} MiB/s, {:.2}s total", cfg.records as f64 / secs, mb / secs, secs);
    println!(
        "latency  p50={:.2}ms p99={:.2}ms p99.9={:.2}ms max={:.2}ms (per request, acks=all)",
        percentile(&lat, 0.50),
        percentile(&lat, 0.99),
        percentile(&lat, 0.999),
        percentile(&lat, 1.0)
    );
    Ok(())
}

pub async fn consume(bootstrap: &str, topic: &str, partitions: i32, want: u64) -> anyhow::Result<()> {
    let mut conn = Conn::open(bootstrap).await?;
    let mut offsets = vec![0i64; partitions as usize];
    let mut records = 0u64;
    let mut bytes = 0u64;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(120);
    while records < want && Instant::now() < deadline {
        let offs = offsets.clone();
        let mut d = conn
            .call(1, 4, |e| {
                e.i32(-1);
                e.i32(500);
                e.i32(1);
                e.i32(64 << 20);
                e.i8(0);
                e.array_len(1);
                e.string(topic);
                e.array_len(offs.len());
                for (p, o) in offs.iter().enumerate() {
                    e.i32(p as i32);
                    e.i64(*o);
                    e.i32(16 << 20);
                }
            })
            .await?;
        d.i32()?; // throttle
        let topics = d.i32()?;
        for _ in 0..topics {
            d.string()?;
            let parts = d.i32()?;
            for _ in 0..parts {
                let p = d.i32()?;
                let code = d.i16()?;
                d.i64()?;
                d.i64()?;
                let aborted = d.i32()?;
                for _ in 0..aborted.max(0) {
                    d.i64()?;
                    d.i64()?;
                }
                let data = d.nullable_bytes()?.unwrap_or_default();
                if code != 0 {
                    bail!("fetch error code {code} on partition {p}");
                }
                bytes += data.len() as u64;
                for (_, h) in BatchIter::new(&data) {
                    if h.last_offset() >= offsets[p as usize] {
                        records += h.record_count() as u64;
                        offsets[p as usize] = h.last_offset() + 1;
                    }
                }
            }
        }
    }
    let secs = started.elapsed().as_secs_f64();
    println!(
        "consume  {} records, {:.0} records/s, {:.1} MiB/s, {:.2}s",
        records,
        records as f64 / secs,
        bytes as f64 / (1024.0 * 1024.0) / secs,
        secs
    );
    Ok(())
}
