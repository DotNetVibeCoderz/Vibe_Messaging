//! Kafka record batch (magic v2) handling.
//!
//! The broker never decompresses batches on the produce path: it validates the header and CRC,
//! patches the base offset and stores the batch verbatim. Decoding into individual records is
//! only needed by the HTTP gateway, server-side filters and flows.

use std::io::{Read, Write};

use bytes::{BufMut, Bytes, BytesMut};

use crate::codec::varint::{read_varint, read_varlong, varlong_len, write_varlong};
use crate::ProtocolError;

pub const BATCH_HEADER_LEN: usize = 61;
/// baseOffset + batchLength: the part of the header that precedes `batchLength`'s coverage.
pub const LOG_OVERHEAD: usize = 12;

const OFF_BASE_OFFSET: usize = 0;
const OFF_BATCH_LENGTH: usize = 8;
const OFF_LEADER_EPOCH: usize = 12;
const OFF_MAGIC: usize = 16;
const OFF_CRC: usize = 17;
const OFF_ATTRIBUTES: usize = 21;
const OFF_LAST_OFFSET_DELTA: usize = 23;
const OFF_BASE_TIMESTAMP: usize = 27;
const OFF_MAX_TIMESTAMP: usize = 35;
const OFF_PRODUCER_ID: usize = 43;
const OFF_PRODUCER_EPOCH: usize = 51;
const OFF_BASE_SEQUENCE: usize = 53;
const OFF_RECORD_COUNT: usize = 57;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Compression {
    #[default]
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl Compression {
    pub fn from_attributes(attr: i16) -> Result<Self, ProtocolError> {
        Ok(match attr & 0x7 {
            0 => Self::None,
            1 => Self::Gzip,
            2 => Self::Snappy,
            3 => Self::Lz4,
            4 => Self::Zstd,
            _ => return Err(ProtocolError::Malformed("unknown compression codec")),
        })
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "none" | "" => Self::None,
            "gzip" => Self::Gzip,
            "snappy" => Self::Snappy,
            "lz4" => Self::Lz4,
            "zstd" => Self::Zstd,
            _ => return None,
        })
    }
}

#[inline]
fn be_i16(b: &[u8], at: usize) -> i16 {
    i16::from_be_bytes([b[at], b[at + 1]])
}
#[inline]
fn be_i32(b: &[u8], at: usize) -> i32 {
    i32::from_be_bytes(b[at..at + 4].try_into().unwrap())
}
#[inline]
fn be_i64(b: &[u8], at: usize) -> i64 {
    i64::from_be_bytes(b[at..at + 8].try_into().unwrap())
}

/// Borrowed view over one record batch header.
#[derive(Clone, Copy)]
pub struct BatchHeader<'a> {
    raw: &'a [u8],
}

impl<'a> BatchHeader<'a> {
    /// Parses the header of the batch at the start of `raw`. `raw` may contain more batches.
    pub fn parse(raw: &'a [u8]) -> Result<Self, ProtocolError> {
        if raw.len() < BATCH_HEADER_LEN {
            return Err(ProtocolError::Truncated);
        }
        let h = Self { raw };
        if h.magic() != 2 {
            return Err(ProtocolError::UnsupportedMagic(h.magic()));
        }
        let len = h.batch_length();
        if len < (BATCH_HEADER_LEN - LOG_OVERHEAD) as i32 {
            return Err(ProtocolError::Malformed("batch length too small"));
        }
        Ok(h)
    }

    pub fn base_offset(&self) -> i64 {
        be_i64(self.raw, OFF_BASE_OFFSET)
    }
    pub fn batch_length(&self) -> i32 {
        be_i32(self.raw, OFF_BATCH_LENGTH)
    }
    /// Total size of this batch on the wire, header included.
    pub fn total_size(&self) -> usize {
        self.batch_length() as usize + LOG_OVERHEAD
    }
    pub fn magic(&self) -> i8 {
        self.raw[OFF_MAGIC] as i8
    }
    pub fn crc(&self) -> u32 {
        be_i32(self.raw, OFF_CRC) as u32
    }
    pub fn attributes(&self) -> i16 {
        be_i16(self.raw, OFF_ATTRIBUTES)
    }
    pub fn compression(&self) -> Result<Compression, ProtocolError> {
        Compression::from_attributes(self.attributes())
    }
    pub fn is_transactional(&self) -> bool {
        self.attributes() & 0x10 != 0
    }
    pub fn is_control(&self) -> bool {
        self.attributes() & 0x20 != 0
    }
    pub fn last_offset_delta(&self) -> i32 {
        be_i32(self.raw, OFF_LAST_OFFSET_DELTA)
    }
    pub fn last_offset(&self) -> i64 {
        self.base_offset() + self.last_offset_delta() as i64
    }
    pub fn base_timestamp(&self) -> i64 {
        be_i64(self.raw, OFF_BASE_TIMESTAMP)
    }
    pub fn max_timestamp(&self) -> i64 {
        be_i64(self.raw, OFF_MAX_TIMESTAMP)
    }
    pub fn producer_id(&self) -> i64 {
        be_i64(self.raw, OFF_PRODUCER_ID)
    }
    pub fn producer_epoch(&self) -> i16 {
        be_i16(self.raw, OFF_PRODUCER_EPOCH)
    }
    pub fn base_sequence(&self) -> i32 {
        be_i32(self.raw, OFF_BASE_SEQUENCE)
    }
    pub fn record_count(&self) -> i32 {
        be_i32(self.raw, OFF_RECORD_COUNT)
    }

    /// Verifies CRC-32C over attributes..end. Requires the full batch in `raw`.
    pub fn validate_crc(&self) -> Result<(), ProtocolError> {
        let end = self.total_size();
        if self.raw.len() < end {
            return Err(ProtocolError::Truncated);
        }
        let actual = crc32c::crc32c(&self.raw[OFF_ATTRIBUTES..end]);
        if actual != self.crc() {
            return Err(ProtocolError::CrcMismatch);
        }
        Ok(())
    }
}

/// Summary of a validated produce payload (which may hold several batches).
#[derive(Debug, Clone, Copy)]
pub struct BatchSetInfo {
    pub batches: usize,
    pub record_count: i64,
    /// Offset delta of the last record relative to the first batch's first record.
    pub offset_span: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub last_sequence: i32,
    pub transactional: bool,
}

/// Validates every batch in a produce payload: framing, magic, CRC.
pub fn validate_batches(raw: &[u8]) -> Result<BatchSetInfo, ProtocolError> {
    let mut pos = 0;
    let mut info = BatchSetInfo {
        batches: 0,
        record_count: 0,
        offset_span: 0,
        max_timestamp: -1,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        last_sequence: -1,
        transactional: false,
    };
    while pos < raw.len() {
        let h = BatchHeader::parse(&raw[pos..])?;
        h.validate_crc()?;
        if info.batches == 0 {
            info.producer_id = h.producer_id();
            info.producer_epoch = h.producer_epoch();
            info.base_sequence = h.base_sequence();
            info.transactional = h.is_transactional();
        }
        info.last_sequence = if h.base_sequence() >= 0 {
            h.base_sequence().wrapping_add(h.last_offset_delta())
        } else {
            -1
        };
        info.offset_span += h.last_offset_delta() as i64 + 1;
        info.record_count += h.record_count() as i64;
        info.max_timestamp = info.max_timestamp.max(h.max_timestamp());
        info.batches += 1;
        pos += h.total_size();
    }
    if info.batches == 0 {
        return Err(ProtocolError::Malformed("empty record set"));
    }
    if pos != raw.len() {
        return Err(ProtocolError::Truncated);
    }
    Ok(info)
}

/// Rewrites the base offset of every batch in `raw` so the first batch starts at `base`.
/// Returns the last offset written. The CRC does not cover the base offset, so no re-hash.
pub fn assign_offsets(raw: &mut [u8], base: i64) -> i64 {
    let mut pos = 0;
    let mut next = base;
    let mut last = base - 1;
    while pos + BATCH_HEADER_LEN <= raw.len() {
        let delta = be_i32(raw, pos + OFF_LAST_OFFSET_DELTA);
        let len = be_i32(raw, pos + OFF_BATCH_LENGTH) as usize + LOG_OVERHEAD;
        raw[pos..pos + 8].copy_from_slice(&next.to_be_bytes());
        // Single-node leader epoch is always 0.
        raw[pos + OFF_LEADER_EPOCH..pos + OFF_LEADER_EPOCH + 4].copy_from_slice(&0i32.to_be_bytes());
        last = next + delta as i64;
        next = last + 1;
        pos += len;
    }
    last
}

/// Iterates over complete batches in a buffer holding stored (offset-assigned) batches.
pub struct BatchIter<'a> {
    raw: &'a [u8],
    pos: usize,
}

impl<'a> BatchIter<'a> {
    pub fn new(raw: &'a [u8]) -> Self {
        Self { raw, pos: 0 }
    }
    pub fn position(&self) -> usize {
        self.pos
    }
}

impl<'a> Iterator for BatchIter<'a> {
    /// (position of the batch in the buffer, header)
    type Item = (usize, BatchHeader<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + BATCH_HEADER_LEN > self.raw.len() {
            return None;
        }
        let h = BatchHeader::parse(&self.raw[self.pos..]).ok()?;
        if self.pos + h.total_size() > self.raw.len() {
            return None;
        }
        let at = self.pos;
        self.pos += h.total_size();
        Some((at, h))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

/// A decoded record with absolute offset and timestamp.
#[derive(Debug, Clone)]
pub struct Record {
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
}

impl Record {
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers.iter().find(|h| h.key == name).and_then(|h| h.value.as_deref())
    }
}

fn decompress(codec: Compression, data: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    let err = |_| ProtocolError::Compression;
    Ok(match codec {
        Compression::None => data.to_vec(),
        Compression::Gzip => {
            let mut out = Vec::with_capacity(data.len() * 4);
            flate2::read::GzDecoder::new(data).read_to_end(&mut out).map_err(err)?;
            out
        }
        Compression::Snappy => decompress_snappy(data)?,
        Compression::Lz4 => {
            let mut out = Vec::with_capacity(data.len() * 4);
            lz4_flex::frame::FrameDecoder::new(data).read_to_end(&mut out).map_err(err)?;
            out
        }
        Compression::Zstd => zstd::stream::decode_all(data).map_err(err)?,
    })
}

const XERIAL_MAGIC: &[u8] = b"\x82SNAPPY\x00";

/// Kafka's Java client writes snappy in xerial framing; librdkafka may write raw snappy.
fn decompress_snappy(data: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    if data.len() >= 16 && data.starts_with(XERIAL_MAGIC) {
        let mut pos = 16;
        let mut out = Vec::with_capacity(data.len() * 4);
        while pos + 4 <= data.len() {
            let len = be_i32(data, pos) as usize;
            pos += 4;
            let block = data.get(pos..pos + len).ok_or(ProtocolError::Compression)?;
            out.extend(snap::raw::Decoder::new().decompress_vec(block).map_err(|_| ProtocolError::Compression)?);
            pos += len;
        }
        Ok(out)
    } else {
        snap::raw::Decoder::new().decompress_vec(data).map_err(|_| ProtocolError::Compression)
    }
}

fn compress(codec: Compression, data: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    let err = |_| ProtocolError::Compression;
    Ok(match codec {
        Compression::None => data.to_vec(),
        Compression::Gzip => {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            enc.write_all(data).map_err(err)?;
            enc.finish().map_err(err)?
        }
        Compression::Snappy => {
            // xerial framing for maximum client compatibility.
            let block = snap::raw::Encoder::new().compress_vec(data).map_err(|_| ProtocolError::Compression)?;
            let mut out = Vec::with_capacity(block.len() + 20);
            out.extend_from_slice(XERIAL_MAGIC);
            out.extend_from_slice(&1i32.to_be_bytes());
            out.extend_from_slice(&1i32.to_be_bytes());
            out.extend_from_slice(&(block.len() as i32).to_be_bytes());
            out.extend(block);
            out
        }
        Compression::Lz4 => {
            let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
            enc.write_all(data).map_err(err)?;
            enc.finish().map_err(|_| ProtocolError::Compression)?
        }
        Compression::Zstd => zstd::stream::encode_all(data, 3).map_err(err)?,
    })
}

/// Decodes all records of the batch at the start of `raw`, skipping control batches.
pub fn decode_batch(raw: &[u8]) -> Result<Vec<Record>, ProtocolError> {
    let h = BatchHeader::parse(raw)?;
    if h.is_control() {
        return Ok(Vec::new());
    }
    let end = h.total_size();
    let body = raw.get(BATCH_HEADER_LEN..end).ok_or(ProtocolError::Truncated)?;
    let decompressed;
    let body = match h.compression()? {
        Compression::None => body,
        c => {
            decompressed = decompress(c, body)?;
            &decompressed[..]
        }
    };
    let base_offset = h.base_offset();
    let base_ts = h.base_timestamp();
    let log_append_time = h.attributes() & 0x08 != 0;
    let count = h.record_count().max(0) as usize;
    let mut out = Vec::with_capacity(count);
    let mut pos = 0;
    let body_bytes = Bytes::copy_from_slice(body);
    for _ in 0..count {
        let len = read_varint(body, &mut pos)? as usize;
        let start = pos;
        pos += 1; // attributes
        let ts_delta = read_varlong(body, &mut pos)?;
        let off_delta = read_varint(body, &mut pos)?;
        let key = read_field(&body_bytes, body, &mut pos)?;
        let value = read_field(&body_bytes, body, &mut pos)?;
        let hcount = read_varint(body, &mut pos)?.max(0) as usize;
        let mut headers = Vec::with_capacity(hcount);
        for _ in 0..hcount {
            let k = read_field(&body_bytes, body, &mut pos)?.unwrap_or_default();
            let v = read_field(&body_bytes, body, &mut pos)?;
            headers.push(Header { key: String::from_utf8_lossy(&k).into_owned(), value: v });
        }
        if pos != start + len {
            return Err(ProtocolError::Malformed("record length mismatch"));
        }
        out.push(Record {
            offset: base_offset + off_delta as i64,
            timestamp: if log_append_time { h.max_timestamp() } else { base_ts + ts_delta },
            key,
            value,
            headers,
        });
    }
    Ok(out)
}

fn read_field(all: &Bytes, buf: &[u8], pos: &mut usize) -> Result<Option<Bytes>, ProtocolError> {
    let len = read_varint(buf, pos)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if *pos + len > buf.len() {
        return Err(ProtocolError::Truncated);
    }
    let b = all.slice(*pos..*pos + len);
    *pos += len;
    Ok(Some(b))
}

/// Outcome of [`rebuild_batch`].
#[derive(Debug)]
pub enum Rebuilt {
    /// Every record was kept: reuse the original bytes.
    Unchanged,
    /// No record was kept: drop the batch.
    Empty,
    /// Some records were kept: a new batch holding only those.
    New(BytesMut),
}

/// Rewrites the stored batch at the start of `raw`, keeping only the records for which `keep`
/// returns true (log compaction). The new batch keeps the original base offset, last offset
/// delta, attributes (compression, timestamp type, transactional flag) and producer fields,
/// so offsets never change and consumers continue after the batch exactly as before.
/// Control batches are never rewritten.
pub fn rebuild_batch(raw: &[u8], mut keep: impl FnMut(&Record) -> bool) -> Result<Rebuilt, ProtocolError> {
    let h = BatchHeader::parse(raw)?;
    if h.is_control() {
        return Ok(Rebuilt::Unchanged);
    }
    let records = decode_batch(raw)?;
    let kept: Vec<&Record> = records.iter().filter(|r| keep(r)).collect();
    if kept.len() == records.len() {
        return Ok(Rebuilt::Unchanged);
    }
    if kept.is_empty() {
        return Ok(Rebuilt::Empty);
    }
    let compression = h.compression()?;
    let log_append_time = h.attributes() & 0x08 != 0;
    let base_offset = h.base_offset();
    let base_ts = if log_append_time { h.base_timestamp() } else { kept[0].timestamp };
    let max_ts = if log_append_time { h.max_timestamp() } else { kept.iter().map(|r| r.timestamp).max().unwrap_or(-1) };
    let mut body = Vec::with_capacity(raw.len());
    for r in &kept {
        // Log-append-time batches carry one timestamp for all records (deltas are ignored).
        let ts_delta = if log_append_time { 0 } else { r.timestamp - base_ts };
        let off_delta = r.offset - base_offset;
        let field_len = |b: &Option<Bytes>| match b {
            Some(b) => varlong_len(b.len() as i64) + b.len(),
            None => varlong_len(-1),
        };
        let mut len = 1 + varlong_len(ts_delta) + varlong_len(off_delta);
        len += field_len(&r.key) + field_len(&r.value);
        len += varlong_len(r.headers.len() as i64);
        for hd in &r.headers {
            len += varlong_len(hd.key.len() as i64) + hd.key.len() + field_len(&hd.value);
        }
        write_varlong(&mut body, len as i64);
        body.push(0);
        write_varlong(&mut body, ts_delta);
        write_varlong(&mut body, off_delta);
        write_field(&mut body, r.key.as_deref());
        write_field(&mut body, r.value.as_deref());
        write_varlong(&mut body, r.headers.len() as i64);
        for hd in &r.headers {
            write_field(&mut body, Some(hd.key.as_bytes()));
            write_field(&mut body, hd.value.as_deref());
        }
    }
    let payload = compress(compression, &body)?;
    let total = BATCH_HEADER_LEN + payload.len();
    let mut b = BytesMut::with_capacity(total);
    b.put_i64(base_offset);
    b.put_i32((total - LOG_OVERHEAD) as i32);
    b.put_i32(0);
    b.put_i8(2);
    b.put_u32(0); // crc placeholder
    b.put_i16(h.attributes());
    b.put_i32(h.last_offset_delta());
    b.put_i64(base_ts);
    b.put_i64(max_ts);
    b.put_i64(h.producer_id());
    b.put_i16(h.producer_epoch());
    b.put_i32(h.base_sequence());
    b.put_i32(kept.len() as i32);
    b.put_slice(&payload);
    let crc = crc32c::crc32c(&b[OFF_ATTRIBUTES..]);
    b[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_be_bytes());
    Ok(Rebuilt::New(b))
}

/// Decodes every record in a buffer of stored batches, dropping records below `min_offset`.
pub fn decode_records(raw: &[u8], min_offset: i64) -> Result<Vec<Record>, ProtocolError> {
    let mut out = Vec::new();
    for (pos, h) in BatchIter::new(raw) {
        if h.last_offset() < min_offset {
            continue;
        }
        out.extend(decode_batch(&raw[pos..])?.into_iter().filter(|r| r.offset >= min_offset));
    }
    Ok(out)
}

/// Record to be encoded by [`BatchBuilder`].
#[derive(Debug, Clone, Default)]
pub struct NewRecord {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
    pub timestamp: Option<i64>,
}

/// Builds a single magic v2 record batch (base offset 0, to be assigned by the broker).
pub struct BatchBuilder {
    body: Vec<u8>,
    count: i32,
    base_ts: i64,
    max_ts: i64,
    compression: Compression,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

impl BatchBuilder {
    pub fn new(compression: Compression) -> Self {
        Self {
            body: Vec::with_capacity(1024),
            count: 0,
            base_ts: -1,
            max_ts: -1,
            compression,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
        }
    }

    pub fn idempotent(mut self, producer_id: i64, epoch: i16, base_sequence: i32) -> Self {
        self.producer_id = producer_id;
        self.producer_epoch = epoch;
        self.base_sequence = base_sequence;
        self
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn estimated_size(&self) -> usize {
        BATCH_HEADER_LEN + self.body.len()
    }

    pub fn push(&mut self, rec: &NewRecord, now_ms: i64) {
        let ts = rec.timestamp.unwrap_or(now_ms);
        if self.count == 0 {
            self.base_ts = ts;
        }
        self.max_ts = self.max_ts.max(ts);
        let ts_delta = ts - self.base_ts;
        let off_delta = self.count as i64;

        let field_len = |b: &Option<Bytes>| match b {
            Some(b) => varlong_len(b.len() as i64) + b.len(),
            None => varlong_len(-1),
        };
        let mut len = 1 + varlong_len(ts_delta) + varlong_len(off_delta);
        len += field_len(&rec.key) + field_len(&rec.value);
        len += varlong_len(rec.headers.len() as i64);
        for h in &rec.headers {
            len += varlong_len(h.key.len() as i64) + h.key.len() + field_len(&h.value);
        }

        let out = &mut self.body;
        write_varlong(out, len as i64);
        out.push(0);
        write_varlong(out, ts_delta);
        write_varlong(out, off_delta);
        write_field(out, rec.key.as_deref());
        write_field(out, rec.value.as_deref());
        write_varlong(out, rec.headers.len() as i64);
        for h in &rec.headers {
            write_field(out, Some(h.key.as_bytes()));
            write_field(out, h.value.as_deref());
        }
        self.count += 1;
    }

    pub fn build(self) -> Result<BytesMut, ProtocolError> {
        let payload = compress(self.compression, &self.body)?;
        let total = BATCH_HEADER_LEN + payload.len();
        let mut b = BytesMut::with_capacity(total);
        b.put_i64(0);
        b.put_i32((total - LOG_OVERHEAD) as i32);
        b.put_i32(0);
        b.put_i8(2);
        b.put_u32(0); // crc placeholder
        b.put_i16(self.compression as i16);
        b.put_i32((self.count - 1).max(0));
        b.put_i64(self.base_ts);
        b.put_i64(self.max_ts);
        b.put_i64(self.producer_id);
        b.put_i16(self.producer_epoch);
        b.put_i32(self.base_sequence);
        b.put_i32(self.count);
        b.put_slice(&payload);
        let crc = crc32c::crc32c(&b[OFF_ATTRIBUTES..]);
        b[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_be_bytes());
        Ok(b)
    }
}

fn write_field(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        Some(b) => {
            write_varlong(out, b.len() as i64);
            out.extend_from_slice(b);
        }
        None => write_varlong(out, -1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(n: usize, c: Compression) -> BytesMut {
        let mut b = BatchBuilder::new(c);
        for i in 0..n {
            b.push(
                &NewRecord {
                    key: Some(Bytes::from(format!("k{i}"))),
                    value: Some(Bytes::from(format!("{{\"n\":{i}}}"))),
                    headers: vec![Header { key: "region".into(), value: Some(Bytes::from_static(b"ID")) }],
                    timestamp: Some(1_000 + i as i64),
                },
                0,
            );
        }
        b.build().unwrap()
    }

    #[test]
    fn build_validate_decode_all_codecs() {
        for c in [Compression::None, Compression::Gzip, Compression::Snappy, Compression::Lz4, Compression::Zstd] {
            let mut raw = sample(5, c);
            let info = validate_batches(&raw).unwrap();
            assert_eq!(info.record_count, 5);
            assert_eq!(info.offset_span, 5);
            assert_eq!(assign_offsets(&mut raw, 100), 104);
            // CRC still valid after patching the base offset.
            validate_batches(&raw).unwrap();
            let recs = decode_records(&raw, 102).unwrap();
            assert_eq!(recs.len(), 3);
            assert_eq!(recs[0].offset, 102);
            assert_eq!(recs[0].timestamp, 1_002);
            assert_eq!(recs[0].header("region"), Some(&b"ID"[..]));
            assert_eq!(&recs[2].value.as_ref().unwrap()[..], b"{\"n\":4}");
        }
    }

    #[test]
    fn rebuild_keeps_offsets_and_attributes() {
        for c in [Compression::None, Compression::Lz4, Compression::Zstd] {
            let mut raw = sample(6, c);
            assign_offsets(&mut raw, 40);
            let Rebuilt::New(out) = rebuild_batch(&raw, |r| r.offset % 2 == 1).unwrap() else { panic!("expected a new batch") };
            let info = validate_batches(&out).unwrap();
            assert_eq!(info.record_count, 3);
            // The last offset delta is preserved so the next batch still continues at 46.
            let h = BatchHeader::parse(&out).unwrap();
            assert_eq!((h.base_offset(), h.last_offset()), (40, 45));
            assert_eq!(h.compression().unwrap(), c);
            let recs = decode_records(&out, 0).unwrap();
            assert_eq!(recs.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![41, 43, 45]);
            assert_eq!(recs.iter().map(|r| r.timestamp).collect::<Vec<_>>(), vec![1_001, 1_003, 1_005]);
            assert_eq!(&recs[1].key.as_ref().unwrap()[..], b"k3");
            assert_eq!(recs[2].header("region"), Some(&b"ID"[..]));
        }
        let raw = sample(3, Compression::None);
        assert!(matches!(rebuild_batch(&raw, |_| true).unwrap(), Rebuilt::Unchanged));
        assert!(matches!(rebuild_batch(&raw, |_| false).unwrap(), Rebuilt::Empty));
    }

    #[test]
    fn crc_mismatch_detected() {
        let mut raw = sample(2, Compression::None);
        let last = raw.len() - 1;
        raw[last] ^= 0xff;
        assert!(matches!(validate_batches(&raw), Err(ProtocolError::CrcMismatch)));
    }

    #[test]
    fn multiple_batches_assign_contiguous_offsets() {
        let mut raw = sample(3, Compression::None);
        raw.extend_from_slice(&sample(2, Compression::None));
        assert_eq!(validate_batches(&raw).unwrap().offset_span, 5);
        assert_eq!(assign_offsets(&mut raw, 10), 14);
        let offs: Vec<i64> = BatchIter::new(&raw).map(|(_, h)| h.base_offset()).collect();
        assert_eq!(offs, vec![10, 13]);
    }
}

/// Kafka's default partitioner hash (murmur2), so keyed records land on the same partition
/// whether they are produced over HTTP or by a Java / librdkafka client.
pub fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;
    let len = data.len();
    let mut h: u32 = SEED ^ len as u32;
    let chunks = data.chunks_exact(4);
    let tail = chunks.remainder();
    for c in chunks {
        let mut k = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    match tail.len() {
        3 => {
            h ^= (tail[2] as u32) << 16;
            h ^= (tail[1] as u32) << 8;
            h ^= tail[0] as u32;
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= (tail[1] as u32) << 8;
            h ^= tail[0] as u32;
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= tail[0] as u32;
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// Partition for a key, identical to Kafka's `DefaultPartitioner` for keyed records.
pub fn partition_for_key(key: &[u8], partitions: i32) -> i32 {
    ((murmur2(key) & 0x7fff_ffff) % partitions.max(1)) as i32
}

#[cfg(test)]
mod murmur_tests {
    #[test]
    fn matches_kafka_reference_values() {
        // Reference values from Kafka's UtilsTest.testMurmur2.
        assert_eq!(super::murmur2(b"21"), -973932308);
        assert_eq!(super::murmur2(b"foobar"), -790332482);
        assert_eq!(super::murmur2(b"a-little-bit-long-string"), -985981536);
        assert_eq!(super::murmur2(b"a-little-bit-longer-string"), -1486304829);
        assert_eq!(super::murmur2(b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"), -58897971);
        assert_eq!(super::murmur2(b"abc"), 479470107);
    }
}
