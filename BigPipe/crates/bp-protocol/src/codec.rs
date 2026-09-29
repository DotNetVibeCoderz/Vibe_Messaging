//! Primitive Kafka wire types.
//!
//! [`Decoder`] consumes a request frame held in a [`BytesMut`]. Byte fields are carved out with
//! `split_to`, so record batches keep their own uniquely owned buffer: the broker can patch the
//! base offset in place and freeze it without copying the payload.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::ProtocolError;

pub type Result<T> = std::result::Result<T, ProtocolError>;

pub struct Decoder {
    buf: BytesMut,
}

impl Decoder {
    pub fn new(buf: BytesMut) -> Self {
        Self { buf }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    fn need(&self, n: usize) -> Result<()> {
        if self.buf.len() < n {
            Err(ProtocolError::Truncated)
        } else {
            Ok(())
        }
    }

    pub fn bool(&mut self) -> Result<bool> {
        Ok(self.i8()? != 0)
    }

    pub fn i8(&mut self) -> Result<i8> {
        self.need(1)?;
        Ok(self.buf.get_i8())
    }

    pub fn i16(&mut self) -> Result<i16> {
        self.need(2)?;
        Ok(self.buf.get_i16())
    }

    pub fn i32(&mut self) -> Result<i32> {
        self.need(4)?;
        Ok(self.buf.get_i32())
    }

    pub fn i64(&mut self) -> Result<i64> {
        self.need(8)?;
        Ok(self.buf.get_i64())
    }

    pub fn uvarint(&mut self) -> Result<u32> {
        let mut value: u32 = 0;
        let mut shift = 0;
        loop {
            let b = self.i8()? as u8;
            value |= ((b & 0x7f) as u32) << shift;
            if b & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift > 28 {
                return Err(ProtocolError::Malformed("varint too long"));
            }
        }
    }

    fn take(&mut self, n: usize) -> Result<BytesMut> {
        self.need(n)?;
        Ok(self.buf.split_to(n))
    }

    fn utf8(b: BytesMut) -> Result<String> {
        String::from_utf8(b.to_vec()).map_err(|_| ProtocolError::Malformed("invalid utf-8"))
    }

    pub fn string(&mut self) -> Result<String> {
        self.nullable_string()?.ok_or(ProtocolError::Malformed("unexpected null string"))
    }

    pub fn nullable_string(&mut self) -> Result<Option<String>> {
        let len = self.i16()?;
        if len < 0 {
            return Ok(None);
        }
        let b = self.take(len as usize)?;
        Ok(Some(Self::utf8(b)?))
    }

    pub fn compact_string(&mut self) -> Result<String> {
        self.compact_nullable_string()?.ok_or(ProtocolError::Malformed("unexpected null string"))
    }

    pub fn compact_nullable_string(&mut self) -> Result<Option<String>> {
        let len = self.uvarint()?;
        if len == 0 {
            return Ok(None);
        }
        let b = self.take(len as usize - 1)?;
        Ok(Some(Self::utf8(b)?))
    }

    pub fn bytes(&mut self) -> Result<BytesMut> {
        self.nullable_bytes()?.ok_or(ProtocolError::Malformed("unexpected null bytes"))
    }

    pub fn nullable_bytes(&mut self) -> Result<Option<BytesMut>> {
        let len = self.i32()?;
        if len < 0 {
            return Ok(None);
        }
        Ok(Some(self.take(len as usize)?))
    }

    /// Classic array length; `None` for a null array.
    pub fn array_len(&mut self) -> Result<Option<usize>> {
        let len = self.i32()?;
        if len < 0 {
            return Ok(None);
        }
        let len = len as usize;
        // Each element takes at least one byte; reject absurd lengths before allocating.
        if len > self.buf.len() {
            return Err(ProtocolError::Malformed("array length exceeds frame"));
        }
        Ok(Some(len))
    }

    pub fn array<T>(&mut self, mut f: impl FnMut(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let n = self.array_len()?.unwrap_or(0);
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(f(self)?);
        }
        Ok(out)
    }

    pub fn nullable_array<T>(&mut self, mut f: impl FnMut(&mut Self) -> Result<T>) -> Result<Option<Vec<T>>> {
        match self.array_len()? {
            None => Ok(None),
            Some(n) => {
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    out.push(f(self)?);
                }
                Ok(Some(out))
            }
        }
    }

    pub fn compact_array<T>(&mut self, mut f: impl FnMut(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let n = self.uvarint()? as usize;
        let n = n.saturating_sub(1);
        if n > self.buf.len() {
            return Err(ProtocolError::Malformed("array length exceeds frame"));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(f(self)?);
        }
        Ok(out)
    }

    /// Skips a tagged-field section (flexible versions). BigPipe defines no tagged fields.
    pub fn tagged_fields(&mut self) -> Result<()> {
        let n = self.uvarint()?;
        for _ in 0..n {
            let _tag = self.uvarint()?;
            let size = self.uvarint()? as usize;
            self.take(size)?;
        }
        Ok(())
    }
}

/// Response writer. Large record payloads are appended as separate chunks so fetch responses
/// never copy record data into the header buffer.
#[derive(Default)]
pub struct Encoder {
    buf: BytesMut,
    chunks: Vec<Bytes>,
    chunked_len: usize,
}

impl Encoder {
    pub fn new() -> Self {
        Self { buf: BytesMut::with_capacity(256), chunks: Vec::new(), chunked_len: 0 }
    }

    pub fn len(&self) -> usize {
        self.chunked_len + self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn bool(&mut self, v: bool) {
        self.buf.put_i8(v as i8);
    }
    pub fn i8(&mut self, v: i8) {
        self.buf.put_i8(v);
    }
    pub fn i16(&mut self, v: i16) {
        self.buf.put_i16(v);
    }
    pub fn i32(&mut self, v: i32) {
        self.buf.put_i32(v);
    }
    pub fn i64(&mut self, v: i64) {
        self.buf.put_i64(v);
    }

    pub fn uvarint(&mut self, mut v: u32) {
        while v >= 0x80 {
            self.buf.put_u8((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.put_u8(v as u8);
    }

    pub fn string(&mut self, s: &str) {
        self.buf.put_i16(s.len() as i16);
        self.buf.put_slice(s.as_bytes());
    }

    pub fn nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.string(s),
            None => self.buf.put_i16(-1),
        }
    }

    pub fn compact_string(&mut self, s: &str) {
        self.uvarint(s.len() as u32 + 1);
        self.buf.put_slice(s.as_bytes());
    }

    pub fn compact_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.compact_string(s),
            None => self.uvarint(0),
        }
    }

    pub fn bytes(&mut self, b: &[u8]) {
        self.buf.put_i32(b.len() as i32);
        self.buf.put_slice(b);
    }

    pub fn nullable_bytes(&mut self, b: Option<&[u8]>) {
        match b {
            Some(b) => self.bytes(b),
            None => self.buf.put_i32(-1),
        }
    }

    /// Writes a `NULLABLE_BYTES` field whose content is a list of chunks, without copying them.
    pub fn records(&mut self, chunks: Option<&[Bytes]>) {
        let Some(chunks) = chunks else {
            self.buf.put_i32(-1);
            return;
        };
        let total: usize = chunks.iter().map(Bytes::len).sum();
        self.buf.put_i32(total as i32);
        for c in chunks {
            if c.len() < 512 {
                self.buf.put_slice(c);
            } else {
                self.flush_buf();
                self.chunked_len += c.len();
                self.chunks.push(c.clone());
            }
        }
    }

    pub fn array_len(&mut self, n: usize) {
        self.buf.put_i32(n as i32);
    }

    pub fn compact_array_len(&mut self, n: usize) {
        self.uvarint(n as u32 + 1);
    }

    pub fn empty_tagged_fields(&mut self) {
        self.buf.put_u8(0);
    }

    fn flush_buf(&mut self) {
        if !self.buf.is_empty() {
            let b = self.buf.split().freeze();
            self.chunked_len += b.len();
            self.chunks.push(b);
        }
    }

    /// Finishes the response and prefixes it with the 4-byte frame size.
    pub fn finish_frame(mut self) -> Vec<Bytes> {
        self.flush_buf();
        let size = self.chunked_len as i32;
        let mut out = Vec::with_capacity(self.chunks.len() + 1);
        out.push(Bytes::copy_from_slice(&size.to_be_bytes()));
        out.extend(self.chunks);
        out
    }

    /// Finishes into a single contiguous buffer (no frame prefix). Used for client-side encoding.
    pub fn finish_contiguous(mut self) -> BytesMut {
        self.flush_buf();
        let mut out = BytesMut::with_capacity(self.chunked_len);
        for c in self.chunks {
            out.extend_from_slice(&c);
        }
        out
    }
}

/// Zig-zag varint helpers used inside record batches.
pub mod varint {
    use crate::ProtocolError;

    pub fn read_varint(buf: &[u8], pos: &mut usize) -> Result<i32, ProtocolError> {
        let v = read_varlong(buf, pos)?;
        i32::try_from(v).map_err(|_| ProtocolError::Malformed("varint overflow"))
    }

    pub fn read_varlong(buf: &[u8], pos: &mut usize) -> Result<i64, ProtocolError> {
        let mut raw: u64 = 0;
        let mut shift = 0;
        loop {
            let b = *buf.get(*pos).ok_or(ProtocolError::Truncated)?;
            *pos += 1;
            raw |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return Err(ProtocolError::Malformed("varlong too long"));
            }
        }
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }

    pub fn write_varlong(out: &mut Vec<u8>, v: i64) {
        let mut raw = ((v << 1) ^ (v >> 63)) as u64;
        while raw >= 0x80 {
            out.push((raw as u8) | 0x80);
            raw >>= 7;
        }
        out.push(raw as u8);
    }

    pub fn varlong_len(v: i64) -> usize {
        let mut raw = ((v << 1) ^ (v >> 63)) as u64;
        let mut n = 1;
        while raw >= 0x80 {
            raw >>= 7;
            n += 1;
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::varint::*;
    use super::*;

    #[test]
    fn varlong_roundtrip() {
        for v in [0i64, 1, -1, 63, -64, 64, 300, -300, i32::MAX as i64, i64::MIN, i64::MAX] {
            let mut out = Vec::new();
            write_varlong(&mut out, v);
            assert_eq!(out.len(), varlong_len(v));
            let mut pos = 0;
            assert_eq!(read_varlong(&out, &mut pos).unwrap(), v);
            assert_eq!(pos, out.len());
        }
    }

    #[test]
    fn strings_roundtrip() {
        let mut e = Encoder::new();
        e.string("hello");
        e.nullable_string(None);
        e.compact_string("héllo");
        e.compact_nullable_string(None);
        let mut d = Decoder::new(e.finish_contiguous());
        assert_eq!(d.string().unwrap(), "hello");
        assert_eq!(d.nullable_string().unwrap(), None);
        assert_eq!(d.compact_string().unwrap(), "héllo");
        assert_eq!(d.compact_nullable_string().unwrap(), None);
        assert_eq!(d.remaining(), 0);
    }

    #[test]
    fn truncated_is_error() {
        let mut d = Decoder::new(BytesMut::from(&[0u8, 5, b'a'][..]));
        assert!(matches!(d.string(), Err(ProtocolError::Truncated)));
    }
}
