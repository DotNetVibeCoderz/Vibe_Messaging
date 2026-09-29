//! BigPipe wire protocol: Kafka-compatible request/response codec and record batch format.
//!
//! Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.

pub mod codec;
pub mod messages;
pub mod records;

pub use codec::{Decoder, Encoder};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("frame truncated")]
    Truncated,
    #[error("malformed request: {0}")]
    Malformed(&'static str),
    #[error("unsupported api key {0}")]
    UnsupportedApi(i16),
    #[error("unsupported record batch magic {0}")]
    UnsupportedMagic(i8),
    #[error("record batch CRC mismatch")]
    CrcMismatch,
    #[error("compression codec error")]
    Compression,
}
