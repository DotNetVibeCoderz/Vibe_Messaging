//! BigPipe HTTP surfaces.
//!
//! - [`data_router`] (port 8082): produce/consume over REST, Server-Sent Events streaming with
//!   server-side filters, server-assigned consumer groups and share groups. The Python,
//!   TypeScript, Go and Java SDKs use this API.
//! - [`admin_router`] (port 9644): topics, configs, storage-mode migration, groups and lag,
//!   message browser, flows, cluster info. Used by the .NET control plane, Console and `bpctl`.
//! - [`metrics_router`] (port 9645): OpenMetrics.
//!
//! Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.

mod admin;
mod data;

use std::sync::Arc;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use bp_broker::{Broker, BrokerError};
use bp_protocol::records::Record;
use serde_json::{Value, json};

pub use admin::admin_router;
pub use data::data_router;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const CREDIT: &str = "Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil";

pub type AppState = Arc<Broker>;

/// Uniform JSON error: `{"error": {"code": "...", "message": "..."}}`.
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, code: "bad_request", message: msg.into() }
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::NOT_FOUND, code: "not_found", message: msg.into() }
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::CONFLICT, code: "conflict", message: msg.into() }
    }
    pub fn unauthorized() -> Self {
        Self { status: StatusCode::UNAUTHORIZED, code: "unauthorized", message: "missing or invalid API key".into() }
    }
}

impl From<BrokerError> for ApiError {
    fn from(e: BrokerError) -> Self {
        let status = StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let code = match &e {
            BrokerError::UnknownTopic(_) => "unknown_topic",
            BrokerError::TopicExists(_) => "topic_exists",
            BrokerError::InvalidTopic(_) => "invalid_topic",
            BrokerError::InvalidPartitions(_) => "invalid_partitions",
            BrokerError::InvalidConfig(_) => "invalid_config",
            BrokerError::InvalidRecord(_) => "invalid_record",
            BrokerError::TooLarge(_) => "too_large",
            BrokerError::Transactional => "unsupported",
            BrokerError::Storage(bp_storage::StorageError::OffsetOutOfRange { .. }) => "offset_out_of_range",
            BrokerError::Storage(_) => "storage_error",
        };
        Self { status, code, message: e.to_string() }
    }
}

impl From<bp_expr::ExprError> for ApiError {
    fn from(e: bp_expr::ExprError) -> Self {
        Self { status: StatusCode::BAD_REQUEST, code: "invalid_filter", message: e.to_string() }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": { "code": self.code, "message": self.message } }))).into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

/// JSON shape of a record returned by every HTTP API.
///
/// `value` / `key` carry UTF-8 text; binary payloads are base64 with `*_encoding: "base64"`.
pub fn record_json(topic: &str, partition: i32, r: &Record) -> Value {
    let (value, value_enc) = encode_payload(r.value.as_deref());
    let (key, key_enc) = encode_payload(r.key.as_deref());
    let headers: serde_json::Map<String, Value> = r
        .headers
        .iter()
        .map(|h| (h.key.clone(), h.value.as_deref().map(|v| Value::String(String::from_utf8_lossy(v).into_owned())).unwrap_or(Value::Null)))
        .collect();
    json!({
        "topic": topic,
        "partition": partition,
        "offset": r.offset,
        "timestamp": r.timestamp,
        "key": key,
        "key_encoding": key_enc,
        "value": value,
        "value_encoding": value_enc,
        "headers": headers,
    })
}

fn encode_payload(b: Option<&[u8]>) -> (Value, &'static str) {
    match b {
        None => (Value::Null, "utf8"),
        Some(b) => match std::str::from_utf8(b) {
            Ok(s) => (Value::String(s.to_string()), "utf8"),
            Err(_) => (Value::String(base64::engine::general_purpose::STANDARD.encode(b)), "base64"),
        },
    }
}

pub fn metrics_router(broker: AppState) -> axum::Router {
    use axum::routing::get;
    axum::Router::new()
        .route(
            "/metrics",
            get(|axum::extract::State(b): axum::extract::State<AppState>| async move {
                let extra = format!(
                    "# HELP bp_topics Topics hosted\n# TYPE bp_topics gauge\nbp_topics {}\n# HELP bp_diskless_files_referenced Live diskless objects\n# TYPE bp_diskless_files_referenced gauge\nbp_diskless_files_referenced {}\n# HELP bp_object_cache_bytes Object cache size\n# TYPE bp_object_cache_bytes gauge\nbp_object_cache_bytes {}\n",
                    b.topics().len(),
                    b.diskless_files_referenced(),
                    b.store.cached_bytes()
                );
                (
                    [(axum::http::header::CONTENT_TYPE, "application/openmetrics-text; version=1.0.0; charset=utf-8")],
                    b.metrics.render_openmetrics(&extra),
                )
            }),
        )
        .with_state(broker)
}
