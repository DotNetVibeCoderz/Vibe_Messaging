//! Data-plane REST API (port 8082).

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::Engine;
use bp_broker::share::{AckAction, ShareOptions};
use bp_broker::{Broker, RecordView};
use bp_expr::Expr;
use bp_protocol::records::{Compression, Header, NewRecord};
use bytes::Bytes;
use futures::Stream;
use serde::Deserialize;
use serde_json::{Value, json};
use tower_http::cors::CorsLayer;

use crate::{ApiError, ApiResult, AppState, CREDIT, VERSION, record_json};

pub fn data_router(b: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/topics", get(list_topics))
        .route("/v1/topics/{topic}/records", post(produce))
        .route("/v1/topics/{topic}/partitions/{partition}/records", get(read_partition))
        .route("/v1/topics/{topic}/stream", get(stream_topic))
        .route("/v1/groups/{group}/members", post(join))
        .route("/v1/groups/{group}/members/{member}", delete(leave))
        .route("/v1/groups/{group}/members/{member}/records", get(poll))
        .route("/v1/groups/{group}/members/{member}/commit", post(commit))
        .route("/v1/share/{group}/poll", post(share_poll))
        .route("/v1/share/{group}/ack", post(share_ack))
        .route("/v1/share/{group}", get(share_status))
        .layer(axum::middleware::from_fn_with_state(b.clone(), count_requests))
        .layer(CorsLayer::permissive())
        .with_state(b)
}

async fn count_requests(
    State(b): State<AppState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    b.metrics.http_requests_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    next.run(req).await
}

async fn index(State(b): State<AppState>) -> Json<Value> {
    Json(json!({
        "service": "bigpipe-gateway",
        "version": VERSION,
        "cluster_id": b.cfg.cluster_id,
        "node_id": b.cfg.node_id,
        "credit": CREDIT,
        "docs": "https://github.com/DotNetVibeCoderz/Vibe_Messaging/tree/main/BigPipe/docs",
    }))
}

async fn list_topics(State(b): State<AppState>) -> Json<Value> {
    Json(Value::Array(
        b.topics()
            .iter()
            .map(|t| json!({ "name": &*t.name, "partitions": t.partitions, "mode": t.config.mode.as_str() }))
            .collect(),
    ))
}

// ------------------------------------------------------------------------------ produce ------

#[derive(Deserialize)]
struct ProduceRecord {
    #[serde(default)]
    key: Option<Value>,
    #[serde(default)]
    value: Option<Value>,
    /// "utf8" (default) or "base64" for `value`.
    #[serde(default)]
    value_encoding: Option<String>,
    #[serde(default)]
    key_encoding: Option<String>,
    #[serde(default)]
    headers: HashMap<String, Value>,
    #[serde(default)]
    partition: Option<i32>,
    #[serde(default)]
    timestamp: Option<i64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ProduceBody {
    Wrapped {
        records: Vec<ProduceRecord>,
        #[serde(default)]
        compression: Option<String>,
    },
    Bare(Vec<ProduceRecord>),
}

fn payload(v: Option<Value>, encoding: Option<&str>) -> ApiResult<Option<Bytes>> {
    Ok(match v {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if encoding == Some("base64") => Some(Bytes::from(
            base64::engine::general_purpose::STANDARD
                .decode(s.as_bytes())
                .map_err(|e| ApiError::bad_request(format!("invalid base64: {e}")))?,
        )),
        Some(Value::String(s)) => Some(Bytes::from(s)),
        // Any other JSON value is stored as its compact JSON text.
        Some(other) => Some(Bytes::from(other.to_string())),
    })
}

async fn produce(State(b): State<AppState>, Path(topic): Path<String>, Json(body): Json<ProduceBody>) -> ApiResult<Json<Value>> {
    let (records, compression) = match body {
        ProduceBody::Wrapped { records, compression } => (records, compression),
        ProduceBody::Bare(r) => (r, None),
    };
    if records.is_empty() {
        return Err(ApiError::bad_request("no records to produce"));
    }
    let compression = match compression.as_deref() {
        None => Compression::None,
        Some(c) => Compression::parse(c).ok_or_else(|| ApiError::bad_request(format!("unknown compression `{c}`")))?,
    };
    let meta = b.ensure_topic(&topic).await?;
    let mut items = Vec::with_capacity(records.len());
    for r in records {
        let headers = r
            .headers
            .into_iter()
            .map(|(k, v)| Header {
                key: k,
                value: match v {
                    Value::Null => None,
                    Value::String(s) => Some(Bytes::from(s)),
                    other => Some(Bytes::from(other.to_string())),
                },
            })
            .collect();
        items.push((
            r.partition,
            NewRecord {
                key: payload(r.key, r.key_encoding.as_deref())?,
                value: payload(r.value, r.value_encoding.as_deref())?,
                headers,
                timestamp: r.timestamp,
            },
        ));
    }
    let offsets = b.produce_records(&meta, items, compression).await?;
    Ok(Json(json!({
        "topic": topic,
        "offsets": offsets.iter().map(|(p, o)| json!({"partition": p, "offset": o})).collect::<Vec<_>>(),
    })))
}

// ------------------------------------------------------------------------------ read ---------

#[derive(Deserialize)]
struct ReadQuery {
    #[serde(default)]
    offset: Option<String>,
    #[serde(default)]
    max_bytes: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    filter: Option<String>,
}

async fn resolve_offset(b: &Broker, meta: &bp_broker::TopicMeta, p: i32, spec: Option<&str>) -> ApiResult<i64> {
    Ok(match spec.unwrap_or("earliest") {
        "earliest" | "beginning" => b.partition_info(meta, p).await?.log_start_offset,
        "latest" | "end" => b.high_watermark(meta, p),
        n => n.parse::<i64>().map_err(|_| ApiError::bad_request(format!("invalid offset `{n}`")))?,
    })
}

fn parse_filter(f: Option<&str>) -> ApiResult<Option<Expr>> {
    Ok(match f {
        Some(s) if !s.trim().is_empty() => Some(Expr::parse(s)?),
        _ => None,
    })
}

async fn read_partition(
    State(b): State<AppState>,
    Path((topic, partition)): Path<(String, i32)>,
    Query(q): Query<ReadQuery>,
) -> ApiResult<Json<Value>> {
    let meta = b.topic(&topic).ok_or_else(|| ApiError::not_found(format!("unknown topic `{topic}`")))?;
    let filter = parse_filter(q.filter.as_deref())?;
    let mut offset = resolve_offset(&b, &meta, partition, q.offset.as_deref()).await?;
    let limit = q.limit.unwrap_or(500).clamp(1, 10_000);
    let max_bytes = q.max_bytes.unwrap_or(1 << 20).clamp(1024, 64 << 20);
    let deadline = Instant::now() + Duration::from_millis(q.timeout_ms.unwrap_or(0).min(60_000));
    if partition < 0 || partition >= meta.partitions {
        return Err(ApiError::not_found(format!("unknown partition {topic}-{partition}")));
    }
    let mut out = Vec::new();
    let mut hw;
    loop {
        let mut watch = vec![meta.hw[partition as usize].clone()];
        watch[0].borrow_and_update();
        let (recs, next, h) = b.read_records(&meta, partition, offset, max_bytes).await?;
        hw = h;
        let mut complete = true;
        for r in recs {
            if out.len() >= limit {
                complete = false;
                break;
            }
            offset = r.offset + 1;
            if filter.as_ref().is_none_or(|f| f.matches(&RecordView { topic: &topic, partition, record: &r })) {
                out.push(record_json(&topic, partition, &r));
            }
        }
        if complete {
            offset = offset.max(next);
        }
        if !out.is_empty() || Instant::now() >= deadline {
            break;
        }
        if offset < hw {
            continue; // everything so far was filtered out; keep scanning
        }
        Broker::wait_for_data(&mut watch, deadline).await;
    }
    Ok(Json(json!({ "records": out, "next_offset": offset, "high_watermark": hw })))
}

// ------------------------------------------------------------------------------ SSE ----------

#[derive(Deserialize)]
struct StreamQuery {
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    partitions: Option<String>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    group: Option<String>,
}

struct LeaveOnDrop {
    broker: AppState,
    group: String,
    member: String,
}

impl Drop for LeaveOnDrop {
    fn drop(&mut self) {
        self.broker.coordinator.http_leave(&self.group, &self.member);
    }
}

/// Streams records as Server-Sent Events (`event: record`). With `group`, the connection
/// joins a server-assigned group and commits as it goes; otherwise it tails the partitions.
async fn stream_topic(
    State(b): State<AppState>,
    Path(topic): Path<String>,
    Query(q): Query<StreamQuery>,
) -> ApiResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let meta = b.topic(&topic).ok_or_else(|| ApiError::not_found(format!("unknown topic `{topic}`")))?;
    let filter = parse_filter(q.filter.as_deref())?;
    let from = q.from.clone().unwrap_or_else(|| "latest".into());
    let mut partitions: Vec<i32> = match &q.partitions {
        Some(s) => s.split(',').filter_map(|p| p.trim().parse().ok()).filter(|p| *p >= 0 && *p < meta.partitions).collect(),
        None => (0..meta.partitions).collect(),
    };
    let guard = match &q.group {
        Some(g) => {
            let member = b
                .coordinator
                .http_join(g, vec![topic.clone()], true, from.clone(), q.filter.clone(), Duration::from_secs(45))
                .map_err(|_| ApiError::conflict(format!("group `{g}` is in use by Kafka-protocol consumers")))?;
            Some(LeaveOnDrop { broker: b.clone(), group: g.clone(), member })
        }
        None => None,
    };
    let mut positions: HashMap<i32, i64> = HashMap::new();
    if guard.is_none() {
        for p in &partitions {
            positions.insert(*p, resolve_offset(&b, &meta, *p, Some(&from)).await?);
        }
    }
    let stream = async_stream::stream! {
        let _guard = guard;
        let mut generation = -1;
        loop {
            // Group mode: refresh the assignment (also serves as the session heartbeat).
            if let Some(g) = &_guard {
                let snap = b.coordinator.with_http_member(&g.group, &g.member, |t| b.partitions_of(t), |m, generation_now| (m.assigned.clone(), generation_now));
                let Some((assigned, generation_now)) = snap else { break };
                if generation_now != generation {
                    generation = generation_now;
                    partitions = assigned.iter().map(|(_, p)| *p).collect();
                    positions.clear();
                    for p in &partitions {
                        let pos = match b.coordinator.committed(&g.group, &topic, *p) {
                            Some(c) => c.offset,
                            None => resolve_offset(&b, &meta, *p, Some(&from)).await.unwrap_or(0),
                        };
                        positions.insert(*p, pos);
                    }
                    yield Ok(Event::default().event("assignment").data(json!({"generation": generation_now, "partitions": partitions}).to_string()));
                }
            }
            let mut watch: Vec<_> = partitions.iter().filter_map(|p| meta.hw.get(*p as usize).cloned()).collect();
            for w in &mut watch { w.borrow_and_update(); }
            let mut sent = 0usize;
            for p in partitions.clone() {
                let pos = positions.get(&p).copied().unwrap_or(0);
                if pos >= b.high_watermark(&meta, p) {
                    continue;
                }
                match b.read_records(&meta, p, pos, 1 << 20).await {
                    Ok((recs, next, _)) => {
                        for r in &recs {
                            if filter.as_ref().is_none_or(|f| f.matches(&RecordView { topic: &topic, partition: p, record: r })) {
                                sent += 1;
                                yield Ok(Event::default().event("record").id(format!("{p}:{}", r.offset)).data(record_json(&topic, p, r).to_string()));
                            }
                        }
                        positions.insert(p, next.max(pos));
                        if let Some(g) = &_guard {
                            b.coordinator.commit(&g.group, &topic, p, next.max(pos), None);
                        }
                    }
                    Err(bp_broker::BrokerError::Storage(bp_storage::StorageError::OffsetOutOfRange { log_start, .. })) => {
                        positions.insert(p, log_start);
                    }
                    Err(e) => {
                        yield Ok(Event::default().event("error").data(json!({"message": e.to_string()}).to_string()));
                        return;
                    }
                }
            }
            if sent == 0 {
                Broker::wait_for_data(&mut watch, Instant::now() + Duration::from_secs(10)).await;
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

// ------------------------------------------------------------------------------ groups -------

#[derive(Deserialize)]
struct JoinBody {
    topics: Vec<String>,
    #[serde(default = "yes")]
    auto_commit: bool,
    #[serde(default)]
    offset_reset: Option<String>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    session_timeout_ms: Option<u64>,
}

fn yes() -> bool {
    true
}

async fn join(State(b): State<AppState>, Path(group): Path<String>, Json(body): Json<JoinBody>) -> ApiResult<Json<Value>> {
    if body.topics.is_empty() {
        return Err(ApiError::bad_request("subscribe to at least one topic"));
    }
    parse_filter(body.filter.as_deref())?;
    for t in &body.topics {
        b.ensure_topic(t).await?;
    }
    let member = b
        .coordinator
        .http_join(
            &group,
            body.topics,
            body.auto_commit,
            body.offset_reset.unwrap_or_else(|| "earliest".into()),
            body.filter,
            Duration::from_millis(body.session_timeout_ms.unwrap_or(30_000).clamp(1_000, 300_000)),
        )
        .map_err(|_| ApiError::conflict(format!("group `{group}` is in use by Kafka-protocol consumers")))?;
    Ok(Json(json!({ "group": group, "member_id": member })))
}

async fn leave(State(b): State<AppState>, Path((group, member)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    if !b.coordinator.http_leave(&group, &member) {
        return Err(ApiError::not_found("unknown member"));
    }
    Ok(Json(json!({ "left": true })))
}

#[derive(Deserialize)]
struct PollQuery {
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    max_records: Option<usize>,
    #[serde(default)]
    max_bytes: Option<usize>,
}

struct Session {
    assigned: Vec<(String, i32)>,
    positions: HashMap<(String, i32), i64>,
    filter: Option<String>,
    reset: String,
    auto_commit: bool,
    generation: i32,
}

fn unknown_member() -> ApiError {
    ApiError::not_found("unknown member: the session expired or the member left; join again")
}

async fn poll(
    State(b): State<AppState>,
    Path((group, member)): Path<(String, String)>,
    Query(q): Query<PollQuery>,
) -> ApiResult<Json<Value>> {
    let snap = b
        .coordinator
        .with_http_member(&group, &member, |t| b.partitions_of(t), |m, generation_now| Session {
            assigned: m.assigned.clone(),
            positions: m.positions.clone(),
            filter: m.filter.clone(),
            reset: m.offset_reset.clone(),
            auto_commit: m.auto_commit,
            generation: generation_now,
        })
        .ok_or_else(unknown_member)?;
    let filter = parse_filter(snap.filter.as_deref())?;
    let max_records = q.max_records.unwrap_or(500).clamp(1, 10_000);
    let max_bytes = q.max_bytes.unwrap_or(4 << 20).clamp(1024, 64 << 20);
    let deadline = Instant::now() + Duration::from_millis(q.timeout_ms.unwrap_or(1000).min(30_000));
    let mut positions = snap.positions;
    let mut out = Vec::new();
    loop {
        let mut watch = Vec::new();
        for (t, p) in &snap.assigned {
            let Some(meta) = b.topic(t) else { continue };
            if let Some(w) = meta.hw.get(*p as usize) {
                let mut w = w.clone();
                w.borrow_and_update();
                watch.push(w);
            }
            let key = (t.clone(), *p);
            let pos = match positions.get(&key) {
                Some(p) => *p,
                None => match b.coordinator.committed(&group, t, *p) {
                    Some(c) => c.offset,
                    None => resolve_offset(&b, &meta, *p, Some(&snap.reset)).await?,
                },
            };
            positions.insert(key.clone(), pos);
            if out.len() >= max_records || pos >= b.high_watermark(&meta, *p) {
                continue;
            }
            let budget = (max_bytes / snap.assigned.len().max(1)).max(64 * 1024);
            let (recs, next, _) = match b.read_records(&meta, *p, pos, budget).await {
                Ok(r) => r,
                Err(bp_broker::BrokerError::Storage(bp_storage::StorageError::OffsetOutOfRange { log_start, high_watermark, .. })) => {
                    let reset = if snap.reset == "latest" { high_watermark } else { log_start };
                    positions.insert(key, reset);
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            let mut newpos = pos;
            let mut complete = true;
            for r in &recs {
                if out.len() >= max_records {
                    complete = false;
                    break;
                }
                newpos = r.offset + 1;
                if filter.as_ref().is_none_or(|f| f.matches(&RecordView { topic: t, partition: *p, record: r })) {
                    out.push(record_json(t, *p, r));
                }
            }
            if complete {
                newpos = newpos.max(next);
            }
            positions.insert(key, newpos);
        }
        if !out.is_empty() || Instant::now() >= deadline {
            break;
        }
        Broker::wait_for_data(&mut watch, deadline).await;
    }
    let stored = b.coordinator.with_http_member(&group, &member, |t| b.partitions_of(t), |m, generation_now| {
        if generation_now == snap.generation {
            for (k, v) in &positions {
                if m.assigned.contains(k) {
                    m.positions.insert(k.clone(), *v);
                }
            }
        }
        (m.assigned.clone(), generation_now)
    });
    let (assigned, generation) = stored.ok_or_else(unknown_member)?;
    if snap.auto_commit && generation == snap.generation {
        for ((t, p), o) in &positions {
            b.coordinator.commit(&group, t, *p, *o, None);
        }
    }
    Ok(Json(json!({
        "member_id": member,
        "generation": generation,
        "assignment": assigned.iter().map(|(t, p)| json!({"topic": t, "partition": p})).collect::<Vec<_>>(),
        "records": out,
    })))
}

#[derive(Deserialize, Default)]
struct CommitBody {
    #[serde(default)]
    offsets: Option<Vec<CommitEntry>>,
}

#[derive(Deserialize)]
struct CommitEntry {
    topic: String,
    partition: i32,
    offset: i64,
}

async fn commit(
    State(b): State<AppState>,
    Path((group, member)): Path<(String, String)>,
    body: Option<Json<CommitBody>>,
) -> ApiResult<Json<Value>> {
    let explicit = body.and_then(|Json(b)| b.offsets);
    let positions = b
        .coordinator
        .with_http_member(&group, &member, |t| b.partitions_of(t), |m, _| m.positions.clone())
        .ok_or_else(unknown_member)?;
    let entries: Vec<(String, i32, i64)> = match explicit {
        Some(list) => list.into_iter().map(|e| (e.topic, e.partition, e.offset)).collect(),
        None => positions.into_iter().map(|((t, p), o)| (t, p, o)).collect(),
    };
    for (t, p, o) in &entries {
        b.coordinator.commit(&group, t, *p, *o, None);
    }
    Ok(Json(json!({
        "committed": entries.iter().map(|(t, p, o)| json!({"topic": t, "partition": p, "offset": o})).collect::<Vec<_>>()
    })))
}

// ------------------------------------------------------------------------------ share --------

#[derive(Deserialize)]
struct SharePollBody {
    #[serde(default)]
    member: Option<String>,
    topics: Vec<String>,
    #[serde(default)]
    max_records: Option<usize>,
    #[serde(default)]
    lock_ms: Option<u64>,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    dlq_topic: Option<String>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    offset_reset: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

async fn share_poll(State(b): State<AppState>, Path(group): Path<String>, Json(body): Json<SharePollBody>) -> ApiResult<Json<Value>> {
    let member = body.member.clone().unwrap_or_else(|| format!("share-{}", b.allocate_producer_id()));
    let filter = parse_filter(body.filter.as_deref())?;
    let opts = ShareOptions {
        max_attempts: body.max_attempts.unwrap_or(5),
        lock: Duration::from_millis(body.lock_ms.unwrap_or(30_000).clamp(100, 3_600_000)),
        dlq_topic: body.dlq_topic.clone(),
        offset_reset: body.offset_reset.clone().unwrap_or_else(|| "earliest".into()),
    };
    let deadline = Instant::now() + Duration::from_millis(body.timeout_ms.unwrap_or(0).min(30_000));
    let max = body.max_records.unwrap_or(100).clamp(1, 5_000);
    let deliveries = loop {
        let mut watch: Vec<_> = body
            .topics
            .iter()
            .filter_map(|t| b.topic(t))
            .flat_map(|m| m.hw.clone())
            .collect();
        for w in &mut watch {
            w.borrow_and_update();
        }
        let d = b.shares.poll(&b, &group, &member, &body.topics, max, filter.as_ref(), &opts).await?;
        if !d.is_empty() || Instant::now() >= deadline {
            break d;
        }
        // Wake on new data, or periodically to pick up expired locks.
        Broker::wait_for_data(&mut watch, deadline.min(Instant::now() + Duration::from_millis(500))).await;
    };
    let records: Vec<Value> = deliveries
        .iter()
        .map(|d| {
            let mut v = record_json(&d.topic, d.partition, &d.record);
            v["delivery_count"] = json!(d.delivery_count);
            v
        })
        .collect();
    Ok(Json(json!({ "group": group, "member": member, "records": records })))
}

#[derive(Deserialize)]
struct ShareAckBody {
    member: String,
    acks: Vec<ShareAck>,
}

#[derive(Deserialize)]
struct ShareAck {
    topic: String,
    partition: i32,
    offset: i64,
    action: AckAction,
}

async fn share_ack(State(b): State<AppState>, Path(group): Path<String>, Json(body): Json<ShareAckBody>) -> ApiResult<Json<Value>> {
    let acks: Vec<_> = body.acks.into_iter().map(|a| (a.topic, a.partition, a.offset, a.action)).collect();
    let results = b.shares.ack(&b, &group, &body.member, &acks).await?;
    Ok(Json(json!({ "results": results })))
}

async fn share_status(State(b): State<AppState>, Path(group): Path<String>) -> Json<Value> {
    Json(json!({ "group": group, "partitions": b.shares.status(&group).await }))
}

#[allow(dead_code)]
fn _assert_send(b: Arc<Broker>) -> impl Send {
    b
}
