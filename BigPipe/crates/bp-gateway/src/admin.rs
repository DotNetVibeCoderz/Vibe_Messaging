//! Admin REST API (port 9644).

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use bp_broker::flow::{FlowSpec, Processor};
use bp_broker::{Broker, RecordView, TopicMeta};
use bp_storage::StorageMode;
use serde::Deserialize;
use serde_json::{Value, json};
use tower_http::cors::CorsLayer;

use crate::{ApiError, ApiResult, AppState, CREDIT, VERSION, record_json};

pub fn admin_router(b: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/cluster", get(cluster))
        .route("/v1/metrics", get(metrics))
        .route("/v1/topics", get(list_topics).post(create_topic))
        .route("/v1/topics/{name}", get(get_topic).delete(delete_topic))
        .route("/v1/topics/{name}/config", axum::routing::patch(patch_config))
        .route("/v1/topics/{name}/partitions", post(add_partitions))
        .route("/v1/topics/{name}/migrate", post(migrate))
        .route("/v1/topics/{name}/compact", post(compact))
        .route("/v1/topics/{name}/messages", get(browse))
        .route("/v1/groups", get(list_groups))
        .route("/v1/groups/{id}", get(get_group).delete(delete_group))
        .route("/v1/groups/{id}/reset", post(reset_group))
        .route("/v1/share-groups", get(list_share_groups))
        .route("/v1/flows", get(list_flows).post(deploy_flow))
        .route("/v1/flows/{name}", axum::routing::delete(delete_flow))
        .route("/v1/flows/{name}/pause", post(pause_flow))
        .route("/v1/flows/{name}/resume", post(resume_flow))
        .route("/v1/expr/validate", post(validate_expr))
        .layer(axum::middleware::from_fn_with_state(b.clone(), auth))
        .layer(CorsLayer::permissive())
        .with_state(b)
}

/// Optional bearer-token check (`admin_api_key` in bigpipe.yaml).
async fn auth(State(b): State<AppState>, headers: HeaderMap, req: axum::extract::Request, next: axum::middleware::Next) -> axum::response::Response {
    use axum::response::IntoResponse;
    if let Some(key) = &b.cfg.admin_api_key {
        let ok = req.uri().path() == "/healthz"
            || req.method() == axum::http::Method::OPTIONS
            || headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .is_some_and(|v| v == key);
        if !ok {
            return ApiError::unauthorized().into_response();
        }
    }
    next.run(req).await
}

async fn cluster(State(b): State<AppState>) -> Json<Value> {
    let c = &b.cfg;
    Json(json!({
        "cluster_id": c.cluster_id,
        "node_id": c.node_id,
        "version": VERSION,
        "uptime_seconds": b.started.elapsed().as_secs(),
        "shards": b.shard_count(),
        "object_store": b.store.url(),
        "listeners": {
            "kafka": format!("{}:{}", c.advertised_host, c.advertised_port),
            "http": c.http_addr,
            "admin": c.admin_addr,
            "metrics": c.metrics_addr,
        },
        "topics": b.topics().len(),
        "groups": b.coordinator.list().len(),
        "defaults": {
            "partitions": c.default_partitions,
            "storage_mode": c.default_storage_mode,
            "auto_create_topics": c.auto_create_topics,
        },
        "credit": CREDIT,
    }))
}

async fn metrics(State(b): State<AppState>) -> Json<Value> {
    let mut v = serde_json::to_value(b.metrics.snapshot()).unwrap_or_default();
    v["object_cache_bytes"] = json!(b.store.cached_bytes());
    v["diskless_files_referenced"] = json!(b.diskless_files_referenced());
    Json(v)
}

async fn topic_json(b: &Broker, t: &TopicMeta, detail: bool) -> Value {
    let mut parts = Vec::new();
    let mut records: i64 = 0;
    let (mut local, mut remote, mut diskless) = (0u64, 0u64, 0u64);
    for p in 0..t.partitions {
        if let Ok(info) = b.partition_info(t, p).await {
            records += info.high_watermark - info.log_start_offset;
            local += info.local_bytes;
            remote += info.remote_bytes;
            diskless += info.diskless_bytes;
            if detail {
                parts.push(serde_json::to_value(&info).unwrap_or_default());
            }
        }
    }
    let effective: BTreeMap<String, String> = bp_storage::config::KNOWN_KEYS
        .iter()
        .filter_map(|k| t.config.describe(k).map(|v| (k.to_string(), v)))
        .collect();
    let mut v = json!({
        "name": &*t.name,
        "partitions": t.partitions,
        "mode": t.config.mode.as_str(),
        "internal": t.is_internal(),
        "created_ms": t.created_ms,
        "records": records,
        "bytes": { "local": local, "remote": remote, "diskless": diskless },
        "config": t.raw_config,
        "effective_config": effective,
    });
    if detail {
        v["partition_details"] = Value::Array(parts);
    }
    v
}

async fn list_topics(State(b): State<AppState>) -> Json<Value> {
    let mut out = Vec::new();
    for t in b.topics() {
        out.push(topic_json(&b, &t, false).await);
    }
    Json(Value::Array(out))
}

#[derive(Deserialize)]
struct CreateTopic {
    name: String,
    #[serde(default)]
    partitions: Option<i32>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    config: BTreeMap<String, Value>,
}

fn config_string(v: Value) -> String {
    match v {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

async fn create_topic(State(b): State<AppState>, Json(body): Json<CreateTopic>) -> ApiResult<(axum::http::StatusCode, Json<Value>)> {
    let mut config: BTreeMap<String, String> = body.config.into_iter().map(|(k, v)| (k, config_string(v))).collect();
    if let Some(m) = body.mode {
        config.insert("bigpipe.storage.mode".into(), m);
    }
    let t = b.create_topic(&body.name, body.partitions.unwrap_or(0), config).await?;
    Ok((axum::http::StatusCode::CREATED, Json(topic_json(&b, &t, false).await)))
}

async fn get_topic(State(b): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    let t = b.topic(&name).ok_or_else(|| ApiError::not_found(format!("unknown topic `{name}`")))?;
    Ok(Json(topic_json(&b, &t, true).await))
}

async fn delete_topic(State(b): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    b.delete_topic(&name).await?;
    Ok(Json(json!({ "deleted": name })))
}

#[derive(Deserialize)]
struct PatchConfig {
    config: BTreeMap<String, Value>,
}

async fn patch_config(State(b): State<AppState>, Path(name): Path<String>, Json(body): Json<PatchConfig>) -> ApiResult<Json<Value>> {
    let changes = body
        .config
        .into_iter()
        .map(|(k, v)| (k, if v.is_null() { None } else { Some(config_string(v)) }))
        .collect();
    let t = b.alter_topic_config(&name, changes).await?;
    Ok(Json(topic_json(&b, &t, false).await))
}

#[derive(Deserialize)]
struct AddPartitions {
    count: i32,
}

async fn add_partitions(State(b): State<AppState>, Path(name): Path<String>, Json(body): Json<AddPartitions>) -> ApiResult<Json<Value>> {
    let t = b.add_partitions(&name, body.count).await?;
    Ok(Json(topic_json(&b, &t, false).await))
}

#[derive(Deserialize)]
struct Migrate {
    to: String,
}

/// Online storage-mode migration: new appends go to the new mode, offsets never change.
async fn migrate(State(b): State<AppState>, Path(name): Path<String>, Json(body): Json<Migrate>) -> ApiResult<Json<Value>> {
    let mode = StorageMode::parse(&body.to)
        .ok_or_else(|| ApiError::bad_request(format!("unknown mode `{}` (use local, tiered or diskless)", body.to)))?;
    let before = b.topic(&name).ok_or_else(|| ApiError::not_found(format!("unknown topic `{name}`")))?;
    let mut hw = BTreeMap::new();
    for p in 0..before.partitions {
        hw.insert(p, b.high_watermark(&before, p));
    }
    let mut changes = BTreeMap::new();
    changes.insert("bigpipe.storage.mode".to_string(), Some(mode.as_str().to_string()));
    let t = b.alter_topic_config(&name, changes).await?;
    Ok(Json(json!({
        "topic": name,
        "from": before.config.mode.as_str(),
        "to": t.config.mode.as_str(),
        "switch_offsets": hw,
        "note": "offsets below switch_offsets stay in the previous storage; new data uses the new mode",
    })))
}

/// Compacts a `cleanup.policy=compact` topic now (normally it happens in the background once
/// `min.cleanable.dirty.ratio` of the log is dirty).
async fn compact(State(b): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    let t = b.topic(&name).ok_or_else(|| ApiError::not_found(format!("unknown topic `{name}`")))?;
    let results = b.compact(&t).await?;
    let partitions: Vec<Value> = results
        .into_iter()
        .map(|(p, st)| match st {
            Some(st) => json!({
                "partition": p, "compacted": true, "segments": st.segments,
                "records_before": st.records_before, "records_after": st.records_after,
                "bytes_before": st.bytes_before, "bytes_after": st.bytes_after, "duration_ms": st.duration_ms,
            }),
            None => json!({ "partition": p, "compacted": false, "reason": "no sealed segments old enough to compact" }),
        })
        .collect();
    Ok(Json(json!({ "topic": name, "partitions": partitions })))
}

#[derive(Deserialize)]
struct BrowseQuery {
    #[serde(default)]
    partition: Option<i32>,
    #[serde(default)]
    offset: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    filter: Option<String>,
}

/// Message browser: newest (or from an offset) records across partitions, with optional filter.
async fn browse(State(b): State<AppState>, Path(name): Path<String>, Query(q): Query<BrowseQuery>) -> ApiResult<Json<Value>> {
    let t = b.topic(&name).ok_or_else(|| ApiError::not_found(format!("unknown topic `{name}`")))?;
    let filter = match q.filter.as_deref() {
        Some(s) if !s.trim().is_empty() => Some(bp_expr::Expr::parse(s)?),
        _ => None,
    };
    let limit = q.limit.unwrap_or(50).clamp(1, 1000);
    let parts: Vec<i32> = match q.partition {
        Some(p) => vec![p],
        None => (0..t.partitions).collect(),
    };
    let mut out = Vec::new();
    for p in parts {
        let info = b.partition_info(&t, p).await?;
        let start = match q.offset.as_deref() {
            Some("earliest") => info.log_start_offset,
            Some(n) if n.parse::<i64>().is_ok() => n.parse::<i64>().unwrap().max(info.log_start_offset),
            // Default: the newest `limit` records of each partition.
            _ => (info.high_watermark - limit as i64).max(info.log_start_offset),
        };
        let mut pos = start;
        let mut taken = 0;
        // Scan up to ~8 MiB per partition looking for matches.
        let mut scanned = 0usize;
        while pos < info.high_watermark && taken < limit && scanned < 8 << 20 {
            let (recs, next, _) = b.read_records(&t, p, pos, 1 << 20).await?;
            scanned += 1 << 20;
            for r in &recs {
                if taken >= limit {
                    break;
                }
                if filter.as_ref().is_none_or(|f| f.matches(&RecordView { topic: &name, partition: p, record: r })) {
                    out.push(record_json(&name, p, r));
                    taken += 1;
                }
            }
            if next <= pos {
                break;
            }
            pos = next;
        }
    }
    out.sort_by_key(|r| std::cmp::Reverse(r["timestamp"].as_i64().unwrap_or(0)));
    out.truncate(limit);
    Ok(Json(json!({ "topic": name, "records": out })))
}

async fn group_json(b: &Broker, id: &str) -> Option<Value> {
    let summary = b.coordinator.describe(id);
    let committed = b.coordinator.committed_all(id);
    if summary.is_none() && committed.is_empty() {
        return None;
    }
    let mut lag_total: i64 = 0;
    let mut offsets = Vec::new();
    for ((t, p), c) in committed {
        let hw = b.topic(&t).map(|m| b.high_watermark(&m, p)).unwrap_or(c.offset);
        let lag = (hw - c.offset).max(0);
        lag_total += lag;
        offsets.push(json!({ "topic": t, "partition": p, "committed": c.offset, "high_watermark": hw, "lag": lag, "commit_ms": c.commit_ms }));
    }
    Some(json!({
        "group_id": id,
        "summary": summary,
        "offsets": offsets,
        "lag": lag_total,
    }))
}

async fn list_groups(State(b): State<AppState>) -> Json<Value> {
    let mut out = Vec::new();
    for (id, _) in b.coordinator.list() {
        if let Some(mut g) = group_json(&b, &id).await {
            // Keep the list light: drop per-partition detail, keep which topics are consumed.
            g["partitions"] = json!(g["offsets"].as_array().map(|a| a.len()).unwrap_or(0));
            let mut topics: Vec<String> = g["offsets"]
                .as_array()
                .map(|a| a.iter().filter_map(|o| o["topic"].as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            topics.sort();
            topics.dedup();
            g["topics"] = json!(topics);
            g.as_object_mut().map(|o| o.remove("offsets"));
            out.push(g);
        }
    }
    Json(Value::Array(out))
}

async fn get_group(State(b): State<AppState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    group_json(&b, &id).await.map(Json).ok_or_else(|| ApiError::not_found(format!("unknown group `{id}`")))
}

async fn delete_group(State(b): State<AppState>, Path(id): Path<String>) -> ApiResult<Json<Value>> {
    b.coordinator
        .delete_group(&id)
        .map_err(|_| ApiError::conflict("group still has active members; stop the consumers first"))?;
    Ok(Json(json!({ "deleted": id })))
}

#[derive(Deserialize)]
struct ResetBody {
    topic: String,
    /// earliest | latest | offset
    to: String,
    #[serde(default)]
    offset: Option<i64>,
    #[serde(default)]
    partitions: Option<Vec<i32>>,
}

async fn reset_group(State(b): State<AppState>, Path(id): Path<String>, Json(body): Json<ResetBody>) -> ApiResult<Json<Value>> {
    let t = b.topic(&body.topic).ok_or_else(|| ApiError::not_found(format!("unknown topic `{}`", body.topic)))?;
    if b.coordinator.describe(&id).is_some_and(|g| !g.members.is_empty()) {
        return Err(ApiError::conflict("group has active members; stop the consumers before resetting offsets"));
    }
    let parts = body.partitions.clone().unwrap_or_else(|| (0..t.partitions).collect());
    let mut out = Vec::new();
    for p in parts {
        let info = b.partition_info(&t, p).await?;
        let off = match body.to.as_str() {
            "earliest" => info.log_start_offset,
            "latest" => info.high_watermark,
            "offset" => body.offset.ok_or_else(|| ApiError::bad_request("`offset` is required when to=offset"))?,
            other => return Err(ApiError::bad_request(format!("unknown reset target `{other}`"))),
        };
        b.coordinator.commit(&id, &body.topic, p, off, None);
        out.push(json!({ "partition": p, "offset": off }));
    }
    Ok(Json(json!({ "group_id": id, "topic": body.topic, "reset": out })))
}

async fn list_share_groups(State(b): State<AppState>) -> Json<Value> {
    let mut out = Vec::new();
    for name in b.shares.names() {
        out.push(json!({ "group": name, "partitions": b.shares.status(&name).await }));
    }
    Json(Value::Array(out))
}

async fn list_flows(State(b): State<AppState>) -> Json<Value> {
    Json(serde_json::to_value(b.flows.list()).unwrap_or_default())
}

/// Accepts the native JSON spec, or the Kubernetes-style YAML `Flow` document with
/// `spec.input.bigpipe.topic`, `spec.pipeline.processors` and `spec.output.bigpipe.topic`.
async fn deploy_flow(State(b): State<AppState>, headers: HeaderMap, body: String) -> ApiResult<(axum::http::StatusCode, Json<Value>)> {
    let is_yaml = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("yaml"));
    let spec = if is_yaml { flow_from_yaml(&body)? } else {
        serde_json::from_str::<FlowSpec>(&body).map_err(|e| ApiError::bad_request(format!("invalid flow spec: {e}")))?
    };
    b.flows.start(&b, spec.clone(), true).map_err(ApiError::bad_request)?;
    Ok((axum::http::StatusCode::CREATED, Json(serde_json::to_value(spec).unwrap_or_default())))
}

fn flow_from_yaml(src: &str) -> ApiResult<FlowSpec> {
    let doc: Value = serde_yaml_ng::from_str(src).map_err(|e| ApiError::bad_request(format!("invalid YAML: {e}")))?;
    let name = doc["metadata"]["name"].as_str().or(doc["name"].as_str()).ok_or_else(|| ApiError::bad_request("metadata.name is required"))?;
    let spec = if doc["spec"].is_object() { &doc["spec"] } else { &doc };
    let input = spec["input"]["bigpipe"]["topic"].as_str().or(spec["input"].as_str()).ok_or_else(|| ApiError::bad_request("spec.input.bigpipe.topic is required"))?;
    let output = spec["output"]["bigpipe"]["topic"].as_str().or(spec["output"].as_str()).ok_or_else(|| ApiError::bad_request("spec.output.bigpipe.topic is required"))?;
    let mut processors = Vec::new();
    let empty = Vec::new();
    let procs = spec["pipeline"]["processors"].as_array().or(spec["processors"].as_array()).unwrap_or(&empty);
    for p in procs {
        if let Some(s) = p["filter"].as_str() {
            processors.push(Processor::Filter(s.to_string()));
        } else if let Some(s) = p["mapping"].as_str() {
            processors.push(Processor::Mapping(s.to_string()));
        } else if let Some(s) = p["route"].as_str() {
            processors.push(Processor::Route(s.to_string()));
        } else {
            return Err(ApiError::bad_request(format!("unsupported processor `{p}` (use filter, mapping or route)")));
        }
    }
    Ok(FlowSpec {
        name: name.to_string(),
        input: input.to_string(),
        output: output.to_string(),
        processors,
        start_from: spec["start_from"].as_str().unwrap_or("earliest").to_string(),
        dlq: spec["dlq"].as_str().map(str::to_string),
        paused: false,
    })
}

async fn delete_flow(State(b): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    if !b.flows.delete(&b, &name) {
        return Err(ApiError::not_found(format!("unknown flow `{name}`")));
    }
    Ok(Json(json!({ "deleted": name })))
}

async fn pause_flow(State(b): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    b.flows.set_paused(&b, &name, true).map_err(ApiError::not_found)?;
    Ok(Json(json!({ "flow": name, "paused": true })))
}

async fn resume_flow(State(b): State<AppState>, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    b.flows.set_paused(&b, &name, false).map_err(ApiError::not_found)?;
    Ok(Json(json!({ "flow": name, "paused": false })))
}

#[derive(Deserialize)]
struct ValidateExpr {
    expr: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    sample: Option<Value>,
}

/// Checks a filter or mapping and optionally evaluates it against a sample JSON value.
async fn validate_expr(Json(body): Json<ValidateExpr>) -> ApiResult<Json<Value>> {
    let sample = body.sample.map(|s| bp_expr::SimpleRecord { value: Some(s.to_string().into_bytes()), ..Default::default() });
    if body.kind.as_deref() == Some("mapping") {
        let m = bp_expr::Mapping::parse(&body.expr)?;
        let result = sample.map(|s| m.apply(&s));
        return Ok(Json(json!({ "valid": true, "result": result })));
    }
    let e = bp_expr::Expr::parse(&body.expr)?;
    let result = sample.map(|s| e.eval(&s));
    Ok(Json(json!({ "valid": true, "result": result })))
}
