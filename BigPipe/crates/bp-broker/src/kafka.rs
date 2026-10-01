//! Kafka wire protocol server (port 9092).
//!
//! Requests on a connection are handled concurrently but responses are written strictly in
//! request order, as Kafka clients require. Produce requests are the exception to the
//! concurrency: their appends are enqueued to the shards in arrival order (only the wait for the
//! result runs concurrently), because idempotent producers with several requests in flight
//! depend on that order. Fetch responses are written as a list of chunks so record data is
//! never copied into the response buffer.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use bp_protocol::messages::{self as m, Request, RequestHeader, api, error};
use bp_protocol::{Decoder, Encoder};
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::broker::{Broker, BrokerError};

pub async fn serve(broker: Arc<Broker>, listener: TcpListener) -> std::io::Result<()> {
    loop {
        let (sock, peer) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        let broker = broker.clone();
        tokio::spawn(async move {
            broker.metrics.connections_open.fetch_add(1, Relaxed);
            broker.metrics.connections_total.fetch_add(1, Relaxed);
            if let Err(e) = connection(broker.clone(), sock, peer).await {
                tracing::debug!(%peer, error = %e, "connection closed");
            }
            broker.metrics.connections_open.fetch_sub(1, Relaxed);
        });
    }
}

type Pending = BoxFuture<'static, Option<Vec<Bytes>>>;

async fn connection(broker: Arc<Broker>, sock: TcpStream, peer: SocketAddr) -> std::io::Result<()> {
    let (mut rd, mut wr) = sock.into_split();
    // In-order response queue; bounded so a client cannot pipeline unbounded work.
    let (tx, mut rx) = mpsc::channel::<tokio::task::JoinHandle<Option<Vec<Bytes>>>>(64);
    let writer = tokio::spawn(async move {
        while let Some(h) = rx.recv().await {
            let Ok(Some(chunks)) = h.await else { continue };
            for c in chunks {
                wr.write_all(&c).await?;
            }
        }
        wr.shutdown().await
    });
    let max = broker.cfg.max_request_bytes;
    let host = peer.ip().to_string();
    loop {
        let mut len = [0u8; 4];
        if rd.read_exact(&mut len).await.is_err() {
            break;
        }
        let size = i32::from_be_bytes(len);
        if size <= 0 || size as usize > max {
            tracing::warn!(%peer, size, "invalid request size; closing");
            break;
        }
        let mut frame = BytesMut::zeroed(size as usize);
        rd.read_exact(&mut frame).await?;
        broker.metrics.requests_total.fetch_add(1, Relaxed);
        let is_produce = frame.len() >= 2 && i16::from_be_bytes([frame[0], frame[1]]) == api::PRODUCE;
        let fut = if is_produce { produce_in_order(broker.clone(), frame).await } else { handle(broker.clone(), frame, host.clone()) };
        if tx.send(tokio::spawn(fut)).await.is_err() {
            break;
        }
    }
    drop(tx);
    let _ = writer.await;
    Ok(())
}

fn handle(broker: Arc<Broker>, frame: BytesMut, host: String) -> Pending {
    Box::pin(async move {
        let mut d = Decoder::new(frame);
        let h = match m::decode_header(&mut d) {
            Ok(h) => h,
            Err(e) => {
                broker.metrics.request_errors_total.fetch_add(1, Relaxed);
                tracing::warn!(error = %e, "undecodable request header");
                return None;
            }
        };
        if !m::version_supported(h.api_key, h.api_version) {
            if h.api_key == api::API_VERSIONS {
                // Tell the client which versions we speak, using the v0 layout.
                let mut e = m::response_encoder(h.correlation_id);
                m::encode_api_versions(&mut e, 0, error::UNSUPPORTED_VERSION);
                return Some(e.finish_frame());
            }
            tracing::warn!(api = m::api_name(h.api_key), version = h.api_version, "unsupported api version");
            broker.metrics.request_errors_total.fetch_add(1, Relaxed);
            return None;
        }
        let req = match Request::decode(&h, &mut d) {
            Ok(r) => r,
            Err(e) => {
                broker.metrics.request_errors_total.fetch_add(1, Relaxed);
                tracing::warn!(api = m::api_name(h.api_key), version = h.api_version, error = %e, "malformed request");
                return None;
            }
        };
        let mut e = m::response_encoder(h.correlation_id);
        let respond = dispatch(&broker, &h, req, &mut e, &host).await;
        respond.then(|| e.finish_frame())
    })
}

fn broker_info(b: &Broker) -> m::BrokerInfo {
    m::BrokerInfo {
        node_id: b.cfg.node_id,
        host: b.cfg.advertised_host.clone(),
        port: b.cfg.advertised_port,
        rack: b.cfg.rack.clone(),
    }
}

/// Returns false when no response must be sent (acks=0 produce).
async fn dispatch(b: &Arc<Broker>, h: &RequestHeader, req: Request, e: &mut Encoder, host: &str) -> bool {
    let v = h.api_version;
    let client_id = h.client_id.clone().unwrap_or_default();
    match req {
        Request::ApiVersions => m::encode_api_versions(e, v, error::NONE),
        Request::Metadata(r) => {
            let mut topics = Vec::new();
            let names: Vec<String> = match r.topics {
                None => b.topics().iter().map(|t| t.name.to_string()).collect(),
                Some(n) => n,
            };
            for name in names {
                let meta = if r.allow_auto_topic_creation { b.ensure_topic(&name).await } else {
                    b.topic(&name).ok_or_else(|| BrokerError::UnknownTopic(name.clone()))
                };
                topics.push(match meta {
                    Ok(t) => m::TopicMetadata {
                        error_code: error::NONE,
                        is_internal: t.is_internal(),
                        partitions: (0..t.partitions)
                            .map(|p| m::PartitionMetadata {
                                error_code: error::NONE,
                                index: p,
                                leader: b.cfg.node_id,
                                replicas: vec![b.cfg.node_id],
                            })
                            .collect(),
                        name,
                    },
                    Err(err) => m::TopicMetadata { error_code: err.kafka_code(), name, is_internal: false, partitions: vec![] },
                });
            }
            m::MetadataResponse {
                brokers: vec![broker_info(b)],
                cluster_id: b.cfg.cluster_id.clone(),
                controller_id: b.cfg.node_id,
                topics,
            }
            .encode(e, v);
        }
        Request::Produce(r) => {
            let acks = r.acks;
            let work = enqueue_produce(b, r).await;
            let grouped = complete_produce(b, work).await;
            if acks == 0 {
                return false;
            }
            m::encode_produce(e, v, &grouped);
        }
        Request::Fetch(r) => fetch(b, r, e, v).await,
        Request::ListOffsets(r) => {
            let mut out = Vec::new();
            for (name, parts) in r.topics {
                let meta = b.topic(&name);
                let mut ps = Vec::new();
                for (p, ts) in parts {
                    ps.push(match &meta {
                        None => m::ListOffsetsPartitionResponse { partition: p, error_code: error::UNKNOWN_TOPIC_OR_PARTITION, timestamp: -1, offset: -1 },
                        Some(t) => match b.list_offset(t, p, ts).await {
                            Ok((offset, timestamp)) => m::ListOffsetsPartitionResponse { partition: p, error_code: error::NONE, timestamp, offset },
                            Err(err) => m::ListOffsetsPartitionResponse { partition: p, error_code: err.kafka_code(), timestamp: -1, offset: -1 },
                        },
                    });
                }
                out.push((name, ps));
            }
            m::encode_list_offsets(e, v, &out);
        }
        Request::FindCoordinator(_) => m::encode_find_coordinator(e, v, error::NONE, &broker_info(b)),
        Request::JoinGroup(r) => {
            let rx = b.coordinator.join(r, &client_id, host);
            let resp = rx.await.unwrap_or_else(|_| m::JoinGroupResponse::error(error::REBALANCE_IN_PROGRESS, ""));
            resp.encode(e, v);
        }
        Request::SyncGroup(r) => {
            let rx = b.coordinator.sync(&r.group_id, r.generation_id, &r.member_id, r.assignments);
            let (code, assignment) = rx.await.unwrap_or((error::REBALANCE_IN_PROGRESS, Bytes::new()));
            m::encode_sync_group(e, v, code, &assignment);
        }
        Request::Heartbeat(r) => {
            let code = b.coordinator.heartbeat(&r.group_id, r.generation_id, &r.member_id);
            m::encode_heartbeat(e, v, code);
        }
        Request::LeaveGroup(r) => {
            let res = b.coordinator.leave(&r.group_id, &r.member_ids);
            let top = if v < 3 { res.first().map(|(_, c)| *c).unwrap_or(error::NONE) } else { error::NONE };
            m::encode_leave_group(e, v, top, &res);
        }
        Request::OffsetCommit(r) => {
            let code = b.coordinator.check_commit(&r.group_id, r.generation_id, &r.member_id);
            let mut out = Vec::new();
            for (name, parts) in r.topics {
                let known = b.topic(&name).is_some();
                let mut ps = Vec::new();
                for p in parts {
                    let c = if code != error::NONE {
                        code
                    } else if !known {
                        error::UNKNOWN_TOPIC_OR_PARTITION
                    } else {
                        b.coordinator.commit(&r.group_id, &name, p.partition, p.offset, p.metadata);
                        error::NONE
                    };
                    ps.push((p.partition, c));
                }
                out.push((name, ps));
            }
            m::encode_offset_commit(e, v, &out);
        }
        Request::OffsetFetch(r) => {
            let topics: Vec<(String, Vec<i32>)> = match r.topics {
                Some(t) => t,
                None => {
                    let mut map: BTreeMap<String, Vec<i32>> = BTreeMap::new();
                    for ((t, p), _) in b.coordinator.committed_all(&r.group_id) {
                        map.entry(t).or_default().push(p);
                    }
                    map.into_iter().collect()
                }
            };
            let out: Vec<_> = topics
                .into_iter()
                .map(|(name, parts)| {
                    let ps = parts
                        .into_iter()
                        .map(|p| match b.coordinator.committed(&r.group_id, &name, p) {
                            Some(c) => m::OffsetFetchPartition { partition: p, offset: c.offset, metadata: c.metadata, error_code: error::NONE },
                            None => m::OffsetFetchPartition { partition: p, offset: -1, metadata: None, error_code: error::NONE },
                        })
                        .collect();
                    (name, ps)
                })
                .collect();
            m::encode_offset_fetch(e, v, &out, error::NONE);
        }
        Request::CreateTopics(r) => {
            let mut out = Vec::new();
            for t in r.topics {
                let configs: BTreeMap<String, String> =
                    t.configs.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))).collect();
                if t.replication_factor > 1 {
                    out.push((t.name, error::INVALID_REPLICATION_FACTOR, Some("this BigPipe node runs with replication factor 1".to_string())));
                    continue;
                }
                if r.validate_only {
                    let code = if b.topic(&t.name).is_some() { error::TOPIC_ALREADY_EXISTS } else { error::NONE };
                    out.push((t.name, code, None));
                    continue;
                }
                match b.create_topic(&t.name, t.num_partitions, configs).await {
                    Ok(_) => out.push((t.name, error::NONE, None)),
                    Err(err) => out.push((t.name, err.kafka_code(), Some(err.to_string()))),
                }
            }
            m::encode_create_topics(e, v, &out);
        }
        Request::DeleteTopics(r) => {
            let mut out = Vec::new();
            for name in r.names {
                let code = match b.delete_topic(&name).await {
                    Ok(()) => error::NONE,
                    Err(err) => err.kafka_code(),
                };
                out.push((name, code));
            }
            m::encode_delete_topics(e, v, &out);
        }
        Request::InitProducerId(r) => {
            if r.transactional_id.is_some() {
                m::encode_init_producer_id(e, error::TRANSACTIONAL_ID_AUTHORIZATION_FAILED, -1, -1);
            } else {
                m::encode_init_producer_id(e, error::NONE, b.allocate_producer_id(), 0);
            }
        }
        Request::DescribeGroups(r) => {
            let groups: Vec<m::DescribedGroup> = r
                .groups
                .into_iter()
                .map(|g| match b.coordinator.describe(&g) {
                    Some(s) => m::DescribedGroup {
                        error_code: error::NONE,
                        group_id: g,
                        state: s.state,
                        protocol_type: s.protocol_type,
                        protocol: s.protocol,
                        members: s
                            .members
                            .into_iter()
                            .map(|mm| m::DescribedMember {
                                member_id: mm.member_id,
                                client_id: mm.client_id,
                                client_host: mm.client_host,
                                metadata: mm.raw_metadata,
                                assignment: mm.raw_assignment,
                            })
                            .collect(),
                    },
                    None => m::DescribedGroup {
                        error_code: error::NONE,
                        group_id: g,
                        state: "Dead".into(),
                        protocol_type: String::new(),
                        protocol: String::new(),
                        members: vec![],
                    },
                })
                .collect();
            m::encode_describe_groups(e, v, &groups);
        }
        Request::ListGroups => m::encode_list_groups(e, v, &b.coordinator.list()),
        Request::DescribeConfigs(r) => {
            let res: Vec<m::DescribedConfigResource> = r
                .resources
                .into_iter()
                .map(|(rtype, name, keys)| {
                    // resource type 2 = topic, 4 = broker
                    if rtype == 2 {
                        match b.topic(&name) {
                            Some(t) => {
                                let keys: Vec<String> = keys.unwrap_or_else(|| {
                                    bp_storage::config::KNOWN_KEYS.iter().map(|s| s.to_string()).collect()
                                });
                                let configs = keys
                                    .iter()
                                    .map(|k| {
                                        let explicit = t.raw_config.get(k).cloned();
                                        let is_default = explicit.is_none();
                                        (k.clone(), explicit.or_else(|| t.config.describe(k)), is_default)
                                    })
                                    .collect();
                                m::DescribedConfigResource { error_code: error::NONE, resource_type: rtype, name, configs }
                            }
                            None => m::DescribedConfigResource {
                                error_code: error::UNKNOWN_TOPIC_OR_PARTITION,
                                resource_type: rtype,
                                name,
                                configs: vec![],
                            },
                        }
                    } else {
                        m::DescribedConfigResource { error_code: error::NONE, resource_type: rtype, name, configs: vec![] }
                    }
                })
                .collect();
            m::encode_describe_configs(e, v, &res);
        }
    }
    true
}

/// Decodes a produce request and enqueues its appends before the connection reads the next
/// request; returns the future that waits for the results and encodes the response.
async fn produce_in_order(broker: Arc<Broker>, frame: BytesMut) -> Pending {
    let mut d = Decoder::new(frame);
    let decoded = m::decode_header(&mut d).ok().filter(|h| m::version_supported(h.api_key, h.api_version)).and_then(|h| {
        match Request::decode(&h, &mut d) {
            Ok(Request::Produce(r)) => Some((h, r)),
            _ => None,
        }
    });
    let Some((h, r)) = decoded else {
        broker.metrics.request_errors_total.fetch_add(1, Relaxed);
        tracing::warn!("malformed produce request");
        return Box::pin(async { None });
    };
    let acks = r.acks;
    let work = enqueue_produce(&broker, r).await;
    Box::pin(async move {
        let grouped = complete_produce(&broker, work).await;
        if acks == 0 {
            return None;
        }
        let mut e = m::response_encoder(h.correlation_id);
        m::encode_produce(&mut e, h.api_version, &grouped);
        Some(e.finish_frame())
    })
}

/// One partition of a produce request: enqueued, or already answered with an error.
type ProduceWork = Vec<(String, i32, Result<crate::broker::PendingAppend, BrokerError>)>;

async fn enqueue_produce(b: &Arc<Broker>, r: m::ProduceRequest) -> ProduceWork {
    let mut work = Vec::new();
    for t in r.topics {
        // Transactions are on the roadmap; reject them clearly instead of corrupting semantics.
        let meta = if r.transactional_id.is_some() { Err(BrokerError::Transactional) } else { b.ensure_topic(&t.name).await };
        for p in t.partitions {
            let pending = match (&meta, p.records) {
                (Err(BrokerError::Transactional), _) => Err(BrokerError::Transactional),
                (Err(_), _) => Err(BrokerError::UnknownTopic(t.name.clone())),
                (Ok(_), None) => Err(BrokerError::InvalidRecord("missing records".into())),
                (Ok(meta), Some(raw)) => b.produce_enqueue(meta, p.index, raw),
            };
            work.push((t.name.clone(), p.index, pending));
        }
    }
    work
}

async fn complete_produce(b: &Arc<Broker>, work: ProduceWork) -> Vec<(String, Vec<m::ProducePartitionResponse>)> {
    let results = futures::future::join_all(work.into_iter().map(|(name, index, pending)| async move {
        let r = match pending {
            Ok(p) => b.finish_append(p).await,
            Err(e) => Err(e),
        };
        let resp = match r {
            Ok(a) => m::ProducePartitionResponse {
                index,
                error_code: error::NONE,
                base_offset: a.base_offset,
                log_start_offset: a.log_start,
                error_message: None,
            },
            Err(err) => {
                tracing::debug!(topic = %name, partition = index, error = %err, "produce failed");
                m::ProducePartitionResponse {
                    index,
                    error_code: err.kafka_code(),
                    base_offset: -1,
                    log_start_offset: -1,
                    error_message: Some(err.to_string()),
                }
            }
        };
        (name, resp)
    }))
    .await;
    let mut grouped: Vec<(String, Vec<m::ProducePartitionResponse>)> = Vec::new();
    for (name, resp) in results {
        match grouped.iter_mut().find(|(n, _)| *n == name) {
            Some((_, v)) => v.push(resp),
            None => grouped.push((name, vec![resp])),
        }
    }
    grouped
}

async fn fetch(b: &Arc<Broker>, r: m::FetchRequest, e: &mut Encoder, v: i16) {
    let deadline = Instant::now() + Duration::from_millis(r.max_wait_ms.clamp(0, 60_000) as u64);
    let max_total = (r.max_bytes.max(1)) as usize;
    let metas: Vec<_> = r.topics.iter().map(|t| b.topic(&t.name)).collect();
    let mut watchers: Vec<_> = r
        .topics
        .iter()
        .zip(&metas)
        .flat_map(|(t, meta)| {
            t.partitions.iter().filter_map(move |p| meta.as_ref().and_then(|m| m.hw.get(p.partition as usize).cloned()))
        })
        .collect();
    loop {
        for w in &mut watchers {
            w.borrow_and_update();
        }
        let mut total = 0usize;
        let mut any_error = false;
        let mut out = Vec::with_capacity(r.topics.len());
        for (t, meta) in r.topics.iter().zip(&metas) {
            let mut parts = Vec::with_capacity(t.partitions.len());
            for p in &t.partitions {
                let resp = match meta {
                    None => m::FetchPartitionResponse {
                        partition: p.partition,
                        error_code: error::UNKNOWN_TOPIC_OR_PARTITION,
                        high_watermark: -1,
                        log_start_offset: -1,
                        records: vec![],
                    },
                    Some(meta) => {
                        let budget = (p.partition_max_bytes.max(1) as usize).min(max_total.saturating_sub(total).max(1));
                        match b.read(meta, p.partition, p.fetch_offset, budget).await {
                            Ok(d) if total > 0 && total + d.bytes() > max_total => m::FetchPartitionResponse {
                                // Over the response budget: report position without data.
                                partition: p.partition,
                                error_code: error::NONE,
                                high_watermark: d.high_watermark,
                                log_start_offset: d.log_start,
                                records: vec![],
                            },
                            Ok(d) => {
                                total += d.bytes();
                                m::FetchPartitionResponse {
                                    partition: p.partition,
                                    error_code: error::NONE,
                                    high_watermark: d.high_watermark,
                                    log_start_offset: d.log_start,
                                    records: d.chunks,
                                }
                            }
                            Err(err) => m::FetchPartitionResponse {
                                partition: p.partition,
                                error_code: err.kafka_code(),
                                high_watermark: b.high_watermark(meta, p.partition),
                                log_start_offset: -1,
                                records: vec![],
                            },
                        }
                    }
                };
                any_error |= resp.error_code != error::NONE;
                parts.push(resp);
            }
            out.push((t.name.clone(), parts));
        }
        if any_error || total >= r.min_bytes.max(1) as usize || Instant::now() >= deadline {
            m::encode_fetch(e, v, &out);
            return;
        }
        Broker::wait_for_data(&mut watchers, deadline).await;
    }
}
