//! Kafka request decoding and response encoding for the API subset BigPipe serves.
//!
//! BigPipe advertises only non-flexible versions (plus ApiVersions v3), so every client —
//! Java 4.x, librdkafka, franz-go, KafkaJS, kafka-python — negotiates down to the classic
//! encoding. Versions are listed in [`SUPPORTED_APIS`].

use bytes::{Bytes, BytesMut};

use crate::codec::{Decoder, Encoder, Result};
use crate::ProtocolError;

pub mod api {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const OFFSET_FETCH: i16 = 9;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const JOIN_GROUP: i16 = 11;
    pub const HEARTBEAT: i16 = 12;
    pub const LEAVE_GROUP: i16 = 13;
    pub const SYNC_GROUP: i16 = 14;
    pub const DESCRIBE_GROUPS: i16 = 15;
    pub const LIST_GROUPS: i16 = 16;
    pub const API_VERSIONS: i16 = 18;
    pub const CREATE_TOPICS: i16 = 19;
    pub const DELETE_TOPICS: i16 = 20;
    pub const INIT_PRODUCER_ID: i16 = 22;
    pub const DESCRIBE_CONFIGS: i16 = 32;
}

/// (api key, min version, max version)
pub const SUPPORTED_APIS: &[(i16, i16, i16)] = &[
    (api::PRODUCE, 3, 8),
    (api::FETCH, 4, 11),
    (api::LIST_OFFSETS, 1, 5),
    (api::METADATA, 1, 8),
    (api::OFFSET_COMMIT, 2, 7),
    (api::OFFSET_FETCH, 1, 5),
    (api::FIND_COORDINATOR, 0, 2),
    (api::JOIN_GROUP, 0, 5),
    (api::HEARTBEAT, 0, 3),
    (api::LEAVE_GROUP, 0, 3),
    (api::SYNC_GROUP, 0, 3),
    (api::DESCRIBE_GROUPS, 0, 4),
    (api::LIST_GROUPS, 0, 2),
    (api::API_VERSIONS, 0, 3),
    (api::CREATE_TOPICS, 0, 4),
    (api::DELETE_TOPICS, 0, 3),
    (api::INIT_PRODUCER_ID, 0, 1),
    (api::DESCRIBE_CONFIGS, 0, 3),
];

pub fn api_name(key: i16) -> &'static str {
    match key {
        api::PRODUCE => "Produce",
        api::FETCH => "Fetch",
        api::LIST_OFFSETS => "ListOffsets",
        api::METADATA => "Metadata",
        api::OFFSET_COMMIT => "OffsetCommit",
        api::OFFSET_FETCH => "OffsetFetch",
        api::FIND_COORDINATOR => "FindCoordinator",
        api::JOIN_GROUP => "JoinGroup",
        api::HEARTBEAT => "Heartbeat",
        api::LEAVE_GROUP => "LeaveGroup",
        api::SYNC_GROUP => "SyncGroup",
        api::DESCRIBE_GROUPS => "DescribeGroups",
        api::LIST_GROUPS => "ListGroups",
        api::API_VERSIONS => "ApiVersions",
        api::CREATE_TOPICS => "CreateTopics",
        api::DELETE_TOPICS => "DeleteTopics",
        api::INIT_PRODUCER_ID => "InitProducerId",
        api::DESCRIBE_CONFIGS => "DescribeConfigs",
        _ => "Unknown",
    }
}

pub fn version_supported(key: i16, version: i16) -> bool {
    SUPPORTED_APIS.iter().any(|&(k, min, max)| k == key && version >= min && version <= max)
}

pub mod error {
    pub const UNKNOWN_SERVER_ERROR: i16 = -1;
    pub const NONE: i16 = 0;
    pub const OFFSET_OUT_OF_RANGE: i16 = 1;
    pub const CORRUPT_MESSAGE: i16 = 2;
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub const LEADER_NOT_AVAILABLE: i16 = 5;
    pub const REQUEST_TIMED_OUT: i16 = 7;
    pub const MESSAGE_TOO_LARGE: i16 = 10;
    pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    pub const NOT_COORDINATOR: i16 = 16;
    pub const INVALID_TOPIC_EXCEPTION: i16 = 17;
    pub const RECORD_LIST_TOO_LARGE: i16 = 18;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
    pub const INVALID_GROUP_ID: i16 = 24;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const INVALID_SESSION_TIMEOUT: i16 = 26;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const UNSUPPORTED_VERSION: i16 = 35;
    pub const TOPIC_ALREADY_EXISTS: i16 = 36;
    pub const INVALID_PARTITIONS: i16 = 37;
    pub const INVALID_REPLICATION_FACTOR: i16 = 38;
    pub const INVALID_REQUEST: i16 = 42;
    pub const INVALID_CONFIG: i16 = 40;
    pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
    pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
    pub const INVALID_PRODUCER_EPOCH: i16 = 47;
    pub const GROUP_ID_NOT_FOUND: i16 = 69;
    pub const UNKNOWN_PRODUCER_ID: i16 = 59;
    pub const NON_EMPTY_GROUP: i16 = 68;
    pub const INVALID_RECORD: i16 = 87;
    pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
}

#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

/// Decodes just the request header; the body is decoded by [`Request::decode`].
pub fn decode_header(d: &mut Decoder) -> Result<RequestHeader> {
    let api_key = d.i16()?;
    let api_version = d.i16()?;
    let correlation_id = d.i32()?;
    let client_id = d.nullable_string()?;
    // ApiVersions v3+ uses request header v2 (flexible) — skip its tagged fields.
    if api_key == api::API_VERSIONS && api_version >= 3 {
        d.tagged_fields()?;
    }
    Ok(RequestHeader { api_key, api_version, correlation_id, client_id })
}

// ---------------------------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
pub enum Request {
    ApiVersions,
    Metadata(MetadataRequest),
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    ListOffsets(ListOffsetsRequest),
    FindCoordinator(FindCoordinatorRequest),
    JoinGroup(JoinGroupRequest),
    SyncGroup(SyncGroupRequest),
    Heartbeat(HeartbeatRequest),
    LeaveGroup(LeaveGroupRequest),
    OffsetCommit(OffsetCommitRequest),
    OffsetFetch(OffsetFetchRequest),
    CreateTopics(CreateTopicsRequest),
    DeleteTopics(DeleteTopicsRequest),
    InitProducerId(InitProducerIdRequest),
    DescribeGroups(DescribeGroupsRequest),
    ListGroups,
    DescribeConfigs(DescribeConfigsRequest),
}

#[derive(Debug)]
pub struct MetadataRequest {
    /// `None` = all topics.
    pub topics: Option<Vec<String>>,
    pub allow_auto_topic_creation: bool,
}

#[derive(Debug)]
pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopic>,
}

#[derive(Debug)]
pub struct ProduceTopic {
    pub name: String,
    pub partitions: Vec<ProducePartition>,
}

#[derive(Debug)]
pub struct ProducePartition {
    pub index: i32,
    /// Uniquely owned slice of the request frame (patched in place by the broker).
    pub records: Option<BytesMut>,
}

#[derive(Debug)]
pub struct FetchRequest {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: Vec<FetchTopic>,
}

#[derive(Debug)]
pub struct FetchTopic {
    pub name: String,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug)]
pub struct FetchPartition {
    pub partition: i32,
    pub fetch_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug)]
pub struct ListOffsetsRequest {
    pub isolation_level: i8,
    pub topics: Vec<(String, Vec<(i32, i64)>)>,
}

#[derive(Debug)]
pub struct FindCoordinatorRequest {
    pub key: String,
    pub key_type: i8,
}

#[derive(Debug)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<(String, Bytes)>,
}

#[derive(Debug)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub assignments: Vec<(String, Bytes)>,
}

#[derive(Debug)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
}

#[derive(Debug)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_ids: Vec<String>,
}

#[derive(Debug)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub topics: Vec<(String, Vec<OffsetCommitPartition>)>,
}

#[derive(Debug)]
pub struct OffsetCommitPartition {
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
}

#[derive(Debug)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    /// `None` = all committed partitions of the group.
    pub topics: Option<Vec<(String, Vec<i32>)>>,
}

#[derive(Debug)]
pub struct CreateTopicsRequest {
    pub topics: Vec<CreatableTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug)]
pub struct CreatableTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: Vec<(String, Option<String>)>,
}

#[derive(Debug)]
pub struct DeleteTopicsRequest {
    pub names: Vec<String>,
}

#[derive(Debug)]
pub struct InitProducerIdRequest {
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: i32,
}

#[derive(Debug)]
pub struct DescribeGroupsRequest {
    pub groups: Vec<String>,
}

#[derive(Debug)]
pub struct DescribeConfigsRequest {
    pub resources: Vec<(i8, String, Option<Vec<String>>)>,
}

impl Request {
    pub fn decode(h: &RequestHeader, d: &mut Decoder) -> Result<Request> {
        let v = h.api_version;
        Ok(match h.api_key {
            api::API_VERSIONS => {
                if v >= 3 {
                    let _name = d.compact_string()?;
                    let _ver = d.compact_string()?;
                    d.tagged_fields()?;
                }
                Request::ApiVersions
            }
            api::METADATA => {
                let topics = d.nullable_array(|d| d.string())?;
                let allow = if v >= 4 { d.bool()? } else { true };
                if v >= 8 {
                    d.bool()?;
                    d.bool()?;
                }
                // v0 semantics: empty list means all topics. v1+: null means all.
                let topics = match topics {
                    Some(t) if t.is_empty() && v == 0 => None,
                    t => t,
                };
                Request::Metadata(MetadataRequest { topics, allow_auto_topic_creation: allow })
            }
            api::PRODUCE => {
                let transactional_id = if v >= 3 { d.nullable_string()? } else { None };
                let acks = d.i16()?;
                let timeout_ms = d.i32()?;
                let topics = d.array(|d| {
                    let name = d.string()?;
                    let partitions = d.array(|d| {
                        Ok(ProducePartition { index: d.i32()?, records: d.nullable_bytes()? })
                    })?;
                    Ok(ProduceTopic { name, partitions })
                })?;
                Request::Produce(ProduceRequest { transactional_id, acks, timeout_ms, topics })
            }
            api::FETCH => {
                let _replica = d.i32()?;
                let max_wait_ms = d.i32()?;
                let min_bytes = d.i32()?;
                let max_bytes = if v >= 3 { d.i32()? } else { i32::MAX };
                let isolation_level = if v >= 4 { d.i8()? } else { 0 };
                let (session_id, session_epoch) = if v >= 7 { (d.i32()?, d.i32()?) } else { (0, -1) };
                let topics = d.array(|d| {
                    let name = d.string()?;
                    let partitions = d.array(|d| {
                        let partition = d.i32()?;
                        if v >= 9 {
                            d.i32()?; // current_leader_epoch
                        }
                        let fetch_offset = d.i64()?;
                        if v >= 5 {
                            d.i64()?; // log_start_offset (followers only)
                        }
                        let partition_max_bytes = d.i32()?;
                        Ok(FetchPartition { partition, fetch_offset, partition_max_bytes })
                    })?;
                    Ok(FetchTopic { name, partitions })
                })?;
                if v >= 7 {
                    d.array(|d| {
                        d.string()?;
                        d.array(|d| d.i32())
                    })?;
                }
                if v >= 11 {
                    d.string()?; // rack_id
                }
                Request::Fetch(FetchRequest {
                    max_wait_ms,
                    min_bytes,
                    max_bytes,
                    isolation_level,
                    session_id,
                    session_epoch,
                    topics,
                })
            }
            api::LIST_OFFSETS => {
                let _replica = d.i32()?;
                let isolation_level = if v >= 2 { d.i8()? } else { 0 };
                let topics = d.array(|d| {
                    let name = d.string()?;
                    let parts = d.array(|d| {
                        let p = d.i32()?;
                        if v >= 4 {
                            d.i32()?;
                        }
                        let ts = d.i64()?;
                        if v == 0 {
                            d.i32()?;
                        }
                        Ok((p, ts))
                    })?;
                    Ok((name, parts))
                })?;
                Request::ListOffsets(ListOffsetsRequest { isolation_level, topics })
            }
            api::FIND_COORDINATOR => {
                let key = d.string()?;
                let key_type = if v >= 1 { d.i8()? } else { 0 };
                Request::FindCoordinator(FindCoordinatorRequest { key, key_type })
            }
            api::JOIN_GROUP => {
                let group_id = d.string()?;
                let session_timeout_ms = d.i32()?;
                let rebalance_timeout_ms = if v >= 1 { d.i32()? } else { session_timeout_ms };
                let member_id = d.string()?;
                let group_instance_id = if v >= 5 { d.nullable_string()? } else { None };
                let protocol_type = d.string()?;
                let protocols = d.array(|d| Ok((d.string()?, d.bytes()?.freeze())))?;
                Request::JoinGroup(JoinGroupRequest {
                    group_id,
                    session_timeout_ms,
                    rebalance_timeout_ms,
                    member_id,
                    group_instance_id,
                    protocol_type,
                    protocols,
                })
            }
            api::SYNC_GROUP => {
                let group_id = d.string()?;
                let generation_id = d.i32()?;
                let member_id = d.string()?;
                if v >= 3 {
                    d.nullable_string()?;
                }
                let assignments = d.array(|d| Ok((d.string()?, d.bytes()?.freeze())))?;
                Request::SyncGroup(SyncGroupRequest { group_id, generation_id, member_id, assignments })
            }
            api::HEARTBEAT => {
                let group_id = d.string()?;
                let generation_id = d.i32()?;
                let member_id = d.string()?;
                if v >= 3 {
                    d.nullable_string()?;
                }
                Request::Heartbeat(HeartbeatRequest { group_id, generation_id, member_id })
            }
            api::LEAVE_GROUP => {
                let group_id = d.string()?;
                let member_ids = if v >= 3 {
                    d.array(|d| {
                        let m = d.string()?;
                        d.nullable_string()?;
                        Ok(m)
                    })?
                } else {
                    vec![d.string()?]
                };
                Request::LeaveGroup(LeaveGroupRequest { group_id, member_ids })
            }
            api::OFFSET_COMMIT => {
                let group_id = d.string()?;
                let generation_id = if v >= 1 { d.i32()? } else { -1 };
                let member_id = if v >= 1 { d.string()? } else { String::new() };
                if v >= 7 {
                    d.nullable_string()?;
                }
                if (2..=4).contains(&v) {
                    d.i64()?; // retention_time_ms
                }
                let topics = d.array(|d| {
                    let name = d.string()?;
                    let parts = d.array(|d| {
                        let partition = d.i32()?;
                        let offset = d.i64()?;
                        if v >= 6 {
                            d.i32()?;
                        }
                        if v == 1 {
                            d.i64()?;
                        }
                        let metadata = d.nullable_string()?;
                        Ok(OffsetCommitPartition { partition, offset, metadata })
                    })?;
                    Ok((name, parts))
                })?;
                Request::OffsetCommit(OffsetCommitRequest { group_id, generation_id, member_id, topics })
            }
            api::OFFSET_FETCH => {
                let group_id = d.string()?;
                let topics = d.nullable_array(|d| Ok((d.string()?, d.array(|d| d.i32())?)))?;
                Request::OffsetFetch(OffsetFetchRequest { group_id, topics })
            }
            api::CREATE_TOPICS => {
                let topics = d.array(|d| {
                    let name = d.string()?;
                    let num_partitions = d.i32()?;
                    let replication_factor = d.i16()?;
                    d.array(|d| {
                        d.i32()?;
                        d.array(|d| d.i32())
                    })?;
                    let configs = d.array(|d| Ok((d.string()?, d.nullable_string()?)))?;
                    Ok(CreatableTopic { name, num_partitions, replication_factor, configs })
                })?;
                let timeout_ms = d.i32()?;
                let validate_only = if v >= 1 { d.bool()? } else { false };
                Request::CreateTopics(CreateTopicsRequest { topics, timeout_ms, validate_only })
            }
            api::DELETE_TOPICS => {
                let names = d.array(|d| d.string())?;
                d.i32()?;
                Request::DeleteTopics(DeleteTopicsRequest { names })
            }
            api::INIT_PRODUCER_ID => Request::InitProducerId(InitProducerIdRequest {
                transactional_id: d.nullable_string()?,
                transaction_timeout_ms: d.i32()?,
            }),
            api::DESCRIBE_GROUPS => {
                let groups = d.array(|d| d.string())?;
                if v >= 3 {
                    d.bool()?;
                }
                Request::DescribeGroups(DescribeGroupsRequest { groups })
            }
            api::LIST_GROUPS => Request::ListGroups,
            api::DESCRIBE_CONFIGS => {
                let resources = d.array(|d| Ok((d.i8()?, d.string()?, d.nullable_array(|d| d.string())?)))?;
                if v >= 1 {
                    d.bool()?;
                }
                if v >= 3 {
                    d.bool()?;
                }
                Request::DescribeConfigs(DescribeConfigsRequest { resources })
            }
            other => return Err(ProtocolError::UnsupportedApi(other)),
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------------------------

/// Starts a response frame body with the (v0) response header.
pub fn response_encoder(correlation_id: i32) -> Encoder {
    let mut e = Encoder::new();
    e.i32(correlation_id);
    e
}

pub fn encode_api_versions(e: &mut Encoder, v: i16, error_code: i16) {
    e.i16(error_code);
    if v >= 3 {
        e.compact_array_len(SUPPORTED_APIS.len());
        for &(k, min, max) in SUPPORTED_APIS {
            e.i16(k);
            e.i16(min);
            e.i16(max);
            e.empty_tagged_fields();
        }
        e.i32(0);
        e.empty_tagged_fields();
    } else {
        e.array_len(SUPPORTED_APIS.len());
        for &(k, min, max) in SUPPORTED_APIS {
            e.i16(k);
            e.i16(min);
            e.i16(max);
        }
        if v >= 1 {
            e.i32(0);
        }
    }
}

pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

pub struct TopicMetadata {
    pub error_code: i16,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

pub struct PartitionMetadata {
    pub error_code: i16,
    pub index: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
}

pub struct MetadataResponse {
    pub brokers: Vec<BrokerInfo>,
    pub cluster_id: String,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataResponse {
    pub fn encode(&self, e: &mut Encoder, v: i16) {
        if v >= 3 {
            e.i32(0);
        }
        e.array_len(self.brokers.len());
        for b in &self.brokers {
            e.i32(b.node_id);
            e.string(&b.host);
            e.i32(b.port);
            if v >= 1 {
                e.nullable_string(b.rack.as_deref());
            }
        }
        if v >= 2 {
            e.nullable_string(Some(&self.cluster_id));
        }
        if v >= 1 {
            e.i32(self.controller_id);
        }
        e.array_len(self.topics.len());
        for t in &self.topics {
            e.i16(t.error_code);
            e.string(&t.name);
            if v >= 1 {
                e.bool(t.is_internal);
            }
            e.array_len(t.partitions.len());
            for p in &t.partitions {
                e.i16(p.error_code);
                e.i32(p.index);
                e.i32(p.leader);
                if v >= 7 {
                    e.i32(0);
                }
                e.array_len(p.replicas.len());
                for r in &p.replicas {
                    e.i32(*r);
                }
                e.array_len(p.replicas.len());
                for r in &p.replicas {
                    e.i32(*r);
                }
                if v >= 5 {
                    e.array_len(0);
                }
            }
            if v >= 8 {
                e.i32(i32::MIN);
            }
        }
        if v >= 8 {
            e.i32(i32::MIN);
        }
    }
}

pub struct ProducePartitionResponse {
    pub index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_start_offset: i64,
    pub error_message: Option<String>,
}

pub fn encode_produce(e: &mut Encoder, v: i16, topics: &[(String, Vec<ProducePartitionResponse>)]) {
    e.array_len(topics.len());
    for (name, parts) in topics {
        e.string(name);
        e.array_len(parts.len());
        for p in parts {
            e.i32(p.index);
            e.i16(p.error_code);
            e.i64(p.base_offset);
            if v >= 2 {
                e.i64(-1); // log_append_time_ms
            }
            if v >= 5 {
                e.i64(p.log_start_offset);
            }
            if v >= 8 {
                e.array_len(0);
                e.nullable_string(p.error_message.as_deref());
            }
        }
    }
    if v >= 1 {
        e.i32(0);
    }
}

pub struct FetchPartitionResponse {
    pub partition: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub records: Vec<Bytes>,
}

pub fn encode_fetch(e: &mut Encoder, v: i16, topics: &[(String, Vec<FetchPartitionResponse>)]) {
    if v >= 1 {
        e.i32(0);
    }
    if v >= 7 {
        e.i16(0);
        e.i32(0); // session id: BigPipe answers every fetch as a full fetch
    }
    e.array_len(topics.len());
    for (name, parts) in topics {
        e.string(name);
        e.array_len(parts.len());
        for p in parts {
            e.i32(p.partition);
            e.i16(p.error_code);
            e.i64(p.high_watermark);
            if v >= 4 {
                e.i64(p.high_watermark); // last_stable_offset
            }
            if v >= 5 {
                e.i64(p.log_start_offset);
            }
            if v >= 4 {
                e.array_len(0); // aborted_transactions
            }
            if v >= 11 {
                e.i32(-1);
            }
            e.records(Some(&p.records));
        }
    }
}

pub struct ListOffsetsPartitionResponse {
    pub partition: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
}

pub fn encode_list_offsets(e: &mut Encoder, v: i16, topics: &[(String, Vec<ListOffsetsPartitionResponse>)]) {
    if v >= 2 {
        e.i32(0);
    }
    e.array_len(topics.len());
    for (name, parts) in topics {
        e.string(name);
        e.array_len(parts.len());
        for p in parts {
            e.i32(p.partition);
            e.i16(p.error_code);
            e.i64(p.timestamp);
            e.i64(p.offset);
            if v >= 4 {
                e.i32(0);
            }
        }
    }
}

pub fn encode_find_coordinator(e: &mut Encoder, v: i16, error_code: i16, node: &BrokerInfo) {
    if v >= 1 {
        e.i32(0);
    }
    e.i16(error_code);
    if v >= 1 {
        e.nullable_string(None);
    }
    e.i32(node.node_id);
    e.string(&node.host);
    e.i32(node.port);
}

pub struct JoinGroupResponse {
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<(String, Bytes)>,
}

impl JoinGroupResponse {
    pub fn error(code: i16, member_id: &str) -> Self {
        Self {
            error_code: code,
            generation_id: -1,
            protocol_name: String::new(),
            leader: String::new(),
            member_id: member_id.to_string(),
            members: Vec::new(),
        }
    }

    pub fn encode(&self, e: &mut Encoder, v: i16) {
        if v >= 2 {
            e.i32(0);
        }
        e.i16(self.error_code);
        e.i32(self.generation_id);
        e.string(&self.protocol_name);
        e.string(&self.leader);
        e.string(&self.member_id);
        e.array_len(self.members.len());
        for (id, meta) in &self.members {
            e.string(id);
            if v >= 5 {
                e.nullable_string(None);
            }
            e.bytes(meta);
        }
    }
}

pub fn encode_sync_group(e: &mut Encoder, v: i16, error_code: i16, assignment: &[u8]) {
    if v >= 1 {
        e.i32(0);
    }
    e.i16(error_code);
    e.bytes(assignment);
}

pub fn encode_heartbeat(e: &mut Encoder, v: i16, error_code: i16) {
    if v >= 1 {
        e.i32(0);
    }
    e.i16(error_code);
}

pub fn encode_leave_group(e: &mut Encoder, v: i16, error_code: i16, members: &[(String, i16)]) {
    if v >= 1 {
        e.i32(0);
    }
    e.i16(error_code);
    if v >= 3 {
        e.array_len(members.len());
        for (m, code) in members {
            e.string(m);
            e.nullable_string(None);
            e.i16(*code);
        }
    }
}

pub fn encode_offset_commit(e: &mut Encoder, v: i16, topics: &[(String, Vec<(i32, i16)>)]) {
    if v >= 3 {
        e.i32(0);
    }
    e.array_len(topics.len());
    for (name, parts) in topics {
        e.string(name);
        e.array_len(parts.len());
        for (p, code) in parts {
            e.i32(*p);
            e.i16(*code);
        }
    }
}

pub struct OffsetFetchPartition {
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
    pub error_code: i16,
}

pub fn encode_offset_fetch(e: &mut Encoder, v: i16, topics: &[(String, Vec<OffsetFetchPartition>)], error_code: i16) {
    if v >= 3 {
        e.i32(0);
    }
    e.array_len(topics.len());
    for (name, parts) in topics {
        e.string(name);
        e.array_len(parts.len());
        for p in parts {
            e.i32(p.partition);
            e.i64(p.offset);
            if v >= 5 {
                e.i32(-1);
            }
            e.nullable_string(p.metadata.as_deref());
            e.i16(p.error_code);
        }
    }
    if v >= 2 {
        e.i16(error_code);
    }
}

pub fn encode_create_topics(e: &mut Encoder, v: i16, results: &[(String, i16, Option<String>)]) {
    if v >= 2 {
        e.i32(0);
    }
    e.array_len(results.len());
    for (name, code, msg) in results {
        e.string(name);
        e.i16(*code);
        if v >= 1 {
            e.nullable_string(msg.as_deref());
        }
    }
}

pub fn encode_delete_topics(e: &mut Encoder, v: i16, results: &[(String, i16)]) {
    if v >= 1 {
        e.i32(0);
    }
    e.array_len(results.len());
    for (name, code) in results {
        e.string(name);
        e.i16(*code);
    }
}

pub fn encode_init_producer_id(e: &mut Encoder, error_code: i16, producer_id: i64, epoch: i16) {
    e.i32(0);
    e.i16(error_code);
    e.i64(producer_id);
    e.i16(epoch);
}

pub struct DescribedGroup {
    pub error_code: i16,
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol: String,
    pub members: Vec<DescribedMember>,
}

pub struct DescribedMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Bytes,
    pub assignment: Bytes,
}

pub fn encode_describe_groups(e: &mut Encoder, v: i16, groups: &[DescribedGroup]) {
    if v >= 1 {
        e.i32(0);
    }
    e.array_len(groups.len());
    for g in groups {
        e.i16(g.error_code);
        e.string(&g.group_id);
        e.string(&g.state);
        e.string(&g.protocol_type);
        e.string(&g.protocol);
        e.array_len(g.members.len());
        for m in &g.members {
            e.string(&m.member_id);
            if v >= 4 {
                e.nullable_string(None);
            }
            e.string(&m.client_id);
            e.string(&m.client_host);
            e.bytes(&m.metadata);
            e.bytes(&m.assignment);
        }
        if v >= 3 {
            e.i32(i32::MIN);
        }
    }
}

pub fn encode_list_groups(e: &mut Encoder, v: i16, groups: &[(String, String)]) {
    if v >= 1 {
        e.i32(0);
    }
    e.i16(0);
    e.array_len(groups.len());
    for (id, ptype) in groups {
        e.string(id);
        e.string(ptype);
    }
}

pub struct DescribedConfigResource {
    pub error_code: i16,
    pub resource_type: i8,
    pub name: String,
    pub configs: Vec<(String, Option<String>, bool)>, // (name, value, is_default)
}

pub fn encode_describe_configs(e: &mut Encoder, v: i16, resources: &[DescribedConfigResource]) {
    e.i32(0);
    e.array_len(resources.len());
    for r in resources {
        e.i16(r.error_code);
        e.nullable_string(None);
        e.i8(r.resource_type);
        e.string(&r.name);
        e.array_len(r.configs.len());
        for (name, value, is_default) in &r.configs {
            e.string(name);
            e.nullable_string(value.as_deref());
            e.bool(false); // read_only
            if v == 0 {
                e.bool(*is_default);
            } else {
                // config_source: 5 = DEFAULT_CONFIG, 1 = DYNAMIC_TOPIC_CONFIG
                e.i8(if *is_default { 5 } else { 1 });
            }
            e.bool(false); // is_sensitive
            if v >= 1 {
                e.array_len(0);
            }
            if v >= 3 {
                e.i8(0);
                e.nullable_string(None);
            }
        }
    }
}
