//! Group coordinator.
//!
//! - Classic Kafka consumer groups (JoinGroup / SyncGroup / Heartbeat / LeaveGroup), with the
//!   delayed initial rebalance, session expiry and generation fencing Kafka clients expect.
//! - Server-assigned HTTP groups used by the REST gateway and the Python / TypeScript / Go /
//!   Java SDKs: the broker computes the assignment (KIP-848 style), clients just poll.
//! - Durable committed offsets shared by both, persisted in `meta/offsets.log`.
//!
//! The coordinator is control-path code; a single mutex is fine here. Waiting clients park on
//! oneshot channels outside the lock.

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use bp_protocol::messages::{JoinGroupRequest, JoinGroupResponse, error};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::metrics::Metrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl GroupState {
    pub fn kafka_name(&self) -> &'static str {
        match self {
            GroupState::Empty => "Empty",
            GroupState::PreparingRebalance => "PreparingRebalance",
            GroupState::CompletingRebalance => "CompletingRebalance",
            GroupState::Stable => "Stable",
        }
    }
}

struct Member {
    id: String,
    client_id: String,
    client_host: String,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    protocols: Vec<(String, Bytes)>,
    assignment: Bytes,
    last_seen: Instant,
}

struct Group {
    state: GroupState,
    protocol_type: String,
    protocol: String,
    generation: i32,
    leader: Option<String>,
    members: Vec<Member>,
    pending_joins: HashMap<String, oneshot::Sender<JoinGroupResponse>>,
    pending_syncs: HashMap<String, oneshot::Sender<(i16, Bytes)>>,
    /// Bumped on every rebalance start so stale timers are ignored.
    rebalance_seq: u64,
    /// Initial rebalance of an empty group completes only when its delay timer fires.
    initial: bool,
}

impl Group {
    fn new() -> Self {
        Self {
            state: GroupState::Empty,
            protocol_type: String::new(),
            protocol: String::new(),
            generation: 0,
            leader: None,
            members: Vec::new(),
            pending_joins: HashMap::new(),
            pending_syncs: HashMap::new(),
            rebalance_seq: 0,
            initial: false,
        }
    }

    fn member(&mut self, id: &str) -> Option<&mut Member> {
        self.members.iter_mut().find(|m| m.id == id)
    }
}

// ---------------------------------------------------------------------------------------------
// Server-assigned HTTP groups
// ---------------------------------------------------------------------------------------------

pub struct HttpMember {
    pub id: String,
    pub topics: Vec<String>,
    pub assigned: Vec<(String, i32)>,
    /// Next offset to deliver per assigned partition (in-memory session state).
    pub positions: HashMap<(String, i32), i64>,
    pub auto_commit: bool,
    pub offset_reset: String,
    pub filter: Option<String>,
    last_seen: Instant,
    session_timeout: Duration,
}

struct HttpGroup {
    generation: i32,
    members: BTreeMap<String, HttpMember>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpAssignment {
    pub generation: i32,
    pub partitions: Vec<(String, i32)>,
}

// ---------------------------------------------------------------------------------------------
// Offsets
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedOffset {
    pub offset: i64,
    pub metadata: Option<String>,
    pub commit_ms: i64,
}

#[derive(Serialize, Deserialize)]
struct OffsetLine {
    g: String,
    t: String,
    p: i32,
    o: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    m: Option<String>,
    ts: i64,
}

struct OffsetStore {
    map: HashMap<String, BTreeMap<(String, i32), CommittedOffset>>,
    log: File,
}

impl OffsetStore {
    fn open(path: PathBuf) -> std::io::Result<Self> {
        let mut map: HashMap<String, BTreeMap<(String, i32), CommittedOffset>> = HashMap::new();
        if let Ok(f) = File::open(&path) {
            for line in BufReader::new(f).lines().map_while(Result::ok) {
                if let Ok(l) = serde_json::from_str::<OffsetLine>(&line) {
                    let e = map.entry(l.g).or_default();
                    if l.o < 0 {
                        e.remove(&(l.t, l.p));
                    } else {
                        e.insert((l.t, l.p), CommittedOffset { offset: l.o, metadata: l.m, commit_ms: l.ts });
                    }
                }
            }
        }
        // Compact on startup.
        let tmp = path.with_extension("log.tmp");
        {
            let mut w = std::io::BufWriter::new(File::create(&tmp)?);
            for (g, parts) in &map {
                for ((t, p), c) in parts {
                    let line = OffsetLine { g: g.clone(), t: t.clone(), p: *p, o: c.offset, m: c.metadata.clone(), ts: c.commit_ms };
                    serde_json::to_writer(&mut w, &line)?;
                    w.write_all(b"\n")?;
                }
            }
            w.flush()?;
        }
        std::fs::rename(&tmp, &path)?;
        let log = OpenOptions::new().append(true).open(&path)?;
        Ok(Self { map, log })
    }

    fn commit(&mut self, group: &str, topic: &str, partition: i32, offset: i64, metadata: Option<String>) {
        let ts = bp_storage::partition::now_ms();
        let line = OffsetLine { g: group.into(), t: topic.into(), p: partition, o: offset, m: metadata.clone(), ts };
        if let Ok(mut s) = serde_json::to_vec(&line) {
            s.push(b'\n');
            if let Err(e) = self.log.write_all(&s) {
                tracing::error!(error = %e, "offset log write failed");
            }
        }
        self.map
            .entry(group.to_string())
            .or_default()
            .insert((topic.to_string(), partition), CommittedOffset { offset, metadata, commit_ms: ts });
    }

    fn delete_group(&mut self, group: &str) {
        if let Some(parts) = self.map.remove(group) {
            for (t, p) in parts.keys() {
                let line = OffsetLine { g: group.into(), t: t.clone(), p: *p, o: -1, m: None, ts: 0 };
                if let Ok(mut s) = serde_json::to_vec(&line) {
                    s.push(b'\n');
                    let _ = self.log.write_all(&s);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Coordinator
// ---------------------------------------------------------------------------------------------

#[derive(Clone)]
pub struct GroupConfig {
    pub initial_delay: Duration,
    pub min_session_ms: i32,
    pub max_session_ms: i32,
}

pub struct Coordinator {
    groups: Mutex<HashMap<String, Group>>,
    http: Mutex<HashMap<String, HttpGroup>>,
    offsets: Mutex<OffsetStore>,
    cfg: GroupConfig,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupSummary {
    pub group_id: String,
    pub kind: &'static str,
    pub state: String,
    pub protocol_type: String,
    pub protocol: String,
    pub generation: i32,
    pub members: Vec<MemberSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemberSummary {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    /// Partitions assigned to this member (decoded from the consumer protocol when possible).
    #[serde(serialize_with = "serialize_tps")]
    pub assignment: Vec<(String, i32)>,
    #[serde(skip)]
    pub raw_metadata: Bytes,
    #[serde(skip)]
    pub raw_assignment: Bytes,
}

impl Coordinator {
    pub fn open(data_dir: &std::path::Path, cfg: GroupConfig, metrics: Arc<Metrics>) -> std::io::Result<Arc<Self>> {
        std::fs::create_dir_all(data_dir.join("meta"))?;
        let offsets = OffsetStore::open(data_dir.join("meta").join("offsets.log"))?;
        let c = Arc::new(Self {
            groups: Mutex::new(HashMap::new()),
            http: Mutex::new(HashMap::new()),
            offsets: Mutex::new(offsets),
            cfg,
            metrics,
        });
        let weak = Arc::downgrade(&c);
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_millis(250));
            loop {
                t.tick().await;
                let Some(c) = weak.upgrade() else { return };
                c.expire_members();
            }
        });
        Ok(c)
    }

    // ------------------------------------------------------------------ classic protocol ------

    pub fn join(
        self: &Arc<Self>,
        req: JoinGroupRequest,
        client_id: &str,
        client_host: &str,
    ) -> oneshot::Receiver<JoinGroupResponse> {
        let (tx, rx) = oneshot::channel();
        let fail = |tx: oneshot::Sender<JoinGroupResponse>, code: i16, member: &str| {
            let _ = tx.send(JoinGroupResponse::error(code, member));
        };
        if req.group_id.is_empty() {
            fail(tx, error::INVALID_GROUP_ID, &req.member_id);
            return rx;
        }
        if req.session_timeout_ms < self.cfg.min_session_ms || req.session_timeout_ms > self.cfg.max_session_ms {
            fail(tx, error::INVALID_SESSION_TIMEOUT, &req.member_id);
            return rx;
        }
        if self.http.lock().get(&req.group_id).is_some_and(|g| !g.members.is_empty()) {
            fail(tx, error::INCONSISTENT_GROUP_PROTOCOL, &req.member_id);
            return rx;
        }
        let mut groups = self.groups.lock();
        let g = groups.entry(req.group_id.clone()).or_insert_with(Group::new);
        if !g.members.is_empty() {
            let compatible = g.protocol_type == req.protocol_type
                && req.protocols.iter().any(|(name, _)| {
                    g.members.iter().all(|m| m.protocols.iter().any(|(n, _)| n == name))
                });
            let rejoining_alone = g.members.len() == 1 && g.members[0].id == req.member_id;
            if !compatible && !rejoining_alone {
                fail(tx, error::INCONSISTENT_GROUP_PROTOCOL, &req.member_id);
                return rx;
            }
        }
        let member_id = if req.member_id.is_empty() {
            format!("{}-{}", if client_id.is_empty() { "member" } else { client_id }, uuid::Uuid::new_v4())
        } else if g.member(&req.member_id).is_none() {
            fail(tx, error::UNKNOWN_MEMBER_ID, &req.member_id);
            return rx;
        } else {
            req.member_id.clone()
        };
        g.protocol_type = req.protocol_type.clone();
        let session_timeout = Duration::from_millis(req.session_timeout_ms as u64);
        let rebalance_timeout = Duration::from_millis(req.rebalance_timeout_ms.max(req.session_timeout_ms) as u64);
        let changed = match g.member(&member_id) {
            Some(m) => {
                let changed = m.protocols != req.protocols;
                m.protocols = req.protocols;
                m.session_timeout = session_timeout;
                m.rebalance_timeout = rebalance_timeout;
                m.last_seen = Instant::now();
                changed
            }
            None => {
                g.members.push(Member {
                    id: member_id.clone(),
                    client_id: client_id.to_string(),
                    client_host: client_host.to_string(),
                    session_timeout,
                    rebalance_timeout,
                    protocols: req.protocols,
                    assignment: Bytes::new(),
                    last_seen: Instant::now(),
                });
                true
            }
        };
        g.pending_joins.insert(member_id.clone(), tx);
        match g.state {
            GroupState::PreparingRebalance => {}
            GroupState::Stable if !changed && g.leader.as_deref() != Some(member_id.as_str()) => {
                // Follower rejoining with identical metadata: still requires a rebalance in
                // Kafka semantics so the leader can reassign; keep it simple and rebalance.
                self.prepare_rebalance(&req.group_id, g);
            }
            _ => self.prepare_rebalance(&req.group_id, g),
        }
        self.maybe_complete(g);
        rx
    }

    fn prepare_rebalance(self: &Arc<Self>, group_id: &str, g: &mut Group) {
        let was_empty = g.state == GroupState::Empty;
        g.state = GroupState::PreparingRebalance;
        g.rebalance_seq += 1;
        for (_, tx) in g.pending_syncs.drain() {
            let _ = tx.send((error::REBALANCE_IN_PROGRESS, Bytes::new()));
        }
        g.initial = was_empty;
        let wait = if was_empty {
            self.cfg.initial_delay
        } else {
            g.members.iter().map(|m| m.rebalance_timeout).max().unwrap_or(Duration::from_secs(10))
        };
        let seq = g.rebalance_seq;
        let me = Arc::downgrade(self);
        let group_id = group_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            let Some(me) = me.upgrade() else { return };
            let mut groups = me.groups.lock();
            let Some(g) = groups.get_mut(&group_id) else { return };
            if g.rebalance_seq != seq || g.state != GroupState::PreparingRebalance {
                return;
            }
            // Members that did not rejoin in time are dropped from the generation.
            let pending = &g.pending_joins;
            g.members.retain(|m| pending.contains_key(&m.id));
            g.initial = false;
            me.complete_join(g);
        });
    }

    fn maybe_complete(&self, g: &mut Group) {
        if g.state == GroupState::PreparingRebalance
            && !g.initial
            && !g.members.is_empty()
            && g.members.iter().all(|m| g.pending_joins.contains_key(&m.id))
        {
            self.complete_join(g);
        }
    }

    fn complete_join(&self, g: &mut Group) {
        if g.members.is_empty() {
            g.state = GroupState::Empty;
            g.leader = None;
            g.pending_joins.clear();
            return;
        }
        g.generation += 1;
        self.metrics.rebalances_total.fetch_add(1, Relaxed);
        // First protocol (in the first member's preference order) that every member supports.
        let protocol = g.members[0]
            .protocols
            .iter()
            .map(|(n, _)| n.clone())
            .find(|name| g.members.iter().all(|m| m.protocols.iter().any(|(n, _)| n == name)))
            .unwrap_or_default();
        g.protocol = protocol.clone();
        if !g.leader.as_ref().is_some_and(|l| g.members.iter().any(|m| &m.id == l)) {
            g.leader = Some(g.members[0].id.clone());
        }
        let leader = g.leader.clone().unwrap_or_default();
        let all: Vec<(String, Bytes)> = g
            .members
            .iter()
            .map(|m| {
                let meta = m.protocols.iter().find(|(n, _)| *n == protocol).map(|(_, b)| b.clone()).unwrap_or_default();
                (m.id.clone(), meta)
            })
            .collect();
        for m in &mut g.members {
            m.last_seen = Instant::now();
        }
        g.state = GroupState::CompletingRebalance;
        for (member_id, tx) in g.pending_joins.drain() {
            let _ = tx.send(JoinGroupResponse {
                error_code: error::NONE,
                generation_id: g.generation,
                protocol_name: protocol.clone(),
                leader: leader.clone(),
                members: if member_id == leader { all.clone() } else { Vec::new() },
                member_id,
            });
        }
    }

    pub fn sync(&self, group_id: &str, generation: i32, member_id: &str, assignments: Vec<(String, Bytes)>) -> oneshot::Receiver<(i16, Bytes)> {
        let (tx, rx) = oneshot::channel();
        let mut groups = self.groups.lock();
        let Some(g) = groups.get_mut(group_id) else {
            let _ = tx.send((error::UNKNOWN_MEMBER_ID, Bytes::new()));
            return rx;
        };
        let Some(m) = g.member(member_id) else {
            let _ = tx.send((error::UNKNOWN_MEMBER_ID, Bytes::new()));
            return rx;
        };
        m.last_seen = Instant::now();
        if generation != g.generation {
            let _ = tx.send((error::ILLEGAL_GENERATION, Bytes::new()));
            return rx;
        }
        match g.state {
            GroupState::PreparingRebalance => {
                let _ = tx.send((error::REBALANCE_IN_PROGRESS, Bytes::new()));
            }
            GroupState::Stable => {
                let a = g.member(member_id).map(|m| m.assignment.clone()).unwrap_or_default();
                let _ = tx.send((error::NONE, a));
            }
            GroupState::Empty => {
                let _ = tx.send((error::UNKNOWN_MEMBER_ID, Bytes::new()));
            }
            GroupState::CompletingRebalance => {
                g.pending_syncs.insert(member_id.to_string(), tx);
                if g.leader.as_deref() == Some(member_id) {
                    let map: HashMap<String, Bytes> = assignments.into_iter().collect();
                    for m in &mut g.members {
                        m.assignment = map.get(&m.id).cloned().unwrap_or_default();
                    }
                    g.state = GroupState::Stable;
                    let assigned: HashMap<String, Bytes> =
                        g.members.iter().map(|m| (m.id.clone(), m.assignment.clone())).collect();
                    for (id, tx) in g.pending_syncs.drain() {
                        let _ = tx.send((error::NONE, assigned.get(&id).cloned().unwrap_or_default()));
                    }
                }
            }
        }
        rx
    }

    pub fn heartbeat(&self, group_id: &str, generation: i32, member_id: &str) -> i16 {
        let mut groups = self.groups.lock();
        let Some(g) = groups.get_mut(group_id) else { return error::UNKNOWN_MEMBER_ID };
        let state = g.state;
        let gen_ok = g.generation == generation;
        let Some(m) = g.member(member_id) else { return error::UNKNOWN_MEMBER_ID };
        m.last_seen = Instant::now();
        match state {
            GroupState::PreparingRebalance => error::REBALANCE_IN_PROGRESS,
            _ if !gen_ok => error::ILLEGAL_GENERATION,
            GroupState::Empty => error::UNKNOWN_MEMBER_ID,
            _ => error::NONE,
        }
    }

    pub fn leave(self: &Arc<Self>, group_id: &str, member_ids: &[String]) -> Vec<(String, i16)> {
        let mut groups = self.groups.lock();
        let Some(g) = groups.get_mut(group_id) else {
            return member_ids.iter().map(|m| (m.clone(), error::UNKNOWN_MEMBER_ID)).collect();
        };
        let mut out = Vec::new();
        let mut removed = false;
        for id in member_ids {
            let before = g.members.len();
            g.members.retain(|m| &m.id != id);
            if g.members.len() < before {
                removed = true;
                g.pending_joins.remove(id);
                g.pending_syncs.remove(id);
                out.push((id.clone(), error::NONE));
            } else {
                out.push((id.clone(), error::UNKNOWN_MEMBER_ID));
            }
        }
        if removed {
            self.after_member_loss(group_id, g);
        }
        out
    }

    fn after_member_loss(self: &Arc<Self>, group_id: &str, g: &mut Group) {
        if g.members.is_empty() {
            g.state = GroupState::Empty;
            g.leader = None;
            g.rebalance_seq += 1;
            for (_, tx) in g.pending_syncs.drain() {
                let _ = tx.send((error::REBALANCE_IN_PROGRESS, Bytes::new()));
            }
        } else if g.state == GroupState::PreparingRebalance {
            self.maybe_complete(g);
        } else {
            self.prepare_rebalance(group_id, g);
            g.initial = false;
        }
    }

    fn expire_members(self: &Arc<Self>) {
        let now = Instant::now();
        {
            let mut groups = self.groups.lock();
            let ids: Vec<String> = groups.keys().cloned().collect();
            for id in ids {
                let g = groups.get_mut(&id).unwrap();
                let pending = &g.pending_joins;
                let before = g.members.len();
                g.members.retain(|m| pending.contains_key(&m.id) || now.duration_since(m.last_seen) <= m.session_timeout);
                if g.members.len() < before {
                    tracing::info!(group = %id, expired = before - g.members.len(), "consumer session(s) expired");
                    self.after_member_loss(&id, g);
                }
            }
        }
        let mut http = self.http.lock();
        for g in http.values_mut() {
            let before = g.members.len();
            g.members.retain(|_, m| now.duration_since(m.last_seen) <= m.session_timeout);
            if g.members.len() < before {
                g.generation += 1;
                for m in g.members.values_mut() {
                    m.assigned.clear(); // recomputed lazily on next poll
                }
            }
        }
    }

    // ------------------------------------------------------------------ offsets ---------------

    /// Validates a commit against group membership. `generation < 0` = standalone consumer.
    pub fn check_commit(&self, group_id: &str, generation: i32, member_id: &str) -> i16 {
        let groups = self.groups.lock();
        let Some(g) = groups.get(group_id) else { return error::NONE };
        if generation < 0 {
            return if g.members.is_empty() { error::NONE } else { error::ILLEGAL_GENERATION };
        }
        if !g.members.iter().any(|m| m.id == member_id) {
            return error::UNKNOWN_MEMBER_ID;
        }
        if g.generation != generation {
            return error::ILLEGAL_GENERATION;
        }
        if g.state == GroupState::PreparingRebalance {
            return error::REBALANCE_IN_PROGRESS;
        }
        error::NONE
    }

    pub fn commit(&self, group_id: &str, topic: &str, partition: i32, offset: i64, metadata: Option<String>) {
        self.offsets.lock().commit(group_id, topic, partition, offset, metadata);
    }

    pub fn committed(&self, group_id: &str, topic: &str, partition: i32) -> Option<CommittedOffset> {
        self.offsets.lock().map.get(group_id).and_then(|m| m.get(&(topic.to_string(), partition)).cloned())
    }

    pub fn committed_all(&self, group_id: &str) -> Vec<((String, i32), CommittedOffset)> {
        self.offsets.lock().map.get(group_id).map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default()
    }

    pub fn delete_group(&self, group_id: &str) -> Result<(), i16> {
        {
            let groups = self.groups.lock();
            if groups.get(group_id).is_some_and(|g| !g.members.is_empty()) {
                return Err(error::NON_EMPTY_GROUP);
            }
        }
        if self.http.lock().get(group_id).is_some_and(|g| !g.members.is_empty()) {
            return Err(error::NON_EMPTY_GROUP);
        }
        self.groups.lock().remove(group_id);
        self.http.lock().remove(group_id);
        self.offsets.lock().delete_group(group_id);
        Ok(())
    }

    // ------------------------------------------------------------------ introspection ---------

    pub fn list(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> =
            self.groups.lock().iter().map(|(id, g)| (id.clone(), g.protocol_type.clone())).collect();
        for id in self.http.lock().keys() {
            out.push((id.clone(), "http".into()));
        }
        for id in self.offsets.lock().map.keys() {
            if !out.iter().any(|(g, _)| g == id) {
                out.push((id.clone(), "consumer".into()));
            }
        }
        out.sort();
        out
    }

    pub fn describe(&self, group_id: &str) -> Option<GroupSummary> {
        if let Some(g) = self.groups.lock().get(group_id) {
            return Some(GroupSummary {
                group_id: group_id.to_string(),
                kind: "kafka",
                state: if g.members.is_empty() { "Empty".into() } else { g.state.kafka_name().into() },
                protocol_type: g.protocol_type.clone(),
                protocol: g.protocol.clone(),
                generation: g.generation,
                members: g
                    .members
                    .iter()
                    .map(|m| MemberSummary {
                        member_id: m.id.clone(),
                        client_id: m.client_id.clone(),
                        client_host: m.client_host.clone(),
                        assignment: decode_consumer_assignment(&m.assignment),
                        raw_metadata: m.protocols.iter().find(|(n, _)| *n == g.protocol).map(|(_, b)| b.clone()).unwrap_or_default(),
                        raw_assignment: m.assignment.clone(),
                    })
                    .collect(),
            });
        }
        if let Some(g) = self.http.lock().get(group_id) {
            return Some(GroupSummary {
                group_id: group_id.to_string(),
                kind: "http",
                state: if g.members.is_empty() { "Empty".into() } else { "Stable".into() },
                protocol_type: "http".into(),
                protocol: "server-assigned".into(),
                generation: g.generation,
                members: g
                    .members
                    .values()
                    .map(|m| MemberSummary {
                        member_id: m.id.clone(),
                        client_id: "http".into(),
                        client_host: String::new(),
                        assignment: m.assigned.clone(),
                        raw_metadata: Bytes::new(),
                        raw_assignment: Bytes::new(),
                    })
                    .collect(),
            });
        }
        self.offsets.lock().map.contains_key(group_id).then(|| GroupSummary {
            group_id: group_id.to_string(),
            kind: "offsets-only",
            state: "Empty".into(),
            protocol_type: "consumer".into(),
            protocol: String::new(),
            generation: 0,
            members: Vec::new(),
        })
    }

    // ------------------------------------------------------------------ HTTP groups -----------

    pub fn http_join(
        &self,
        group_id: &str,
        topics: Vec<String>,
        auto_commit: bool,
        offset_reset: String,
        filter: Option<String>,
        session_timeout: Duration,
    ) -> Result<String, i16> {
        if self.groups.lock().get(group_id).is_some_and(|g| !g.members.is_empty()) {
            return Err(error::INCONSISTENT_GROUP_PROTOCOL);
        }
        let mut http = self.http.lock();
        let g = http.entry(group_id.to_string()).or_insert_with(|| HttpGroup { generation: 0, members: BTreeMap::new() });
        let id = format!("http-{}", uuid::Uuid::new_v4());
        g.members.insert(
            id.clone(),
            HttpMember {
                id: id.clone(),
                topics,
                assigned: Vec::new(),
                positions: HashMap::new(),
                auto_commit,
                offset_reset,
                filter,
                last_seen: Instant::now(),
                session_timeout,
            },
        );
        g.generation += 1;
        for m in g.members.values_mut() {
            m.assigned.clear();
        }
        Ok(id)
    }

    pub fn http_leave(&self, group_id: &str, member_id: &str) -> bool {
        let mut http = self.http.lock();
        let Some(g) = http.get_mut(group_id) else { return false };
        let removed = g.members.remove(member_id).is_some();
        if removed {
            g.generation += 1;
            for m in g.members.values_mut() {
                m.assigned.clear();
            }
        }
        removed
    }

    /// Runs `f` with the member's session, (re)computing its assignment first when needed.
    /// `partitions_of` returns the partition count of a topic (0 when unknown).
    pub fn with_http_member<R>(
        &self,
        group_id: &str,
        member_id: &str,
        partitions_of: impl Fn(&str) -> i32,
        f: impl FnOnce(&mut HttpMember, i32) -> R,
    ) -> Option<R> {
        let mut http = self.http.lock();
        let g = http.get_mut(group_id)?;
        if !g.members.contains_key(member_id) {
            return None;
        }
        let needs = g.members.get(member_id).is_some_and(|m| m.assigned.is_empty());
        if needs {
            // Round-robin every subscribed partition across the members subscribing to it.
            let mut all: Vec<(String, i32)> = Vec::new();
            let mut topics: Vec<&String> = g.members.values().flat_map(|m| m.topics.iter()).collect();
            topics.sort();
            topics.dedup();
            for t in topics {
                for p in 0..partitions_of(t) {
                    all.push((t.clone(), p));
                }
            }
            let ids: Vec<String> = g.members.keys().cloned().collect();
            let mut next = 0usize;
            let mut plan: HashMap<String, Vec<(String, i32)>> = HashMap::new();
            for tp in all {
                let subs: Vec<&String> = ids.iter().filter(|id| g.members[*id].topics.contains(&tp.0)).collect();
                if subs.is_empty() {
                    continue;
                }
                let owner = subs[next % subs.len()].clone();
                next += 1;
                plan.entry(owner).or_default().push(tp);
            }
            for (id, m) in g.members.iter_mut() {
                m.assigned = plan.remove(id).unwrap_or_default();
                let owned = &m.assigned;
                m.positions.retain(|tp, _| owned.contains(tp));
            }
        }
        let generation = g.generation;
        let m = g.members.get_mut(member_id)?;
        m.last_seen = Instant::now();
        Some(f(m, generation))
    }
}

fn serialize_tps<S: serde::Serializer>(tps: &[(String, i32)], s: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;
    let mut seq = s.serialize_seq(Some(tps.len()))?;
    for (t, p) in tps {
        seq.serialize_element(&serde_json::json!({ "topic": t, "partition": p }))?;
    }
    seq.end()
}

/// Decodes a Kafka `ConsumerProtocolAssignment` (version, [topic, [partition]], user data).
pub fn decode_consumer_assignment(b: &[u8]) -> Vec<(String, i32)> {
    let mut out = Vec::new();
    let mut d = bp_protocol::Decoder::new(bytes::BytesMut::from(b));
    if b.len() < 6 || d.i16().is_err() {
        return out;
    }
    let _ = d.array(|d| {
        let t = d.string()?;
        let ps = d.array(|d| d.i32())?;
        for p in ps {
            out.push((t.clone(), p));
        }
        Ok(())
    });
    out
}
