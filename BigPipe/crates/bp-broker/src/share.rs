//! Share groups: queue semantics on top of a log (KIP-932 style), exposed over HTTP.
//!
//! Many consumers read the same partitions; each record is *acquired* by one consumer for a
//! lock period and must be acknowledged: `accept` (done), `release` (redeliver to anyone) or
//! `reject` (give up). After `max_attempts` deliveries — or on reject — the record is copied
//! to the group's dead-letter topic, a feature queue workloads usually build by hand.
//!
//! The share-partition start offset (everything below it is finished) is persisted as the
//! committed offset of group `share:<name>`; in-flight locks are in memory, so a restart
//! redelivers unfinished records (at-least-once).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bp_expr::Expr;
use bp_protocol::records::{Header, NewRecord, Record};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::broker::{Broker, BrokerError};

const MAX_IN_FLIGHT_PER_PARTITION: usize = 5_000;
static ROTATION: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckAction {
    Accept,
    Release,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotState {
    Available,
    Acquired { until: Instant },
    Done,
}

struct Slot {
    state: SlotState,
    attempts: u32,
    member: String,
    record: Record,
}

#[derive(Default)]
struct SharePartition {
    initialized: bool,
    /// Share-partition start offset: all offsets below are finished.
    spso: i64,
    /// Next offset to read from the log.
    fetched: i64,
    slots: BTreeMap<i64, Slot>,
}

pub struct ShareOptions {
    pub max_attempts: u32,
    pub lock: Duration,
    pub dlq_topic: Option<String>,
    pub offset_reset: String,
}

pub struct ShareDelivery {
    pub topic: String,
    pub partition: i32,
    pub delivery_count: u32,
    pub record: Record,
}

#[derive(Default)]
struct ShareGroup {
    parts: HashMap<(String, i32), SharePartition>,
    dlq_topic: Option<String>,
    max_attempts: u32,
}

#[derive(Default)]
pub struct ShareGroups {
    groups: Mutex<HashMap<String, Arc<tokio::sync::Mutex<ShareGroup>>>>,
}

#[derive(Serialize)]
pub struct SharePartitionStatus {
    pub topic: String,
    pub partition: i32,
    pub start_offset: i64,
    pub in_flight: usize,
    pub acquired: usize,
}

fn group_key(name: &str) -> String {
    format!("share:{name}")
}

impl ShareGroups {
    fn group(&self, name: &str) -> Arc<tokio::sync::Mutex<ShareGroup>> {
        self.groups.lock().entry(name.to_string()).or_default().clone()
    }

    pub fn names(&self) -> Vec<String> {
        self.groups.lock().keys().cloned().collect()
    }

    pub async fn status(&self, name: &str) -> Vec<SharePartitionStatus> {
        let g = self.group(name);
        let g = g.lock().await;
        let mut out: Vec<_> = g
            .parts
            .iter()
            .map(|((t, p), sp)| SharePartitionStatus {
                topic: t.clone(),
                partition: *p,
                start_offset: sp.spso,
                in_flight: sp.slots.len(),
                acquired: sp.slots.values().filter(|s| matches!(s.state, SlotState::Acquired { .. })).count(),
            })
            .collect();
        out.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
        out
    }

    /// Acquires up to `max_records` records for `member` across the topics' partitions.
    pub async fn poll(
        &self,
        broker: &Broker,
        name: &str,
        member: &str,
        topics: &[String],
        max_records: usize,
        filter: Option<&Expr>,
        opts: &ShareOptions,
    ) -> Result<Vec<ShareDelivery>, BrokerError> {
        let g = self.group(name);
        let mut g = g.lock().await;
        g.max_attempts = opts.max_attempts.max(1);
        if opts.dlq_topic.is_some() {
            g.dlq_topic = opts.dlq_topic.clone();
        }
        let now = Instant::now();
        let mut out = Vec::new();
        let mut dead: Vec<(String, i32, Record, u32)> = Vec::new();
        for topic in topics {
            let Some(meta) = broker.topic(topic) else { continue };
            // Rotate the starting partition so members spread across partitions.
            let start = ROTATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % meta.partitions.max(1) as usize;
            for i in 0..meta.partitions as usize {
                if out.len() >= max_records {
                    break;
                }
                let p = ((start + i) % meta.partitions as usize) as i32;
                let key = (topic.clone(), p);
                let max_attempts = g.max_attempts;
                let sp = g.parts.entry(key.clone()).or_default();
                if !sp.initialized {
                    let committed = broker.coordinator.committed(&group_key(name), topic, p).map(|c| c.offset);
                    let init = match committed {
                        Some(o) => o,
                        None if opts.offset_reset == "latest" => broker.high_watermark(&meta, p),
                        None => broker.partition_info(&meta, p).await?.log_start_offset,
                    };
                    sp.spso = init;
                    sp.fetched = init;
                    sp.initialized = true;
                }
                // 1. Expired locks become available again (or dead after too many attempts).
                for s in sp.slots.values_mut() {
                    if let SlotState::Acquired { until } = s.state {
                        if until <= now {
                            if s.attempts >= max_attempts {
                                s.state = SlotState::Done;
                                dead.push((topic.clone(), p, s.record.clone(), s.attempts));
                            } else {
                                s.state = SlotState::Available;
                            }
                        }
                    }
                }
                // 2. Redeliveries first.
                for s in sp.slots.values_mut() {
                    if out.len() >= max_records {
                        break;
                    }
                    if s.state == SlotState::Available {
                        s.state = SlotState::Acquired { until: now + opts.lock };
                        s.attempts += 1;
                        s.member = member.to_string();
                        out.push(ShareDelivery { topic: topic.clone(), partition: p, delivery_count: s.attempts, record: s.record.clone() });
                    }
                }
                // 3. New records from the log.
                while out.len() < max_records && sp.slots.len() < MAX_IN_FLIGHT_PER_PARTITION {
                    let hw = broker.high_watermark(&meta, p);
                    if sp.fetched >= hw {
                        break;
                    }
                    let (recs, next, _) = match broker.read_records(&meta, p, sp.fetched, 1 << 20).await {
                        Ok(r) => r,
                        Err(BrokerError::Storage(bp_storage::StorageError::OffsetOutOfRange { log_start, .. })) => {
                            sp.fetched = log_start;
                            sp.spso = sp.spso.max(log_start);
                            continue;
                        }
                        Err(e) => return Err(e),
                    };
                    if next <= sp.fetched {
                        break;
                    }
                    let mut took_all = true;
                    for r in recs {
                        if out.len() >= max_records || sp.slots.len() >= MAX_IN_FLIGHT_PER_PARTITION {
                            took_all = false;
                            break;
                        }
                        sp.fetched = r.offset + 1;
                        if filter.is_some_and(|f| !f.matches(&crate::RecordView { topic, partition: p, record: &r })) {
                            // Filtered-out records are finished for this group.
                            sp.slots.insert(r.offset, Slot { state: SlotState::Done, attempts: 0, member: String::new(), record: r });
                            continue;
                        }
                        out.push(ShareDelivery { topic: topic.clone(), partition: p, delivery_count: 1, record: r.clone() });
                        sp.slots.insert(
                            r.offset,
                            Slot { state: SlotState::Acquired { until: now + opts.lock }, attempts: 1, member: member.to_string(), record: r },
                        );
                    }
                    if took_all {
                        // Skip past control batches / trailing offsets without records.
                        sp.fetched = sp.fetched.max(next);
                    }
                }
                advance(broker, name, topic, p, sp);
            }
        }
        let dlq = g.dlq_topic.clone();
        drop(g);
        self.dead_letter(broker, name, dlq, dead, "max delivery attempts exceeded").await;
        Ok(out)
    }

    pub async fn ack(
        &self,
        broker: &Broker,
        name: &str,
        member: &str,
        acks: &[(String, i32, i64, AckAction)],
    ) -> Result<Vec<bool>, BrokerError> {
        let g = self.group(name);
        let mut g = g.lock().await;
        let max_attempts = g.max_attempts.max(1);
        let mut results = Vec::with_capacity(acks.len());
        let mut dead = Vec::new();
        let mut touched = Vec::new();
        for (topic, p, offset, action) in acks {
            let Some(sp) = g.parts.get_mut(&(topic.clone(), *p)) else {
                results.push(false);
                continue;
            };
            let ok = match sp.slots.get_mut(offset) {
                Some(s) if matches!(s.state, SlotState::Acquired { .. }) && s.member == member => {
                    match action {
                        AckAction::Accept => s.state = SlotState::Done,
                        AckAction::Release if s.attempts < max_attempts => s.state = SlotState::Available,
                        AckAction::Release | AckAction::Reject => {
                            s.state = SlotState::Done;
                            dead.push((topic.clone(), *p, s.record.clone(), s.attempts));
                        }
                    }
                    true
                }
                _ => false,
            };
            results.push(ok);
            touched.push((topic.clone(), *p));
        }
        touched.sort();
        touched.dedup();
        for (t, p) in touched {
            if let Some(sp) = g.parts.get_mut(&(t.clone(), p)) {
                advance(broker, name, &t, p, sp);
            }
        }
        let dlq = g.dlq_topic.clone();
        drop(g);
        self.dead_letter(broker, name, dlq, dead, "rejected by consumer").await;
        Ok(results)
    }

    async fn dead_letter(&self, broker: &Broker, group: &str, dlq: Option<String>, dead: Vec<(String, i32, Record, u32)>, reason: &str) {
        if dead.is_empty() {
            return;
        }
        let Some(dlq) = dlq else {
            tracing::warn!(group, count = dead.len(), reason, "records archived without a dead-letter topic");
            return;
        };
        let Ok(meta) = broker.ensure_topic(&dlq).await else { return };
        let items = dead
            .into_iter()
            .map(|(t, p, r, attempts)| {
                let mut headers = r.headers.clone();
                headers.push(hdr("bigpipe.dlq.topic", &t));
                headers.push(hdr("bigpipe.dlq.partition", &p.to_string()));
                headers.push(hdr("bigpipe.dlq.offset", &r.offset.to_string()));
                headers.push(hdr("bigpipe.dlq.group", group));
                headers.push(hdr("bigpipe.dlq.attempts", &attempts.to_string()));
                headers.push(hdr("bigpipe.dlq.reason", reason));
                (None, NewRecord { key: r.key.clone(), value: r.value.clone(), headers, timestamp: None })
            })
            .collect();
        if let Err(e) = broker.produce_records(&meta, items, Default::default()).await {
            tracing::error!(group, dlq, error = %e, "dead-letter publish failed");
        }
    }
}

fn hdr(k: &str, v: &str) -> Header {
    Header { key: k.to_string(), value: Some(Bytes::copy_from_slice(v.as_bytes())) }
}

/// Moves the start offset past finished records and persists it.
fn advance(broker: &Broker, group: &str, topic: &str, p: i32, sp: &mut SharePartition) {
    let before = sp.spso;
    while let Some((&off, s)) = sp.slots.first_key_value() {
        if s.state != SlotState::Done {
            break;
        }
        sp.slots.remove(&off);
        sp.spso = off + 1;
    }
    if sp.slots.is_empty() {
        sp.spso = sp.spso.max(sp.fetched);
    }
    if sp.spso != before {
        broker.coordinator.commit(&group_key(group), topic, p, sp.spso, None);
    }
}
