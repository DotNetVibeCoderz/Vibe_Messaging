//! BigPipe Flow: managed topic-to-topic pipelines running inside the broker.
//!
//! A flow reads an input topic, applies processors in order (`filter`, `mapping`, `route`),
//! and writes to an output topic. Progress is committed as group `__flow:<name>`, so flows
//! resume where they stopped (at-least-once). Definitions persist in `meta/flows.json`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use bp_expr::{Expr, Mapping};
use bp_protocol::records::{Header, NewRecord};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::broker::{Broker, BrokerError};
use crate::RecordView;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Processor {
    /// Keep records for which the expression is true.
    Filter(String),
    /// Rewrite the value with a bpql mapping program (`root.x = ...`).
    Mapping(String),
    /// Pick the output topic per record; the expression must return a topic name.
    Route(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlowSpec {
    pub name: String,
    pub input: String,
    pub output: String,
    #[serde(default)]
    pub processors: Vec<Processor>,
    /// `earliest` (default) or `latest` for the first run.
    #[serde(default = "earliest")]
    pub start_from: String,
    /// Records whose processing fails go here (optional).
    #[serde(default)]
    pub dlq: Option<String>,
    #[serde(default)]
    pub paused: bool,
}

fn earliest() -> String {
    "earliest".into()
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct FlowStats {
    pub records_in: u64,
    pub records_out: u64,
    pub records_filtered: u64,
    pub errors: u64,
    pub last_error: Option<String>,
    pub running: bool,
}

struct Compiled {
    steps: Vec<Step>,
}

enum Step {
    Filter(Expr),
    Mapping(Mapping),
    Route(Expr),
}

impl Compiled {
    fn new(spec: &FlowSpec) -> Result<Self, String> {
        let mut steps = Vec::new();
        for p in &spec.processors {
            steps.push(match p {
                Processor::Filter(s) => Step::Filter(Expr::parse(s).map_err(|e| format!("filter: {e}"))?),
                Processor::Mapping(s) => Step::Mapping(Mapping::parse(s).map_err(|e| format!("mapping: {e}"))?),
                Processor::Route(s) => Step::Route(Expr::parse(s).map_err(|e| format!("route: {e}"))?),
            });
        }
        Ok(Self { steps })
    }
}

struct Running {
    spec: FlowSpec,
    stop: Arc<AtomicBool>,
    records_in: Arc<AtomicU64>,
    records_out: Arc<AtomicU64>,
    filtered: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
}

#[derive(Default)]
pub struct Flows {
    flows: Mutex<BTreeMap<String, Running>>,
}

#[derive(Serialize)]
pub struct FlowStatus {
    #[serde(flatten)]
    pub spec: FlowSpec,
    pub stats: FlowStats,
}

impl Flows {
    fn path(b: &Broker) -> std::path::PathBuf {
        b.cfg.data_dir.join("meta").join("flows.json")
    }

    fn persist(&self, b: &Broker) {
        let specs: Vec<FlowSpec> = self.flows.lock().values().map(|r| r.spec.clone()).collect();
        if let Ok(json) = serde_json::to_vec_pretty(&specs) {
            let _ = std::fs::write(Self::path(b), json);
        }
    }

    pub fn restore(b: &Arc<Broker>) {
        let specs: Vec<FlowSpec> =
            std::fs::read(Self::path(b)).ok().and_then(|x| serde_json::from_slice(&x).ok()).unwrap_or_default();
        for s in specs {
            if let Err(e) = b.flows.start(b, s, false) {
                tracing::warn!(error = %e, "flow failed to restore");
            }
        }
    }

    pub fn list(&self) -> Vec<FlowStatus> {
        self.flows
            .lock()
            .values()
            .map(|r| FlowStatus {
                spec: r.spec.clone(),
                stats: FlowStats {
                    records_in: r.records_in.load(Relaxed),
                    records_out: r.records_out.load(Relaxed),
                    records_filtered: r.filtered.load(Relaxed),
                    errors: r.errors.load(Relaxed),
                    last_error: r.last_error.lock().clone(),
                    running: !r.spec.paused && !r.stop.load(Relaxed),
                },
            })
            .collect()
    }

    /// Validates and starts (or replaces) a flow.
    pub fn start(&self, b: &Arc<Broker>, spec: FlowSpec, persist: bool) -> Result<(), String> {
        if !crate::broker::valid_topic_name(&spec.name) {
            return Err("flow name must use [a-zA-Z0-9._-]".into());
        }
        let compiled = Compiled::new(&spec)?;
        self.stop(&spec.name, false);
        let running = Running {
            spec: spec.clone(),
            stop: Arc::new(AtomicBool::new(spec.paused)),
            records_in: Default::default(),
            records_out: Default::default(),
            filtered: Default::default(),
            errors: Default::default(),
            last_error: Default::default(),
        };
        if !spec.paused {
            let ctx = FlowCtx {
                broker: b.clone(),
                spec: spec.clone(),
                compiled,
                stop: running.stop.clone(),
                records_in: running.records_in.clone(),
                records_out: running.records_out.clone(),
                filtered: running.filtered.clone(),
                errors: running.errors.clone(),
                last_error: running.last_error.clone(),
            };
            tokio::spawn(ctx.run());
        }
        self.flows.lock().insert(spec.name.clone(), running);
        if persist {
            self.persist(b);
        }
        tracing::info!(flow = %spec.name, input = %spec.input, output = %spec.output, "flow deployed");
        Ok(())
    }

    fn stop(&self, name: &str, remove: bool) -> bool {
        let mut flows = self.flows.lock();
        let Some(r) = flows.get(name) else { return false };
        r.stop.store(true, Relaxed);
        if remove {
            flows.remove(name);
        }
        true
    }

    pub fn delete(&self, b: &Broker, name: &str) -> bool {
        let ok = self.stop(name, true);
        if ok {
            self.persist(b);
        }
        ok
    }

    pub fn set_paused(&self, b: &Arc<Broker>, name: &str, paused: bool) -> Result<(), String> {
        let spec = self.flows.lock().get(name).map(|r| r.spec.clone()).ok_or("unknown flow")?;
        self.start(b, FlowSpec { paused, ..spec }, true)
    }
}

struct FlowCtx {
    broker: Arc<Broker>,
    spec: FlowSpec,
    compiled: Compiled,
    stop: Arc<AtomicBool>,
    records_in: Arc<AtomicU64>,
    records_out: Arc<AtomicU64>,
    filtered: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
}

impl FlowCtx {
    fn group(&self) -> String {
        format!("__flow:{}", self.spec.name)
    }

    async fn run(self) {
        while !self.stop.load(Relaxed) {
            if let Err(e) = self.step().await {
                self.errors.fetch_add(1, Relaxed);
                *self.last_error.lock() = Some(e.to_string());
                tracing::warn!(flow = %self.spec.name, error = %e, "flow step failed; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    /// One pass over all input partitions; waits for data when caught up.
    async fn step(&self) -> Result<(), BrokerError> {
        let b = &self.broker;
        let input = b.ensure_topic(&self.spec.input).await?;
        let mut watch: Vec<_> = input.hw.clone();
        for w in &mut watch {
            w.borrow_and_update();
        }
        let mut moved = false;
        for p in 0..input.partitions {
            let pos = match b.coordinator.committed(&self.group(), &input.name, p) {
                Some(c) => c.offset,
                None if self.spec.start_from == "latest" => b.high_watermark(&input, p),
                None => b.partition_info(&input, p).await?.log_start_offset,
            };
            if pos >= b.high_watermark(&input, p) {
                continue;
            }
            let (recs, next, _) = match b.read_records(&input, p, pos, 1 << 20).await {
                Ok(r) => r,
                Err(BrokerError::Storage(bp_storage::StorageError::OffsetOutOfRange { log_start, .. })) => {
                    b.coordinator.commit(&self.group(), &input.name, p, log_start, None);
                    continue;
                }
                Err(e) => return Err(e),
            };
            let mut outputs: BTreeMap<String, Vec<(Option<i32>, NewRecord)>> = BTreeMap::new();
            let mut dlq: Vec<(Option<i32>, NewRecord)> = Vec::new();
            for r in &recs {
                self.records_in.fetch_add(1, Relaxed);
                match self.apply(&input.name, p, r) {
                    Ok(Some((topic, rec))) => outputs.entry(topic).or_default().push((None, rec)),
                    Ok(None) => {
                        self.filtered.fetch_add(1, Relaxed);
                    }
                    Err(msg) => {
                        self.errors.fetch_add(1, Relaxed);
                        *self.last_error.lock() = Some(msg.clone());
                        let mut headers = r.headers.clone();
                        headers.push(Header { key: "bigpipe.flow.error".into(), value: Some(Bytes::from(msg)) });
                        dlq.push((None, NewRecord { key: r.key.clone(), value: r.value.clone(), headers, timestamp: Some(r.timestamp) }));
                    }
                }
            }
            for (topic, items) in outputs {
                let n = items.len() as u64;
                let meta = b.ensure_topic(&topic).await?;
                b.produce_records(&meta, items, Default::default()).await?;
                self.records_out.fetch_add(n, Relaxed);
            }
            if let (Some(t), false) = (&self.spec.dlq, dlq.is_empty()) {
                let meta = b.ensure_topic(t).await?;
                b.produce_records(&meta, dlq, Default::default()).await?;
            }
            b.coordinator.commit(&self.group(), &input.name, p, next.max(pos), None);
            moved = true;
        }
        if !moved {
            Broker::wait_for_data(&mut watch, Instant::now() + Duration::from_millis(500)).await;
        }
        Ok(())
    }

    fn apply(&self, topic: &str, partition: i32, r: &bp_protocol::records::Record) -> Result<Option<(String, NewRecord)>, String> {
        let mut out_topic = self.spec.output.clone();
        let mut value = r.value.clone();
        for step in &self.compiled.steps {
            // Later steps see the value produced by earlier mappings.
            let current = bp_protocol::records::Record { value: value.clone(), ..r.clone() };
            let view = RecordView { topic, partition, record: &current };
            match step {
                Step::Filter(e) => {
                    if !e.matches(&view) {
                        return Ok(None);
                    }
                }
                Step::Mapping(m) => {
                    let v = m.apply(&view);
                    value = match v {
                        serde_json::Value::Null => None,
                        serde_json::Value::String(s) => Some(Bytes::from(s)),
                        other => Some(Bytes::from(serde_json::to_vec(&other).map_err(|e| e.to_string())?)),
                    };
                }
                Step::Route(e) => match e.eval(&view) {
                    serde_json::Value::String(s) if crate::broker::valid_topic_name(&s) => out_topic = s,
                    other => return Err(format!("route expression returned `{other}`, not a topic name")),
                },
            }
        }
        Ok(Some((out_topic, NewRecord { key: r.key.clone(), value, headers: r.headers.clone(), timestamp: Some(r.timestamp) })))
    }
}
