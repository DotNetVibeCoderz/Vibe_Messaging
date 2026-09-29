//! `bpctl` — the BigPipe command line.
//!
//! Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.

mod bench;

use std::collections::BTreeMap;
use std::io::BufRead;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use comfy_table::{Table, presets::UTF8_BORDERS_ONLY};
use futures::StreamExt;
use serde_json::{Value, json};

#[derive(Parser)]
#[command(
    name = "bpctl",
    version,
    about = "BigPipe command line",
    after_help = "Environment: BIGPIPE_ADMIN (http://localhost:9644), BIGPIPE_HTTP (http://localhost:8082), BIGPIPE_API_KEY.\nDibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil."
)]
struct Cli {
    #[arg(long, env = "BIGPIPE_ADMIN", default_value = "http://localhost:9644", global = true)]
    admin: String,
    #[arg(long, env = "BIGPIPE_HTTP", default_value = "http://localhost:8082", global = true)]
    http: String,
    #[arg(long, env = "BIGPIPE_API_KEY", global = true)]
    api_key: Option<String>,
    /// Print raw JSON instead of tables.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Cluster information.
    Cluster,
    /// Manage topics.
    #[command(subcommand)]
    Topic(TopicCmd),
    /// Produce records: one per stdin line (or --value).
    Produce {
        topic: String,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        value: Option<String>,
        #[arg(long)]
        partition: Option<i32>,
        /// Header as k=v (repeatable).
        #[arg(long = "header", short = 'H')]
        headers: Vec<String>,
    },
    /// Consume records (streams until Ctrl-C or --count).
    Consume {
        topic: String,
        #[arg(long)]
        group: Option<String>,
        #[arg(long)]
        from_beginning: bool,
        /// Server-side filter, e.g. `header("region") == "ID" && this.amount > 1000`.
        #[arg(long)]
        filter: Option<String>,
        #[arg(long, short = 'n')]
        count: Option<usize>,
    },
    /// Consumer groups.
    #[command(subcommand)]
    Group(GroupCmd),
    /// Managed pipelines (BigPipe Flow).
    #[command(subcommand)]
    Flow(FlowCmd),
    /// Load test over the Kafka protocol.
    Bench {
        #[arg(long, default_value = "localhost:9092")]
        bootstrap: String,
        #[arg(long, default_value = "bench")]
        topic: String,
        #[arg(long, default_value_t = 6)]
        partitions: i32,
        #[arg(long, default_value_t = 1_000_000)]
        records: u64,
        #[arg(long, default_value_t = 1024)]
        record_size: usize,
        #[arg(long, default_value_t = 500)]
        batch: usize,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value = "none")]
        compression: String,
        #[arg(long, default_value = "local")]
        mode: String,
        /// Also measure consume throughput.
        #[arg(long)]
        consume: bool,
    },
}

#[derive(Subcommand)]
enum TopicCmd {
    List,
    Create {
        name: String,
        #[arg(long, short, default_value_t = 3)]
        partitions: i32,
        /// local | tiered | diskless
        #[arg(long, short, default_value = "local")]
        mode: String,
        /// key=value (repeatable).
        #[arg(long = "config", short = 'c')]
        configs: Vec<String>,
    },
    Describe {
        name: String,
    },
    Delete {
        name: String,
    },
    /// Set (key=value) or unset (key=) topic configuration.
    Config {
        name: String,
        #[arg(required = true)]
        settings: Vec<String>,
    },
    /// Change storage mode online; offsets never change.
    Migrate {
        name: String,
        #[arg(long)]
        to: String,
    },
    AddPartitions {
        name: String,
        count: i32,
    },
}

#[derive(Subcommand)]
enum GroupCmd {
    List,
    Describe { id: String },
    Delete { id: String },
    /// Reset committed offsets: --to earliest|latest|offset [--offset N].
    Reset {
        id: String,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        offset: Option<i64>,
    },
}

#[derive(Subcommand)]
enum FlowCmd {
    List,
    /// Deploy from a YAML or JSON file.
    Deploy { file: std::path::PathBuf },
    Delete { name: String },
    Pause { name: String },
    Resume { name: String },
}

struct Api {
    c: reqwest::Client,
    admin: String,
    http: String,
    key: Option<String>,
}

impl Api {
    fn req(&self, m: reqwest::Method, url: String) -> reqwest::RequestBuilder {
        let r = self.c.request(m, url);
        match &self.key {
            Some(k) => r.bearer_auth(k),
            None => r,
        }
    }

    async fn send(&self, r: reqwest::RequestBuilder) -> anyhow::Result<Value> {
        let resp = r.send().await.context("request failed — is bigpiped running?")?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            let msg = body["error"]["message"].as_str().unwrap_or("request failed");
            bail!("{status}: {msg}");
        }
        Ok(body)
    }

    async fn get(&self, path: &str) -> anyhow::Result<Value> {
        self.send(self.req(reqwest::Method::GET, format!("{}{path}", self.admin))).await
    }
    async fn post(&self, path: &str, body: Value) -> anyhow::Result<Value> {
        self.send(self.req(reqwest::Method::POST, format!("{}{path}", self.admin)).json(&body)).await
    }
    async fn delete(&self, path: &str) -> anyhow::Result<Value> {
        self.send(self.req(reqwest::Method::DELETE, format!("{}{path}", self.admin))).await
    }
}

fn kv(items: &[String]) -> anyhow::Result<BTreeMap<String, Value>> {
    items
        .iter()
        .map(|s| {
            let (k, v) = s.split_once('=').with_context(|| format!("expected key=value, got `{s}`"))?;
            Ok((k.to_string(), if v.is_empty() { Value::Null } else { Value::String(v.to_string()) }))
        })
        .collect()
}

fn human_bytes(n: u64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < units.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1} {}", units[i])
}

fn table(header: &[&str]) -> Table {
    let mut t = Table::new();
    t.load_style(UTF8_BORDERS_ONLY).set_header(header.to_vec());
    t
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let api = Api { c: reqwest::Client::new(), admin: cli.admin.trim_end_matches('/').into(), http: cli.http.trim_end_matches('/').into(), key: cli.api_key.clone() };
    let print = |v: &Value| println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());

    match cli.cmd {
        Cmd::Cluster => {
            let v = api.get("/v1/cluster").await?;
            if cli.json {
                print(&v);
            } else {
                println!("cluster   {} (node {})", v["cluster_id"].as_str().unwrap_or("-"), v["node_id"]);
                println!("version   {}   uptime {}s   shards {}", v["version"].as_str().unwrap_or("-"), v["uptime_seconds"], v["shards"]);
                println!("kafka     {}", v["listeners"]["kafka"].as_str().unwrap_or("-"));
                println!("objects   {}", v["object_store"].as_str().unwrap_or("-"));
                println!("topics    {}   groups {}", v["topics"], v["groups"]);
                println!("\n{}", v["credit"].as_str().unwrap_or(""));
            }
        }
        Cmd::Topic(t) => match t {
            TopicCmd::List => {
                let v = api.get("/v1/topics").await?;
                if cli.json {
                    return Ok(print(&v));
                }
                let mut tb = table(&["TOPIC", "PARTITIONS", "MODE", "RECORDS", "LOCAL", "OBJECT STORAGE"]);
                for t in v.as_array().into_iter().flatten() {
                    let remote = t["bytes"]["remote"].as_u64().unwrap_or(0) + t["bytes"]["diskless"].as_u64().unwrap_or(0);
                    tb.add_row(vec![
                        t["name"].as_str().unwrap_or("").to_string(),
                        t["partitions"].to_string(),
                        t["mode"].as_str().unwrap_or("").to_string(),
                        t["records"].to_string(),
                        human_bytes(t["bytes"]["local"].as_u64().unwrap_or(0)),
                        human_bytes(remote),
                    ]);
                }
                println!("{tb}");
            }
            TopicCmd::Create { name, partitions, mode, configs } => {
                let v = api.post("/v1/topics", json!({"name": name, "partitions": partitions, "mode": mode, "config": kv(&configs)?})).await?;
                if cli.json { print(&v) } else { println!("created topic {name} ({partitions} partitions, {mode})") }
            }
            TopicCmd::Describe { name } => print(&api.get(&format!("/v1/topics/{name}")).await?),
            TopicCmd::Delete { name } => {
                api.delete(&format!("/v1/topics/{name}")).await?;
                println!("deleted topic {name}");
            }
            TopicCmd::Config { name, settings } => {
                let v = api
                    .send(api.req(reqwest::Method::PATCH, format!("{}/v1/topics/{name}/config", api.admin)).json(&json!({"config": kv(&settings)?})))
                    .await?;
                if cli.json { print(&v) } else { println!("updated {name}") }
            }
            TopicCmd::Migrate { name, to } => {
                let v = api.post(&format!("/v1/topics/{name}/migrate"), json!({"to": to})).await?;
                if cli.json {
                    print(&v)
                } else {
                    println!("{name}: {} -> {} (new data only; existing offsets unchanged)", v["from"].as_str().unwrap_or("?"), v["to"].as_str().unwrap_or("?"));
                }
            }
            TopicCmd::AddPartitions { name, count } => {
                api.post(&format!("/v1/topics/{name}/partitions"), json!({"count": count})).await?;
                println!("{name} now has {count} partitions");
            }
        },
        Cmd::Produce { topic, key, value, partition, headers } => {
            let headers: BTreeMap<String, Value> = kv(&headers)?;
            let lines: Vec<String> = match value {
                Some(v) => vec![v],
                None => std::io::stdin().lock().lines().map_while(Result::ok).filter(|l| !l.is_empty()).collect(),
            };
            for chunk in lines.chunks(500) {
                let records: Vec<Value> = chunk
                    .iter()
                    .map(|l| json!({"key": key, "value": l, "partition": partition, "headers": headers}))
                    .collect();
                let v = api.send(api.req(reqwest::Method::POST, format!("{}/v1/topics/{topic}/records", api.http)).json(&json!({"records": records}))).await?;
                for o in v["offsets"].as_array().into_iter().flatten() {
                    println!("{topic}-{}@{}", o["partition"], o["offset"]);
                }
            }
        }
        Cmd::Consume { topic, group, from_beginning, filter, count } => {
            let mut url = reqwest::Url::parse(&format!("{}/v1/topics/{topic}/stream", api.http))?;
            {
                let mut q = url.query_pairs_mut();
                q.append_pair("from", if from_beginning { "earliest" } else { "latest" });
                if let Some(g) = &group {
                    q.append_pair("group", g);
                }
                if let Some(f) = &filter {
                    q.append_pair("filter", f);
                }
            }
            let resp = api.req(reqwest::Method::GET, url.to_string()).send().await?;
            if !resp.status().is_success() {
                let body: Value = resp.json().await.unwrap_or(Value::Null);
                bail!("{}", body["error"]["message"].as_str().unwrap_or("stream failed"));
            }
            let mut stream = resp.bytes_stream();
            let mut buf = String::new();
            let mut seen = 0usize;
            while let Some(chunk) = stream.next().await {
                buf.push_str(&String::from_utf8_lossy(&chunk?));
                while let Some(i) = buf.find('\n') {
                    let line = buf[..i].trim().to_string();
                    buf.drain(..=i);
                    let Some(data) = line.strip_prefix("data:") else { continue };
                    let Ok(r) = serde_json::from_str::<Value>(data.trim()) else { continue };
                    if r.get("offset").is_none() {
                        continue;
                    }
                    if cli.json {
                        println!("{r}");
                    } else {
                        println!(
                            "{}-{}@{}  {}  {}",
                            topic,
                            r["partition"],
                            r["offset"],
                            r["key"].as_str().unwrap_or("-"),
                            r["value"].as_str().unwrap_or("")
                        );
                    }
                    seen += 1;
                    if count.is_some_and(|c| seen >= c) {
                        return Ok(());
                    }
                }
            }
        }
        Cmd::Group(g) => match g {
            GroupCmd::List => {
                let v = api.get("/v1/groups").await?;
                if cli.json {
                    return Ok(print(&v));
                }
                let mut tb = table(&["GROUP", "KIND", "STATE", "MEMBERS", "PARTITIONS", "LAG"]);
                for g in v.as_array().into_iter().flatten() {
                    let s = &g["summary"];
                    tb.add_row(vec![
                        g["group_id"].as_str().unwrap_or("").to_string(),
                        s["kind"].as_str().unwrap_or("-").to_string(),
                        s["state"].as_str().unwrap_or("-").to_string(),
                        s["members"].as_array().map(|m| m.len()).unwrap_or(0).to_string(),
                        g["partitions"].to_string(),
                        g["lag"].to_string(),
                    ]);
                }
                println!("{tb}");
            }
            GroupCmd::Describe { id } => {
                let v = api.get(&format!("/v1/groups/{id}")).await?;
                if cli.json {
                    return Ok(print(&v));
                }
                println!("group {id}  state {}  lag {}", v["summary"]["state"].as_str().unwrap_or("-"), v["lag"]);
                let mut tb = table(&["TOPIC", "PARTITION", "COMMITTED", "END", "LAG"]);
                for o in v["offsets"].as_array().into_iter().flatten() {
                    tb.add_row(vec![o["topic"].as_str().unwrap_or("").to_string(), o["partition"].to_string(), o["committed"].to_string(), o["high_watermark"].to_string(), o["lag"].to_string()]);
                }
                println!("{tb}");
            }
            GroupCmd::Delete { id } => {
                api.delete(&format!("/v1/groups/{id}")).await?;
                println!("deleted group {id}");
            }
            GroupCmd::Reset { id, topic, to, offset } => {
                print(&api.post(&format!("/v1/groups/{id}/reset"), json!({"topic": topic, "to": to, "offset": offset})).await?);
            }
        },
        Cmd::Flow(f) => match f {
            FlowCmd::List => {
                let v = api.get("/v1/flows").await?;
                if cli.json {
                    return Ok(print(&v));
                }
                let mut tb = table(&["FLOW", "INPUT", "OUTPUT", "IN", "OUT", "FILTERED", "ERRORS", "STATE"]);
                for f in v.as_array().into_iter().flatten() {
                    let s = &f["stats"];
                    tb.add_row(vec![
                        f["name"].as_str().unwrap_or("").to_string(),
                        f["input"].as_str().unwrap_or("").to_string(),
                        f["output"].as_str().unwrap_or("").to_string(),
                        s["records_in"].to_string(),
                        s["records_out"].to_string(),
                        s["records_filtered"].to_string(),
                        s["errors"].to_string(),
                        if s["running"].as_bool() == Some(true) { "running".into() } else { "paused".into() },
                    ]);
                }
                println!("{tb}");
            }
            FlowCmd::Deploy { file } => {
                let text = std::fs::read_to_string(&file).with_context(|| format!("reading {}", file.display()))?;
                let is_json = file.extension().is_some_and(|e| e == "json");
                let r = api
                    .req(reqwest::Method::POST, format!("{}/v1/flows", api.admin))
                    .header("content-type", if is_json { "application/json" } else { "application/yaml" })
                    .body(text);
                let v = api.send(r).await?;
                println!("deployed flow {}", v["name"].as_str().unwrap_or("?"));
            }
            FlowCmd::Delete { name } => {
                api.delete(&format!("/v1/flows/{name}")).await?;
                println!("deleted flow {name}");
            }
            FlowCmd::Pause { name } => {
                api.post(&format!("/v1/flows/{name}/pause"), json!({})).await?;
                println!("paused {name}");
            }
            FlowCmd::Resume { name } => {
                api.post(&format!("/v1/flows/{name}/resume"), json!({})).await?;
                println!("resumed {name}");
            }
        },
        Cmd::Bench { bootstrap, topic, partitions, records, record_size, batch, concurrency, compression, mode, consume } => {
            let compression = bp_protocol::records::Compression::parse(&compression).context("unknown compression")?;
            let _ = api.delete(&format!("/v1/topics/{topic}")).await;
            api.post("/v1/topics", json!({"name": topic, "partitions": partitions, "mode": mode})).await?;
            bench::produce(bench::ProduceBench {
                bootstrap: bootstrap.clone(),
                topic: topic.clone(),
                partitions,
                records,
                record_size,
                batch_records: batch.max(1),
                concurrency: concurrency.max(1),
                compression,
            })
            .await?;
            if consume {
                bench::consume(&bootstrap, &topic, partitions, records).await?;
            }
        }
    }
    Ok(())
}
