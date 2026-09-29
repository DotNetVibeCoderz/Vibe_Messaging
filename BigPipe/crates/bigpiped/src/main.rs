//! `bigpiped` — the BigPipe node.
//!
//! Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.

use std::path::PathBuf;

use anyhow::Context;
use bp_broker::{Broker, NodeConfig};
use clap::Parser;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(
    name = "bigpiped",
    version,
    about = "BigPipe — high performance realtime stream processing platform",
    after_help = "Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil."
)]
struct Args {
    /// Path to bigpipe.yaml.
    #[arg(short, long, env = "BIGPIPE_CONFIG")]
    config: Option<PathBuf>,
    /// `dev` runs every role in one process with local defaults.
    #[arg(long, default_value = "dev")]
    mode: String,
    #[arg(long, env = "BIGPIPE_DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[arg(long, env = "BIGPIPE_KAFKA_ADDR")]
    kafka_addr: Option<String>,
    #[arg(long, env = "BIGPIPE_ADVERTISED_HOST")]
    advertised_host: Option<String>,
    #[arg(long, env = "BIGPIPE_ADVERTISED_PORT")]
    advertised_port: Option<i32>,
    #[arg(long, env = "BIGPIPE_HTTP_ADDR")]
    http_addr: Option<String>,
    #[arg(long, env = "BIGPIPE_ADMIN_ADDR")]
    admin_addr: Option<String>,
    #[arg(long, env = "BIGPIPE_METRICS_ADDR")]
    metrics_addr: Option<String>,
    /// Object storage URL: s3://bucket/prefix, gs://..., az://container/prefix, or a directory.
    #[arg(long, env = "BIGPIPE_OBJECT_STORE")]
    object_store: Option<String>,
    /// Shards (threads). 0 = one per CPU core.
    #[arg(long, env = "BIGPIPE_SHARDS")]
    shards: Option<usize>,
    #[arg(long, env = "BIGPIPE_DEFAULT_STORAGE_MODE")]
    default_storage_mode: Option<String>,
    #[arg(long, env = "BIGPIPE_ADMIN_API_KEY")]
    admin_api_key: Option<String>,
    /// Log format: text or json.
    #[arg(long, env = "BIGPIPE_LOG_FORMAT", default_value = "text")]
    log_format: String,
}

fn load_config(args: &Args) -> anyhow::Result<NodeConfig> {
    let mut cfg = match &args.config {
        Some(p) => {
            let text = std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?;
            serde_yaml_ng::from_str(&text).with_context(|| format!("parsing {}", p.display()))?
        }
        None => NodeConfig::default(),
    };
    if args.mode != "dev" && args.config.is_none() {
        anyhow::bail!("--mode {} requires --config", args.mode);
    }
    macro_rules! set {
        ($field:ident) => {
            if let Some(v) = &args.$field {
                cfg.$field = v.clone();
            }
        };
    }
    set!(data_dir);
    set!(kafka_addr);
    set!(advertised_host);
    set!(advertised_port);
    set!(http_addr);
    set!(admin_addr);
    set!(metrics_addr);
    set!(shards);
    set!(default_storage_mode);
    if let Some(v) = &args.object_store {
        cfg.object_store_url = v.clone();
    }
    if args.admin_api_key.is_some() {
        cfg.admin_api_key = args.admin_api_key.clone();
    }
    Ok(cfg)
}

/// Binds `addr`. `0.0.0.0:<port>` becomes a dual-stack `[::]:<port>` socket when IPv6 is
/// available, so clients resolving `localhost` to `::1` connect too.
async fn bind(addr: &str) -> std::io::Result<tokio::net::TcpListener> {
    if let Some(port) = addr.strip_prefix("0.0.0.0:") {
        if let Ok(port) = port.parse::<u16>() {
            let dual = (|| -> std::io::Result<std::net::TcpListener> {
                let s = socket2::Socket::new(socket2::Domain::IPV6, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
                s.set_only_v6(false)?;
                #[cfg(not(windows))]
                s.set_reuse_address(true)?;
                s.bind(&std::net::SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, port)).into())?;
                s.listen(1024)?;
                s.set_nonblocking(true)?;
                Ok(s.into())
            })();
            if let Ok(l) = dual {
                return tokio::net::TcpListener::from_std(l);
            }
        }
    }
    tokio::net::TcpListener::bind(addr).await
}

async fn serve_http(addr: String, router: axum::Router, what: &'static str) -> anyhow::Result<()> {
    let listener = bind(&addr).await.with_context(|| format!("binding {what} on {addr}"))?;
    tracing::info!(%addr, "{what} listening");
    axum::serve(listener, router).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let filter = tracing_subscriber::EnvFilter::try_from_env("BIGPIPE_LOG").unwrap_or_else(|_| "info".into());
    if args.log_format == "json" {
        tracing_subscriber::fmt().json().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    let cfg = load_config(&args)?;
    println!(
        r#"
  ____  _       ____  _
 | __ )(_) __ _|  _ \(_)_ __   ___
 |  _ \| |/ _` | |_) | | '_ \ / _ \
 | |_) | | (_| |  __/| | |_) |  __/
 |____/|_|\__, |_|   |_| .__/ \___|   v{}
          |___/        |_|
  Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil
"#,
        env!("CARGO_PKG_VERSION")
    );
    let broker = Broker::start(cfg.clone()).await?;

    let kafka = bind(&cfg.kafka_addr).await.with_context(|| format!("binding Kafka on {}", cfg.kafka_addr))?;
    tracing::info!(addr = %cfg.kafka_addr, advertised = %format!("{}:{}", cfg.advertised_host, cfg.advertised_port), "Kafka protocol listening");
    let k = tokio::spawn(bp_broker::kafka::serve(broker.clone(), kafka));
    let h = tokio::spawn(serve_http(cfg.http_addr.clone(), bp_gateway::data_router(broker.clone()), "HTTP gateway"));
    let a = tokio::spawn(serve_http(cfg.admin_addr.clone(), bp_gateway::admin_router(broker.clone()), "Admin API"));
    let m = tokio::spawn(serve_http(cfg.metrics_addr.clone(), bp_gateway::metrics_router(broker.clone()), "Metrics"));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
        r = k => tracing::error!(result = ?r, "Kafka listener stopped"),
        r = h => tracing::error!(result = ?r, "HTTP gateway stopped"),
        r = a => tracing::error!(result = ?r, "Admin API stopped"),
        r = m => tracing::error!(result = ?r, "Metrics listener stopped"),
    }
    broker.shutdown().await;
    tracing::info!("bye");
    Ok(())
}
