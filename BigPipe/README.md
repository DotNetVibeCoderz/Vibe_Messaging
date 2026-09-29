<p align="center">
  <img src="assets/brand/bigpipe-icon-256.png" width="120" alt="BigPipe" />
</p>

<h1 align="center">BigPipe</h1>

<p align="center">
  <b>High-performance, Kafka-compatible realtime streaming, with storage you choose per topic.</b><br/>
  Rust data plane · .NET 10 control plane and ecosystem · SDKs for .NET, Python, TypeScript, Go and Java
</p>

<p align="center">
  <a href="README.md">English</a> · <a href="README.id.md">Bahasa Indonesia</a> · <a href="docs/README.md">Documentation</a>
</p>

![BigPipe Console](docs/images/console-overview.png)

## Why BigPipe

- **Speaks Kafka.** Existing producers, consumers and tools (Java client, librdkafka, kafka-python, franz-go …) connect to port 9092 unchanged.
- **Storage is a per-topic choice.** Use `local` for millisecond latency, `tiered` for long, cheap retention, and `diskless` for broker nodes with no disk that write straight to S3, GCS or Azure Blob. You can **migrate a live topic** between modes, and offsets never change.
- **Built for throughput.** A thread-per-core Rust broker with no locks on the hot path, zero-copy record batches stored in Kafka wire format, group commit, and fsync off the request path. One laptop does ~450 MiB/s of produce, or ~5 M small records/s ([benchmarks](docs/en/operations.md#benchmarks)).
- **More than a log.** It has server-side filters (bpql), in-broker pipelines (BigPipe Flow), share groups (work queues with a DLQ), HTTP/SSE access, and HTTP consumer groups.
- **Analytics where your data flows.** BigPipe.Streams (Kafka Streams–style), plus realtime analytics with ML.NET, TorchSharp, GravicodeScience, HF.Net, MediaPipe.NET, and user scripts in C#, Python or JavaScript.
- **Tools included:** a web Console, a Confluent-compatible Schema Registry, an AdminApi gateway with roles and audit, `bpctl`, the BigPipe Gallery desktop app and .NET notebooks.

## Quick start

```bash
cargo build --release -p bigpiped -p bpctl
./target/release/bigpiped --mode dev            # Kafka :9092 · HTTP :8082 · admin :9644 · metrics :9645

./target/release/bpctl topic create orders -p 6
echo '{"id":1,"amount":150000}' | ./target/release/bpctl produce orders --key order-1
./target/release/bpctl consume orders --from-beginning --filter 'this.amount > 100000'
./target/release/bpctl topic migrate orders --to diskless     # online, offsets unchanged

dotnet run -c Release --project control-plane/src/BigPipe.Console   # http://localhost:8080
dotnet run -c Release --project samples/BigPipe.Gallery             # desktop gallery of 14 use cases
```

Or use Docker: `docker compose -f deploy/docker/docker-compose.yml up -d --build` (includes MinIO).

## Clients

| | Install | Hello |
|---|---|---|
| .NET | `dotnet add package BigPipe.Client` | `await producer.SendAsync("orders", "k", order)` |
| Python | `pip install bigpipe` | `await Producer(bootstrap="localhost:9092").send("orders", {...})` |
| TypeScript | `npm install bigpipe-client` | `await new Producer({bootstrap:["localhost:9092"]}).send({topic:"orders", value:{...}})` |
| Go | `go get github.com/DotNetVibeCoderz/Vibe_Messaging/BigPipe/sdk/go` | `p.Send(ctx, &bigpipe.Record{Topic: "orders", Value: b})` |
| Java | `io.gravicode:bigpipe-client` | `producer.send("orders", "k", json).join()` |
| any Kafka client | — | `bootstrap.servers=localhost:9092` |

.NET extras: `BigPipe.Streams`, `BigPipe.Analytics`, `.Analytics.Scripting`, `.Analytics.ML`, `.Analytics.Torch` and `.Analytics.Gravicode`.

## Screenshots

| Console: topic configuration and online migration | Console: message browser with bpql filter |
|---|---|
| ![](docs/images/console-topic-config.png) | ![](docs/images/console-topic-messages.png) |
| **Console: flows** | **Console (dark)** |
| ![](docs/images/console-flows.png) | ![](docs/images/console-overview-dark.png) |
| **Gallery: produce and consume** | **Gallery: ML.NET anomaly detection** |
| ![](docs/images/gallery-hello.png) | ![](docs/images/gallery-anomaly.png) |
| **Gallery: dynamic scripting (C#, Python, JS)** | **Gallery: sentiment with MediaPipe.NET (dark)** |
| ![](docs/images/gallery-scripting.png) | ![](docs/images/gallery-sentiment-dark.png) |

## Repository layout

```
crates/            Rust: bp-protocol, bp-expr, bp-storage, bp-broker, bp-gateway, bigpiped
tools/bpctl        CLI and load generator
sdk/dotnet         BigPipe.Client, BigPipe.Streams, BigPipe.Analytics(.Scripting/.ML/.Torch/.Gravicode) + tests
sdk/python|typescript|go|java
control-plane/src  BigPipe.Console (Blazor), BigPipe.SchemaRegistry, BigPipe.AdminApi
samples/           BigPipe.Gallery (Avalonia), BigPipe.Demo, notebooks/
tests/             compat (librdkafka, kafka-python), integration (local / S3 / Azure)
deploy/            Docker, compose, systemd, sample config
docs/              en/ and id/ documentation, images/
```

## Documentation

[Getting started](docs/en/getting-started.md) · [Architecture](docs/en/architecture.md) · [SDKs](docs/en/sdks.md) · [HTTP & admin API](docs/en/http-api.md) · [bpql, Flow, share groups](docs/en/bpql-and-flows.md) · [Streams & analytics](docs/en/streams-and-analytics.md) · [Console & Gallery](docs/en/apps.md) · [Operations](docs/en/operations.md)

Roadmap: [PLAN.md](PLAN.md) · Progress: [Progress.md](Progress.md)

## Status

Version 0.1.0 is a **single-node** release. The Kafka protocol, the three storage modes, online migration, groups, share groups, flows, the HTTP gateway, every SDK and the .NET ecosystem all work and are tested. Replication (Raft), transactions, compaction and the other items in the design are planned; see [PLAN.md](PLAN.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).

---

<p align="center"><b>Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil</b><br/><sub>Made by Gravicode Studios, led by Kang Fadhil</sub></p>
