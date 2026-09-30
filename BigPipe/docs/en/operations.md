# Operations: configuration, deployment, benchmarks, troubleshooting

[English](operations.md) · [Bahasa Indonesia](../id/operations.md)

## Node configuration

Settings are applied in this order, and later ones win: built-in defaults, then the YAML file (`--config` / `BIGPIPE_CONFIG`), then environment variables, then command-line flags. `deploy/config/bigpipe.yaml` lists every key with its default.

| YAML key | Flag / env | Default | Meaning |
|---|---|---|---|
| `data_dir` | `--data-dir` / `BIGPIPE_DATA_DIR` | `./data` | segments, offsets, metadata, local object store |
| `kafka_addr` | `--kafka-addr` / `BIGPIPE_KAFKA_ADDR` | `0.0.0.0:9092` | Kafka listener (dual-stack IPv4/IPv6) |
| `advertised_host`, `advertised_port` | `--advertised-host` / `--advertised-port` | `localhost`, `9092` | address returned to clients in Metadata |
| `http_addr` | `--http-addr` / `BIGPIPE_HTTP_ADDR` | `0.0.0.0:8082` | HTTP data gateway |
| `admin_addr` | `--admin-addr` / `BIGPIPE_ADMIN_ADDR` | `0.0.0.0:9644` | admin API |
| `metrics_addr` | `--metrics-addr` / `BIGPIPE_METRICS_ADDR` | `0.0.0.0:9645` | `/metrics` |
| `shards` | `--shards` / `BIGPIPE_SHARDS` | `0` (= CPU cores) | event-loop threads |
| `object_store_url` | `--object-store` / `BIGPIPE_OBJECT_STORE` | `<data_dir>/objects` | where tiered and diskless data go |
| `object_cache_bytes` | | 256 MiB | LRU cache for object data |
| `diskless_linger_ms` / `diskless_max_file_bytes` | | 100 ms / 8 MiB | diskless batching window |
| `auto_create_topics`, `default_partitions` | | `true`, `3` | topics are created on first produce |
| `default_storage_mode` | `--default-storage-mode` | `local` | mode for new topics |
| `admin_api_key` | `--admin-api-key` / `BIGPIPE_ADMIN_API_KEY` | none | when set, the admin API requires `Authorization: Bearer <key>` |
| `max_request_bytes` | | 100 MiB | largest accepted request |
| | `BIGPIPE_LOG` | `info` | log filter, e.g. `info,bp_broker=debug` |
| | `BIGPIPE_LOG_FORMAT` | `text` | `json` for structured logs |

## Topic configuration

Set at creation (`bpctl topic create t -c key=value`, `"config":{}` in the admin API, or any Kafka client's CreateTopics). Change them later with `bpctl topic config t key=value` (`key=` resets to the default) or `PATCH /v1/topics/{t}/config`. Kafka's `DescribeConfigs` shows the effective values.

| Key | Default | Meaning |
|---|---|---|
| `bigpipe.storage.mode` | `local` | `local`, `tiered` or `diskless`; changing it performs an online migration |
| `retention.ms` / `retention.bytes` | 7 days / `-1` | whole segments older or beyond the size are deleted (`-1` = unlimited) |
| `segment.bytes` / `segment.ms` | 256 MiB / 7 days | when the active segment is rolled |
| `local.retention.ms` | 1 hour | tiered: how long uploaded segments stay on local disk |
| `bigpipe.durability` | `flush` | `flush`: ack after the OS page cache (Kafka's default); `fsync`: sync before acking |
| `flush.ms` | `-1` | periodic fsync of the active segment (`-1` = leave it to the OS) |
| `max.message.bytes` | 8 MiB | largest record batch |
| `bigpipe.cache.bytes` | 4 MiB | per-partition read cache for recent data |
| `bigpipe.schema.validation` | `none` | `strict`: reject records whose value is not valid JSON; `lenient` is accepted but does not check yet |
| `cleanup.policy` | `delete` | stored for compatibility; compaction is on the roadmap |

## Object storage

| URL | Credentials (environment) |
|---|---|
| `s3://bucket/prefix` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`; for MinIO or other S3-compatible services also `AWS_ENDPOINT=http://host:9000`, `AWS_ALLOW_HTTP=true` and, when needed, `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false` |
| `gs://bucket/prefix` | `GOOGLE_SERVICE_ACCOUNT` (path to the JSON key) or workload identity |
| `az://container/prefix` | `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY` (or SAS / managed identity variables) |
| a directory or `file:///path` | none (development, or a shared NFS mount) |
| `memory://` | none (tests only) |

Layout inside the prefix: `diskless/<ms>-<node>-<seq>.bpd` (shared diskless files) and `tiered/<topic>/<partition>/<base-offset>.log` (uploaded segments). The integration suite `tests/integration/test_features.py` has been run against a local directory, an S3-compatible service and Azure Blob Storage.

## Deployment

**Docker Compose** (MinIO + bigpiped + Schema Registry + AdminApi + Console):

```bash
docker compose -f deploy/docker/docker-compose.yml up -d --build
```

The broker advertises itself as `bigpiped`. For Kafka clients running on the host, add `127.0.0.1 bigpiped` to your hosts file.

**Container images:** `deploy/docker/Dockerfile` (a multi-stage Rust build into a slim Debian image that runs as a non-root user) and `deploy/docker/Dockerfile.dotnet` (`--build-arg APP=BigPipe.Console|BigPipe.SchemaRegistry|BigPipe.AdminApi`).

**systemd:** `deploy/systemd/bigpiped.service`, with the config at `/etc/bigpipe/bigpipe.yaml`, a `bigpipe` user and `LimitNOFILE=1048576`.

**Production checklist**

- Set `advertised_host` to the name that clients can resolve.
- Protect the admin API with `admin_api_key` and put the AdminApi gateway (roles and audit) in front of it for people and automation.
- Use `tiered` or `diskless` for topics that need long retention. Local disks then only hold hot data.
- Choose `bigpipe.durability=fsync` for topics where losing the last few milliseconds in a power failure is unacceptable (this release runs one node, so there is no replication yet).
- Scrape `:9645/metrics` with Prometheus.

## Benchmarks

The numbers below were **measured with `bpctl bench` on one development laptop**: Intel Core i7-8650U (4 cores / 8 threads, 1.9 GHz), 16 GB RAM, SATA SSD, Windows 11, release build, with client and broker on the same machine. They show what a single small node can do. They are not a tuned server benchmark, and the targets in `solution-design.md` are separate goals.

| Workload | Command | Result |
|---|---|---|
| Produce 1 KiB records, acks=all, 4 connections | `bpctl bench --records 1000000 --record-size 1024 --concurrency 4 --consume` | 447–480 MiB/s (~460–490 k records/s); p50 1.8–2.0 ms, p99 ~12 ms per request of 500 records |
| Consume the same data | (same run) | 344–402 MiB/s |
| Produce 100-byte records, lz4, batches of 2000, 8 connections | `bpctl bench --topic small --records 5000000 --record-size 100 --batch 2000 --compression lz4 --concurrency 8` | ~5.0 M records/s; p50 0.39 ms |
| Diskless, objects on local disk, 8 connections | `bpctl bench --topic dl --mode diskless --records 200000 --concurrency 8 --consume` | produce p50 ~123 ms (the 100 ms batching window + write), consume 275 MiB/s |

To reproduce, start `bigpiped --mode dev` and run the commands. `bpctl bench --help` lists all options.

## Monitoring

- `GET :9645/metrics`: OpenMetrics counters and histograms (see [HTTP API](http-api.md#metrics-9645)).
- `GET :9644/v1/metrics`: a JSON snapshot used by the Console.
- Logs: `BIGPIPE_LOG=info,bp_broker=debug`, and `BIGPIPE_LOG_FORMAT=json` for log shippers.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| A Kafka client connects, then times out | The client reconnects to the `advertised_host:advertised_port` from Metadata. Make that name resolvable from the client (Docker: `bigpiped`). |
| `localhost` works in the browser but not for a client | Some clients resolve `localhost` to `::1`. `bigpiped` listens dual-stack on `0.0.0.0` / `[::]`, so check that no firewall blocks IPv6. |
| Diskless produce takes ~100 ms | This is expected: records wait for the batching window (`diskless_linger_ms`). Lower it for latency, or raise it for fewer object writes. |
| S3-compatible service returns `NoSuchKey` / `NoSuchBucket` | Set `AWS_ENDPOINT` to the service root (no bucket in the path) and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false` for path-style services such as MinIO. |
| HTTP consumer gets `404 unknown member` | The member stopped polling for longer than `session_timeout_ms` (default 30 s) and was removed. Join again; the SDKs do this automatically. |
| `401` from the admin API | `admin_api_key` is set. Send `Authorization: Bearer <key>` (Console: `BigPipe:ApiKey`, bpctl: `--api-key` / `BIGPIPE_API_KEY`). |
| `DllNotFoundException: MklImports` on Linux (ML.NET SR-CNN / SSA) | Intel OpenMP is missing: `sudo apt-get install libomp-dev`, and link it as `libiomp5.so` on the loader path (see [ML.NET](streams-and-analytics.md#mlnet-bigpipeanalyticsml)). |
| ONNX / MediaPipe crash on Windows in a notebook | Call `OnnxRuntimeNative.EnsureLoaded()` first (the analytics types do it automatically) so the NuGet ONNX Runtime is used instead of the one in System32. |

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
