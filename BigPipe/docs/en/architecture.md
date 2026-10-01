# Architecture and concepts

[English](architecture.md) · [Bahasa Indonesia](../id/architecture.md)

BigPipe is a Kafka-protocol-compatible streaming platform. The split rule is simple: **anything that touches every record is written in Rust; anything humans touch or that runs per configuration is written in .NET.**

```
            Kafka clients (Java, librdkafka, franz-go, BigPipe.Client ...)     HTTP / SSE clients (Python, TS, Go, Java SDKs, curl)
                        │ :9092                                                              │ :8082
┌───────────────────────▼────────────────────────────────────────────────────────────────────▼──────────────────────┐
│ bigpiped (Rust, one binary)                                                                                       │
│  ┌──────────────┐  ┌──────────────────┐  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │ Kafka codec  │  │ HTTP gateway     │  │ Shards: one OS thread + event loop per core, each owns partitions  │ │
│  │ (zero-copy)  │  │ REST · SSE ·     │─▶│  ├─ local:    segment log + sparse index                            │ │
│  └──────┬───────┘  │ groups · share   │  │  ├─ tiered:   sealed segments ─▶ object storage, local evicted      │ │
│         └─────────▶└──────────────────┘  │  └─ diskless: extents that point into shared objects                │ │
│  Group coordinator · share groups · BigPipe Flow · bpql       └──────────────┬──────────────────────────────────────┘ │
│  Diskless agent (batches all partitions into one object) ─────────────────────┤                                        │
│  Admin API :9644 · metrics :9645                                              ▼                                        │
└───────────────────────────────────────────────────────── object storage: S3 · GCS · Azure Blob · MinIO · local dir ─┘
                        ▲ :9644 (admin)             ▲ internal topics (__bp_schemas, __bp_audit)
┌───────────────────────┴───────────────────────────┴───────────────────────────────────────────────────────────────┐
│ Control plane (.NET 10): Console (Blazor) · Schema Registry · AdminApi gateway (RBAC + audit)                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

The data plane never depends on the control plane. Produce and consume keep working while the Console, the registry or the gateway are down.

## Rust workspace

| Crate | Role |
|---|---|
| `bp-protocol` | Kafka wire codec: request decoding, response encoding, record batch v2 (validation, offset assignment, CRC32C), compression (gzip, snappy, lz4, zstd), murmur2 partitioner |
| `bp-expr` | **bpql**, the expression and mapping language used by filters, share groups and flows |
| `bp-storage` | segment log, sparse index, retention, tiering, diskless extents, object storage (S3/GCS/Azure/local) with an LRU cache, idempotent-producer state |
| `bp-broker` | shards, the diskless agent, group coordinator, share groups, BigPipe Flow, the Kafka server, metrics |
| `bp-gateway` | axum HTTP gateway (data plane on 8082) and admin API (9644) |
| `bigpiped` | the binary: CLI, YAML config, listeners |
| `tools/bpctl` | CLI for admin, produce/consume and load generation |

## Thread-per-core shards

At startup `bigpiped` creates one shard per CPU core (or `shards:` in the config). Each shard is an OS thread with a single-threaded async runtime, and it **owns** a subset of partitions. Network tasks send commands (append, read, list offsets) to the owning shard through a channel. The shard drains them in groups, so a burst of produce requests turns into one write per partition (group commit). Partition state is never shared between threads, so the hot path takes no locks.

fsync never runs on a shard thread. Sealed segments are synced by a background flusher. Per topic, you choose `bigpipe.durability=flush` (the default: acknowledge after the write reaches the OS page cache, as Kafka does) or `fsync` (sync before acknowledging). `flush.ms` adds periodic syncs of the active segment.

## Record path

1. The Kafka codec parses a Produce request without copying record data (`BytesMut::split_to`).
2. Batches are validated (magic v2, CRC32C, lengths). Idempotent producers are de-duplicated by `(producer id, epoch, sequence)`.
3. The shard assigns offsets by patching the batch header in place, then appends the **exact Kafka wire bytes** to the segment.
4. A Fetch request reads byte ranges straight from the segment files or the object cache, and they go out as-is.

Because data stays in wire format, consume does no re-encoding. Compressed batches stay compressed end to end.

## Storage modes (per topic)

Set with `bigpipe.storage.mode` when you create a topic, or change it later with a migration.

| Mode | Writes go to | Produce latency | Cost profile | Good for |
|---|---|---|---|---|
| `local` | segment files on local disk | milliseconds | disk per node | low latency, hot data |
| `tiered` | local disk, sealed segments uploaded to object storage, local copies evicted after `local.retention.ms` | milliseconds | small disks, long retention | long history, replay |
| `diskless` | object storage via the diskless agent | ~100 ms (the batching window) | no broker disk, object requests are shared | high volume, cost first, stateless brokers |

### Diskless in detail

The diskless agent collects appends from **every** diskless partition for `diskless_linger_ms` (default 100 ms) or up to `diskless_max_file_bytes` (8 MiB). It then writes them as **one** object (`diskless/<ms>-<node>-<seq>.bpd`). Only after the object store confirms the write are offsets assigned and the producers acknowledged. The partition keeps a small **extent** (offset range → object, byte range). Reads fetch that byte range (cached in the LRU object cache) and patch the base offset on the way out. One PUT covers many partitions, so the number of object requests stays low as the partition count grows.

### Online migration

A topic can move between modes while producers and consumers are running:

```bash
bpctl topic migrate orders --to diskless
```

The partition records a **switch offset**. Everything below it stays where it was written, and new appends go to the new mode. Offsets never change, and consumers do not notice. Moving from diskless back to local works the same way.

## Log compaction

Topics with `cleanup.policy=compact` keep the **latest record for every key** instead of expiring data by age. This is what Kafka Streams changelogs, CDC tables and "current state" topics need. With `compact,delete`, both apply.

- **What is compacted:** sealed local segments. The active segment is never touched, and records younger than `min.compaction.lag.ms` are left alone.
- **When:** in the background, once at least `min.cleanable.dirty.ratio` (default 0.5) of a partition's sealed bytes has not been cleaned yet. You can also run it now with `bpctl topic compact <topic>` or `POST /v1/topics/{t}/compact`.
- **How:** the shard hands a job to a blocking thread. That thread builds a map from each key's 128-bit fingerprint to its latest offset, then rewrites each segment into a new file *generation* (`<base>-<n>.log`), keeping only those records. Batches keep their offsets and compression. The shard swaps the new files in only if the segment did not change in the meantime. Old files are deleted after 60 s, so reads that are already planned can finish.
- **Deletes:** a record with a null value (a *tombstone*) removes its key. The tombstone itself is kept for `delete.retention.ms` (default 24 h), so consumers can see the delete.
- **Offsets never change, and neither does the log start.** Consumers simply skip the gaps, as with Kafka.
- **Rules:** compaction works on `local` topics only (as with Kafka's tiered storage). Every record needs a key; a record without one is rejected with `INVALID_RECORD`.

## Consumer groups

- **Kafka groups:** the classic protocol (JoinGroup, SyncGroup, Heartbeat, OffsetCommit, OffsetFetch). Assignment is done by the client as usual. Committed offsets are stored in an append-only log per node.
- **HTTP groups:** for clients that only speak HTTP. The broker assigns partitions itself (`POST /v1/groups/{g}/members`), and members poll and commit over REST. The Python, TypeScript, Go and Java SDKs use this.
- **Share groups** (KIP-932 style): many workers read the same partitions like a queue. Each record is locked to one worker, then *accepted*, *released* (redelivered) or *rejected*. After `max_attempts` deliveries, or when rejected, a record goes to the dead-letter topic.

## BigPipe Flow

Flows are managed topic-to-topic pipelines that run inside the broker, so you don't need a separate stream processor for routing and cleanup. A flow reads an input topic and applies the `filter`, `mapping` and `route` processors (written in bpql). It writes to an output topic, or to a topic computed per record. Progress is committed as the group `__flow:<name>`, so a restarted flow continues where it stopped (at-least-once). See [bpql, flows and share groups](bpql-and-flows.md).

## .NET side

| Project | What it is |
|---|---|
| `BigPipe.Client` | fully managed Kafka client (System.IO.Pipelines, pooled buffers, hardware CRC32C, idempotent producer, consumer groups) plus admin, HTTP and share-group clients and DI extensions |
| `BigPipe.Streams` | Kafka Streams–style DSL: KStream/KTable, tumbling/hopping/sliding/session windows, state stores backed by changelog topics, interactive queries |
| `BigPipe.Analytics` (+ `.Scripting`, `.ML`, `.Torch`, `.Gravicode`) | realtime analytics: windows, statistics, sketches, anomaly detection, runtime scripts, ML.NET, TorchSharp, GravicodeScience, HF.Net, MediaPipe.NET |
| `BigPipe.SchemaRegistry` | Confluent-compatible schema registry (JSON Schema, Avro, Protobuf) stored in `__bp_schemas` |
| `BigPipe.AdminApi` | authenticated gateway to the admin API with Viewer/Operator/Admin roles and an audit trail in `__bp_audit` |
| `BigPipe.Console` | Blazor Server web console |

## Where this is heading

This release is a single-node data plane. Replication (Raft), transactions, compaction, io_uring and the other items in the design document are tracked in [PLAN.md](../../PLAN.md).

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
