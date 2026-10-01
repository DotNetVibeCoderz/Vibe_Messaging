# BigPipe roadmap · Peta jalan BigPipe

This plan maps [`solution-design.md`](solution-design.md) to what exists today and what comes next. Progress on each item is tracked in [Progress.md](Progress.md).

*Rencana ini memetakan [`solution-design.md`](solution-design.md) ke apa yang sudah ada dan apa yang berikutnya. Progres tiap item dicatat di [Progress.md](Progress.md).*

Legend · Keterangan: ✅ done / selesai · 🟡 partial / sebagian · ⬜ planned / direncanakan

## 0.1: delivered · sudah dirilis

| Area | Item | Status | Notes · Catatan |
|---|---|---|---|
| Data plane | Kafka wire protocol (Produce, Fetch, ListOffsets, Metadata, groups, CreateTopics/DeleteTopics, InitProducerId, DescribeConfigs, ApiVersions) | ✅ | tested with librdkafka and kafka-python · diuji dengan librdkafka dan kafka-python |
| Data plane | Thread-per-core shards, group commit, zero-copy wire-format storage | ✅ | tokio `current_thread` per shard (all OSes) · per shard (semua OS) |
| Data plane | Idempotent producer, lz4/zstd/snappy/gzip | ✅ | |
| Storage | `local`, `tiered`, `diskless` per topic | ✅ | S3, GCS, Azure, local dir · direktori lokal |
| Storage | Online storage-mode migration (offset-preserving) | ✅ | extents with switch offsets · extent dengan switch offset |
| Storage | Time and size retention, background fsync, per-topic durability | ✅ | |
| Consumption | Classic consumer groups, HTTP groups, share groups with DLQ | ✅ | |
| Processing | bpql filters, BigPipe Flow (filter/mapping/route) | ✅ | |
| Access | HTTP gateway (REST + SSE), admin API, OpenMetrics | ✅ | |
| Control plane | Console (Blazor, EN/ID, light/dark), Schema Registry, AdminApi (RBAC + audit) | ✅ | |
| SDKs | .NET (native Kafka), Python, TypeScript, Go, Java (HTTP) | ✅ | |
| .NET ecosystem | BigPipe.Streams; Analytics + Scripting (C#/Python/JS), ML.NET, TorchSharp, GravicodeScience, HF.Net, MediaPipe.NET | ✅ | |
| Samples | BigPipe Gallery (Avalonia, 14 cases), Demo, 7 notebooks | ✅ | |
| Tooling | `bpctl` (admin, produce/consume, bench), Docker, compose, systemd | ✅ | compose not yet run in CI · compose belum dijalankan di CI |
| Docs | Bilingual docs with screenshots · dokumentasi dwibahasa dengan screenshot | ✅ | |

## 0.5: next · berikutnya

| Area | Item | Design § | Status |
|---|---|---|---|
| Replication | Multi-Raft per partition, controller quorum, multi-node clusters · Raft per partisi, quorum controller, cluster multi-node | §8.1–8.2 | ⬜ |
| Replication | Leader balancing and partition movement · penyeimbangan leader dan perpindahan partisi | §8.3 | ⬜ |
| Semantics | Transactions and exactly-once (AddPartitionsToTxn, EndTxn, TxnOffsetCommit) · transaksi dan exactly-once | §9.3 | ⬜ |
| Semantics | KIP-848 incremental group protocol; Kafka share-group protocol (KIP-932 wire APIs) | §9.1–9.2 | ⬜ (share groups exist over HTTP today · share group sudah ada lewat HTTP) |
| Protocol | Flexible (tagged-field) API versions, SASL/SCRAM, TLS, ACL APIs | §6.2, §15 | ⬜ |
| Storage | Log compaction (`cleanup.policy=compact`) · compaction log | §7 | ✅ (local topics; merging small cleaned segments still to do · segmen kecil hasil compaction belum digabung) |
| Storage | Diskless file compaction (merge small objects) and metadata sharding · compaction file diskless dan sharding metadata | §7.3 | 🟡 (retention deletes whole files · retensi menghapus file utuh) |
| Performance | io_uring / glommio backend on Linux · backend io_uring / glommio di Linux | §6.1 | ⬜ |
| Performance | Deterministic simulator `bp-sim`, fuzzing, Jepsen · simulator deterministik, fuzzing, Jepsen | §24 | ⬜ |
| Security | OIDC for Console/AdminApi, mTLS · OIDC untuk Console/AdminApi, mTLS | §15 | ⬜ (API keys + roles today · saat ini API key + role) |
| Control plane | Kubernetes Operator and CRDs, Admin gRPC (`proto/`) | §13, §18.2 | ⬜ (REST admin API today · saat ini admin API REST) |
| SDKs | Shared Rust core (`bigpipe-core`) with C ABI and bindings (PyO3, napi-rs, cgo, JNI) | §19.1 | ⬜ (SDKs use HTTP today · saat ini SDK memakai HTTP) |

## 1.0 and beyond · 1.0 dan seterusnya

| Item | Design § | Status |
|---|---|---|
| WebAssembly transforms (wasmtime, WIT interface) · transform WebAssembly | §10 | ⬜ |
| Iceberg topics (tables written from topics) · topic Iceberg | §11 | ⬜ |
| BigPipe Mirror (offset-preserving replication), multi-region RPO=0 · replikasi dengan offset tetap | §12 | ⬜ |
| BigPipe Connect (.NET connectors) · konektor .NET | §14.2 | ⬜ |
| BPP native protocol over QUIC (port 9093) · protokol native BPP lewat QUIC | §6.2 | ⬜ |
| Design sample apps: PayGuard, FleetPulse, ShopStream, LegacyLift, … · aplikasi contoh dari desain | §21 | 🟡 (use cases covered by the Gallery · kasus sudah ditampilkan di Gallery) |
| Public, reproducible benchmark on server hardware · benchmark publik yang bisa diulang di hardware server | §17 | 🟡 (laptop numbers in docs · angka laptop di dokumentasi) |
| Serverless multi-tenant, streaming SQL, geo-partitioning · serverless multi-tenant, SQL streaming, geo-partitioning | §25 (2.0) | ⬜ |

## Principles kept · Prinsip yang dijaga

- The data plane keeps serving produce and consume when the control plane is down. · *Data plane tetap melayani produce dan consume ketika control plane mati.*
- Every new feature stays reachable by standard Kafka clients, or it is added alongside them (HTTP, SSE). · *Setiap fitur baru tetap bisa dijangkau klien Kafka standar, atau ditambahkan di sampingnya (HTTP, SSE).*
- Performance claims come only from reproducible measurements. · *Klaim performa hanya berasal dari pengukuran yang bisa diulang.*

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
