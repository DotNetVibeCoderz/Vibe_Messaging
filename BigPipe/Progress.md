# Progress · Progres

Development log for BigPipe. Newest entries on top. The roadmap is in [PLAN.md](PLAN.md).

*Catatan pengembangan BigPipe. Entri terbaru di atas. Peta jalan ada di [PLAN.md](PLAN.md).*

## 2026-10-02: log compaction · log compaction (roadmap 0.5)

**Done · Selesai**

- `cleanup.policy=compact` and `compact,delete` with `min.cleanable.dirty.ratio`, `min.compaction.lag.ms` and `delete.retention.ms`. Compaction runs in the background on a blocking thread, writes a new segment *generation* and swaps it in only if the segment did not change. Offsets and the log start never move, tombstones live for `delete.retention.ms`, and compact-only topics ignore time and size retention. · Compaction berjalan di latar belakang di thread blocking, menulis *generasi* segment baru, dan hanya dipasang jika segment tidak berubah; offset dan log start tidak pernah bergeser.
- Trigger now: `bpctl topic compact`, `POST /v1/topics/{t}/compact`, and `compact` in every SDK admin client. Stats per partition are in the topic details, plus `bp_compactions_total` and `bp_compaction_removed_records_total`. · Pemicu manual dan statistik di semua klien admin dan metrik.
- Kafka parity: compaction is refused for tiered and diskless topics, and null-key records are rejected with `INVALID_RECORD`. · Paritas Kafka: ditolak untuk topic tiered/diskless, record tanpa key ditolak.
- BigPipe.Streams creates store changelogs as compacted topics through the new `KafkaAdminClient` (Kafka `CreateTopics`). · Changelog BigPipe.Streams kini dibuat sebagai topic compacted.
- **Fix:** pipelined produce requests on one Kafka connection could reach a partition out of order, and idempotent producers with several requests in flight then failed with `OUT_OF_ORDER_SEQUENCE_NUMBER`. Appends are now enqueued in arrival order and only the wait runs concurrently. Benchmarks are unchanged (1 KiB: 441 MiB/s; 100 B lz4: 6.4 M records/s). · **Perbaikan:** request produce yang di-pipeline di satu koneksi bisa sampai ke partisi tidak berurutan; kini di-enqueue sesuai urutan datang.

**Verification · Verifikasi**

| Suite | Result · Hasil |
|---|---|
| `cargo test --workspace` | all pass, including 6 new compaction tests · semua lulus |
| `tests/integration/test_features.py` | 29/29, three runs in a row · tiga kali berturut-turut |
| `tests/compat/python_compat.py` | 17/17 |
| `dotnet test` | 40/40 |

## 2026-09-29: 0.1.0 complete · 0.1.0 selesai

**Done · Selesai**

- Moved into the `Vibe_Messaging` monorepo as `BigPipe/`. Package and repository URLs now point to `github.com/DotNetVibeCoderz/Vibe_Messaging`, the Go module is `github.com/DotNetVibeCoderz/Vibe_Messaging/BigPipe/sdk/go`, and CI is `.github/workflows/bigpipe-ci.yml`. All suites below were re-run from the new location. · Dipindahkan ke monorepo `Vibe_Messaging` sebagai `BigPipe/`; semua suite dijalankan ulang dari lokasi baru.
- Bilingual documentation (EN/ID): README, 8 guides in `docs/en` and `docs/id`, with screenshots. · Dokumentasi dwibahasa: README, 8 panduan di `docs/en` dan `docs/id`, dengan screenshot.
- `PLAN.md`, `Progress.md`, `LICENSE` (Apache-2.0).
- bpql mappings accept trailing `# comments` (a `#` inside strings is kept). · Mapping bpql menerima komentar `#` di akhir baris.
- The compose file advertises the broker as `bigpiped`, so the Schema Registry and other containers can reach it. · File compose mengiklankan broker sebagai `bigpiped` agar kontainer lain bisa menjangkaunya.
- Benchmarks re-measured on the release build (see [operations](docs/en/operations.md#benchmarks)). · Benchmark diukur ulang pada build release.

**Released · Dirilis**

- NuGet: BigPipe.Client, .Streams, .Analytics, .Analytics.Scripting, .Analytics.ML, .Analytics.Torch, .Analytics.Gravicode 0.1.0 (with symbols · dengan simbol)
- PyPI: `bigpipe` 0.1.0
- Go: tag `BigPipe/sdk/go/v0.1.0`
- npm: `bigpipe-client` 0.1.0

**Verification · Verifikasi**

| Suite | Result · Hasil |
|---|---|
| `cargo test --workspace --release` | all pass (21 tests) · semua lulus |
| `dotnet test sdk/dotnet/tests/BigPipe.Client.Tests` | 40 pass (client, streams, analytics, registry) |
| `tests/compat/python_compat.py` (librdkafka, kafka-python) | 17/17 |
| `tests/integration/test_features.py` on local / S3-compatible / Azure Blob | 22/22 each · masing-masing |
| Python / TypeScript / Go / Java SDK tests | 4 / 4 / pass / 2 |
| Gallery `--run-all` | 14/14 cases |
| Notebooks (`tools/notebooks/run_notebooks.py`) against the packages from nuget.org · terhadap paket dari nuget.org | 7/7 |
| GitHub Actions `bigpipe-ci.yml` (Ubuntu + Windows, compat and SDKs) | green · lulus |

**Fixed after release · Diperbaiki setelah rilis**

- CI on Ubuntu: ML.NET's MKL (SR-CNN, SSA) needs `libiomp5.so`. CI now installs LLVM's libomp and links it, and the Linux prerequisite is documented. · CI Ubuntu: MKL ML.NET butuh `libiomp5.so`; CI kini memasang libomp LLVM, dan prasyarat Linux sudah didokumentasikan.

**Not verified · Belum diverifikasi**

- `docker compose` (Docker was not available on the dev machine). · Docker tidak tersedia di mesin pengembangan.

## Earlier milestones · Tonggak sebelumnya

| Milestone · Tonggak | Contents · Isi |
|---|---|
| Rust data plane | codec, record batches and compression, shards, segment log, tiered and diskless storage, online migration, groups, share groups, flows, bpql, HTTP gateway, admin API, metrics, `bpctl` |
| Performance pass · Optimasi | fsync moved to a background flusher (p99 from tens of ms to a few ms under load), group commit, dual-stack listener · fsync dipindah ke flusher latar belakang, group commit, listener dual-stack |
| .NET client | managed Kafka client (Pipelines, hardware CRC32C), admin/HTTP/share clients, DI, Schema Registry serializer |
| BigPipe.Streams | DSL, windows, changelog-backed stores, repartitioning, interactive queries |
| Analytics | core, scripting (Roslyn/IronPython/Jint), ML.NET, TorchSharp, GravicodeScience, HF.Net, MediaPipe.NET (with an ONNX Runtime loader fix for Windows) |
| Control plane | Console (Blazor, plant-schematic design, EN/ID, dark mode), Schema Registry, AdminApi |
| SDKs | Python, TypeScript, Go, Java |
| Samples | Gallery (Avalonia, 14 cases, screenshot mode), Demo, 7 notebooks |
| Cloud storage · Penyimpanan cloud | integration suite passed on an S3-compatible service and on Azure Blob · suite integrasi lulus di layanan kompatibel S3 dan Azure Blob |
| Packaging | 7 NuGet packages with icon, Python sdist/wheel, npm package, Java jar |

## Next · Berikutnya

See [PLAN.md → 0.5](PLAN.md#05-next--berikutnya): Raft replication, transactions, compaction, io_uring, OIDC, Operator.

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
