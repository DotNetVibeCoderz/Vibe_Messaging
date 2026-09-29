# Progress · Progres

Development log for BigPipe. Newest entries on top. The roadmap is in [PLAN.md](PLAN.md).

*Catatan pengembangan BigPipe. Entri terbaru di atas. Peta jalan ada di [PLAN.md](PLAN.md).*

## 2026-09-29: 0.1.0 complete · 0.1.0 selesai

**Done · Selesai**

- Moved into the `Vibe_Messaging` monorepo as `BigPipe/`. Package and repository URLs now point to `github.com/DotNetVibeCoderz/Vibe_Messaging`, the Go module is `github.com/DotNetVibeCoderz/Vibe_Messaging/BigPipe/sdk/go`, and CI is `.github/workflows/bigpipe-ci.yml`. All suites below were re-run from the new location. · Dipindahkan ke monorepo `Vibe_Messaging` sebagai `BigPipe/`; semua suite dijalankan ulang dari lokasi baru.
- Bilingual documentation (EN/ID): README, 8 guides in `docs/en` and `docs/id`, with screenshots. · Dokumentasi dwibahasa: README, 8 panduan di `docs/en` dan `docs/id`, dengan screenshot.
- `PLAN.md`, `Progress.md`, `LICENSE` (Apache-2.0).
- bpql mappings accept trailing `# comments` (a `#` inside strings is kept). · Mapping bpql menerima komentar `#` di akhir baris.
- The compose file advertises the broker as `bigpiped`, so the Schema Registry and other containers can reach it. · File compose mengiklankan broker sebagai `bigpiped` agar kontainer lain bisa menjangkaunya.
- Benchmarks re-measured on the release build (see [operations](docs/en/operations.md#benchmarks)). · Benchmark diukur ulang pada build release.

**Verification · Verifikasi**

| Suite | Result · Hasil |
|---|---|
| `cargo test --workspace --release` | all pass (21 tests) · semua lulus |
| `dotnet test sdk/dotnet/tests/BigPipe.Client.Tests` | 40 pass (client, streams, analytics, registry) |
| `tests/compat/python_compat.py` (librdkafka, kafka-python) | 17/17 |
| `tests/integration/test_features.py` on local / S3-compatible / Azure Blob | 22/22 each · masing-masing |
| Python / TypeScript / Go / Java SDK tests | 4 / 4 / pass / 2 |
| Gallery `--run-all` | 14/14 cases |
| Notebooks (`tools/notebooks/run_notebooks.py`, local feed) | 7/7 (before the move · sebelum pemindahan) |

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
