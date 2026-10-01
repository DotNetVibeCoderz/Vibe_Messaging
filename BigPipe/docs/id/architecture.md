# Arsitektur dan konsep

[English](../en/architecture.md) · [Bahasa Indonesia](architecture.md)

BigPipe adalah platform streaming yang kompatibel dengan protokol Kafka. Aturan pembagiannya sederhana: **semua yang menyentuh setiap record ditulis dengan Rust; semua yang disentuh manusia atau berjalan per konfigurasi ditulis dengan .NET.**

```
            Klien Kafka (Java, librdkafka, franz-go, BigPipe.Client ...)       Klien HTTP / SSE (SDK Python, TS, Go, Java, curl)
                        │ :9092                                                              │ :8082
┌───────────────────────▼────────────────────────────────────────────────────────────────────▼──────────────────────┐
│ bigpiped (Rust, satu binary)                                                                                      │
│  ┌──────────────┐  ┌──────────────────┐  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │ Codec Kafka  │  │ Gateway HTTP     │  │ Shard: satu thread OS + event loop per core, masing-masing memiliki │ │
│  │ (zero-copy)  │  │ REST · SSE ·     │─▶│ partisi                                                             │ │
│  └──────┬───────┘  │ group · share    │  │  ├─ local:    segment log + sparse index                            │ │
│         └─────────▶└──────────────────┘  │  ├─ tiered:   segment tertutup ─▶ object storage, salinan lokal     │ │
│                                          │  │             dihapus                                             │ │
│                                          │  └─ diskless: extent yang menunjuk ke objek bersama                 │ │
│  Group coordinator · share group · BigPipe Flow · bpql         └──────────────┬──────────────────────────────────────┘ │
│  Agen diskless (menggabungkan semua partisi ke satu objek) ────────────────────┤                                        │
│  Admin API :9644 · metrics :9645                                               ▼                                        │
└───────────────────────────────────────────────────────── object storage: S3 · GCS · Azure Blob · MinIO · direktori lokal ┘
                        ▲ :9644 (admin)             ▲ topic internal (__bp_schemas, __bp_audit)
┌───────────────────────┴───────────────────────────┴───────────────────────────────────────────────────────────────┐
│ Control plane (.NET 10): Console (Blazor) · Schema Registry · gateway AdminApi (RBAC + audit)                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Data plane tidak pernah bergantung pada control plane. Produce dan consume tetap berjalan meskipun Console, registry, atau gateway sedang mati.

## Workspace Rust

| Crate | Peran |
|---|---|
| `bp-protocol` | codec wire Kafka: decode request, encode response, record batch v2 (validasi, penetapan offset, CRC32C), kompresi (gzip, snappy, lz4, zstd), partitioner murmur2 |
| `bp-expr` | **bpql**, bahasa ekspresi dan mapping untuk filter, share group, dan flow |
| `bp-storage` | segment log, sparse index, retensi, tiering, extent diskless, object storage (S3/GCS/Azure/lokal) dengan cache LRU, state producer idempoten |
| `bp-broker` | shard, agen diskless, group coordinator, share group, BigPipe Flow, server Kafka, metrik |
| `bp-gateway` | gateway HTTP axum (data plane di 8082) dan admin API (9644) |
| `bigpiped` | binary utama: CLI, konfigurasi YAML, listener |
| `tools/bpctl` | CLI untuk admin, produce/consume, dan uji beban |

## Shard thread-per-core

Saat mulai, `bigpiped` membuat satu shard per core CPU (atau sesuai `shards:` di konfigurasi). Setiap shard adalah thread OS dengan runtime async single-thread, dan **memiliki** sebagian partisi. Task jaringan mengirim perintah (append, read, list offsets) ke shard pemilik lewat channel. Shard mengambil perintah secara berkelompok, sehingga banyak request produce sekaligus menjadi satu penulisan per partisi (group commit). State partisi tidak pernah dibagi antar-thread, jadi jalur panas tidak memakai lock.

fsync tidak pernah dijalankan di thread shard. Segment yang sudah tertutup disinkronkan oleh flusher di latar belakang. Per topic, Anda memilih `bigpipe.durability=flush` (default: ack setelah data masuk page cache OS, seperti Kafka) atau `fsync` (sinkron sebelum ack). `flush.ms` menambahkan sinkronisasi berkala untuk segment aktif.

## Jalur record

1. Codec Kafka mem-parse request Produce tanpa menyalin data record (`BytesMut::split_to`).
2. Batch divalidasi (magic v2, CRC32C, panjang). Duplikat dari producer idempoten dibuang berdasarkan `(producer id, epoch, sequence)`.
3. Shard menetapkan offset dengan menambal header batch di tempat, lalu menambahkan **byte wire Kafka apa adanya** ke segment.
4. Request Fetch membaca rentang byte langsung dari file segment atau cache objek, lalu mengirimkannya apa adanya.

Karena data tetap dalam format wire, consume tidak melakukan encode ulang. Batch terkompresi tetap terkompresi dari ujung ke ujung.

## Mode penyimpanan (per topic)

Diatur dengan `bigpipe.storage.mode` saat membuat topic, atau diubah kemudian lewat migrasi.

| Mode | Tulisan masuk ke | Latensi produce | Profil biaya | Cocok untuk |
|---|---|---|---|---|
| `local` | file segment di disk lokal | milidetik | disk per node | latensi rendah, data panas |
| `tiered` | disk lokal; segment tertutup diunggah ke object storage, salinan lokal dihapus setelah `local.retention.ms` | milidetik | disk kecil, retensi panjang | riwayat panjang, replay |
| `diskless` | object storage lewat agen diskless | ~100 ms (jendela batching) | tanpa disk broker, request objek dipakai bersama | volume tinggi, hemat biaya, broker stateless |

### Diskless lebih rinci

Agen diskless mengumpulkan append dari **semua** partisi diskless selama `diskless_linger_ms` (default 100 ms) atau hingga `diskless_max_file_bytes` (8 MiB). Lalu agen menulisnya sebagai **satu** objek (`diskless/<ms>-<node>-<seq>.bpd`). Offset baru ditetapkan, dan producer baru menerima ack, setelah object store mengonfirmasi penulisan. Partisi menyimpan **extent** kecil (rentang offset → objek, rentang byte). Pembacaan mengambil rentang byte tersebut (disimpan di cache objek LRU) dan menambal base offset saat dikirim. Satu PUT mencakup banyak partisi, jadi jumlah request objek tetap rendah walaupun jumlah partisi bertambah.

### Migrasi online

Topic bisa berpindah mode selagi producer dan consumer berjalan:

```bash
bpctl topic migrate orders --to diskless
```

Partisi mencatat **switch offset**. Semua data di bawahnya tetap di tempat ia ditulis, dan append baru masuk ke mode baru. Offset tidak pernah berubah, dan consumer tidak merasakan perbedaan. Kembali dari diskless ke local bekerja dengan cara yang sama.

## Log compaction

Topic dengan `cleanup.policy=compact` menyimpan **record terbaru untuk setiap key**, bukan menghapus data berdasarkan umur. Inilah yang dibutuhkan changelog Kafka Streams, tabel CDC, dan topic "state terkini". Dengan `compact,delete`, keduanya berlaku.

- **Yang di-compact:** segment lokal yang sudah tertutup. Segment aktif tidak pernah disentuh, dan record yang lebih muda dari `min.compaction.lag.ms` dibiarkan.
- **Kapan:** di latar belakang, begitu minimal `min.cleanable.dirty.ratio` (default 0,5) dari byte segment tertutup sebuah partisi belum dibersihkan. Bisa juga dijalankan langsung dengan `bpctl topic compact <topic>` atau `POST /v1/topics/{t}/compact`.
- **Caranya:** shard menyerahkan job ke thread blocking. Thread itu membuat peta dari fingerprint 128-bit setiap key ke offset terbarunya, lalu menulis ulang setiap segment ke *generasi* file baru (`<base>-<n>.log`) yang hanya berisi record tersebut. Batch tetap memakai offset dan kompresi aslinya. Shard baru memasang file baru jika segment tidak berubah selama proses. File lama dihapus setelah 60 detik, agar pembacaan yang sudah direncanakan tetap selesai.
- **Penghapusan:** record dengan value null (*tombstone*) menghapus key-nya. Tombstone itu sendiri disimpan selama `delete.retention.ms` (default 24 jam), agar consumer sempat melihat penghapusannya.
- **Offset tidak pernah berubah, begitu juga log start.** Consumer cukup melewati celahnya, seperti di Kafka.
- **Aturan:** compaction hanya untuk topic `local` (seperti tiered storage di Kafka). Setiap record wajib punya key; record tanpa key ditolak dengan `INVALID_RECORD`.

## Consumer group

- **Group Kafka:** protokol klasik (JoinGroup, SyncGroup, Heartbeat, OffsetCommit, OffsetFetch). Assignment dilakukan klien seperti biasa. Offset yang di-commit disimpan di log append-only per node.
- **Group HTTP:** untuk klien yang hanya berbicara HTTP. Broker sendiri yang membagi partisi (`POST /v1/groups/{g}/members`), dan member melakukan poll dan commit lewat REST. SDK Python, TypeScript, Go, dan Java memakai cara ini.
- **Share group** (gaya KIP-932): banyak worker membaca partisi yang sama seperti antrean. Setiap record dikunci untuk satu worker, lalu *accept*, *release* (dikirim ulang), atau *reject*. Setelah `max_attempts` pengiriman, atau saat di-reject, record masuk ke dead-letter topic.

## BigPipe Flow

Flow adalah pipeline topic-ke-topic terkelola yang berjalan di dalam broker, jadi Anda tidak perlu stream processor terpisah untuk routing dan pembersihan data. Flow membaca topic input dan menjalankan processor `filter`, `mapping`, dan `route` (ditulis dalam bpql). Hasilnya ditulis ke topic output, atau ke topic yang dihitung per record. Progres di-commit sebagai group `__flow:<nama>`, jadi flow yang di-restart melanjutkan dari posisi terakhir (at-least-once). Lihat [bpql, Flow, dan share group](bpql-and-flows.md).

## Sisi .NET

| Proyek | Isi |
|---|---|
| `BigPipe.Client` | klien Kafka managed penuh (System.IO.Pipelines, buffer pool, CRC32C hardware, producer idempoten, consumer group), ditambah klien admin, HTTP, share group, dan ekstensi DI |
| `BigPipe.Streams` | DSL ala Kafka Streams: KStream/KTable, window tumbling/hopping/sliding/session, state store dengan topic changelog, interactive query |
| `BigPipe.Analytics` (+ `.Scripting`, `.ML`, `.Torch`, `.Gravicode`) | analitik realtime: window, statistik, sketch, deteksi anomali, skrip runtime, ML.NET, TorchSharp, GravicodeScience, HF.Net, MediaPipe.NET |
| `BigPipe.SchemaRegistry` | schema registry kompatibel Confluent (JSON Schema, Avro, Protobuf) yang disimpan di `__bp_schemas` |
| `BigPipe.AdminApi` | gateway admin API ber-autentikasi dengan role Viewer/Operator/Admin dan jejak audit di `__bp_audit` |
| `BigPipe.Console` | konsol web Blazor Server |

## Arah pengembangan

Rilis ini adalah data plane satu node. Replikasi (Raft), transaksi, compaction, io_uring, dan item lain dari dokumen desain dilacak di [PLAN.md](../../PLAN.md).

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
