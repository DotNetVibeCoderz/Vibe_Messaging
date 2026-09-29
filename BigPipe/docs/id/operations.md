# Operasional: konfigurasi, deployment, benchmark, troubleshooting

[English](../en/operations.md) · [Bahasa Indonesia](operations.md)

## Konfigurasi node

Urutan penerapan pengaturan, di mana yang belakangan menang: nilai bawaan, lalu file YAML (`--config` / `BIGPIPE_CONFIG`), lalu variabel lingkungan, lalu flag command line. `deploy/config/bigpipe.yaml` berisi semua key beserta nilai default-nya.

| Key YAML | Flag / env | Default | Arti |
|---|---|---|---|
| `data_dir` | `--data-dir` / `BIGPIPE_DATA_DIR` | `./data` | segment, offset, metadata, object store lokal |
| `kafka_addr` | `--kafka-addr` / `BIGPIPE_KAFKA_ADDR` | `0.0.0.0:9092` | listener Kafka (dual-stack IPv4/IPv6) |
| `advertised_host`, `advertised_port` | `--advertised-host` / `--advertised-port` | `localhost`, `9092` | alamat yang dikirim ke klien di Metadata |
| `http_addr` | `--http-addr` / `BIGPIPE_HTTP_ADDR` | `0.0.0.0:8082` | gateway data HTTP |
| `admin_addr` | `--admin-addr` / `BIGPIPE_ADMIN_ADDR` | `0.0.0.0:9644` | admin API |
| `metrics_addr` | `--metrics-addr` / `BIGPIPE_METRICS_ADDR` | `0.0.0.0:9645` | `/metrics` |
| `shards` | `--shards` / `BIGPIPE_SHARDS` | `0` (= jumlah core CPU) | thread event loop |
| `object_store_url` | `--object-store` / `BIGPIPE_OBJECT_STORE` | `<data_dir>/objects` | tujuan data tiered dan diskless |
| `object_cache_bytes` | | 256 MiB | cache LRU untuk data objek |
| `diskless_linger_ms` / `diskless_max_file_bytes` | | 100 ms / 8 MiB | jendela batching diskless |
| `auto_create_topics`, `default_partitions` | | `true`, `3` | topic dibuat otomatis saat produce pertama |
| `default_storage_mode` | `--default-storage-mode` | `local` | mode untuk topic baru |
| `admin_api_key` | `--admin-api-key` / `BIGPIPE_ADMIN_API_KEY` | tidak ada | jika diatur, admin API membutuhkan `Authorization: Bearer <key>` |
| `max_request_bytes` | | 100 MiB | ukuran request terbesar yang diterima |
| | `BIGPIPE_LOG` | `info` | filter log, mis. `info,bp_broker=debug` |
| | `BIGPIPE_LOG_FORMAT` | `text` | `json` untuk log terstruktur |

## Konfigurasi topic

Diatur saat membuat topic (`bpctl topic create t -c key=value`, `"config":{}` di admin API, atau CreateTopics dari klien Kafka mana pun). Ubah kemudian dengan `bpctl topic config t key=value` (`key=` mengembalikan ke default) atau `PATCH /v1/topics/{t}/config`. `DescribeConfigs` Kafka menampilkan nilai efektifnya.

| Key | Default | Arti |
|---|---|---|
| `bigpipe.storage.mode` | `local` | `local`, `tiered`, atau `diskless`; mengubahnya berarti migrasi online |
| `retention.ms` / `retention.bytes` | 7 hari / `-1` | segment yang lebih tua atau melebihi ukuran dihapus utuh (`-1` = tanpa batas) |
| `segment.bytes` / `segment.ms` | 256 MiB / 7 hari | kapan segment aktif diganti |
| `local.retention.ms` | 1 jam | tiered: berapa lama segment yang sudah diunggah tetap ada di disk lokal |
| `bigpipe.durability` | `flush` | `flush`: ack setelah masuk page cache OS (default Kafka); `fsync`: sinkron sebelum ack |
| `flush.ms` | `-1` | fsync berkala untuk segment aktif (`-1` = diserahkan ke OS) |
| `max.message.bytes` | 8 MiB | batch record terbesar |
| `bigpipe.cache.bytes` | 4 MiB | cache baca per partisi untuk data terbaru |
| `bigpipe.schema.validation` | `none` | `strict`: tolak record yang value-nya bukan JSON valid; `lenient` diterima tetapi belum melakukan pengecekan |
| `cleanup.policy` | `delete` | disimpan untuk kompatibilitas; compaction ada di roadmap |

## Object storage

| URL | Kredensial (variabel lingkungan) |
|---|---|
| `s3://bucket/prefix` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`; untuk MinIO atau layanan kompatibel S3 lain juga `AWS_ENDPOINT=http://host:9000`, `AWS_ALLOW_HTTP=true`, dan bila perlu `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false` |
| `gs://bucket/prefix` | `GOOGLE_SERVICE_ACCOUNT` (path ke key JSON) atau workload identity |
| `az://container/prefix` | `AZURE_STORAGE_ACCOUNT_NAME` dan `AZURE_STORAGE_ACCOUNT_KEY` (atau variabel SAS / managed identity) |
| direktori atau `file:///path` | tidak perlu (pengembangan, atau mount NFS bersama) |
| `memory://` | tidak perlu (hanya untuk tes) |

Tata letak di dalam prefix: `diskless/<ms>-<node>-<seq>.bpd` (file diskless bersama) dan `tiered/<topic>/<partition>/<base-offset>.log` (segment yang diunggah). Suite integrasi `tests/integration/test_features.py` sudah dijalankan terhadap direktori lokal, layanan kompatibel S3, dan Azure Blob Storage.

## Deployment

**Docker Compose** (MinIO + bigpiped + Schema Registry + AdminApi + Console):

```bash
docker compose -f deploy/docker/docker-compose.yml up -d --build
```

Broker mengiklankan dirinya sebagai `bigpiped`. Untuk klien Kafka yang berjalan di host, tambahkan `127.0.0.1 bigpiped` ke file hosts.

**Image container:** `deploy/docker/Dockerfile` (build Rust multi-stage ke image Debian slim yang berjalan sebagai user non-root) dan `deploy/docker/Dockerfile.dotnet` (`--build-arg APP=BigPipe.Console|BigPipe.SchemaRegistry|BigPipe.AdminApi`).

**systemd:** `deploy/systemd/bigpiped.service`, dengan konfigurasi di `/etc/bigpipe/bigpipe.yaml`, user `bigpipe`, dan `LimitNOFILE=1048576`.

**Daftar periksa produksi**

- Atur `advertised_host` ke nama yang bisa di-resolve oleh klien.
- Lindungi admin API dengan `admin_api_key`, dan pasang gateway AdminApi (role dan audit) di depannya untuk pengguna dan otomasi.
- Pakai `tiered` atau `diskless` untuk topic yang butuh retensi panjang. Disk lokal jadi hanya menyimpan data panas.
- Pilih `bigpipe.durability=fsync` untuk topic yang tidak boleh kehilangan beberapa milidetik terakhir saat listrik padam (rilis ini berjalan satu node, jadi belum ada replikasi).
- Ambil metrik `:9645/metrics` dengan Prometheus.

## Benchmark

Angka di bawah ini **diukur dengan `bpctl bench` di satu laptop pengembangan**: Intel Core i7-8650U (4 core / 8 thread, 1,9 GHz), RAM 16 GB, SSD SATA, Windows 11, build release, dengan klien dan broker di mesin yang sama. Angka ini menunjukkan kemampuan satu node kecil. Ini bukan benchmark server yang sudah di-tuning, dan target di `solution-design.md` adalah sasaran yang terpisah.

| Beban kerja | Perintah | Hasil |
|---|---|---|
| Produce record 1 KiB, acks=all, 4 koneksi | `bpctl bench --records 1000000 --record-size 1024 --concurrency 4 --consume` | 447–480 MiB/s (~460–490 ribu record/detik); p50 1,8–2,0 ms, p99 ~12 ms per request berisi 500 record |
| Consume data yang sama | (run yang sama) | 344–402 MiB/s |
| Produce record 100 byte, lz4, batch 2000, 8 koneksi | `bpctl bench --topic small --records 5000000 --record-size 100 --batch 2000 --compression lz4 --concurrency 8` | ~5,0 juta record/detik; p50 0,39 ms |
| Diskless, objek di disk lokal, 8 koneksi | `bpctl bench --topic dl --mode diskless --records 200000 --concurrency 8 --consume` | produce p50 ~123 ms (jendela batching 100 ms + penulisan), consume 275 MiB/s |

Untuk mengulanginya, jalankan `bigpiped --mode dev` lalu perintah di atas. `bpctl bench --help` menampilkan semua opsi.

## Pemantauan

- `GET :9645/metrics`: counter dan histogram OpenMetrics (lihat [API HTTP](http-api.md#metrik-9645)).
- `GET :9644/v1/metrics`: snapshot JSON yang dipakai Console.
- Log: `BIGPIPE_LOG=info,bp_broker=debug`, dan `BIGPIPE_LOG_FORMAT=json` untuk pengirim log.

## Troubleshooting

| Gejala | Penyebab dan solusi |
|---|---|
| Klien Kafka tersambung lalu timeout | Klien menyambung ulang ke `advertised_host:advertised_port` dari Metadata. Pastikan nama itu bisa di-resolve dari klien (Docker: `bigpiped`). |
| `localhost` bisa di browser tetapi tidak bisa untuk klien | Sebagian klien me-resolve `localhost` ke `::1`. `bigpiped` mendengarkan dual-stack di `0.0.0.0` / `[::]`, jadi periksa bahwa firewall tidak memblokir IPv6. |
| Produce diskless butuh ~100 ms | Ini normal: record menunggu jendela batching (`diskless_linger_ms`). Turunkan untuk latensi lebih rendah, atau naikkan agar penulisan objek lebih sedikit. |
| Layanan kompatibel S3 membalas `NoSuchKey` / `NoSuchBucket` | Atur `AWS_ENDPOINT` ke root layanan (tanpa bucket di path) dan `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false` untuk layanan path-style seperti MinIO. |
| Consumer HTTP mendapat `404 unknown member` | Member berhenti poll lebih lama dari `session_timeout_ms` (default 30 detik) sehingga dikeluarkan. Bergabung lagi; SDK melakukannya otomatis. |
| `401` dari admin API | `admin_api_key` sedang aktif. Kirim `Authorization: Bearer <key>` (Console: `BigPipe:ApiKey`, bpctl: `--api-key` / `BIGPIPE_API_KEY`). |
| Crash ONNX / MediaPipe di notebook Windows | Panggil `OnnxRuntimeNative.EnsureLoaded()` lebih dulu (tipe analitik melakukannya otomatis) agar ONNX Runtime dari NuGet yang dipakai, bukan yang ada di System32. |

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
