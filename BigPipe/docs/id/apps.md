# Console, Gallery, dan aplikasi contoh

[English](../en/apps.md) · [Bahasa Indonesia](apps.md)

## BigPipe Console (web)

`control-plane/src/BigPipe.Console` adalah aplikasi Blazor Server. Console berkomunikasi dengan admin API (9644), gateway HTTP (8082), dan Schema Registry (8081). Tampilannya terinspirasi dari skema instalasi proses: data digambarkan sebagai material yang mengalir lewat pipa menuju tangki.

```bash
dotnet run -c Release --project control-plane/src/BigPipe.Console
# http://localhost:8080  (pengaturan: BigPipe:AdminUrl, BigPipe:HttpUrl, BigPipe:RegistryUrl, BigPipe:ApiKey)
```

| Halaman | Yang bisa dilakukan |
|---|---|
| **Ikhtisar** | skema langsung dengan producer di kiri, topic sebagai pipa (materialnya menunjukkan mode penyimpanan, kecepatan alirannya mengikuti laju tulis), dan consumer group di kanan; record masuk, data masuk, data keluar, dan lag consumer |
| **Topic** | daftar dengan mode penyimpanan, partisi, jumlah record, dan byte (lokal / object storage / diskless); membuat topic |
| **Detail topic** | partisi dan offset, konfigurasi efektif (bisa diedit langsung), **migrasi online** antara local, tiered, dan diskless, penjelajah pesan dengan filter bpql, live tail, dan form producer |
| **Consumer group** | member, assignment, offset ter-commit dan lag per partisi, reset offset (earliest / latest / offset tertentu) |
| **Flow** | deploy, pause, resume, dan hapus pipeline BigPipe Flow dari editor YAML gaya Kubernetes (format yang sama dengan `bpctl flow deploy`), dengan statistik langsung (masuk / keluar / tersaring / error) |
| **Skema** | subject, versi, level kompatibilitas, dan teks skema dari registry |
| **Tentang** | versi, listener, dan kredit |

Tombol di header mengganti bahasa (English / Bahasa Indonesia) dan tema (terang / gelap). Kedua pilihan disimpan di cookie.

| | |
|---|---|
| ![Ikhtisar](../images/console-overview-id.png) | ![Ikhtisar (gelap)](../images/console-overview-dark.png) |
| ![Topic](../images/console-topics.png) | ![Konfigurasi dan migrasi topic](../images/console-topic-config.png) |
| ![Penjelajah pesan](../images/console-topic-messages.png) | ![Consumer group](../images/console-groups.png) |
| ![Flow](../images/console-flows.png) | ![Skema](../images/console-schemas.png) |

## BigPipe Gallery (desktop, Avalonia)

`samples/BigPipe.Gallery` adalah aplikasi desktop lintas platform (Windows, macOS, Linux) yang dibangun dengan Avalonia UI. Isinya **14 kasus penggunaan yang bisa dijalankan**. Setiap kasus disertai contoh kode C#, dan sebagian besar juga dalam Python, TypeScript, Go, Java, `bpctl`, atau `curl`. Tekan **Run against BigPipe** dan kasus itu dijalankan langsung ke cluster Anda, dengan output dan metrik tampil di samping kode.

```bash
dotnet run -c Release --project samples/BigPipe.Gallery
dotnet run -c Release --project samples/BigPipe.Gallery -- --run-all        # uji cepat semua kasus tanpa UI
```

Pengaturan: variabel lingkungan `BIGPIPE_BOOTSTRAP`, `BIGPIPE_HTTP`, `BIGPIPE_ADMIN`, dan `BIGPIPE_REGISTRY` (default: localhost dengan port standar).

| Kategori | Kasus |
|---|---|
| Getting started | Produce and consume |
| Storage | Local, tiered and diskless · Online storage migration |
| Consumption | Consumer groups and lag · Share groups: work queues with a DLQ · Server-side filters over SSE |
| Processing | BigPipe Flow pipelines · Schema Registry and evolution · Windowed aggregation (BigPipe.Streams) |
| Realtime analytics & AI | Anomaly detection with ML.NET · Deep learning detector (TorchSharp) · Dynamic scripting: C#, Python, JavaScript · Review sentiment (MediaPipe.NET · GraviText) · Customer segments (GraviLearn · GraviFrame) |

| | |
|---|---|
| ![Hello](../images/gallery-hello.png) | ![Hello (gelap)](../images/gallery-hello-dark.png) |
| ![Share group](../images/gallery-share-groups.png) | ![Skrip](../images/gallery-scripting.png) |
| ![Deteksi anomali](../images/gallery-anomaly.png) | ![Sentimen](../images/gallery-sentiment-dark.png) |

## BigPipe Demo (pembangkit trafik)

`samples/BigPipe.Demo` menjaga beban kerja yang realistis tetap berjalan, jadi Console selalu punya data untuk ditampilkan. Demo menjalankan pesanan dan pembayaran di topic local, clickstream dan telemetri IoT di topic diskless, log audit di penyimpanan tiered, sebuah flow, dan dua consumer group (satu mengikuti, satu tertinggal).

```bash
dotnet run -c Release --project samples/BigPipe.Demo
```

## Notebook

Lihat [Stream processing dan analitik realtime → Notebook](streams-and-analytics.md#notebook).

## Membuat ulang screenshot

```bash
python tools/screenshots/capture.py                       # halaman Console (Playwright + Chrome terpasang)
dotnet run --project samples/BigPipe.Gallery -- --screenshot docs/images          # Gallery, terang
dotnet run --project samples/BigPipe.Gallery -- --screenshot docs/images --dark   # Gallery, gelap
```

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
