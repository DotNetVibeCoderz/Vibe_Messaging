# Memulai

[English](../en/getting-started.md) · [Bahasa Indonesia](getting-started.md)

Panduan ini menjalankan node BigPipe di mesin Anda, lalu mencoba produce/consume pertama lewat command line, klien Kafka, dan gateway HTTP.

## 1. Prasyarat

| Tool | Versi | Dibutuhkan untuk |
|---|---|---|
| Rust | 1.85+ (stable) | broker `bigpiped` dan CLI `bpctl` |
| .NET SDK | 10.0 | SDK .NET, Streams, Analytics, Console, Schema Registry, Gallery, notebook |
| Python / Node.js / Go / Java | 3.9+ / 18+ / 1.22+ / 17+ | hanya jika memakai SDK tersebut |
| Docker (opsional) | 24+ | `deploy/docker/docker-compose.yml` |

## 2. Build dan jalankan broker

```bash
cargo build --release -p bigpiped -p bpctl
./target/release/bigpiped --mode dev --data-dir ./data
```

`--mode dev` menjalankan semua role dalam satu proses dengan konfigurasi bawaan. Saat mulai, broker mencetak banner berisi daftar listener:

| Port | Listener |
|---|---|
| 9092 | protokol wire Kafka (klien Kafka apa pun bisa dipakai) |
| 8082 | gateway HTTP: produce/fetch REST, streaming SSE, consumer group HTTP, share group |
| 9644 | Admin API (Console, `bpctl`, klien admin SDK) |
| 9645 | `/metrics` dalam format OpenMetrics |

Setiap opsi bisa diatur lewat flag, variabel lingkungan (`BIGPIPE_*`), atau file YAML (`--config deploy/config/bigpipe.yaml`). Lihat [Operasional](operations.md).

## 3. Record pertama dengan `bpctl`

```bash
bpctl topic create orders --partitions 6              # penyimpanan lokal
bpctl topic create clicks -p 12 --mode diskless       # langsung ke object storage
echo '{"id":1,"amount":150000}' | bpctl produce orders --key order-1 -H source=cli
bpctl consume orders --from-beginning -n 10
bpctl consume orders --from-beginning --filter 'this.amount > 100000'
bpctl topic migrate orders --to tiered                # online; offset tidak berubah
bpctl topic create users -c cleanup.policy=compact    # simpan record terbaru per key
bpctl topic compact users                             # compact sekarang (juga berjalan di latar belakang)
bpctl group list
```

## 4. Dari klien Kafka apa pun

BigPipe berbicara protokol Kafka, jadi tool yang sudah ada tetap bisa dipakai tanpa perubahan:

```python
# pip install confluent-kafka
from confluent_kafka import Producer, Consumer
p = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True, "compression.type": "zstd"})
p.produce("orders", key="order-2", value='{"id":2,"amount":99000}'); p.flush()

c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "billing", "auto.offset.reset": "earliest"})
c.subscribe(["orders"])
print(c.poll(5).value())
```

Suite kompatibilitas `tests/compat/python_compat.py` menguji librdkafka (confluent-kafka) dan kafka-python terhadap node yang sedang berjalan.

## 5. Lewat HTTP

```bash
curl -X POST localhost:8082/v1/topics/orders/records \
     -H 'content-type: application/json' \
     -d '{"records":[{"key":"order-3","value":{"id":3,"amount":250000},"headers":{"region":"ID"}}]}'

curl 'localhost:8082/v1/topics/orders/partitions/0/records?offset=earliest&limit=10'

# Server-Sent Events; filter dijalankan broker, jadi hanya record yang cocok yang dikirim lewat jaringan
curl -N -G localhost:8082/v1/topics/orders/stream \
     --data-urlencode from=earliest --data-urlencode 'filter=header("region") == "ID"'
```

## 6. Dengan SDK

```csharp
// dotnet add package BigPipe.Client
await using var producer = new ProducerBuilder<string, string>().WithBootstrap("localhost:9092").Build();
var meta = await producer.SendAsync("orders", "order-4", """{"id":4,"amount":75000}""");
```

Contoh yang sama untuk Python, TypeScript, Go, dan Java ada di [SDK](sdks.md).

## 7. Aplikasi dengan UI

```bash
dotnet run -c Release --project control-plane/src/BigPipe.Console         # http://localhost:8080
dotnet run -c Release --project control-plane/src/BigPipe.SchemaRegistry  # http://localhost:8081
dotnet run -c Release --project samples/BigPipe.Gallery                   # aplikasi desktop
dotnet run -c Release --project samples/BigPipe.Demo                      # membangkitkan trafik
```

![Ikhtisar BigPipe Console](../images/console-overview-id.png)

## 8. Semua di Docker

```bash
docker compose -f deploy/docker/docker-compose.yml up -d --build
```

Perintah ini menjalankan MinIO (S3), `bigpiped` dengan MinIO sebagai object store, Schema Registry (8081), gateway AdminApi (9650), dan Console (8080). Broker mengiklankan dirinya sebagai `bigpiped`. Untuk memakai klien Kafka dari host, tambahkan `127.0.0.1 bigpiped` ke file hosts. Port HTTP (8082) dan admin (9644) tetap bisa dipakai tanpa itu.

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
