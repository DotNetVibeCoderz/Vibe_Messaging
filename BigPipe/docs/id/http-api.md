# Referensi API HTTP dan admin

[English](../en/http-api.md) · [Bahasa Indonesia](http-api.md)

BigPipe menyediakan tiga antarmuka HTTP:

| Port | Antarmuka | Dipakai oleh |
|---|---|---|
| 8082 | **Gateway data**: produce, fetch, SSE, consumer group HTTP, share group | SDK Python/TS/Go/Java, browser, `curl` |
| 9644 | **Admin API**: topic, group, flow, migrasi, penjelajah pesan | Console, `bpctl`, `BigPipeAdminClient` |
| 9650 | **Gateway AdminApi** (.NET, opsional): admin API dengan API key, role, dan log audit | operator dan otomasi |

Semua body berupa JSON. Error berbentuk `{"error":{"code":"not_found","message":"..."}}` dengan status HTTP yang sesuai.

## Format record

Setiap record yang dikembalikan gateway berbentuk seperti ini:

```json
{"topic":"orders","partition":1,"offset":0,"timestamp":1790682099229,
 "key":"k1","key_encoding":"utf8","value":"{\"amount\":5}","value_encoding":"utf8",
 "headers":{"region":"ID"}}
```

Key dan value yang merupakan UTF-8 valid dikembalikan sebagai string (`"utf8"`). Selain itu dikembalikan sebagai base64 (`"base64"`).

---

## Gateway data (8082)

### Produce: `POST /v1/topics/{topic}/records`

```json
{"records":[
  {"key":"order-1","value":{"id":1,"amount":150000},"headers":{"region":"ID"}},
  {"value":"aGVsbG8=","value_encoding":"base64","partition":2,"timestamp":1790000000000}
 ],
 "compression":"zstd"}
```

Respons: `{"topic":"orders","offsets":[{"partition":1,"offset":0}, ...]}`.

- `value` berupa JSON (objek, array, angka) disimpan sebagai teks JSON ringkas. String disimpan apa adanya.
- Tanpa `partition`, record yang punya key memakai partitioner murmur2 Kafka (partisinya sama dengan pilihan klien Java). Record tanpa key disebar ke semua partisi.
- `compression`: `none` (default), `gzip`, `snappy`, `lz4`, atau `zstd`.
- Array langsung (`[{...},{...}]`) juga diterima sebagai body.

### Membaca partisi: `GET /v1/topics/{topic}/partitions/{p}/records`

| Query | Default | Arti |
|---|---|---|
| `offset` | `earliest` | `earliest`, `latest`, atau angka |
| `limit` | 500 | jumlah record maksimum |
| `max_bytes` | 1 MiB | byte payload maksimum |
| `timeout_ms` | 0 | long-poll sampai data datang |
| `filter` | — | ekspresi bpql yang dievaluasi broker |

Respons: `{"records":[...],"next_offset":1,"high_watermark":1}`.

### Stream: `GET /v1/topics/{topic}/stream` (Server-Sent Events)

| Query | Arti |
|---|---|
| `from` | `latest` (default), `earliest`, atau offset |
| `partitions` | daftar dipisah koma, mis. `0,2` |
| `filter` | ekspresi bpql; hanya record yang cocok yang dikirim |
| `group` | bergabung ke consumer group HTTP; partisi dibagi dan offset di-commit otomatis |

Event: `record` (data = JSON record, id = `partition:offset`), `assignment` (jika memakai `group`), dan `error`. Komentar keep-alive dikirim setiap 15 detik.

```js
const es = new EventSource('http://localhost:8082/v1/topics/payments/stream?filter=' +
  encodeURIComponent('this.amount > 1000000'));
es.addEventListener('record', e => console.log(JSON.parse(e.data)));
```

### Consumer group HTTP

Pada group HTTP, broker yang membagi partisi, jadi klien tetap sederhana.

| Panggilan | Body / query | Respons |
|---|---|---|
| `POST /v1/groups/{g}/members` | `{"topics":["orders"],"offset_reset":"earliest","auto_commit":true,"filter":null,"session_timeout_ms":30000}` | `{"group":"g","member_id":"http-..."}` |
| `GET /v1/groups/{g}/members/{m}/records` | `?timeout_ms=1000&max_records=500&max_bytes=` | `{"member_id","generation","assignment":[{"topic","partition"}],"records":[...]}` |
| `POST /v1/groups/{g}/members/{m}/commit` | kosong (commit posisi yang sudah dikirim) atau `{"offsets":[{"topic","partition","offset"}]}` | `{"committed":[...]}` |
| `DELETE /v1/groups/{g}/members/{m}` | — | `{"left":true}` |

Member yang berhenti melakukan poll selama `session_timeout_ms` akan dikeluarkan dan partisinya di-rebalance. Poll setelah itu mengembalikan 404 `unknown member`, dan klien harus bergabung lagi.

### Share group

| Panggilan | Body | Respons |
|---|---|---|
| `POST /v1/share/{g}/poll` | `{"topics":["jobs"],"member":"w1","max_records":100,"lock_ms":30000,"max_attempts":5,"dlq_topic":"jobs.dlq","filter":null,"offset_reset":"earliest","timeout_ms":1000}` | `{"group","member","records":[{...,"delivery_count":1}]}` |
| `POST /v1/share/{g}/ack` | `{"member":"w1","acks":[{"topic":"jobs","partition":0,"offset":7,"action":"accept"}]}` | `{"results":[true]}` |
| `GET /v1/share/{g}` | — | `start_offset`, `in_flight`, dan `acquired` per partisi |

`action` bernilai `accept`, `release` (tersedia lagi), atau `reject` (dikirim ke `dlq_topic`). Record yang kuncinya habis tanpa ack akan dikirim ulang. Setelah `max_attempts` pengiriman, record masuk ke DLQ dengan header `bigpipe.dlq.topic`, `bigpipe.dlq.partition`, `bigpipe.dlq.offset`, `bigpipe.dlq.group`, `bigpipe.dlq.attempts`, dan `bigpipe.dlq.reason`.

---

## Admin API (9644)

Jika `admin_api_key` diatur, setiap panggilan membutuhkan `Authorization: Bearer <key>`.

| Metode dan path | Kegunaan |
|---|---|
| `GET /v1/cluster` | id node, listener, shard, object store, nilai default, jumlah |
| `GET /v1/metrics` | snapshot counter dalam JSON (laju untuk Console) |
| `GET /v1/topics` · `POST /v1/topics` | daftar; buat `{"name","partitions","mode","config":{}}` |
| `GET /v1/topics/{t}` · `DELETE /v1/topics/{t}` | detail (offset per partisi, byte per tier penyimpanan, konfigurasi efektif); hapus |
| `PATCH /v1/topics/{t}/config` | `{"config":{"retention.ms":"86400000","flush.ms":null}}` (`null` mengembalikan ke default) |
| `POST /v1/topics/{t}/partitions` | `{"count":12}`: tambah menjadi 12 partisi |
| `POST /v1/topics/{t}/migrate` | `{"to":"diskless"}`: migrasi penyimpanan online; mengembalikan `switch_offsets` |
| `POST /v1/topics/{t}/compact` | compact topic `cleanup.policy=compact` sekarang; mengembalikan jumlah record dan byte sebelum/sesudah per partisi |
| `GET /v1/topics/{t}/messages` | penjelajah pesan: `?partition=&offset=&limit=&filter=` |
| `GET /v1/groups` · `GET /v1/groups/{g}` · `DELETE /v1/groups/{g}` | daftar, detail (member, offset ter-commit, lag), hapus |
| `POST /v1/groups/{g}/reset` | `{"topic","to":"earliest\|latest\|offset","offset":0,"partitions":[0,1]}`; group harus kosong |
| `GET /v1/share-groups` | share group beserta jumlah record in-flight |
| `GET/POST /v1/flows` · `DELETE /v1/flows/{n}` · `POST /v1/flows/{n}/pause`, `/resume` | BigPipe Flow; `POST` menerima JSON atau YAML (`content-type: application/yaml`) |
| `POST /v1/expr/validate` | `{"expr":"this.amount > 1","kind":"filter\|mapping","sample":{"amount":5}}` → `{"valid":true,"result":true}` |
| `GET /healthz` | liveness |

## Metrik (9645)

`GET /metrics` mengembalikan teks OpenMetrics: `bp_produce_records_total`, `bp_produce_bytes_total`, `bp_fetch_records_total`, `bp_fetch_bytes_total`, `bp_requests_total`, `bp_request_errors_total`, `bp_http_requests_total`, `bp_connections_open`, `bp_connections_total`, `bp_produce_latency_seconds`, `bp_fetch_latency_seconds` (histogram), `bp_diskless_files_total`, `bp_diskless_bytes_total`, `bp_diskless_put_latency_seconds`, `bp_diskless_files_referenced`, `bp_tiered_uploads_total`, `bp_object_cache_bytes`, `bp_group_rebalances_total`, `bp_compactions_total`, `bp_compaction_removed_records_total`, dan `bp_topics`.

## Gateway AdminApi (9650)

`control-plane/src/BigPipe.AdminApi` berada di depan admin API dan menambahkan:

- **API key dengan role:** `Viewer` (GET), `Operator` (buat/ubah), dan `Admin` (hapus, migrasi, flow, reset group). Kirim `Authorization: Bearer <key>` atau `X-Api-Key`. Jika belum ada key yang dikonfigurasi, gateway berjalan dalam mode dev terbuka.
- **Jejak audit:** setiap perubahan dicatat ke topic `__bp_audit` (siapa, apa, status, durasi, IP klien). `GET /v1/audit?limit=100` untuk membacanya.
- `GET /v1/overview` (ringkasan cluster, topic, dan group), `GET /v1/me`, serta OpenAPI di `/openapi/v1.json`.

```json
// appsettings.json
"BigPipe": {
  "AdminUrl": "http://localhost:9644",
  "ApiKeys": [ { "Name": "ci", "Key": "…", "Role": "Operator" } ]
}
```

## Schema Registry (8081)

API REST ini kompatibel dengan Confluent, jadi serializer yang sudah ada tetap bisa dipakai. Route yang didukung: `/subjects`, `/subjects/{s}/versions` (GET/POST), `/subjects/{s}/versions/{v}` and `/schema`, `/subjects/{s}` (lookup dan hapus), `/schemas/ids/{id}`, `/schemas/types`, `/compatibility/subjects/{s}/versions/{v}` dan `/config[/{s}]`. Tipe skema: `AVRO`, `JSON`, dan `PROTOBUF`. Level kompatibilitas: `NONE`, `BACKWARD` (default), `FORWARD`, dan `FULL` (plus varian `_TRANSITIVE`). Skema disimpan di topic `__bp_schemas`, jadi registry sendiri tidak menyimpan state.

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
