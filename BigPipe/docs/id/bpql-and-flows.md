# bpql, BigPipe Flow, dan share group

[English](../en/bpql-and-flows.md) · [Bahasa Indonesia](bpql-and-flows.md)

## bpql: bahasa ekspresi

bpql adalah bahasa ekspresi kecil yang dievaluasi broker untuk setiap record. bpql dipakai di empat tempat:

| Tempat | Contoh |
|---|---|
| Filter consumer (fetch HTTP, SSE, group HTTP, `bpctl consume --filter`) | `header("region") == "ID" && this.amount > 1000000` |
| Filter share group | `this.priority == "high"` |
| Processor `filter` dan `route` pada flow | `"payments." + lower(this.city)` |
| Processor `mapping` pada flow | `root = this` / `root.card = deleted()` |

Ekspresi di-parse sekali lalu dievaluasi terhadap setiap record. Pencarian header dan key tidak melakukan alokasi. Value baru di-parse sebagai JSON jika `this` dipakai, jadi filter berbasis header tetap sangat ringan.

### Sintaks

| Elemen | Contoh |
|---|---|
| Literal | `42`, `3.14`, `"teks"`, `'teks'`, `true`, `false`, `null`, `["a","b"]` |
| Field dari value JSON | `this.amount`, `this.customer.city`, `this.items[0].sku` |
| Perbandingan | `==`, `!=`, `<`, `<=`, `>`, `>=` |
| Logika | `&&` / `and`, `\|\|` / `or`, `!` / `not` |
| Aritmetika / penggabungan | `+`, `-`, `*`, `/`, `%` (`+` menggabungkan string) |
| Keanggotaan | `this.city in ["Jakarta", "Bandung"]` |

### Fungsi

| Fungsi | Hasil |
|---|---|
| `header(nama)` | nilai header sebagai string, atau `null` |
| `key()` · `value()` / `content()` | key dan value sebagai string |
| `json()` · `json(path)` | value hasil parse (sama dengan `this`) atau field bersarang |
| `topic()` · `partition()` · `offset()` · `timestamp()` · `now()` | metadata record; timestamp dalam ms |
| `lower(s)` · `upper(s)` · `len(x)` / `length(x)` | fungsi string (`len` juga untuk array) |
| `contains(s, sub)` · `starts_with(s, p)` / `startsWith` · `ends_with(s, p)` / `endsWith` | pengecekan string |
| `number(x)` · `string(x)` · `exists(x)` · `coalesce(a, b, ...)` | konversi dan penanganan null |
| `abs(x)` · `round(x[, digit])` · `floor(x)` · `ceil(x)` | matematika |
| `uuid()` | UUID acak (untuk mapping) |
| `deleted()` | dalam mapping, menghapus field yang sedang di-assign |

Field yang tidak ada bernilai `null`. Membandingkan `null` dengan angka menghasilkan `false`, jadi `this.amount > 5` cukup melewati record tanpa `amount` tanpa error.

### Mapping

Mapping adalah daftar assignment yang dipisah baris baru atau `;`. Mapping membangun value JSON baru bernama `root`:

```
root = this                          # mulai dari input
root.total = this.qty * this.price
root.card = deleted()                # buang field sensitif
root.meta.source = topic()           # objek bersarang dibuat otomatis
root.id = coalesce(this.id, uuid())
```

Untuk mencoba ekspresi tanpa deploy apa pun, pakai `POST /v1/expr/validate`:

```bash
curl -X POST localhost:9644/v1/expr/validate -H 'content-type: application/json' \
     -d '{"expr":"this.amount > 1000 && header(\"region\") == \"ID\"","sample":{"amount":5000}}'
```

## BigPipe Flow

**Flow** adalah pipeline terkelola yang berjalan di dalam broker: topic input → processor → topic output. Pakai flow untuk routing, pembersihan, masking, dan enrichment yang biasanya butuh layanan terpisah.

```yaml
apiVersion: bigpipe.io/v1
kind: Flow
metadata:
  name: high-value
spec:
  input:  { bigpipe: { topic: payments } }
  start_from: earliest          # atau latest (hanya untuk jalan pertama)
  dlq: payments.flow-errors     # opsional: record yang gagal diproses
  pipeline:
    processors:
      - filter: 'this.amount > 5000000'
      - mapping: |
          root = this
          root.card = deleted()
          root.flagged = true
      - route: '"payments.high." + lower(this.city)'   # opsional: topic per record
  output: { bigpipe: { topic: payments.high } }
```

```bash
bpctl flow deploy high-value.yaml
bpctl flow list                 # record masuk / keluar / tersaring / error, lag
bpctl flow pause high-value
bpctl flow resume high-value
bpctl flow delete high-value
```

Spesifikasi yang sama juga diterima dalam JSON:

```json
{"name":"high-value","input":"payments","output":"payments.high",
 "processors":[{"filter":"this.amount > 5000000"},{"mapping":"root = this\nroot.card = deleted()"}]}
```

Cara kerjanya:

- Processor dijalankan berurutan. `filter` membuang record, `mapping` menulis ulang value, dan `route` memilih topic output (topic dibuat otomatis).
- Key, header, dan timestamp tetap seperti aslinya.
- Progres di-commit sebagai consumer group `__flow:<nama>`. Setelah restart atau pause, flow melanjutkan dari posisi terakhir (at-least-once). Lag-nya bisa dipantau seperti group lain.
- Definisi flow disimpan di `<data_dir>/meta/flows.json` dan dijalankan kembali bersama broker.

![Flow di Console](../images/console-flows.png)

## Share group

Share group mengubah topic menjadi **antrean kerja**. Berbeda dengan consumer group, berapa pun worker bisa membaca partisi yang sama. Setiap record **dikunci** untuk worker yang menerimanya sampai worker memberi ack:

| Ack | Efek |
|---|---|
| `accept` | selesai; tidak dikirim lagi |
| `release` | langsung dilepas dan dikirim ulang ke worker mana pun |
| `reject` | dikirim ke dead-letter topic |
| *(tanpa ack sebelum `lock_ms`)* | kunci habis dan record dikirim ulang |

Setiap pengiriman membawa `delivery_count`. Jika nilainya melewati `max_attempts`, record masuk ke `dlq_topic` dengan header diagnostik (`bigpipe.dlq.reason`, `bigpipe.dlq.topic`, `bigpipe.dlq.partition`, `bigpipe.dlq.offset`, `bigpipe.dlq.group`, dan `bigpipe.dlq.attempts`).

```python
async with ShareConsumer(group="email-workers", topics=["email-jobs"],
                         max_attempts=5, dlq_topic="email-jobs.dlq") as worker:
    async for job in worker:
        try:
            await send_email(job.json()); job.accept()
        except TemporaryError:
            job.release()
        except Exception:
            job.reject()
```

Share group tersedia lewat HTTP (`/v1/share/{group}/poll` dan `/ack`) dan di semua SDK. Kasus Gallery *Share groups: work queues with a DLQ* menunjukkannya secara langsung.

![Share group di Gallery](../images/gallery-share-groups.png)

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
