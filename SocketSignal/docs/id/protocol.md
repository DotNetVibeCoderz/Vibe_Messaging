# Protokol SocketSignal

*Gravicode Studios, dipimpin oleh Kang Fadhil.*

Semua yang dilakukan SocketSignal melintasi jaringan sebagai **satu objek JSON per pesan teks
WebSocket**. Tidak ada lapisan framing tambahan, tidak ada handshake selain upgrade WebSocket, dan
tidak ada encoding biner — dan itu memang disengaja: browser bisa ikut bicara hanya dengan
`JSON.stringify`, dan Anda bisa membaca hasil tangkapan paket di DevTools tanpa decoder.

Versi protokol: **2**.

## Amplop pesan

Setiap frame adalah sebuah objek. Hanya `type` yang selalu ada.

| Field | Tipe | Arti |
|---|---|---|
| `type` | string | `welcome`, `invoke`, `result`, `ping`, `pong` |
| `id` | string | Id korelasi. Dikembalikan apa adanya pada balasan |
| `method` | string | Nama method (hanya `invoke`) |
| `args` | array | Argumen posisional (hanya `invoke`) |
| `expectReturn` | bool | Apakah sebuah `result` wajib dikirim (hanya `invoke`) |
| `result` | any | Nilai balik (hanya `result`) |
| `error` | string | Pesan kegagalan (hanya `result`) |

Dua aturan menjaga protokol ini tetap bisa berkembang:

- **Field yang tidak dikenal diabaikan.** Sebuah peer boleh menambah field; peer lama melewatinya.
- **Nilai `type` yang tidak dikenal diabaikan, bukan fatal.** Tetap dihitung sebagai tanda hidup.

Implementasi .NET menulis `id` sebagai string berisi angka, tetapi juga menerima angka JSON,
sehingga client buatan sendiri yang mengirim `"id": 42` tetap bekerja.

## Jenis frame

### `welcome` — server ke client, sekali

Dikirim segera setelah koneksi terbentuk, sebelum apa pun yang lain. Sebelum frame ini tiba,
client belum punya id.

```json
{ "type": "welcome", "id": "8f1c...", "protocol": 2, "server": "demo-station" }
```

| Field | Arti |
|---|---|
| `id` | Id koneksi yang diberikan server. Dipakai untuk pengiriman langsung dan keanggotaan grup |
| `protocol` | Versi protokol |
| `server` | Nilai `Name` milik server, untuk log dan diagnostik |

### `invoke` — dua arah

Sebuah pemanggilan method. Bentuknya sama, dari ujung mana pun dikirim.

```json
{ "type": "invoke", "id": "7", "method": "sum", "args": [5, 7], "expectReturn": true }
```

`expectReturn` menentukan apakah penerima wajib membalas dengan `result`:

- `true` — penerima **harus** membalas dengan `result` yang membawa `id` yang sama, baik handler
  berhasil, melempar exception, maupun tidak ada.
- `false` atau tidak ada — kirim dan lupakan. Penerima tidak membalas apa pun, bahkan saat gagal.

`args` bersifat posisional. Nama argumen bukan bagian dari protokol; handler yang ingin argumen
bernama cukup menerima satu argumen berupa objek.

### `result` — balasan atas sebuah `invoke`

```json
{ "type": "result", "id": "7", "result": 12 }
{ "type": "result", "id": "7", "error": "reactor offline" }
```

Tepat satu di antara `result` dan `error` yang bermakna. `error` adalah pesan, bukan stack trace —
tidak ada detail internal proses lawan yang melintasi jaringan.

Penerima yang tidak punya handler untuk method tersebut membalas:

```json
{ "type": "result", "id": "7", "error": "Method 'sum' not found" }
```

Client .NET mengubah `error` yang berakhiran `not found` menjadi `MethodNotFoundException`, dan
selain itu menjadi `SignalInvocationException`.

### `ping` / `pong` — keepalive, dua arah

```json
{ "type": "ping", "id": "12" }
{ "type": "pong", "id": "12" }
```

Sebuah `ping` dibalas `pong` dengan `id` yang sama, dan tidak terjadi apa pun selain itu. Keduanya
dihitung sebagai aktivitas, yang itulah yang diawasi oleh timer idle.

SocketSignal sengaja **tidak** memakai frame ping bawaan WebSocket: browser menanganinya secara
transparan dan JavaScript tidak pernah melihatnya, sehingga client browser tidak akan bisa ikut
serta dalam deteksi keaktifan. Melakukannya di level protokol berarti semua SDK melihat sinyal
yang sama. Karena itu server dan client .NET sama-sama menyetel `KeepAliveInterval =
TimeSpan.Zero` pada socket di bawahnya.

## Satu percakapan utuh

```
client                                  server
  |                                        |
  |------------- upgrade WebSocket ------->|
  |<-- {"type":"welcome","id":"8f1c..."}---|
  |                                        |
  |-- {"type":"invoke","id":"1",           |
  |    "method":"sum","args":[5,7],        |
  |    "expectReturn":true} -------------->|
  |<- {"type":"result","id":"1",           |
  |    "result":12} -----------------------|
  |                                        |
  |-- {"type":"invoke","id":"2",           |   kirim dan lupakan:
  |    "method":"log","args":["hi"]} ----->|   tidak ada balasan
  |                                        |
  |<- {"type":"invoke","id":"a1",          |   server memanggil client
  |    "method":"tick","args":[3],         |
  |    "expectReturn":true} ---------------|
  |-- {"type":"result","id":"a1",          |
  |    "result":true} -------------------->|
  |                                        |
  |<- {"type":"ping","id":"9"} ------------|
  |-- {"type":"pong","id":"9"} ----------->|
```

## Id korelasi

Id hanya perlu unik **dalam satu koneksi dan satu arah**. Masing-masing peer menyimpan tabel
panggilan tertundanya sendiri, sehingga kedua ujung boleh sama-sama memakai `"1"`, `"2"`, `"3"`
tanpa bertabrakan: sebuah `result` dicocokkan dengan tabel milik peer yang mengirim `invoke`
pasangannya.

Implementasi .NET memakai penghitung yang naik terus, diformat langsung ke dalam frame, bukan
GUID. Bedanya 88 byte dan sekitar 118 ns per panggilan — lihat [performance.md](performance.md).

## Grup

Grup **hanya ada di sisi server**. Tidak ada frame "join" dalam protokol: client meminta
dimasukkan dengan memanggil sebuah method yang disediakan aplikasi untuk keperluan itu, lalu
handler-nya memanggil `client.JoinGroup(...)`. Ini disengaja — keanggotaan grup adalah keputusan
otorisasi, dan membiarkan client memasukkan dirinya ke grup mana pun lewat sebuah frame akan
menjadi lubang keamanan.

```csharp
server.Register<string, bool>("join", (client, group) =>
{
    if (!IsAllowed(client, group)) throw new InvalidOperationException("tidak diizinkan");
    client.JoinGroup(group!);
    return ValueTask.FromResult(true);
});
```

Keanggotaan otomatis dilepas ketika koneksi tertutup.

## Menulis client sendiri

Client minimal hanya perlu melakukan empat hal:

1. Buka WebSocket dan baca frame `welcome` untuk mengetahui id-nya.
2. Simpan peta `id -> panggilan tertunda`. Saat `result` tiba, selesaikan atau gagalkan entri yang cocok.
3. Saat `invoke` tiba, cari handler lokal; jika `expectReturn` diset, **selalu** balas — dengan
   `result`, atau dengan `error` ketika handler melempar exception atau tidak ada.
4. Balas `ping` dengan `pong`.

Itu keseluruhan protokolnya. Tiga SDK di [`clients/`](../../clients) masing-masing sekitar 300
baris dan persis melakukan itu. Contoh browser di [README](../../README.md) hanya sepuluh baris.

## Batasan dan framing

Satu frame adalah satu pesan WebSocket utuh; sisi .NET menerima pesan terfragmentasi dan
menyusunnya kembali. `SocketSignalOptions.MaxMessageSize` (bawaan 4 MB) membatasi totalnya — peer
yang melampauinya koneksinya ditutup, bukan dibiarkan menghabiskan memori.

Pesan WebSocket bertipe teks maupun biner sama-sama diterima; SocketSignal sendiri selalu
mengirim teks.
