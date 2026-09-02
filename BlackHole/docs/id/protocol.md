# Format kabel

*BlackHole Messaging — Gravicode Studios, dipimpin oleh Kang Fadhil.*

Setiap bita yang dikirim BlackHole ditulis dan diurai oleh tepat satu tipe,
[`FrameCodec`](../../src/BlackHole/Protocol/FrameCodec.cs). Kedua ujung setiap koneksi melewatinya,
dan itulah yang membuat sisi client dan sisi server mustahil menyimpang satu sama lain.

## Satu frame

```
 0        4      5      6          8                    16
 +--------+------+------+----------+--------------------+---------+---------+
 | Length | Type | Flags| HdrLen   |  CorrelationId     | Header  | Payload |
 | int32  | u8   | u8   | uint16   |  int64             | UTF-8   |  bita   |
 +--------+------+------+----------+--------------------+---------+---------+
 \__ 4 __/\____________ 12 bita header tetap __________/
          \____________ Length menghitung semuanya mulai dari sini __________/
```

Semuanya little-endian.

| Kolom | Ukuran | Arti |
|---|---|---|
| `Length` | 4 | Jumlah bita sisa frame. Tidak menghitung dirinya sendiri. |
| `Type` | 1 | [`MessageType`](../../src/BlackHole/Protocol/MessageType.cs). Nilai angkanya adalah bagian protokol, jangan pernah dipakai ulang. |
| `Flags` | 1 | [`MessageFlags`](../../src/BlackHole/Protocol/MessageType.cs): `Error`, `Compressed` (cadangan), `NoReply`. |
| `HdrLen` | 2 | Panjang `Header` dalam UTF-8. Membatasi header sampai 65.535 bita. |
| `CorrelationId` | 8 | Menjodohkan balasan dengan permintaannya. Dipakai ulang sebagai indeks potongan dan jumlah isi batch. |
| `Header` | *HdrLen* | Kunci perutean, UTF-8. |
| `Payload` | sisanya | Isi pesan. |

Awalan totalnya 16 bita, sehingga payload rata di kelipatan 8 bita di dalam frame dan
**8 bita lebih hemat per pesan** dibanding header berbasis GUID milik v2.

### Alasan tiap pilihan

**Awalan panjang 4 bita, hanya menghitung sisanya.** Pembaca butuh 4 bita untuk tahu apakah satu
frame sudah utuh. Dengan tidak menghitung dirinya sendiri, pemeriksaannya jadi
`buffer.Length >= 4 + length` tanpa penyesuaian yang bisa salah.

**Correlation id int64, bukan GUID.** v2 mengirim `Guid` 16 bita per pesan dan memanggil
`Guid.NewGuid()` per permintaan — pengambilan angka acak kriptografis di jalur panas. Pencacah
interlocked hanya 8 bita, tidak butuh entropi, dan unik selama koneksi hidup — satu-satunya cakupan
di mana korelasi punya arti.

**Panjang header 2 bita.** Header berisi nama metode, topik, dan id stream. 64 KiB jauh melampaui
kebutuhan wajar, dan dua bita yang dihemat menjaga header tetap rapi di 12 bita.

**Header berupa teks UTF-8, bukan id.** Topik bersifat hierarkis dan wildcard mencocokkan
segmen-segmennya, jadi teksnya memang harus ada di kabel. Biayanya dikembalikan oleh
[`HeaderCache`](../../src/BlackHole/Protocol/HeaderCache.cs), yang mengubah bita berulang itu kembali
menjadi instance `string` yang sama tanpa alokasi.

## Arti `Header` dan `CorrelationId`

Kedua kolom ini bermakna ganda tergantung `Type`. Ini satu-satunya tempat protokol meminta Anda
memegang dua gagasan sekaligus, jadi layak dinyatakan terang-terangan:

| Type | `Header` | `CorrelationId` |
|---|---|---|
| `RpcRequest` / `RpcResponse` | nama metode | menjodohkan permintaan dan balasan |
| `Publish` / `Subscribe` / `Unsubscribe` | topik atau filter | tidak dipakai |
| `StreamStart` | id stream | tidak dipakai |
| `StreamChunk` | id stream | indeks potongan, mulai dari nol |
| `StreamEnd` | id stream | jumlah potongan |
| `StreamAbort` | id stream | tidak dipakai; payload berisi alasan (UTF-8) |
| `Batch` | kosong | jumlah pesan di dalamnya |
| `Ping` / `Pong` | kosong | id probe |

## Tipe pesan

```
0x01 RpcRequest      0x10 StreamStart      0x20 Batch
0x02 RpcResponse     0x11 StreamChunk      0x30 Ping
0x03 Publish         0x12 StreamEnd        0x31 Pong
0x04 Subscribe       0x13 StreamAbort
0x05 Ack
0x06 Unsubscribe
```

Celah angkanya disengaja: tipe yang sekerabat berbagi nibble atas, jadi tipe streaming baru mendapat
`0x14`, bukan angka mana pun yang kebetulan kosong.

`Ping` dan `Pong` ditangani di dalam transport. Keduanya tidak pernah dirutekan, sehingga handler
aplikasi tidak pernah melihat lalu lintas keepalive.

## Amplop batch

Payload sebuah `Batch` adalah **rangkaian frame BlackHole yang utuh** — format yang persis sama
seperti di atas, bersarang satu tingkat.

```
Frame batch
+--------+------+-----+--------+------+  payload:
| Length | 0x20 | ... | HdrLen | Corr |  +-------------+-------------+-----
+--------+------+-----+--------+------+  | frame dalam | frame dalam | ...
                                          +-------------+-------------+-----
```

Inilah perbedaan terpenting dari v2, yang menciptakan format *kedua* yang lebih pendek tanpa
correlation id. Dua format berarti dua pengurai, dan perubahan pada satu diam-diam merusak yang lain.
Di sini `BatchReceiver` membongkar dengan `FrameCodec.TryRead` yang sama seperti yang dipakai
transport, jadi tidak ada apa pun yang perlu disamakan.

Batch bersarang diabaikan, bukan dibongkar: hanya satu tingkat, karena batch yang memuat dirinya
sendiri adalah gelung yang tinggal menunggu waktu.

## Deskriptor StreamStart

Payload `StreamStart` membawa
[`StreamDescriptor`](../../src/BlackHole/Protocol/StreamDescriptor.cs) supaya penerima tahu apa yang
akan datang sebelum potongan pertama tiba:

```
+---------------------+----------+--------+----------------+-------------+
| TotalLength (8)     | NameLen  | Name   | ContentTypeLen | ContentType |
| int64, -1 = tak tahu| uint16   | UTF-8  | uint16         | UTF-8       |
+---------------------+----------+--------+----------------+-------------+
```

v2 hanya mengirim id stream, sehingga penerima tidak bisa menyiapkan buffer seukuran datanya,
menampilkan progres, atau memutuskan isinya mau dibawa ke mana. Pengkodeannya biner berurutan tetap,
bukan JSON, supaya `StreamStart` tetap kecil dan penguraiannya ringan alokasi.

## Aturan framing

**Membaca.** `FrameCodec.TryRead` mengembalikan `false` sampai satu frame utuh tertampung; ia tidak
pernah memblokir dan tidak pernah mengonsumsi sebagian. Bila payload kebetulan berada di satu segmen
bersambung — kasus yang umum — `ReadOnlyMemory<byte>` yang dikembalikan **menunjuk langsung ke buffer
transport**, jadi jalur terima tidak menyalin apa pun. Bila payload terpotong antar segmen, codec
meminjam array dari `ArrayPool<byte>.Shared` dan menyerahkannya lewat parameter `out` untuk
dikembalikan oleh pemanggil.

**Masa hidup payload.** Payload yang diterima hanya sah sampai dispatch untuk pesan itu selesai.
Handler yang menyimpan bitanya wajib menyalin — `BlackHoleMessage.ToOwned()` melakukan tepat itu.
Ini satu-satunya aturan yang tidak bisa dipaksakan oleh API, dan itulah harga dari jalur terima
tanpa salinan.

**Kegagalan bersifat fatal.** `BlackHoleProtocolException` berarti aliran bita bukan rangkaian frame
yang sah: panjang negatif, header yang tidak muat di frame-nya, atau frame melebihi `MaxFrameLength`.
Begitu framing hilang, tidak ada cara menyelaraskan ulang, jadi koneksi ditutup — bukan ditebak.
`MaxFrameLength` (bawaan 16 MiB) diperiksa *sebelum* buffer apa pun dibuat, sehingga awalan panjang
yang jahat tidak bisa memaksa proses mengalokasikan memori.

## Kompatibilitas

Format v3 **tidak** kompatibel dengan v2 — bentuk header tetapnya berubah dan tata letak batch
berubah. Kedua ujung harus v3. Lihat [Migrasi dari v2](../migration-v2.md).

---

*Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.*
