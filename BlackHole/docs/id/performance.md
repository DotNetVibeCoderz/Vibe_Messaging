# Performa

*BlackHole Messaging — Gravicode Studios, dipimpin oleh Kang Fadhil.*

Setiap klaim di halaman ini didukung angka di [benchmarks](../benchmarks.md).

## Sasarannya

**Nol alokasi dalam keadaan mapan.** Bukan "sedikit" — nol. Menyusun frame, membaca frame,
mencocokkan topik, serta mengemas dan membongkar batch semuanya terukur **0 B** di bawah
`MemoryDiagnoser` milik BenchmarkDotNet. Yang tersisa hanyalah biaya per-*operasi* yang tak bisa
dihindari oleh API mana pun: `TaskCompletionSource` yang dibutuhkan panggilan RPC yang ditunggu, dan
array yang disimpan pemanggil.

Alokasi lebih penting daripada nanodetik di sini. Pustaka perpesanan duduk di bawah semua kode lain
dalam proses; kalau ia menghasilkan sampah gen-0 per pesan, ia memaksakan jeda GC pada kode yang
tidak pernah memintanya.

## Apa yang dibeli tiap keputusan

### Pipelines, bukan `NetworkStream` + `byte[]`

v2 mengalokasikan satu `byte[]` per frame saat menerima, lalu satu `MemoryStream` dan `BinaryReader`
per pesan untuk menguraikannya. `System.IO.Pipelines` yang memiliki buffer dan menyerahkan tampilan
`ReadOnlySequence<byte>`, jadi frame yang belum utuh tidak butuh alokasi dan frame yang utuh diurai
di tempat.

**Hasilnya: pembacaan 105 ns dan 0 B, datar di semua ukuran payload** — karena tidak ada yang
disalin.

### Pesan berupa struct

`BlackHoleMessage` adalah readonly struct 40 bita. Sebagai class, ia jadi satu alokasi gen-0 per
pesan, di kedua arah. Pada 200.000 pesan/detik, itu 400.000 objek sampah murni per detik.

### Payload tanpa salinan

Bila payload berada di satu segmen bersambung — kasus yang umum — `Payload` menunjuk **langsung ke
buffer transport**. Tidak ada yang disalin saat masuk.

Harganya satu aturan: **payload yang diterima hanya sah sampai handler Anda selesai.** Kalau
disimpan, harus disalin. `BlackHoleMessage.ToOwned()` ada persis untuk itu.

Inilah juga sebabnya `ITransport` punya satu `MessageDispatch` yang mengembalikan `ValueTask`, bukan
event multicast. Transport menunggu dispatch, jadi ia tahu kapan buffer boleh dipakai ulang. Event
yang mengembalikan `void` tidak bisa memberi jaminan itu — itulah kenapa v2 terpaksa menyalin.

### Cache header

Setiap pesan masuk harus menguraikan header UTF-8. Lalu lintas nyata memakai kosakata yang sangat
sedikit, jadi cache direct-mapped berkunci bita mentah mengembalikan instance `string` yang sama:

| | Rerata | Gen0 |
|---|---:|---:|
| `Encoding.UTF8.GetString` | 31,6 ns | 0,0042 |
| `HeaderCache.GetString` | **25,9 ns** | **—** |

18% lebih cepat dan — yang lebih penting — tanpa tekanan gen-0. Pada demo: **20.000 kena, 7 meleset.**

### Correlation id int64

v2 mengirim `Guid` 16 bita per pesan dan memanggil `Guid.NewGuid()` per permintaan — pengambilan
angka acak kriptografis di jalur panas. Pencacah `Interlocked.Increment` hanya 8 bita dan gratis.
**Hemat 8 bita per pesan**, di kedua arah.

### Token tertaut yang malas

`CallAsync` butuh batas waktu. Menautkannya ke token milik pemanggil memerlukan
`CancellationTokenSource` kedua plus satu registrasi — jadi tautan itu hanya dibuat ketika pemanggil
benar-benar menyertakan token yang bisa dibatalkan. Sebagian besar panggilan tidak.

### Buffer berkolam

`PooledBufferWriter` membungkus `ArrayPool<byte>.Shared` dan — bagian pentingnya — `Reset()`
mempertahankan array pinjamannya dan hanya memundurkan kursor. Sebuah `BatchSender` berumur panjang
karenanya bebas alokasi setelah amplop pertamanya.

## Memaksimalkannya

### Batch-kan pesan kecil

Kemenangan terbesar yang tersedia bagi Anda:

| | Pesan/detik | Tulisan soket |
|---|---:|---:|
| Kirim satu per satu | 101.214 | 200.000 |
| **Batch 256** | **2.236.709** | **783** |

22× lipat. Di atas 256, kurvanya datar, jadi pilih ukuran batch sesuai anggaran latensi Anda, bukan
yang terbesar.

### Atau gabungkan ledakan yang sudah di tangan

```csharp
foreach (var pesan in ledakan)
    await transport.WriteAsync(pesan);   // tanpa flush
await transport.FlushAsync();            // satu tulisan soket
```

### Salin hanya kalau disimpan

```csharp
// Dibaca di sini saja: tak perlu salinan.
router.On(MessageType.Publish, (_, m) => _gauge.Set(BitConverter.ToDouble(m.Payload.Span)));

// Disimpan: salin.
router.On(MessageType.Publish, (_, m) => _antrian.Enqueue(m.ToOwned()));
```

### Jangan memblokir loop baca

Handler berjalan di loop baca koneksinya, berurutan. Memblokirnya menghentikan lalu lintas masuk
koneksi itu. Untuk pekerjaan lambat, salin payload lalu serahkan ke antrean.

### Bagikan cache header antar banyak koneksi

```csharp
var options = new TransportOptions { SharedHeaderCache = new HeaderCache(2048) };
```

Dengan ratusan koneksi yang menerbitkan dari kosakata topik yang sama, satu cache bersama lebih kecil
sekaligus lebih "panas" daripada satu cache per koneksi. [IoT gateway](iot-gateway.md) melakukan ini.

### Panaskan lebih dulu kalau topiknya sudah diketahui

```csharp
cache.Prime("sensor/tank-3/temperature");   // pesan pertama pun langsung kena
```

### Setel ukuran potongan stream

16 KiB terukur paling cepat (520 MiB/detik). `FlushThreshold` lebih berpengaruh daripada ukuran
potongan: potongan ditulis tanpa dialirkan sampai ambang itu terlewati, jadi potongan kecil tidak
berarti tulisan soket kecil.

### Server GC untuk server

```xml
<ServerGarbageCollection>true</ServerGarbageCollection>
<ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
<TieredPGO>true</TieredPGO>
```

## Memberi makan UI berlaju tinggi

Thread UI tidak sanggup menyerap 100.000 pembaruan per detik, dan memang tidak perlu — layar paling
banter menyegar 60 kali per detik. Pola yang dipakai IoT gateway:

1. Loop baca menulis ke **ring buffer bebas kunci** (`TraceBuffer`) — tanpa kunci, tanpa alokasi.
2. **Timer dispatcher 33 ms** menerbitkan satu pembaruan gabungan per properti.
3. Grafik menggambar langsung dari ring, satu `InvalidateVisual` per frame.

Biaya render jadi datar, entah 4 perangkat pada 2 Hz atau 40 perangkat pada 200 Hz. Mengikat langsung
ke loop baca akan membanjiri dispatcher dan membekukan jendela.

## Mengukur sendiri

```csharp
long sebelum = GC.GetTotalAllocatedBytes(precise: true);
// ... pekerjaan ...
long teralokasi = GC.GetTotalAllocatedBytes(precise: true) - sebelum;
```

Per koneksi, pustakanya sudah menghitungkan untuk Anda:

```csharp
StatisticsSnapshot stats = transport.Statistics.Snapshot();
Console.WriteLine($"{stats.MessagesReceived:N0} diterima, {stats.ReceiveRate:N0}/detik");
Console.WriteLine($"bolak-balik {stats.LastRoundTrip?.TotalMilliseconds:F2} ms");
```

`Snapshot()` bersifat tak-berubah dan aman diserahkan ke thread UI.

---

*Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.*
