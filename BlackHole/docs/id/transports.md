# Transport

*BlackHole Messaging — Gravicode Studios, dipimpin oleh Kang Fadhil.*

Empat cara membawa protokol yang sama. Format kabel, pola, dan API-nya identik di semuanya —
memilih salah satunya adalah keputusan penyebaran (deployment), bukan keputusan aplikasi.

| | Jangkauan | Latensi (p50) | Paling cocok untuk | Biayanya |
|---|---|---:|---|---|
| **TCP** | ke mana saja | 60 µs | jaringan apa pun, host apa pun | seluruh tumpukan jaringan |
| **Unix socket** | satu mesin | 29 µs | satu mesin, tanpa terlihat di jaringan | satu path berkas yang harus diurus |
| **Named pipe** | satu mesin* | 37 µs | satu mesin di Windows, keamanan lewat ACL | satu instance server per klien |
| **Shared memory** | satu mesin | **3,2 µs** | latensi terendah, laju pesan tertinggi | satu thread khusus dan memori residen per koneksi |

<sub>*Named pipe bisa melintasi mesin di Windows, tetapi bukan itu keunggulannya.</sub>

## Memilih salah satunya

**Mulailah dengan TCP.** Jalan di mana saja, koneksi menganggur tidak memakan biaya, dan 60 µs sudah
memadai untuk hampir semua kebutuhan.

**Pakai Unix socket atau named pipe** ketika kedua proses ada di satu mesin dan Anda ingin port-nya
sama sekali tidak terlihat dari jaringan. Kira-kira dua kali lebih cepat daripada TCP loopback, dan
endpoint-nya berupa berkas atau nama pipe yang izinnya menjadi kendali aksesnya. Utamakan Unix socket
di Linux dan macOS, named pipe di Windows — meski .NET mengimplementasikan named pipe di atas Unix
socket pada platform lain, jadi keduanya tetap bekerja.

**Pakai shared memory** kalau Anda punya segelintir tautan yang memang butuh latensi mikrodetik —
**18× lebih cepat daripada TCP loopback** dan 265.000 panggilan RPC per detik. Ini pilihan yang salah
untuk banyak koneksi yang kebanyakan menganggur: tiap koneksi mendapat thread khusus dan
`2 × RingCapacity` memori residen, dipakai atau tidak.

---

## TCP

```csharp
await using var server = new BlackHoleServer(5000);                       // semua antarmuka
await using var server = new BlackHoleServer(                             // loopback saja
    new IPEndPoint(IPAddress.Loopback, 5000));
server.Start();

await using var client = await BlackHoleClient.ConnectAsync("127.0.0.1", 5000);
```

Mengikat ke `IPAddress.Any` membuka port itu ke jaringan dan, di Windows, memunculkan permintaan
izin firewall. Ikat ke loopback bila Anda tidak membutuhkan keduanya.

## Unix domain socket

```csharp
var listener = new UnixSocketListenerHost("/tmp/blackhole.sock");
await using var server = new BlackHoleServer(listener);
server.Start();

await using var client = await BlackHoleClient.ConnectUnixAsync("/tmp/blackhole.sock");
```

Soketnya adalah **berkas**, dan itu membawa satu kerumitan yang tidak dimiliki port: `bind` gagal
kalau path-nya sudah ada, dan proses yang mati mendadak meninggalkannya. `UnixSocketListenerHost`
menghapus path basi saat `Start` dan menghapus miliknya sendiri saat dispose, jadi memulai ulang
tidak perlu pembersihan manual.

Didukung di Linux, macOS, dan Windows 10 build 17063 ke atas — periksa dengan
`UnixSocketTransport.IsSupported`. Panjang path dibatasi sekitar 100 bita di Unix, jadi buat pendek;
`UnixSocketTransport.TempPath("nama")` memberi Anda satu di direktori temp.

## Named pipe

```csharp
var listener = new NamedPipeListenerHost("blackhole-gateway");
await using var server = new BlackHoleServer(listener);
server.Start();

await using var client = await BlackHoleClient.ConnectPipeAsync("blackhole-gateway");
```

Satu instance server pipe melayani tepat satu klien, jadi listener selalu menyiapkan satu instance
kosong yang menunggu: begitu satu diambil, satu lagi dibuat di belakangnya. `MaxServerInstances`
adalah batas dari sistem operasi — 255 di Windows.

Pipe dibuka dalam **mode bita**, bukan mode pesan: BlackHole sudah membingkai pesannya sendiri, dan
mode pesan hanya akan menumpuk pembingkaian kedua yang mubazir.

## Shared memory

```csharp
var listener = new SharedMemoryListenerHost("blackhole-ipc", slots: 8);
await using var server = new BlackHoleServer(listener);
server.Start();

await using var client = await BlackHoleClient.ConnectSharedMemoryAsync("blackhole-ipc", slots: 8);
```

Satu segmen bernama memuat satu ring bebas-kunci per arah. Mengirim berarti menyalin ke dalam ring
lalu memajukan kursor; menerima berarti menyalin keluar lalu memajukan kursor. **Kernel tidak
terlibat sama sekali setelah segmennya dipetakan** — di situlah latensinya hilang.

### Kolam segmen

Satu segmen membawa satu koneksi, jadi listener-nya berupa kolam: ia membuat `{name}-0` sampai
`{name}-{slots-1}` di awal, dan klien mengklaim satu yang bebas lewat compare-and-exchange atomik
pada penanda hidupnya. Beberapa klien yang berebut kolam yang sama akan mendarat di segmen berbeda;
sebuah slot didaur ulang begitu koneksinya berakhir.

Jaga kolamnya tetap kecil. Tiap slot memakan `2 × RingCapacity` memori residen — 2 MiB masing-masing
secara bawaan — entah terpakai atau tidak.

### Penyetelan

```csharp
var shared = new SharedMemoryOptions
{
    RingCapacity  = 1024 * 1024,                   // per arah, pangkat dua
    SpinCount     = 50,                            // spin ketat sebelum yield
    YieldDuration = TimeSpan.FromMilliseconds(2),  // jendela yield sebelum tidur
    PollInterval  = TimeSpan.FromMilliseconds(1),  // tidur setelah benar-benar menganggur
};
```

Menunggu terjadi dalam tiga fase, dan nilai bawaannya dipilih supaya **tautan yang aktif tidak pernah
sampai ke fase ketiga**: spin (di bawah satu mikrodetik), yield (setengah mikrodetik per percobaan,
tanpa timer), lalu tidur. Jendela yield adalah setelan yang paling menentukan — alasannya ada di
bawah.

### Tata letaknya

```
+--------------------+   header: magic, versi, kapasitas, penanda hidup
| Header (64 bita)   |
+--------------------+
| Ring A ke B        |   kursor tulis (64) | kursor baca (64) | data (kapasitas)
+--------------------+
| Ring B ke A        |   sama lagi
+--------------------+
```

Satu penulis dan satu pembaca per ring, berkoordinasi lewat dua kursor yang selalu naik. Tidak ada
sisi yang menggerakkan kursor milik sisi lain, jadi tidak perlu kunci maupun compare-and-swap di
jalur data. Kedua kursor menempati baris cache 64 bita yang berbeda: menyatukannya akan membuat
pembaca dan penulis bertarung memperebutkan satu baris cache, yang biayanya jauh melebihi penghematan
64 bita itu.

### Pembersihan

Di Windows, segmen hidup di namespace kernel dan lenyap bersama handle terakhirnya. Di platform lain
ia berupa berkas di `/dev/shm` atau direktori temp, dan penghentian yang tidak bersih meninggalkannya
— panggil `SharedMemoryTransport.Cleanup(nama)` untuk menghapusnya. Listener melakukan ini untuk
slot-slot miliknya sendiri.

---

## Hasil pengukuran di mesin ini

.NET 10.0.11, Windows 11, 8 core logis, kedua sisi dalam satu proses. Ulangi dengan
`dotnet run --project src/BlackHole.Benchmarks -c Release -- --transports`; keluaran mentahnya ada di
[transport-comparison.txt](../transport-comparison.txt).

### Latensi RPC, muatan 30 bita

| Transport | p50 | p90 | p99 | panggilan/detik |
|---|---:|---:|---:|---:|
| TCP loopback | 59,5 µs | 87,3 µs | 119,3 µs | 15.448 |
| Unix socket | 29,0 µs | 53,2 µs | 75,0 µs | 27.985 |
| Named pipe | 37,1 µs | 49,5 µs | 76,7 µs | 25.479 |
| **Shared memory** | **3,2 µs** | **4,1 µs** | **9,0 µs** | **271.848** |

### Laju publish, 100.000 pesan kecil

| Transport | satu per satu | di-batch (256) | percepatan |
|---|---:|---:|---:|
| TCP loopback | 100.638/d | 2.022.273/d | 20,1× |
| Unix socket | 475.849/d | 2.357.145/d | 5,0× |
| Named pipe | 74.617/d | 2.058.854/d | 27,6× |
| **Shared memory** | **2.106.376/d** | 1.752.563/d | **0,8×** |

Shared memory adalah satu-satunya transport yang **tidak terbantu oleh batching** — tanpa batch pun
sudah lebih cepat, karena tidak ada syscall yang perlu diamortisasi. Di transport lain, batching
adalah kemenangan terbesar yang tersedia.

### Streaming, 32 MiB dengan potongan 16 KiB

| Transport | Laju |
|---|---:|
| Unix socket | 1.007 MiB/d |
| TCP loopback | 491 MiB/d |
| Named pipe | 351 MiB/d |
| Shared memory | 139 MiB/d *(920 MiB/d bila diukur tersendiri — lihat di bawah)* |

**Baca baris terakhir itu baik-baik.** Diukur sendirian, streaming shared memory mencapai sekitar
920 MiB/detik. Dalam rangkaian perbandingan — setelah tiga transport lain membuat dan membongkar
koneksi di proses yang sama — angkanya jadi 139 MiB/detik. Selisihnya adalah perebutan sumber daya:
tiap koneksi shared memory memegang satu thread khusus yang berputar, jadi ia jauh lebih peka
terhadap mesin yang sibuk dibanding transport berbasis soket. Di mesin dengan core berlebih ia cepat;
di mesin yang padat, dialah yang lebih dulu melemah.

### CPU untuk satu koneksi menganggur

Keempatnya terukur antara 1,6% dan 3,9% dari satu core di sini — cukup dekat dengan batas derau
pengukuran ini untuk dianggap "kurang lebih sama". Biaya polling shared memory muncul saat sumber
daya diperebutkan, bukan saat menganggur.

---

## Dua bug yang layak diketahui

Keduanya ditemukan justru karena membangun transport-transport ini, dan keduanya jenis bug yang
sangat sulit didiagnosis di produksi.

### `SpinWait.SpinOnce()` ternyata tidur

`SpinWait.SpinOnce()` naik menjadi `Thread.Sleep(1)` setelah kira-kira 20 iterasi. Di Windows itu
berubah jadi satu tick timer penuh. Terukur di sini:

| | |
|---|---:|
| 20 × `SpinOnce()` | 22 µs |
| 50 × `SpinOnce()` | **446.091 µs** |
| 50 × `SpinOnce(sleep1Threshold: -1)` | 22 µs |

Satu nilai bawaan itu membuat RPC shared memory memakan **32 milidetik** per bolak-balik — 500×
*lebih lambat* daripada TCP loopback yang seharusnya ia kalahkan. Memberi `-1` mematikan eskalasi itu
dan menurunkannya ke 3,2 µs: selisih 10.000× hanya dari satu argumen. Kalau Anda menulis gelung spin
sendiri, berikan `-1`.

### Gelung yang berputar tidak boleh jalan di thread pool

Gelung baca yang menunggu dengan berputar akan menahan thread tempat ia berjalan. Kalau itu thread
milik pool, dan kedua ujung koneksi melakukannya, pool-nya kehabisan thread — kelanjutan (continuation)
lalu menunggu laju penambahan thread pool yang lambat, dan semuanya tersendat berdurasi milidetik.
Karena itu transport shared memory menjalankan gelung terimanya di thread khusus
(`TaskCreationOptions.LongRunning`) dan membuat penantiannya sinkron, sehingga tidak ada kelanjutan
yang melompat kembali ke pool. Soket sudah parkir dengan benar dan tidak membutuhkan apa pun dari ini.

## Menulis transport sendiri

`StreamTransport` membungkus `Stream` dupleks apa pun dan memberinya protokol lengkap:

```csharp
var transport = new StreamTransport(
    streamSaya,
    options,
    remoteEndPoint: "transport-saya://endpoint",
    kind: "custom",
    isAlive: () => streamSaya.CanRead,
    dedicatedReceiveThread: false);   // true kalau pembacaan Anda berputar, bukan parkir

await using var client = BlackHoleClient.Over(transport);
```

Untuk listener, implementasikan `IListenerHost` — tiga anggota dan dua event — lalu serahkan ke
`new BlackHoleServer(listener)`.

---

*Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.*
