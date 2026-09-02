# Performa

*Gravicode Studios, dipimpin oleh Kang Fadhil.*

v2 adalah penulisan ulang pada codec dan pompa koneksi. Halaman ini menjelaskan apa yang berubah,
apa hasilnya, dan — karena ini lebih penting daripada daftar kemenangan — apa yang masih
mengalokasi memori.

Semua angka di sini diukur langsung di repositori ini:

```bash
dotnet run -c Release --project src/SocketSignal.Benchmarks -- throughput   # end-to-end + alokasi
dotnet run -c Release --project src/SocketSignal.Benchmarks -- micro        # BenchmarkDotNet
```

Pembandingnya adalah implementasi v1 yang asli, dipulihkan dari riwayat git dan disimpan di
`src/SocketSignal.Benchmarks/Baseline/`. Ini bukan lawan buatan yang sengaja dibuat lemah — ini
kode yang memang ada di sini sebelumnya.

Diukur pada .NET 10.0.11, Windows 11, 8 core logis.

## End to end

20.000 round-trip RPC berurutan melalui WebSocket loopback, satu panggilan aktif pada satu waktu:

| versi | panggilan/detik | latensi | alokasi per panggilan |
|---|---:|---:|---:|
| v1 | 6.809 | 146,9 µs | 16.379 B |
| **v2** | **9.989** | **100,1 µs** | **3.311 B** |
| | ×1,47 | −32% | −79,8% |

Angka alokasi adalah yang menentukan apakah sebuah server sanggup memegang sepuluh ribu koneksi.
Nilainya bukan nol, dan bagian terakhir menjelaskan mengapa.

## Codec

Waktu dari BenchmarkDotNet; alokasi diukur pada 200.000 operasi dengan
`GC.GetTotalAllocatedBytes`, cara yang membuat buffer terkumpul tampil jujur — sebuah "rent" dari
pool terlihat seperti alokasi pada kali pertama, dan tidak terlihat sama sekali sesudahnya.

| operasi | v1 | v2 | percepatan | alokasi v1 | alokasi v2 |
|---|---:|---:|---:|---:|---:|
| encode satu frame invoke | 1.386 ns | **327 ns** | ×4,2 | 1.200 B | **0 B** |
| encode, satu argumen bertipe | — | **269 ns** | — | — | **0 B** |
| decode satu frame invoke | 1.119 ns | **314 ns** | ×3,6 | 1.296 B | **0 B** |
| decode dan baca kedua argumen | — | **609 ns** | — | — | **0 B** |
| mencari handler sebuah method | 52,8 ns | **21,8 ns** | ×2,4 | 64 B | **0 B** |
| membuat correlation id | 118 ns | 130 ns | ×0,9 | 88 B | **0 B** |

> Log mentah BenchmarkDotNet di [`benchmark-micro.txt`](../benchmark-micro.txt) menunjukkan angka
> alokasi yang lebih besar untuk decode v1 (21.806 B) daripada 1.296 B di tabel atas. Keduanya
> benar: jalannya BenchmarkDotNet membebankan sewa pool milik `JsonDocument` ke operasi pertama,
> sedangkan pengukuran `-- alloc` menjalankan 200.000 operasi sehingga melihat biaya yang sudah
> teramortisasi — dan itulah yang sebenarnya dibayar koneksi berumur panjang. Tabel di atas memakai
> angka teramortisasi seluruhnya.

## Apa yang berubah, dan mengapa

### Menulis: tanpa string, tanpa array per frame

v1 meng-encode frame dengan menserialisasi sebuah POCO menjadi `string`, lalu menyalinnya ke
`byte[]` baru memakai `Encoding.UTF8.GetBytes`. Dua alokasi dan dua kali lintasan atas data yang
sama, untuk setiap pesan, demi sebuah string yang tidak pernah dibaca siapa pun.

v2 menyimpan satu buffer terkumpul dan satu `Utf8JsonWriter` per koneksi, lalu menulis UTF-8
langsung ke buffer yang akan dikirim ke socket. `Reset` memundurkan keduanya tanpa melepasnya,
sehingga koneksi yang sudah panas meng-encode frame tanpa alokasi.

Writer hanya disentuh selagi koneksi memegang lock pengiriman, dan itu pula yang membuat
pengiriman bersamaan menjadi aman — lihat di bawah.

### Membaca: parse di tempat

v1 mengubah byte yang diterima menjadi `string` UTF-16, mengikatnya ke sebuah POCO, dan memberi
setiap argumen `JsonElement`-nya sendiri — yang berarti `JsonDocument`-nya sendiri pula.

v2 membaca amplop dengan `Utf8JsonReader` langsung di atas buffer penerimaan. `SignalFrame` adalah
`ref struct` yang field-nya merupakan potongan dari buffer itu, sehingga men-decode frame sama
sekali tidak mengalokasi. Argumen tetap berupa JSON mentah sampai ada handler yang memintanya, dan
handler bertipe mendeserialisasinya langsung ke tipe parameternya — argumen `int` tidak berbiaya
alokasi sama sekali.

Buffer disewa dari `ArrayPool` sekali per koneksi, ditumbuhkan di tempat saat ada pesan besar, dan
dipertahankan pada ukuran itu.

### Dispatch: pencarian berbasis UTF-8

`Dictionary<string, Handler>` memaksa satu alokasi UTF-16 untuk setiap frame yang diterima, hanya
demi mencari handler-nya. `Utf8HandlerTable` adalah tabel open-addressing kecil yang ditelusuri
memakai byte mentah dari buffer penerimaan: 64 B dan 52,8 ns menjadi 0 B dan 21,8 ns.

Pendaftaran membangun ulang tabel di bawah lock lalu menukarnya; pembacaan bebas lock dan tidak
pernah melihat tabel yang setengah jadi.

### Id korelasi: penghitung, bukan GUID

v1 menghabiskan `Guid.NewGuid().ToString("N")` — 88 byte dan 32 karakter — untuk setiap panggilan.
v2 memakai `long` yang naik terus, diformat ke buffer stack saat ditulis ke dalam frame.

Ini satu-satunya baris di mana *waktu* v2 sedikit lebih buruk (130 ns lawan 118 ns), karena
benchmark v2 mengukur pembuatan id sekaligus penulisan satu frame ping utuh, sedangkan v1 hanya
mengukur `Guid`-nya. Alokasinya tetap menjadi nol, dan itulah tujuan perubahannya.

Id hanya perlu unik per koneksi dan per arah, jadi sebuah penghitung sudah cukup. Lihat
[protocol.md](protocol.md#id-korelasi).

### Pengiriman diserialisasi

v1 memanggil `WebSocket.SendAsync` dari mana pun pengiriman terjadi. Dua pengiriman bersamaan pada
satu socket akan menyisipkan byte satu sama lain dan merusak aliran data — bug yang muncul sebagai
peer terputus saat beban tinggi, dan sangat sulit ditemukan setelahnya.

v2 memegang `SemaphoreSlim` per koneksi selama proses encode-dan-kirim. Meng-encode di dalam lock
itulah yang memungkinkan buffer dan writer dipakai ulang, jadi kebenaran dan kemenangan alokasi
berasal dari keputusan yang sama.

### Handler berjalan di luar pompa, dengan batas

Menunggu handler di atas loop penerimaan berarti satu handler lambat menghentikan socket. v2
menjalankan setiap invokasi sebagai operasi terpisah, dibatasi semaphore sebesar
`MaxConcurrentInvocations` (bawaan 64). Begitu batas tercapai, pompa berhenti membaca, sehingga
kendali aliran turun ke TCP — itulah katup backpressure-nya.

State per invokasi (id korelasi dan argumen mentah, disalin keluar dari buffer penerimaan agar
buffer bisa dipakai ulang) diambil dari pool kecil, sehingga satu panggilan yang sedang berjalan
hanya berbiaya sebuah task dan sedikit lainnya.

## Apa yang masih mengalokasi

Per panggilan end-to-end, v2 menghabiskan sekitar 3,3 KB. Perlu dijelaskan dengan tepat ke mana
perginya, karena klaim "nol alokasi" yang berhenti di codec saja tidak berguna:

- **Mesin async.** Satu round-trip tertunda beberapa kali di tiap sisi — kirim socket, terima
  socket, handler, balasan. Setiap penundaan mem-boxing sebuah state machine.
- **Panggilan tertunda.** Sebuah `TaskCompletionSource` beserta `Task`-nya per panggilan aktif.
- **Timeout.** `Task.WaitAsync(timeout, ct)` mengalokasi registrasi timer per panggilan. Menyetel
  `CallTimeout = Timeout.InfiniteTimeSpan` menghilangkannya, dengan kehilangan perlindungan yang
  diberikannya.
- **Argumen yang di-boxing.** `CallAsync<int>("sum", 5, 7)` membuat `object[]` dan mem-boxing kedua
  int. Overload satu argumen `CallAsync<TArg, TResult>` menghindari keduanya.
- **Nilai balik.** Handler yang mengembalikan tipe nilai mem-boxing-nya sekali saat keluar.
- **`System.Net.WebSockets` sendiri**, yang punya biaya per operasinya.

Codec, framing, dispatch, dan buffer — semua yang benar-benar dimiliki SocketSignal — bebas
alokasi dalam keadaan mantap. Sisanya milik runtime, dan menguranginya lebih jauh berarti
membangun jalur `IValueTaskSource` yang kerumitannya belum jelas sepadan. Bila Anda punya beban
kerja yang menuntut itu, pompanya hanya satu berkas:
`src/SocketSignal/Hosting/SignalConnection.cs`.

## Cara memerasnya

1. **Pakai overload bertipe.** `Register<int, int, int>` dan `CallAsync<TArg, TResult>` melewati
   `JsonElement`, `object[]`, dan boxing.
2. **Lebih baik satu argumen objek daripada beberapa argumen.** Satu record dideserialisasi dalam
   satu lintasan.
3. **Kirim-dan-lupakan bila tidak butuh jawaban.** `SendAsync` tidak berbiaya panggilan tertunda,
   registrasi timeout, maupun frame balasan.
4. **Setel `CallTimeout` dengan sadar.** Cukup panjang untuk handler yang lambat, cukup pendek
   untuk menyadari peer yang mati.
5. **Naikkan `MaxConcurrentInvocations`** hanya bila handler Anda memang terikat I/O; nilai
   bawaannya adalah batas backpressure, bukan tuas yang perlu di-tuning.
6. **Perhatikan `Statistics`.** `BytesSent / FramesSent` memberi tahu apakah ukuran frame sesuai
   dugaan Anda, dan `CallsFailed` yang naik biasanya berarti timeout yang perlu diperbesar.

## Menghasilkan ulang

```bash
# Round-trip end-to-end, v1 lawan v2, plus alokasi per operasi
dotnet run -c Release --project src/SocketSignal.Benchmarks -- throughput

# BenchmarkDotNet untuk jalur codec dan dispatch
dotnet run -c Release --project src/SocketSignal.Benchmarks -- micro

# Alokasi saja
dotnet run -c Release --project src/SocketSignal.Benchmarks -- alloc
```

Angkanya bergeser mengikuti perangkat keras, versi .NET, dan apakah laptop sedang memakai baterai.
Yang seharusnya tetap adalah bentuknya: jalur codec tidak mengalokasi apa pun, dan satu panggilan
end-to-end berbiaya jauh lebih kecil daripada v1.
