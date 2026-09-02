# Arsitektur

*Gravicode Studios, dipimpin oleh Kang Fadhil.*

Library ini sekitar 1.500 baris. Halaman ini adalah petanya.

```
src/SocketSignal/
├── Protocol/
│   ├── MessageType.cs        pembeda jenis frame
│   ├── SignalFrame.cs        decode: ref struct sebagai jendela ke buffer penerimaan
│   └── SignalWriter.cs       encode: langsung ke buffer milik pemanggil
├── Dispatch/
│   ├── Utf8HandlerTable.cs   nama method (UTF-8) -> handler, tanpa string
│   └── HandlerEntry.cs       bentuk handler bertipe dan tanpa tipe
├── Buffers/
│   └── PooledBufferWriter.cs IBufferWriter yang bisa tumbuh di atas ArrayPool
├── Hosting/
│   ├── SignalConnection.cs   pompa: satu WebSocket, dua arah
│   ├── PendingCall.cs        panggilan yang dikirim peer ini dan sedang ditunggu
│   ├── SocketSignalServer.cs loop penerimaan, grup, fan-out, sapuan keepalive
│   ├── ClientConnection.cs   satu client terhubung, dari sudut pandang server
│   └── SocketSignalClient.cs menghubungi, menyambung ulang, memanggil
├── Diagnostics/SignalStatistics.cs
├── SocketSignalOptions.cs
└── Exceptions.cs
```

## Satu gagasan yang perlu diketahui

**Protokolnya simetris, jadi implementasinya juga.** Sebuah `invoke` bentuknya sama dari ujung
mana pun dikirim, dan kedua ujung membutuhkan empat hal yang sama: loop penerimaan, tabel handler,
tabel panggilan tertunda, dan jalur pengiriman yang diserialisasi.

`SignalConnection` adalah keempatnya. Itulah yang dibungkus `SocketSignalClient`, dan itu pula yang
berada di balik setiap `ClientConnection` di sisi server. Client dan server hanya berbeda pada cara
*memperoleh* socket — satu menghubungi, satu menerima — dan pada apa yang diterima handler sebagai
parameter pertamanya.

v1 menulis loop ini dua kali, sekali di server dan sekali di client, dan itulah sebabnya kedua
salinannya menyimpang: server sebenarnya mampu mencocokkan balasan dari client, tetapi API untuk
memakainya bersifat `internal` dan tidak bisa dijangkau, sehingga panggilan server-ke-client tidak
pernah benar-benar bisa mengembalikan nilai. Berbagi satu loop-lah yang membuat `CallClientAsync`
muncul dengan sendirinya.

## Perjalanan sebuah frame

**Masuk**, di `SignalConnection.RunAsync`:

1. `ReceiveFrameAsync` membaca satu pesan WebSocket utuh ke buffer penerimaan terkumpul,
   menumbuhkannya di tempat bila perlu, dan menolak apa pun yang melebihi `MaxMessageSize`.
2. `SignalFrame.TryParse` men-decode amplopnya di tempat. Tanpa alokasi: setiap field adalah potongan.
3. `DispatchAsync` bercabang berdasarkan `Type`:
   - **`invoke`** → `Utf8HandlerTable.Find` dengan byte mentah nama method. Bila tidak ketemu dan
     `expectReturn` diset, balas error "not found". Bila ketemu, salin id dan args mentah ke sebuah
     `Invocation` terkumpul (supaya buffer penerimaan bisa dipakai ulang), tunggu slot pada gerbang
     invokasi, lalu jalankan handler di luar pompa.
   - **`result`** → ubah id kembali menjadi `long`, ambil `PendingCall` dari tabel, dan selesaikan
     dengan mendeserialisasi hasil mentah langsung ke `TResult`.
   - **`ping`** → balas `pong`.
   - **`welcome`** → picu `Welcomed`, yang ditunggu oleh `ConnectAsync`.
4. Selain itu diabaikan, setelah sebelumnya tetap dihitung sebagai tanda hidup.

**Keluar**, pada setiap pengiriman:

1. Ambil lock pengiriman.
2. `Begin()` memundurkan buffer terkumpul dan `Utf8JsonWriter`.
3. `SignalWriter` menulis frame sebagai UTF-8 ke buffer itu.
4. `WebSocket.SendAsync` atas memori yang sudah ditulis.
5. Lepaskan lock.

Meng-encode di dalam lock itu disengaja: itulah yang memungkinkan satu buffer dan satu writer per
koneksi alih-alih satu per frame, dan itu pula yang mencegah dua pengiriman bersamaan menyisipkan
byte di socket.

## Di mana thread-nya

| | |
|---|---|
| **Loop penerimaan koneksi** | Satu task per server, di `StartAsync`. Menerima lalu menyerahkan; tidak pernah menunggu satu client |
| **Pompa penerimaan** | Satu task per koneksi. Membaca, men-decode, men-dispatch. Tidak pernah menjalankan handler sampai selesai |
| **Handler** | Berjalan di luar pompa, sampai `MaxConcurrentInvocations` sekaligus per koneksi |
| **Keepalive** | Satu task per *server*, menyapu semua koneksi; satu lagi per client |

Keepalive yang berupa satu loop untuk seluruh server, bukan timer per koneksi, itulah yang membuat
sebuah server sanggup memegang banyak koneksi menganggur dengan murah.

## Backpressure

Gerbang invokasi adalah satu-satunya tempat pompa berhenti. Ketika sudah ada
`MaxConcurrentInvocations` handler berjalan pada satu koneksi, pompa berhenti membacanya; buffer
penerimaan OS penuh, jendela TCP menutup, dan peer tersebut direm di lapisan transport. Tidak ada
antrean tak terbatas di mana pun, dan itulah properti yang penting: peer yang membanjiri akan
melambat, bukan menumbuhkan heap server.

## Siklus hidup dan kegagalan

Setiap jalur kegagalan bermuara ke `CloseCoreAsync`, yang berjalan tepat sekali per koneksi dan:

1. Menggagalkan setiap panggilan tertunda dengan `SignalConnectionClosedException` — tidak ada
   pemanggil yang dibiarkan menggantung.
2. Mengirim frame close bila socket masih terbuka, sebisanya.
3. Memicu `Closed`, yang dipakai server untuk mengeluarkan client dari registri dan semua grup, dan
   dipakai client untuk memicu `Disconnected` lalu mulai menyambung ulang bila diminta.

Buffer terkumpul dikembalikan ke pool di `DisposeAsync`.

## Keputusan rancangan yang perlu dinyatakan

**Grup hanya ada di sisi server.** Tidak ada frame "join". Client meminta dimasukkan dengan
memanggil sebuah method yang disediakan aplikasi, sehingga keanggotaan tetap menjadi keputusan
otorisasi, bukan sesuatu yang bisa diberikan client kepada dirinya sendiri.

**Keepalive di level protokol, bukan milik WebSocket.** Browser membalas ping WebSocket secara
transparan dan JavaScript tidak pernah melihatnya, sehingga client browser tidak akan bisa ikut
dalam deteksi keaktifan. Kedua ujung menyetel `KeepAliveInterval` socket ke nol dan memakai frame
`ping`/`pong`.

**Bentuk handler tanpa tipe tetap dipertahankan.**
`Register(string, Func<ClientConnection, JsonElement[], Task<object?>>)` adalah tanda tangan v1.
Ia berbiaya satu `JsonDocument` per panggilan — persis alasan overload bertipe itu ada — tetapi
kode yang ditulis untuk v1 tetap bisa dikompilasi.

**`SignalFrame` sengaja dibuat `ref struct`.** Ia tidak bisa disimpan di field, ditangkap lambda,
atau dipegang melewati `await`, sehingga compiler menegakkan aturan yang jika tidak hanya akan
menjadi komentar: buffer penerimaan dipakai ulang, dan apa pun yang harus hidup lebih lama dari
pemanggilan dispatch wajib disalin.

## Pengujian

`tests/SocketSignal.Tests` terdiri dari dua bagian:

- **`ProtocolTests`** — codec secara terisolasi: parsing, penulisan, round-trip, field tak dikenal,
  masukan sampah, tabel handler setelah dibangun ulang, dan buffer writer yang tumbuh.
- **`EndToEndTests`** — `HttpListener` sungguhan pada port loopback bebas dan WebSocket sungguhan
  terhadapnya, karena kegagalan yang menarik pada library seperti ini semuanya adalah kegagalan
  waktu dan siklus hidup. Timeout, socket putus di tengah panggilan, handler bersamaan, isolasi
  grup, dan panggilan server-ke-client yang tidak bisa dilakukan v1 semuanya diuji.

```bash
dotnet test tests/SocketSignal.Tests/SocketSignal.Tests.csproj
```
