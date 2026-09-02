# Arsitektur

*BlackHole Messaging — Gravicode Studios, dipimpin oleh Kang Fadhil.*

Empat lapisan. Tiap lapisan hanya tahu lapisan di bawahnya, dan sambungan antar dua lapisan selalu
berupa satu tipe saja.

```
        kode Anda
  ┌──────────────────────────────────────────────────────────────┐
  │  Hosting     BlackHoleServer   BlackHoleClient               │  siap pakai
  ├──────────────────────────────────────────────────────────────┤
  │  Patterns    RpcServer/Client  PubSubBroker/Client           │  arti sebuah pesan
  │              StreamSender/Receiver  BatchSender/Receiver     │
  ├──────────────────────────────────────────────────────────────┤
  │  Hosting     MessageRouter                                   │  pesan menuju ke mana
  ├──────────────────────────────────────────────────────────────┤
  │  Transport   ITransport  TcpTransport  TcpListenerHost       │  bita masuk, bita keluar
  ├──────────────────────────────────────────────────────────────┤
  │  Protocol    BlackHoleMessage  FrameCodec  HeaderCache       │  format kabel
  └──────────────────────────────────────────────────────────────┘
```

## Protocol — `src/BlackHole/Protocol/`

`BlackHoleMessage` adalah **readonly struct** berukuran 40 bita. Jalur panas memindahkan jutaan
objek ini, dan kalau berupa class artinya satu alokasi gen-0 per pesan.

`FrameCodec` adalah satu-satunya tempat format kabel ditulis dan dibaca. Ini bukan sekadar kerapian —
ini perbaikan atas cara v2 rusak. Lihat [protokol](protocol.md).

`HeaderCache` mengubah bita header UTF-8 yang berulang kembali menjadi instance `string` yang sama.
Lalu lintas nyata memakai kosakata yang sangat sedikit — beberapa nama metode, beberapa lusin topik
— sehingga cache direct-mapped berkunci bita mentah hampir selalu kena. Pada demo:
**20.000 kena, 7 meleset.**

## Transport — `src/BlackHole/Transport/`

`TcpTransport` dipakai apa adanya oleh sisi yang menghubungi maupun sisi yang menerima. v2 punya dua
kelas nyaris kembar, masing-masing dengan salinan sendiri untuk serialisasi, deserialisasi, dan
loop baca.

Sisi baca memakai `System.IO.Pipelines`. Pipe-lah yang memiliki buffer dan menyerahkan tampilan
`ReadOnlySequence<byte>`, sehingga frame yang belum utuh tidak butuh `byte[]` per pesan dan frame
yang sudah utuh diurai **tanpa satu pun alokasi**.

Penulisan diserialkan di balik satu `SemaphoreSlim`. `SendAsync` menulis lalu mengalirkan;
`WriteAsync` menulis tanpa mengalirkan supaya satu ledakan pesan bisa digabung jadi satu tulisan
soket, lalu `FlushAsync` sekali.

### Satu dispatcher, bukan event

```csharp
public interface ITransport : IAsyncDisposable
{
    MessageDispatch? Dispatcher { get; set; }   // tepat satu
    ValueTask SendAsync(BlackHoleMessage message, CancellationToken ct = default);
    ValueTask WriteAsync(BlackHoleMessage message, CancellationToken ct = default);
    ValueTask FlushAsync(CancellationToken ct = default);
    event Action<ITransport, Exception?>? Closed;
}
```

v2 memakai event multicast `OnMessageReceived`. Setiap objek pola yang menempel di sebuah koneksi
melihat semua pesan lalu mengabaikan yang bukan miliknya; tak satu pun bisa menahan laju, karena
event mengembalikan `void`.

v3 punya tepat satu `Dispatcher` yang mengembalikan `ValueTask` dan ditunggu oleh transport.
Perubahan itulah yang membuat jalur terima sekaligus tanpa salinan dan tetap benar: transport bisa
menahan buffer-nya sampai dispatch selesai, sehingga handler boleh membaca isi pesan di tempat.
Penyebaran ke banyak handler adalah tugas router, satu lapis di atasnya.

### Memulai adalah langkah tersendiri

Sebuah transport bisa dibuat tanpa loop bacanya berjalan:

```csharp
var transport = await TcpTransport.ConnectAsync(host, port, startReceiving: false);
transport.Dispatcher = router.Dispatch;   // pasang dulu
transport.Start();                        // baru izinkan pesan masuk
```

Ini ada karena bug nyata. Ketika transport mulai membaca di dalam konstruktornya, client yang
langsung subscribe begitu tersambung bisa kehilangan subscription itu **secara diam-diam** — pesan
`Subscribe` tiba sebelum server sempat memasang dispatcher-nya. Jarang terjadi saat senggang, dan
konsisten terjadi saat sibuk: jenis bug paling buruk untuk ditemukan di produksi. `BlackHoleServer`
dan `BlackHoleClient` sama-sama memasang dulu baru menjalankan, dan
[dua tes regresi](../../tests/BlackHole.Tests/EndToEndTests.cs) mengunci perilaku ini di kedua arah.

## Routing — `src/BlackHole/Hosting/MessageRouter.cs`

```csharp
var router = new MessageRouter();
rpcServer.AttachTo(router);
pubSubBroker.AttachTo(router);
transport.Dispatcher = router.Dispatch;
```

Pencarian handler adalah indeks array pada bita tipe. Kasus satu handler — yang dalam praktiknya
adalah semua kasus — diteruskan tanpa membuat state machine. Pendaftaran bersifat copy-on-write,
jadi handler boleh ditambah sambil lalu lintas berjalan, dan handler yang melempar exception muncul
lewat `HandlerFaulted`, bukan mematikan koneksi.

## Patterns — `src/BlackHole/Patterns/`

Semua objek pola berbentuk sama: menerima `ITransport`, menyediakan `HandleAsync` bertanda tangan
`MessageDispatch`, dan punya `AttachTo(router)` untuk kasus umum. Semuanya berdiri sendiri — pakai
RPC saja, Pub/Sub saja, atau bawa pola Anda sendiri.

Lihat [pola](patterns.md) untuk pembahasan mendalam.

## Hosting — `src/BlackHole/Hosting/`

`BlackHoleServer` dan `BlackHoleClient` adalah lapisan siap pakai: listener, router, dan semua pola
dirangkai dengan masa hidup yang benar.

Bagian masa hidup inilah yang perlu dibaca:

| Objek | Cakupan | Alasan |
|---|---|---|
| `RpcServer` | seluruh server | Sebuah metode sama untuk semua client. |
| `PubSubBroker` | seluruh server | Topik melintasi koneksi; memang itu tujuannya. |
| `MessageRouter` | per koneksi | Agar tiap koneksi bisa menambah handler sendiri. |
| `StreamReceiver` | **per koneksi** | Dua perangkat bisa sama-sama mengunggah `firmware.bin`. Berbagi satu penerima akan menyelang-nyelingkan keduanya jadi data rusak. |
| `BatchReceiver` | per koneksi | Membongkar amplop ke router koneksi itu. |

`PubSubBroker.RemoveSubscriber` dipanggil dari handler pemutusan koneksi. Tanpa itu, daftar pelanggan
tumbuh selama proses hidup — persis kebocoran yang ada di demo v2.

## Koneksi bersifat simetris

Kedua ujung bisa melayani sekaligus memanggil. `BlackHoleClient` menyediakan `Rpc` (metode yang ia
panggil) *dan* `Handlers` (metode yang ia layani), sehingga server bisa memanggil perangkat yang
menghubungi dari balik NAT, lewat soket yang dibuka perangkat itu sendiri.
[IoT gateway](iot-gateway.md) memakai ini untuk semua perintah perangkat.

## Perilaku thread

- **Satu loop baca per koneksi.** Handler berjalan di sana, berurutan, satu pesan pada satu waktu.
- **Pengiriman diserialkan** di balik kunci per koneksi; thread mana pun boleh mengirim.
- **Handler jangan memblokir.** Memblokir loop baca menghentikan lalu lintas masuk koneksi itu.
- **Pencacah bersifat interlocked** dan aman dibaca dari mana saja, termasuk thread UI.

IoT gateway menunjukkan polanya untuk UI berlaju tinggi: loop baca menulis ke ring bebas-kunci, dan
timer 33 ms menerbitkan satu pembaruan gabungan per frame. Mengikat langsung ke loop baca akan
membanjiri dispatcher.

---

*Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.*
