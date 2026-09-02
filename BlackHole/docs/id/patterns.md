# Pola

*BlackHole Messaging — Gravicode Studios, dipimpin oleh Kang Fadhil.*

Empat pola, semuanya dibangun dengan cara yang sama: sebuah kelas yang menerima `ITransport`,
menyediakan `HandleAsync` bertanda tangan `MessageDispatch`, dan menawarkan `AttachTo(router)` untuk
kasus umum. Pakai satu, pakai keempatnya, atau tulis pola kelima Anda sendiri.

---

## RPC

### Melayani

```csharp
var rpc = new RpcServer();

rpc.RegisterText("upper", teks => teks.ToUpperInvariant());
rpc.Register("echo", request => request.Payload);
rpc.Register("cari", async (request, ct) =>
{
    var pelanggan = await database.FindAsync(request.Text(), ct);
    return JsonSerializer.SerializeToUtf8Bytes(pelanggan);
});

rpc.AttachTo(router);
```

Overload sinkron boleh mengembalikan `request.Payload` langsung — balasannya ditulis sebelum frame
milik handler dilepas, jadi tidak perlu disalin. Begitulah `echo` bisa gratis.

### Memanggil

```csharp
var client = new RpcClient(transport) { DefaultTimeout = TimeSpan.FromSeconds(10) };
client.AttachTo(router);

byte[] hasil = await client.CallAsync("cari", kunci, timeout: TimeSpan.FromSeconds(2));
string teks = await client.CallTextAsync("upper", "halo");
await client.NotifyAsync("log", catatan);   // kirim lalu lupakan, tanpa balasan
```

### Kegagalan adalah hasil yang setara

| Yang terjadi | Yang dilihat pemanggil |
|---|---|
| Handler melempar exception | `RpcException` berisi tipe dan pesannya |
| Metode tidak terdaftar | `RpcException`, seketika |
| Batas waktu terlampaui | `RpcException` |
| Koneksi putus di tengah panggilan | `RpcException` untuk semua panggilan tertunda |

Server **selalu membalas** — metode yang tidak dikenal langsung dibalas dengan tanda
`MessageFlags.Error`. v2 hanya mengabaikan permintaan yang tak bisa dirutekan, jadi pemanggilnya
menunggu selamanya.

### Cara korelasi bekerja

`RpcClient` menyimpan `ConcurrentDictionary<long, Pending>` berkunci pencacah interlocked. Server
mengembalikan id yang sama; client menghapus entri itu dan menyelesaikan `TaskCompletionSource`-nya.
Ratusan panggilan bisa berjalan bersamaan di satu koneksi, dan sebuah
[tes](../../tests/BlackHole.Tests/EndToEndTests.cs) menembakkan 200 panggilan sekaligus lalu memeriksa
setiap jawaban cocok dengan pertanyaannya sendiri.

Hasilnya **memang** disalin keluar dari buffer transport — kelanjutan `await` berjalan setelah
dispatch selesai, jadi mau tidak mau harus disalin.

---

## Pub/Sub

### Wildcard

`+` cocok dengan tepat satu segmen. `#` cocok dengan sisanya dan harus di akhir.

| Filter | `sensor/tank-3/temperature` | `sensor/tank-3/humidity` | `sensor/a/b/temperature` |
|---|:---:|:---:|:---:|
| `sensor/tank-3/temperature` | ✅ | ❌ | ❌ |
| `sensor/+/temperature` | ✅ | ❌ | ❌ |
| `sensor/#` | ✅ | ✅ | ✅ |
| `+/+/temperature` | ✅ | ❌ | ❌ |

Filter persis dicari lewat dictionary; hanya filter wildcard yang ditelusuri, sekitar 35 ns per
filter tanpa alokasi.

### Broker

```csharp
var broker = new PubSubBroker { EchoToPublisher = true };
broker.AttachTo(router);

// Wajib, kalau tidak daftar pelanggan tumbuh selama proses hidup.
listener.ClientDisconnected += (transport, _) => broker.RemoveSubscriber(transport);
```

`BlackHoleServer` sudah membereskannya untuk Anda. Demo v2 tidak, dan membocorkan setiap pelanggan
yang pernah menyambung.

Tiap himpunan pelanggan adalah array tak-berubah yang ditukar di balik kunci, jadi penyebaran membaca
tanpa mengunci dan satu pelanggan lambat tidak bisa menghambat pengiriman ke yang lain. Pelanggan
yang pengirimannya gagal dilewati, bukan diulang — event `Closed`-nya yang akan membersihkannya.

### Ini bukan antrean pesan

Tidak ada penyimpanan permanen, tidak ada jaminan pengiriman, tidak ada penampungan saat offline.
Pesan yang diterbitkan ketika pelanggan sedang terputus akan hilang. Kalau butuh ketahanan, taruh
broker berbasis disk di belakangnya.

---

## Streaming

### Mengirim

```csharp
var pengirim = new StreamSender(transport) { FlushThreshold = 64 * 1024 };

await using var berkas = File.OpenRead("firmware.bin");
long terkirim = await pengirim.SendAsync(
    streamId: "firmware-2026",
    source: berkas,
    descriptor: new StreamDescriptor("firmware.bin", berkas.Length, "application/octet-stream"),
    chunkSize: 16 * 1024,
    progress: new Progress<long>(b => Console.WriteLine($"{b / 1024:N0} KiB")));
```

Buffer potongan dipinjam sekali untuk seluruh transfer. Potongan ditulis **tanpa dialirkan** sampai
`FlushThreshold` bita tertunda, sehingga "satu tulisan soket per 4 KiB" berubah jadi satu per 64 KiB
— itulah sebabnya potongan 4 KiB pun masih mencapai 452 MiB/detik. v2 mengalirkan setiap potongan.

### Menerima

```csharp
var penerima = new StreamReceiver
{
    MaxStreamLength = 256L * 1024 * 1024,
    MaxConcurrentStreams = 64,
};

penerima.Started  += (id, d) => Console.WriteLine($"{id}: {d.Name}, {d.TotalLength:N0} B");
penerima.Progress += (id, diterima, total) => Laporkan(id, diterima, total);
penerima.Completed += (_, e) => Simpan(e.StreamId, e.Data);   // e.Data mati saat handler selesai
penerima.Aborted  += (id, sebab) => Catat(id, sebab);

penerima.AttachTo(router);
```

Supaya data besar tidak menumpuk di memori:

```csharp
penerima.SinkFactory = (id, deskriptor) =>
    File.Create(Path.Combine("uploads", Path.GetFileName(deskriptor.Name)));
```

`MaxStreamLength` dan `MaxConcurrentStreams` bukan pemanis opsional — tanpa keduanya, sebuah peer,
entah jahat atau sekadar bermasalah, bisa mengubah satu stream terbuka menjadi memori proses yang
tak terbatas.

Tiap potongan membawa indeksnya di `CorrelationId` dan penerima memeriksa urutannya, jadi stream
yang kehilangan urutan dibatalkan — bukan diam-diam disusun ulang secara salah.

### Satu penerima per koneksi

Dua perangkat bisa sama-sama mengunggah `firmware.bin`. Kalau berbagi satu `StreamReceiver`,
potongan keduanya akan berselang-seling jadi data rusak. `BlackHoleServer` memberi tiap koneksi
penerima sendiri — lihat [arsitektur](architecture.md#hosting--srcblackholehosting).

---

## Batching

### Kapan menguntungkan

| | Gunakan |
|---|---|
| Banyak pesan kecil, tahan tunda | **Batching.** 22× lipat untuk lalu lintas. |
| Request/response | Jangan — batch tidak bisa ditunggu per pesan. |
| Satu data besar | Jangan — itu tugas streaming. |
| Satu ledakan pesan yang sudah di tangan | `WriteAsync` × N lalu satu `FlushAsync`. |

### Eksplisit

```csharp
await client.Batch.SendBatchAsync(pesan);   // kumpulan ini, satu amplop, sekarang
```

### Otomatis

```csharp
client.Batch.MaxCount = 256;                           // kirim di 256 pesan
client.Batch.MaxBytes = 64 * 1024;                     // atau 64 KiB
client.Batch.MaxDelay = TimeSpan.FromMilliseconds(20); // atau setelah 20 ms
client.Batch.Start();                                  // menyalakan timer jeda

await client.Batch.AddAsync(pesan);
```

Ambang mana pun yang tercapai lebih dulu akan mengirim amplopnya. `MaxDelay` itulah yang membatasi
latensi ketika lalu lintas sedang sepi: tanpanya, sisa pesan terakhir dari sebuah ledakan akan
menunggu batch yang mungkin tak pernah penuh. `AddAsync` menampung ke penulis berkolam yang dipakai
ulang selama pengirim hidup, jadi telemetri yang mengalir tetap tidak mengalokasikan apa pun per
pesan.

### Penerimaan bersifat transparan

```csharp
var batches = new BatchReceiver(router.Dispatch);
batches.AttachTo(router);
```

Pesan di dalamnya didorong kembali melalui router, jadi publish yang di-batch menempuh jalur yang
persis sama dengan yang datang sendirian. Handler `Publish` Anda tidak bisa membedakannya — dan
memang tidak perlu.

Payload sebuah amplop adalah rangkaian frame BlackHole utuh, dibongkar dengan `FrameCodec` yang sama
seperti yang dipakai transport. v2 punya format dalam terpisah yang diurai manual oleh penerimanya.

---

## Menulis pola sendiri

```csharp
// 1. Satu bita tipe yang belum dipakai siapa pun
public const MessageType Heartbeat = (MessageType)0x40;

// 2. Handler bertanda tangan MessageDispatch
ValueTask HandleHeartbeat(ITransport transport, BlackHoleMessage message, CancellationToken ct)
{
    _terakhirTerlihat[message.Header] = DateTimeOffset.UtcNow;
    return ValueTask.CompletedTask;
}

// 3. Daftarkan
router.On(Heartbeat, HandleHeartbeat);
```

Transport tidak peduli tipe pesan; ia tidak perlu tahu tipe Anda ada.

---

*Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.*
