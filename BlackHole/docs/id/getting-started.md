# Panduan awal

*BlackHole Messaging — Gravicode Studios, dipimpin oleh Kang Fadhil.*

## Kebutuhan

- **.NET 10 SDK** atau lebih baru
- Platform apa pun yang didukung .NET 10. CI membangun dan menguji di Linux, Windows, dan macOS.

## Instalasi

```bash
dotnet add package BlackHole.Messaging
```

Nama `BlackHole` sudah dipakai orang lain di nuget.org, jadi paketnya bernama
**BlackHole.Messaging**. Nama assembly dan seluruh namespace tetap `BlackHole.*` — tidak ada kata
"Messaging" di kode Anda.

## Server

```csharp
using BlackHole.Hosting;

await using var server = new BlackHoleServer(5000);

server.Rpc
    .RegisterText("upper", teks => teks.ToUpperInvariant())
    .Register("echo", request => request.Payload)
    .Register("cari", async (request, ct) =>
    {
        Pelanggan p = await database.FindAsync(request.Text(), ct);
        return JsonSerializer.SerializeToUtf8Bytes(p);
    });

server.Start();
Console.WriteLine($"Mendengarkan di {server.EndPoint}");
await Task.Delay(Timeout.Infinite);
```

Port `0` membiarkan sistem operasi memilih; baca `server.EndPoint.Port` sesudahnya untuk tahu port
mana yang dipakai. Agar server sama sekali tidak terlihat dari jaringan — misalnya untuk simulator
atau tes — ikat ke loopback secara eksplisit:

```csharp
var server = new BlackHoleServer(new IPEndPoint(IPAddress.Loopback, 5000));
```

## Client

```csharp
await using var client = await BlackHoleClient.ConnectAsync("127.0.0.1", 5000);

string hasil = await client.Rpc.CallTextAsync("upper", "halo blackhole");
byte[] mentah = await client.Rpc.CallAsync("echo", "bita"u8.ToArray());
```

Kalau client mungkin jalan lebih dulu daripada server:

```csharp
await using var client = await BlackHoleClient.ConnectWithRetryAsync("127.0.0.1", 5000, attempts: 5);
```

### Kegagalan muncul, bukan menggantung

```csharp
try
{
    var hasil = await client.Rpc.CallAsync("berisiko", isi, timeout: TimeSpan.FromSeconds(5));
}
catch (RpcException ex)
{
    // Dilempar bila: handler melempar exception, metode tidak ada,
    // batas waktu terlampaui, atau koneksi putus di tengah panggilan.
    Console.WriteLine($"{ex.Method}: {ex.Message}");
}
```

Setiap panggilan punya batas waktu — `DefaultTimeout` 30 detik. Di v2, balasan yang hilang membuat
pemanggil menunggu selamanya.

## Pub/Sub

```csharp
// Pelanggan (subscriber)
client.PubSub.Received += (topik, isi) =>
{
    // Isi ini mati begitu handler selesai. Salin kalau mau disimpan.
    Console.WriteLine($"{topik}: {Encoding.UTF8.GetString(isi.Span)}");
};

await client.PubSub.SubscribeAsync("sensor/+/temperature");  // satu segmen
await client.PubSub.SubscribeAsync("alarm/#");               // semua di bawahnya

// Penerbit (publisher)
await client.PubSub.PublishAsync("sensor/tank-3/temperature", "28.4");

// Atau dari sisi server
await server.PublishAsync("alarm/floor-1/pump", "pompa terlalu panas"u8.ToArray());
```

`+` cocok dengan tepat satu segmen; `#` cocok dengan sisanya dan harus berada di akhir.

## Streaming

```csharp
// Mengirim
await using var berkas = File.OpenRead("firmware.bin");
long terkirim = await client.OutgoingStreams.SendAsync(
    "firmware-2026",
    berkas,
    new StreamDescriptor("firmware.bin", berkas.Length, "application/octet-stream"),
    chunkSize: 16 * 1024,
    progress: new Progress<long>(b => Console.WriteLine($"{b / 1024:N0} KiB")));

// Menerima
server.ClientConnected += connection =>
{
    connection.Streams.Completed += (_, e) =>
        Console.WriteLine($"{e.StreamId}: {e.Length:N0} bita");
};
```

Supaya unggahan besar tidak menumpuk di memori, beri penerima sebuah sink:

```csharp
connection.Streams.SinkFactory = (id, deskriptor) =>
    File.Create(Path.Combine("uploads", Path.GetFileName(deskriptor.Name)));
```

## Batching

```csharp
// Eksplisit: kumpulan ini, satu amplop, sekarang
await client.Batch.SendBatchAsync(pesan);

// Otomatis: ditampung lalu dikirim saat ambang mana pun tercapai lebih dulu
client.Batch.MaxCount = 256;
client.Batch.MaxBytes = 64 * 1024;
client.Batch.MaxDelay = TimeSpan.FromMilliseconds(20);
client.Batch.Start();

foreach (var bacaan in daftarBacaan)
    await client.Batch.AddAsync(new BlackHoleMessage(MessageType.Publish, topik, bacaan));
```

Nilainya 22× lipat untuk lalu lintas pesan kecil — lihat [benchmarks](../benchmarks.md).

## Server memanggil client

Kedua sisi bisa melayani. Beginilah cara memerintah perangkat yang berada di balik NAT:

```csharp
// Di sisi client
client.Handlers.RegisterText("device/status", _ => "ok: 4 sensor aktif");

// Di sisi server
var pemanggil = new RpcClient(connection.Transport);
connection.Router.On(MessageType.RpcResponse, pemanggil.HandleAsync);
string status = await pemanggil.CallTextAsync("device/status", "?");
```

## Satu aturan yang wajib diingat

**Isi pesan yang diterima hanya sah sampai handler Anda selesai.** Isi itu menunjuk langsung ke
buffer milik transport — itulah sebabnya penerimaan tidak mengalokasikan memori sama sekali. Kalau
mau menyimpannya, salin dulu:

```csharp
client.PubSub.Received += (topik, isi) =>
{
    byte[] milikSaya = isi.ToArray();          // salin, baru antrikan
    _antrian.Enqueue((topik, milikSaya));
};
```

## Selanjutnya

- [Arsitektur](architecture.md) — bagaimana lapisan-lapisannya menyatu
- [Protokol](protocol.md) — format kabelnya, bita per bita
- [Pola](patterns.md) — tiap pola secara mendalam
- [Performa](performance.md) — menjaga alokasi tetap nol
- [IoT Gateway](iot-gateway.md) — aplikasi utuh di atas semuanya

---

*Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.*
