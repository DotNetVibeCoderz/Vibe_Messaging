# Memulai

*Gravicode Studios, dipimpin oleh Kang Fadhil.*

## Instalasi

```bash
dotnet add package SocketSignal
```

Membutuhkan .NET 10.

## Server

```csharp
using SocketSignal;

var server = new SocketSignalServer("http://localhost:8080/ws/");
server.Register<int, int, int>("sum", (client, a, b) => ValueTask.FromResult(a + b));

await server.StartAsync();   // berjalan sampai token dibatalkan
```

Perhatikan prefiksnya `http://`, bukan `ws://`. `SocketSignalServer` dibangun di atas
`HttpListener`, yang prefiksnya memang HTTP; client menghubungi alamat `ws://` yang bersesuaian.
Di Windows, prefiks selain `localhost` membutuhkan URL ACL:

```powershell
netsh http add urlacl url=http://+:8080/ws/ user=DOMAIN\user
```

`StartAsync` tidak kembali selama server masih mendengarkan, jadi jalankan di latar belakang bila
proses yang sama mengerjakan hal lain:

```csharp
using var cts = new CancellationTokenSource();
_ = server.StartAsync(cts.Token);
```

## Client

```csharp
var client = new SocketSignalClient();
await client.ConnectAsync(new Uri("ws://localhost:8080/ws/"));

int total = await client.CallAsync<int>("sum", 5, 7);
```

`ConnectAsync` selesai setelah frame `welcome` dari server tiba, sehingga `client.ClientId` sudah
terisi saat method itu selesai.

## Mendaftarkan method

Ada dua bentuk. Yang bertipe adalah pilihan utama:

```csharp
// Argumen langsung dideserialisasi ke tipe tujuannya.
server.Register<int, int, int>("sum", (client, a, b) => ValueTask.FromResult(a + b));
server.Register<Order, bool>("submit", async (client, order) => await Save(order!));
server.Register<int>("count", client => ValueTask.FromResult(Registry.Count));
```

Bentuk tanpa tipe memberi Anda argumen mentah, dan cocok ketika bentuk datanya berubah-ubah:

```csharp
server.Register("echo", async (client, args) =>
{
    string? text = args[0].GetString();
    return $"echo:{text}";
});
```

Sisi client sama saja, tanpa parameter `ClientConnection`:

```csharp
client.On<string, string>("serverHello", text => ValueTask.FromResult($"terdengar {text}"));
client.On("anything", async args => { /* JsonElement[] */ return null; });
```

Maksimal tiga argumen bertipe didukung. Untuk lebih dari itu, gunakan satu objek:

```csharp
public record PlaceOrder(string Sku, int Quantity, string Note, bool Rush);

server.Register<PlaceOrder, string>("orders.place", (client, order) => ...);
```

## Memanggil

```csharp
// Menunggu nilai balik.
int total = await client.CallAsync<int>("sum", 5, 7);

// Satu argumen bertipe: tanpa object[], tanpa boxing. Jalur cepat untuk panggilan padat.
var quote = await client.CallAsync<Symbol, Quote>("quote", symbol);

// Tanpa balasan.
await client.SendAsync("log", "dicatat di log dek");

// Bentuk v1, masih didukung.
JsonElement? raw = await client.CallAsync("sum", 5, 7);
```

## Memanggil client

```csharp
await server.BroadcastAsync("tick", 42);                       // semua client
await server.SendToClientAsync(id, "tick", 42);                // satu client, lewat id
await server.SendToGroupAsync("operators", "tick", 42);        // satu grup
int hasil = await server.CallClientAsync<int>(id, "double", 21);   // satu client, dengan balikan
```

Di dalam handler, `ClientConnection` adalah si pemanggil, sehingga sebuah method bisa langsung
menjawabnya:

```csharp
server.Register<string, bool>("subscribe", async (client, topic) =>
{
    client.JoinGroup(topic!);
    await client.SendAsync("subscribed", topic);
    return true;
});
```

## Kegagalan

Panggilan gagal dengan jelas, bukan menggantung. Tiga hal yang bisa salah punya exception-nya
masing-masing:

```csharp
try
{
    var result = await client.CallAsync<int>("sum", 5, 7);
}
catch (MethodNotFoundException)      { /* method tidak ada di peer */ }
catch (SignalInvocationException ex) { /* handler melempar: ex.RemoteMessage */ }
catch (SignalTimeoutException)       { /* tidak ada balasan dalam CallTimeout */ }
catch (SignalConnectionClosedException) { /* socket putus di tengah panggilan */ }
```

Keempatnya turunan `SocketSignalException`, jadi satu `catch` cukup bila Anda tidak
mempermasalahkan yang mana.

## Opsi

```csharp
var options = new SocketSignalOptions
{
    CallTimeout = TimeSpan.FromSeconds(10),
    KeepAliveInterval = TimeSpan.FromSeconds(15),
    IdleTimeout = TimeSpan.FromSeconds(60),
    MaxMessageSize = 4 * 1024 * 1024,
    MaxConcurrentInvocations = 64,
};

var server = new SocketSignalServer("http://localhost:8080/ws/", options);
var client = new SocketSignalClient(options);
```

`Timeout.InfiniteTimeSpan` menonaktifkan `CallTimeout` dan `KeepAliveInterval` secara terpisah.
Penjelasan lengkap ada di [referensi API](api-reference.md#socketsignaloptions).

## Menyambung ulang

Nonaktif secara bawaan, karena client yang menyambung ulang diam-diam juga kehilangan diam-diam
seluruh state yang dipegang server untuknya — termasuk keanggotaan grup.

```csharp
var client = new SocketSignalClient { AutoReconnect = true };

client.Connected += id => Console.WriteLine($"terhubung sebagai {id}");
client.Disconnected += why => Console.WriteLine($"terputus: {why}");
client.Reconnecting += attempt => Console.WriteLine($"percobaan {attempt}");
```

Masuk kembali ke grup di dalam handler `Connected` — server tidak mengingat koneksi yang lama.

## Autentikasi

`Authenticate` berjalan sebelum upgrade WebSocket diterima. Kembalikan false untuk menolak dengan
403.

```csharp
server.Authenticate = context =>
{
    string? token = context.Request.QueryString["token"];
    return ValueTask.FromResult(IsValid(token));
};
```

Untuk membawa identitasnya, simpan di koneksi setelah tersambung:

```csharp
server.ClientConnected += client => client.Items["user"] = LookupUser(client);
```

## Mematikan

Keduanya `IAsyncDisposable`. Dispose menutup koneksi yang aktif dan menggagalkan apa pun yang
masih menunggu di atasnya.

```csharp
await using var server = new SocketSignalServer("http://localhost:8080/ws/");
await using var client = new SocketSignalClient();
```

## Selanjutnya

- [Protokol](protocol.md) — format wire
- [Referensi API](api-reference.md) — seluruh anggota publik
- [Performa](performance.md) — jalur cepat, dan kapan pentingnya
- [SDK client](clients.md) — Python, Go, Node.js, browser
