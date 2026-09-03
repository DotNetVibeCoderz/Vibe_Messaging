# SocketSignal

[![NuGet](https://img.shields.io/nuget/v/SocketSignal.svg)](https://www.nuget.org/packages/SocketSignal)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![CI](https://github.com/DotNetVibeCoderz/Vibe_Messaging/actions/workflows/socketsignal-ci.yml/badge.svg)](https://github.com/DotNetVibeCoderz/Vibe_Messaging/actions)

**Bidirectional realtime RPC over raw WebSockets for .NET 10.** A client calls methods on the
server and gets return values back; the server calls methods on one client, a group, or all of
them. The protocol is small enough that a browser can speak it in ten lines of JavaScript, and
the .NET implementation encodes and decodes it without allocating.

[Bahasa Indonesia](#bahasa-indonesia) · [Documentation](docs/) · [Protocol](docs/protocol.md) · [Client SDKs](#client-sdks)

![The sonar console: a plan position indicator and a bearing-time recorder, both fed over SocketSignal](docs/images/sonar-console.png)

*The example application: a sea sonar simulator built with Avalonia. Everything it draws arrives
over SocketSignal from a server in the same process — [read more](docs/sonar-demo.md).*

---

## Install

```bash
dotnet add package SocketSignal
```

## Quick start

```csharp
using SocketSignal;

// ---- server ----
var server = new SocketSignalServer("http://localhost:8080/ws/");

// Arguments deserialise straight into int. No JsonElement, no boxing.
server.Register<int, int, int>("sum", (client, a, b) => ValueTask.FromResult(a + b));

_ = server.StartAsync();

// ---- client ----
var client = new SocketSignalClient();

// A method the server may call on us.
client.On<string, string>("serverHello", text =>
{
    Console.WriteLine(text);
    return ValueTask.FromResult("client received");
});

await client.ConnectAsync(new Uri("ws://localhost:8080/ws/"));

int total = await client.CallAsync<int>("sum", 5, 7);        // 12

// ---- server talking back ----
await server.BroadcastAsync("serverHello", "all hands");                   // everyone
await server.SendToClientAsync(someId, "serverHello", "just for you");     // one client
await server.SendToGroupAsync("operators", "serverHello", "ops only");     // a group
int answer = await server.CallClientAsync<int>(someId, "double", 21);      // and with a result
```

Run the walkthrough:

```bash
dotnet run --project src/SocketSignal.Demo      # server + two clients + a round-trip measurement
dotnet run --project src/SocketSignal.SonarDemo # the Avalonia sonar console
```

## What it does

|   | Feature |
|---|---|
| 1 | Client calls a method on the server |
| 2 | Client calls a method on the server **and gets a return value** |
| 3 | Server calls a method on **every** client (broadcast) |
| 4 | Server calls a method on **one** client, by id — with a return value |
| 5 | Server calls a method on a **group** of clients |

And the things a realtime library needs before it can be trusted with a connection:

- **Calls time out** instead of parking the caller forever, and pending calls fail when the socket
  drops rather than hanging.
- **Errors travel.** A handler that throws surfaces as `SignalInvocationException` on the caller,
  with the remote message. An unknown method raises `MethodNotFoundException`.
- **Keepalive** at the protocol level, so every SDK — browser included — sees the same liveness
  signal, plus idle eviction on the server.
- **Auto-reconnect** with exponential backoff, off by default.
- **Groups** that are lock-free and clean themselves up when a client disconnects.
- **An authentication hook** to vet the upgrade request before it becomes a connection.
- **Per-connection state** (`client.Items`) and **live statistics** for frames, bytes and calls.
- **Backpressure**: a peer that floods faster than handlers drain stops being read.

Full API in [docs/api-reference.md](docs/api-reference.md).

## Performance

v2 is a rewrite of the codec and the connection pump. Every number below is measured on this
repository — reproduce them with `dotnet run -c Release --project src/SocketSignal.Benchmarks`.

**End-to-end RPC round trips over a loopback WebSocket** (20,000 sequential calls, .NET 10.0.11,
Windows 11, 8 logical cores):

| stack | calls/sec | latency | allocated per call |
|---|---:|---:|---:|
| v1 | 6,809 | 146.9 µs | 16,379 B |
| **v2** | **9,989** | **100.1 µs** | **3,311 B** |
| | **×1.47** | **−32%** | **−79.8%** |

**The codec paths, per operation:**

| operation | v1 time | v2 time | v1 allocated | v2 allocated |
|---|---:|---:|---:|---:|
| encode one invoke frame | 1,386 ns | **327 ns** | 1,200 B | **0 B** |
| encode, single typed argument | — | **269 ns** | — | **0 B** |
| decode one invoke frame | 1,119 ns | **314 ns** | 1,296 B | **0 B** |
| decode and read both arguments | — | **609 ns** | — | **0 B** |
| find the handler for a method | 52.8 ns | **21.8 ns** | 64 B | **0 B** |
| mint a correlation id | 118 ns | 130 ns | 88 B | **0 B** |

Where the wins come from — and why the end-to-end figure is not zero either — is written up in
[docs/performance.md](docs/performance.md).

## Client SDKs

The protocol is JSON over a plain WebSocket, so anything that speaks WebSocket can join in.
Three SDKs ship with the same shape as the .NET client:

| Language | Install | Dependencies |
|---|---|---|
| [Python](clients/python) | `pip install socketsignal` | `websockets` |
| [Node.js](clients/nodejs) | `npm install @gravicode/socketsignal` | none — Node 22's global `WebSocket` |
| [Go](clients/go) | `go get github.com/DotNetVibeCoderz/Vibe_Messaging/SocketSignal/clients/go/v2/socketsignal` | `github.com/coder/websocket` |

[![PyPI](https://img.shields.io/pypi/v/socketsignal.svg?label=pypi%20socketsignal)](https://pypi.org/project/socketsignal/)
[![npm](https://img.shields.io/npm/v/%40gravicode%2Fsocketsignal.svg?label=npm%20%40gravicode%2Fsocketsignal)](https://www.npmjs.com/package/@gravicode/socketsignal)

```python
client = SocketSignalClient()

@client.on("serverHello")
async def hello(text): return "python heard you"

await client.connect("ws://localhost:8080/ws/")
print(await client.call("sum", 5, 7))          # 12
```

```javascript
const client = new SocketSignalClient();
client.on("serverHello", (text) => "node heard you");
await client.connect("ws://localhost:8080/ws/");
console.log(await client.call("sum", 5, 7));   // 12
```

```go
client := socketsignal.New(socketsignal.Options{})
client.On("serverHello", func(args []json.RawMessage) (any, error) { return "go heard you", nil })
_ = client.Connect(ctx, "ws://localhost:8080/ws/")

var total int
_ = client.Call(ctx, &total, "sum", 5, 7)      // 12
```

And a browser needs no SDK at all:

```html
<script>
const ws = new WebSocket("ws://localhost:8080/ws/");

ws.onmessage = (ev) => {
  const msg = JSON.parse(ev.data);
  if (msg.type === "invoke" && msg.method === "serverHello") {
    ws.send(JSON.stringify({ type: "result", id: msg.id, result: "hello from the browser" }));
  }
};

function sum(a, b) {
  ws.send(JSON.stringify({ type: "invoke", id: "1", method: "sum", args: [a, b], expectReturn: true }));
}
</script>
```

See [docs/clients.md](docs/clients.md).

## The example application

![Selecting a contact and asking the array to classify it](docs/images/sonar-classify.png)

A sea sonar simulator. The array is a `SocketSignalServer` that keeps the sea state and pushes a
frame to the operators group twenty times a second; the console is a `SocketSignalClient` that
draws two instruments from it — a plan position indicator and a bearing-time recorder. Selecting
a contact and pressing **Classify** is a client-to-server call with a return value; **Active ping**
is another. Nothing on screen is read from local memory, so the demo actually exercises the
library rather than illustrating it. [docs/sonar-demo.md](docs/sonar-demo.md)

## Documentation

| | |
|---|---|
| [Getting started](docs/getting-started.md) | Install, first server, first client |
| [API reference](docs/api-reference.md) | Every public type and method |
| [Protocol](docs/protocol.md) | The wire format, in full |
| [Architecture](docs/architecture.md) | How the pump, codec and dispatch fit together |
| [Performance](docs/performance.md) | What was optimised, measured, and what still allocates |
| [Client SDKs](docs/clients.md) | Python, Go, Node.js and the browser |
| [Sonar demo](docs/sonar-demo.md) | The example application, and its design |

Indonesian: [docs/id/](docs/id/)

## Repository layout

```
src/SocketSignal/             the library
src/SocketSignal.Demo/        console walkthrough; `-- serve` runs a server for the SDK examples
src/SocketSignal.SonarDemo/   the Avalonia sonar console
src/SocketSignal.Benchmarks/  BenchmarkDotNet suite, plus the v1 baseline it measures against
tests/SocketSignal.Tests/     protocol and end-to-end tests
clients/{python,go,nodejs}/   client SDKs
docs/                         documentation, English and Indonesian
```

## Building

```bash
dotnet build SocketSignal.slnx
dotnet test tests/SocketSignal.Tests/SocketSignal.Tests.csproj
dotnet run -c Release --project src/SocketSignal.Benchmarks -- throughput
```

Requires the .NET 10 SDK. On Windows, an `HttpListener` prefix other than localhost needs a URL
ACL reservation.

---

<a name="bahasa-indonesia"></a>

# SocketSignal — Bahasa Indonesia

**Komunikasi realtime dua arah berbasis WebSocket murni untuk .NET 10.** Client dapat memanggil
method di server dan menerima nilai baliknya; server dapat memanggil method di satu client, satu
grup, atau semua client. Protokolnya cukup sederhana sehingga browser bisa bicara langsung dengan
sepuluh baris JavaScript, dan implementasi .NET-nya melakukan encode dan decode tanpa alokasi.

## Instalasi

```bash
dotnet add package SocketSignal
```

## Cara pakai

```csharp
using SocketSignal;

// ---- server ----
var server = new SocketSignalServer("http://localhost:8080/ws/");

// Argumen langsung dideserialisasi menjadi int. Tanpa JsonElement, tanpa boxing.
server.Register<int, int, int>("sum", (client, a, b) => ValueTask.FromResult(a + b));

_ = server.StartAsync();

// ---- client ----
var client = new SocketSignalClient();

client.On<string, string>("serverHello", text =>
{
    Console.WriteLine(text);
    return ValueTask.FromResult("client received");
});

await client.ConnectAsync(new Uri("ws://localhost:8080/ws/"));

int total = await client.CallAsync<int>("sum", 5, 7);        // 12

// ---- server memanggil client ----
await server.BroadcastAsync("serverHello", "semua unit");                  // semua client
await server.SendToClientAsync(someId, "serverHello", "khusus kamu");      // satu client
await server.SendToGroupAsync("operators", "serverHello", "grup ops");     // satu grup
int hasil = await server.CallClientAsync<int>(someId, "double", 21);       // dan menerima balikan
```

Menjalankan contoh:

```bash
dotnet run --project src/SocketSignal.Demo      # server + dua client + pengukuran round-trip
dotnet run --project src/SocketSignal.SonarDemo # konsol sonar Avalonia
```

## Fitur utama

|   | Fitur |
|---|---|
| 1 | Client memanggil method di server |
| 2 | Client memanggil method di server **dan menerima nilai balik** |
| 3 | Server memanggil method ke **semua** client (broadcast) |
| 4 | Server memanggil method ke **satu** client berdasarkan id — dengan nilai balik |
| 5 | Server memanggil method ke satu **grup** client |

Ditambah hal-hal yang dibutuhkan sebuah library realtime agar layak dipercaya memegang koneksi:

- **Timeout pada setiap panggilan**, bukan menggantung selamanya, dan panggilan yang sedang
  berjalan akan gagal dengan jelas ketika socket terputus.
- **Error ikut terkirim.** Handler yang melempar exception muncul sebagai
  `SignalInvocationException` di sisi pemanggil. Method yang tidak dikenal menghasilkan
  `MethodNotFoundException`.
- **Keepalive di level protokol**, sehingga semua SDK — termasuk browser — melihat sinyal
  keaktifan yang sama, plus pemutusan koneksi yang menganggur di sisi server.
- **Auto-reconnect** dengan backoff eksponensial (nonaktif secara bawaan).
- **Grup** tanpa lock yang membersihkan dirinya sendiri saat client terputus.
- **Hook autentikasi** untuk memeriksa permintaan upgrade sebelum menjadi koneksi.
- **State per koneksi** (`client.Items`) dan **statistik langsung** untuk frame, byte, dan panggilan.
- **Backpressure**: peer yang mengirim lebih cepat daripada kemampuan handler akan berhenti dibaca.

Referensi lengkap: [docs/id/api-reference.md](docs/id/api-reference.md).

## Performa

v2 adalah penulisan ulang pada codec dan pompa koneksi. Semua angka di bawah diukur langsung di
repositori ini — jalankan sendiri dengan
`dotnet run -c Release --project src/SocketSignal.Benchmarks`.

**Round-trip RPC lengkap melalui WebSocket loopback** (20.000 panggilan berurutan, .NET 10.0.11,
Windows 11, 8 core logis):

| versi | panggilan/detik | latensi | alokasi per panggilan |
|---|---:|---:|---:|
| v1 | 6.809 | 146,9 µs | 16.379 B |
| **v2** | **9.989** | **100,1 µs** | **3.311 B** |
| | **×1,47** | **−32%** | **−79,8%** |

**Jalur codec, per operasi:**

| operasi | waktu v1 | waktu v2 | alokasi v1 | alokasi v2 |
|---|---:|---:|---:|---:|
| encode satu frame invoke | 1.386 ns | **327 ns** | 1.200 B | **0 B** |
| encode, satu argumen bertipe | — | **269 ns** | — | **0 B** |
| decode satu frame invoke | 1.119 ns | **314 ns** | 1.296 B | **0 B** |
| decode dan baca kedua argumen | — | **609 ns** | — | **0 B** |
| mencari handler sebuah method | 52,8 ns | **21,8 ns** | 64 B | **0 B** |
| membuat correlation id | 118 ns | 130 ns | 88 B | **0 B** |

Penjelasan lengkapnya — termasuk mengapa angka end-to-end tidak nol — ada di
[docs/id/performance.md](docs/id/performance.md).

## SDK client

Protokolnya JSON di atas WebSocket biasa, jadi apa pun yang bisa bicara WebSocket dapat ikut
serta. Tiga SDK disertakan dengan bentuk API yang sama seperti client .NET:

| Bahasa | Instalasi | Dependensi |
|---|---|---|
| [Python](clients/python) | `pip install socketsignal` | `websockets` |
| [Node.js](clients/nodejs) | `npm install @gravicode/socketsignal` | tidak ada — `WebSocket` bawaan Node 22 |
| [Go](clients/go) | `go get github.com/DotNetVibeCoderz/Vibe_Messaging/SocketSignal/clients/go/v2/socketsignal` | `github.com/coder/websocket` |

Browser bahkan tidak memerlukan SDK sama sekali — lihat contoh JavaScript di bagian Inggris di
atas, atau [docs/id/clients.md](docs/id/clients.md).

## Aplikasi contoh

Simulator sonar laut. Array sonar adalah `SocketSignalServer` yang menyimpan keadaan laut dan
mengirim frame ke grup operator dua puluh kali per detik; konsolnya adalah `SocketSignalClient`
yang menggambar dua instrumen dari data itu — sebuah *plan position indicator* dan sebuah
*bearing-time recorder*. Memilih sebuah kontak lalu menekan **Classify** adalah panggilan
client-ke-server yang menunggu nilai balik. Tidak ada satu pun yang dibaca dari memori lokal,
sehingga demo ini benar-benar menguji library, bukan sekadar menggambarkannya.
[docs/id/sonar-demo.md](docs/id/sonar-demo.md)

## Dokumentasi

| | |
|---|---|
| [Memulai](docs/id/getting-started.md) | Instalasi, server pertama, client pertama |
| [Referensi API](docs/id/api-reference.md) | Seluruh tipe dan method publik |
| [Protokol](docs/id/protocol.md) | Format wire secara lengkap |
| [Arsitektur](docs/id/architecture.md) | Bagaimana pompa, codec, dan dispatch bekerja sama |
| [Performa](docs/id/performance.md) | Apa yang dioptimalkan, hasil ukurnya, dan sisa alokasinya |
| [SDK client](docs/id/clients.md) | Python, Go, Node.js, dan browser |
| [Demo sonar](docs/id/sonar-demo.md) | Aplikasi contoh dan rancangan visualnya |

---

## License

MIT. See [LICENSE](LICENSE).

Dibuat oleh **Gravicode Studios**, dipimpin oleh **Kang Fadhil**.

Kalau project ini membantu, boleh traktir pulsa ke saya di:
<https://studios.gravicode.com/products/budax>
