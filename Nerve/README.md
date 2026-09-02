# Nerve ⚡

[![NuGet](https://img.shields.io/nuget/v/Nerve.svg)](https://www.nuget.org/packages/Nerve)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![CI](https://github.com/DotNetVibeCoderz/Vibe_Messaging/actions/workflows/nerve-ci.yml/badge.svg)](https://github.com/DotNetVibeCoderz/Vibe_Messaging/actions/workflows/nerve-ci.yml)

In-process publish/subscribe for .NET 10, shaped like MQTT and about as expensive as a method call.
Topics with `+` and `#` wildcards, retained messages, request/reply, async streams and per-route
statistics — **21 ns and zero allocations** per published message.

**Built by [Gravicode Studios](https://github.com/DotNetVibeCoderz), led by Kang Fadhil.**

[English](#-english) · [Bahasa Indonesia](#-bahasa-indonesia) · [Documentation](docs/) · [Benchmarks](docs/performance.md)

![The agent coordination simulator: one orchestrator, six specialists, impulses in flight](docs/images/agent-sim-flow.png)

---

<a name="english"></a>
## 🇬🇧 English

### Install

```bash
dotnet add package Nerve
```

### Thirty seconds

```csharp
using Nerve;

var nerve = new NerveHub();

// Subscribe. The handler runs on the thread that publishes.
using var reader = nerve.Subscribe<double>("sensor/tank-3/temperature",
    celsius => Console.WriteLine($"{celsius:N1} C"));

// One level with +, everything below with #.
using var all = nerve.Subscribe<double>("sensor/+/temperature", c => Log(c));

await nerve.PublishAsync("sensor/tank-3/temperature", 28.4);

// Ask a question and wait for the answer, over the same topics.
using var responder = nerve.Respond<string, int>("text/length", text => text.Length);
int length = await nerve.RequestAsync<string, int>("text/length", "gravicode");   // 9
```

### What it does

| | |
|---|---|
| **Publish/subscribe** | Routed by topic **and** message type. Synchronous handlers run inline and have finished before `PublishAsync` returns. |
| **Wildcards** | MQTT's `+` and `#`. Matching happens once, when a topic is first seen — a wildcard subscriber costs nothing per message afterwards. |
| **Retained messages** | One value kept per topic and handed to whoever subscribes next, so a late joiner learns the current state without asking. |
| **Request/reply** | `RequestAsync` with a deadline, errors that surface at the call site, and a missing responder reported immediately rather than after the timeout. |
| **Streams** | `IAsyncEnumerable<T>` for consumers that need their own thread. Buffered, drop-oldest, and never able to block a publisher. |
| **Statistics** | Published, delivered, unrouted, errors, drops, routes and subscriptions — counted per route so threads don't share a cache line. |
| **Error isolation** | A subscriber that throws is reported and skipped; the others still get the message. Switch to `Propagate` if you want the failure at the publisher. |

### Measured on this machine

.NET 10.0.11, Windows 11, 8 logical cores. Full output in [docs/benchmark-run.txt](docs/benchmark-run.txt).

| | v1 | v2 | |
|---|---|---|---|
| Publish by topic name | 70.8 ns | **32.4 ns** | 2.2× |
| Publish through a resolved handle | — | **21.0 ns** | 3.4× vs v1 |
| Allocated over 5,000,000 messages | 267 MB | **376 B** | — |
| Gen0 collections over that run | 66 | **0** | — |

Fan-out costs what fan-out costs — 8 subscribers is 83.9 ns, 32 subscribers is 300 ns — and a
wildcard subscriber measures the same as an exact one, because the matching already happened.

### Run it

```bash
dotnet run --project src/Nerve.Demo                        # every feature, end to end
dotnet run --project src/Nerve.AgentSim -- --demo 8        # the simulator below
dotnet test tests/Nerve.Tests                              # 79 tests, about a second
dotnet run --project src/Nerve.Benchmarks -c Release -- --quick
```

### The agent coordination simulator

An orchestrator is handed a list of instructions. It plans each one from the words in it, dispatches
the pieces to six specialists, and folds their answers back into a digest.

No agent holds a reference to another. Every arrow you see is a topic: the orchestrator publishes to
`agents/task/{specialty}` and subscribes to `agents/result/+`, the specialists take work through
`StreamAsync` so they genuinely run in parallel, and the panel itself is just a seventh subscriber
watching the same traffic.

![A finished mission, aggregated from three specialists](docs/images/agent-sim.png)

Every spark on the arbor is a message that was really published. Violet leaving the soma is a
sub-task going out; a specialist's own colour returning is their answer coming back.

```bash
dotnet run --project src/Nerve.AgentSim -- --demo 8
```

### Documentation

| | |
|---|---|
| [Getting started](docs/getting-started.md) | Install, publish, subscribe, and the two mistakes worth avoiding |
| [Patterns](docs/patterns.md) | Wildcards, retained messages, request/reply, streams, waiting |
| [Architecture](docs/architecture.md) | How a publish becomes a handler call, and why it allocates nothing |
| [API reference](docs/api-reference.md) | Every public member |
| [Performance](docs/performance.md) | What was measured, on what, and what it means |
| [Agent simulator](docs/agent-simulator.md) | How the panel is put together |
| [Migrating from v1](docs/migration-v2.md) | What changed and what to do about it |

Bahasa Indonesia: [docs/id/](docs/id/).

---

<a name="bahasa-indonesia"></a>
## 🇮🇩 Bahasa Indonesia

### Instalasi

```bash
dotnet add package Nerve
```

### Tiga puluh detik

```csharp
using Nerve;

var nerve = new NerveHub();

// Berlangganan. Handler berjalan di thread yang mem-publish.
using var pembaca = nerve.Subscribe<double>("sensor/tank-3/temperature",
    celsius => Console.WriteLine($"{celsius:N1} C"));

// Satu level dengan +, seluruh sisanya dengan #.
using var semua = nerve.Subscribe<double>("sensor/+/temperature", c => Catat(c));

await nerve.PublishAsync("sensor/tank-3/temperature", 28.4);

// Bertanya dan menunggu jawabannya, lewat topik yang sama.
using var penjawab = nerve.Respond<string, int>("text/length", teks => teks.Length);
int panjang = await nerve.RequestAsync<string, int>("text/length", "gravicode");   // 9
```

### Apa saja yang tersedia

| | |
|---|---|
| **Publish/subscribe** | Dirutekan berdasarkan topik **dan** tipe pesan. Handler sinkron berjalan langsung dan sudah selesai sebelum `PublishAsync` kembali. |
| **Wildcard** | `+` dan `#` ala MQTT. Pencocokan dilakukan sekali, saat sebuah topik pertama kali muncul — setelah itu subscriber wildcard tidak menambah biaya per pesan. |
| **Retained message** | Satu nilai disimpan per topik dan langsung diberikan kepada subscriber berikutnya, sehingga yang datang terlambat tahu keadaan terkini tanpa perlu bertanya. |
| **Request/reply** | `RequestAsync` dengan batas waktu, error yang muncul di tempat pemanggilan, dan responder yang belum terdaftar dilaporkan seketika — bukan setelah timeout. |
| **Stream** | `IAsyncEnumerable<T>` untuk consumer yang butuh thread sendiri. Ada buffer, membuang yang terlama, dan tidak pernah bisa memblokir publisher. |
| **Statistik** | Published, delivered, unrouted, error, drop, route dan subscription — dihitung per route agar antar-thread tidak berebut cache line. |
| **Isolasi error** | Subscriber yang melempar exception dilaporkan lalu dilewati; subscriber lain tetap menerima pesannya. Gunakan `Propagate` bila ingin kegagalan itu sampai ke publisher. |

### Hasil pengukuran di mesin ini

.NET 10.0.11, Windows 11, 8 core logis. Output lengkapnya di [docs/benchmark-run.txt](docs/benchmark-run.txt).

| | v1 | v2 | |
|---|---|---|---|
| Publish lewat nama topik | 70,8 ns | **32,4 ns** | 2,2× |
| Publish lewat handle yang sudah di-resolve | — | **21,0 ns** | 3,4× dari v1 |
| Alokasi selama 5.000.000 pesan | 267 MB | **376 B** | — |
| Koleksi Gen0 selama run tersebut | 66 | **0** | — |

Fan-out tetap ada biayanya — 8 subscriber 83,9 ns, 32 subscriber 300 ns — dan subscriber wildcard
terukur sama dengan subscriber eksak, karena pencocokannya sudah selesai sebelumnya.

### Menjalankannya

```bash
dotnet run --project src/Nerve.Demo                        # seluruh fitur, dari ujung ke ujung
dotnet run --project src/Nerve.AgentSim -- --demo 8        # simulator di bawah ini
dotnet test tests/Nerve.Tests                              # 79 tes, sekitar satu detik
dotnet run --project src/Nerve.Benchmarks -c Release -- --quick
```

### Simulator koordinasi multi-agent

Sebuah orchestrator menerima daftar instruksi. Ia menyusun rencana dari kata-kata di dalamnya,
mengirim potongan pekerjaannya ke enam agen spesialis, lalu melipat kembali jawaban mereka menjadi
satu ringkasan.

Tidak ada satu agen pun yang memegang referensi ke agen lain. Setiap panah yang terlihat adalah
sebuah topik: orchestrator mem-publish ke `agents/task/{specialty}` dan berlangganan
`agents/result/+`, para spesialis mengambil pekerjaan lewat `StreamAsync` sehingga benar-benar
berjalan paralel, dan panelnya sendiri hanyalah subscriber ketujuh yang menonton lalu lintas yang
sama.

Setiap kilatan pada arbor adalah pesan yang sungguh-sungguh di-publish. Warna ungu yang meninggalkan
soma adalah sub-tugas yang dikirim keluar; warna khas seorang spesialis yang kembali adalah
jawabannya.

```bash
dotnet run --project src/Nerve.AgentSim -- --demo 8
```

### Dokumentasi

Versi Bahasa Indonesia ada di [docs/id/](docs/id/):

| | |
|---|---|
| [Memulai](docs/id/getting-started.md) | Instalasi, publish, subscribe, dan dua kesalahan yang sebaiknya dihindari |
| [Pola pemakaian](docs/id/patterns.md) | Wildcard, retained message, request/reply, stream, menunggu |
| [Arsitektur](docs/id/architecture.md) | Bagaimana sebuah publish menjadi panggilan handler, dan mengapa tanpa alokasi |
| [Referensi API](docs/id/api-reference.md) | Seluruh anggota publik |
| [Performa](docs/id/performance.md) | Apa yang diukur, di mesin apa, dan artinya |
| [Simulator agent](docs/id/agent-simulator.md) | Bagaimana panelnya dirakit |
| [Migrasi dari v1](docs/id/migration-v2.md) | Apa yang berubah dan apa yang perlu dilakukan |

---

## License

MIT. See [LICENSE](LICENSE).

Built by **Gravicode Studios**, led by **Kang Fadhil** — [studios.gravicode.com](https://studios.gravicode.com)
