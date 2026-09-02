# Memulai

Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.

## Instalasi

```bash
dotnet add package Nerve
```

Nerve menyasar .NET 10 dan tidak punya dependensi apa pun.

## Satu hub

```csharp
using Nerve;

var nerve = new NerveHub();
```

Satu hub untuk satu aplikasi adalah hal yang wajar. Daftarkan sebagai singleton dan terima
`INerveHub` di konstruktor, supaya sebuah test bisa menyerahkan hub miliknya sendiri:

```csharp
services.AddSingleton<NerveHub>();
services.AddSingleton<INerveHub>(sp => sp.GetRequiredService<NerveHub>());
```

Ada juga `NerveHub.Shared`, satu instance untuk seluruh proses, bagi aplikasi yang cukup kecil
sehingga mengoper hub ke mana-mana justru lebih merepotkan daripada manfaatnya.

## Publish dan subscribe

```csharp
using IDisposable pembaca = nerve.Subscribe<double>("sensor/tank-3/temperature",
    celsius => Console.WriteLine($"{celsius:N1} C"));

await nerve.PublishAsync("sensor/tank-3/temperature", 28.4);
```

`Subscribe` mengembalikan langganannya. **Dispose langganan itu** — langganan yang tidak pernah
di-dispose akan menahan handler-nya, beserta semua yang ditangkap handler itu, selama hub-nya masih
hidup.

Ada empat bentuk handler yang diterima:

```csharp
nerve.Subscribe<T>(topic, value => { });                        // sinkron
nerve.Subscribe<T>(topic, async value => await Kerjakan(value)); // ValueTask
nerve.Subscribe<T>(topic, (value, token) => Kerja(value, token));// dengan token milik publisher
nerve.Subscribe<T>(topic, v => v.Priority > 3, value => { });    // disaring predikat
```

Yang sinkron jauh lebih murah dari yang lain: ia berjalan dengan satu panggilan delegate saja, tanpa
mesin `ValueTask` sama sekali.

## Publish, ditunggu atau tidak

```csharp
await nerve.PublishAsync(topic, message);   // selesai setelah semua subscriber selesai
nerve.Publish(topic, message);              // kembali tanpa menunggu
```

Keduanya mengantar pesan dengan cara yang sama. Bedanya hanya soal menunggu: `Publish` adalah
`PublishAsync` yang hasilnya dibuang, dan apa pun yang dilempar handler asinkron setelahnya
dilaporkan lewat `HandlerError` — bukan menjadi unobserved task exception.

## Dua kesalahan yang sebaiknya dihindari

### 1. Handler berjalan di thread milik publisher

Nerve tidak menjalankan thread apa pun. Handler sinkron sudah selesai sebelum `Publish` kembali, dan
`await Task.Delay(500)` di dalam sebuah handler berarti lima ratus milidetik yang dihabiskan
publisher untuk menunggu.

Itu default yang tepat — justru itulah yang membuat satu publish berharga 21 ns — tapi artinya
pekerjaan lambat tidak pantas ditaruh di dalam handler. Gunakan stream, yang memberi consumer loop
dan buffer-nya sendiri:

```csharp
await foreach (Reading reading in nerve.StreamAsync<Reading>("sensor/#", cancellationToken: token))
{
    await TulisKeDatabase(reading);   // seberapa lama pun, publisher tidak ikut menunggu
}
```

### 2. Topik dan tipe dua-duanya harus cocok

Sebuah route adalah topik *dan* tipe pesan. Dua baris berikut tidak saling bicara:

```csharp
nerve.Subscribe<int>("counter", v => { });
nerve.Publish("counter", 42L);   // long, bukan int - tidak ada yang menerimanya
```

Tidak ada exception, karena tidak ada cara membedakan topik multi-tipe yang disengaja dari salah
ketik. Kalau sebuah pesan terasa hilang, periksa tipenya lebih dulu —
`nerve.GetStatistics().Unrouted` menghitung setiap pesan yang di-publish ke topik yang tidak
didengarkan siapa pun.

## Error

Secara bawaan, subscriber yang melempar exception akan dilaporkan lalu dilewati, dan subscriber
sisanya tetap menerima pesannya:

```csharp
nerve.HandlerError += error =>
    logger.LogError(error.Exception, "handler pada {Filter} gagal untuk {Topic}",
        error.SubscriptionFilter, error.Topic);
```

Kalau Anda lebih suka kegagalannya sampai ke yang mem-publish:

```csharp
var nerve = new NerveHub(new NerveOptions { ErrorBehavior = HandlerErrorBehavior.Propagate });
```

Dengan begitu `await PublishAsync(...)` melempar `NerveHandlerException` dan subscriber sisanya
dilewati. `Publish` tetap melapor lewat event, karena tidak punya tempat untuk melempar.

## Memeriksa apa yang sedang terjadi

```csharp
NerveStatistics stats = nerve.GetStatistics();
Console.WriteLine(stats);
// published=14 delivered=16 unrouted=1 errors=1 drops=0 routes=10 subs=0
```

`Unrouted` yang paling berguna: ia menghitung pesan yang tidak didengarkan siapa pun, dan itu hampir
selalu berarti salah ketik topik atau tipe yang tidak cocok.

## Selanjutnya

- [patterns.md](patterns.md) — wildcard, retained message, request/reply, stream
- [architecture.md](architecture.md) — apa yang sebenarnya terjadi saat publish
