# Migrasi dari v1

Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.

v1 adalah satu berkas: sebuah `ConcurrentDictionary<string, List<HandlerWrapper>>`, handler yang
di-box di balik `Func<object, Task>`, dan uji `is T` di dalam masing-masingnya. v2 mempertahankan
bentuk API-nya dan mengganti seluruh isinya. README v1 disimpan di [legacy-readme.md](../legacy-readme.md).

## Yang tetap bisa dikompilasi

```csharp
var nerve = new NerveHub();
using var sub = nerve.Subscribe<double>("sensor/suhu", suhu => Console.WriteLine(suhu));
await nerve.PublishAsync("sensor/suhu", 25.5);
nerve.Publish("chat/general", "Halo dunia!");
```

Semuanya tidak berubah. `Subscribe` tetap mengembalikan `IDisposable`, `Publish` tetap tanpa
menunggu, dan handler tetap berjalan di thread milik publisher.

## Yang berubah

### `PublishAsync` mengembalikan `ValueTask`, bukan `Task`

```csharp
await nerve.PublishAsync("t", 1);        // tidak berubah
Task t = nerve.PublishAsync("t", 1);     // tidak lagi bisa dikompilasi
```

Menunggunya tidak terpengaruh. Kalau Anda memang butuh `Task`, panggil `.AsTask()`.

Inilah yang membuat publish ke subscriber sinkron tidak mengalokasikan apa pun.

### Handler `Func<T, Task>` kini menjadi `Func<T, ValueTask>`

```csharp
// v1
nerve.Subscribe<double>("t", async v => { await Kerjakan(v); });
```

Lambda itu tetap bisa dikompilasi — lambda `async` menyesuaikan diri dengan apa pun yang diminta
parameternya. Yang tidak lagi bisa adalah mengoper **method group** yang mengembalikan `Task`:

```csharp
nerve.Subscribe<double>("t", HandleAsync);            // kalau HandleAsync mengembalikan Task: tidak
nerve.Subscribe<double>("t", async v => await HandleAsync(v));   // ya
```

Lebih baik lagi, ubah handler-nya supaya mengembalikan `ValueTask`.

Overload yang mengembalikan `Task` dihapus, bukan dipertahankan berdampingan dengan yang `ValueTask`,
karena keberadaan keduanya membuat **setiap lambda async menjadi ambigu** — `CS0121` pada cara
paling umum menulis sebuah handler. Satu overload lebih berharga daripada kompatibilitasnya.

### Exception handler tidak lagi dicetak ke konsol

v1 menulis `[Nerve Error] ...` ke `Console`. v2 melaporkannya:

```csharp
nerve.HandlerError += error =>
    logger.LogError(error.Exception, "handler pada {Filter} gagal untuk {Topic}",
        error.SubscriptionFilter, error.Topic);

// atau saat konstruksi:
var nerve = new NerveHub(new NerveOptions { OnError = e => logger.LogError(e.Exception, "...") });
```

Selebihnya perilakunya sama: kegagalannya diisolasi dan subscriber sisanya tetap menerima pesannya.
Kalau Anda lebih suka kegagalan itu sampai ke publisher, berikan
`ErrorBehavior = HandlerErrorBehavior.Propagate`.

### Topik divalidasi

Mem-publish ke topik yang mengandung `+` atau `#` kini melempar `ArgumentException`, begitu pula
berlangganan ke filter salah bentuk seperti `a/#/b`. Di v1 keduanya cuma string biasa yang diam-diam
tidak cocok dengan apa pun.

Validasinya terjadi saat sebuah route pertama kali dibuat, bukan pada setiap publish, jadi tidak
menambah biaya per pesan.

### Topik kosong tidak lagi bocor

v1 tidak pernah menghapus entri sebuah topik setelah langganan terakhirnya berhenti, sehingga proses
yang memakai banyak topik berumur pendek akan terus membengkak. v2 juga menyimpan satu route per
topik — tetapi tanpa alokasi `List` per topik dan tanpa lock, dan route itulah yang menyimpan hasil
resolusi wildcard. Kalau Anda membuat topik berbeda tanpa batas, buatlah hub baru untuk tiap
angkatan, jangan satu hub untuk seumur hidup proses.

## Yang baru

| | |
|---|---|
| **Wildcard** | `+` dan `#`, dicocokkan sekali per topik, bukan sekali per pesan. |
| **Retained message** | `PublishRetainedAsync`, diantar ke siapa pun yang berlangganan berikutnya. |
| **Request/reply** | `Respond` dan `RequestAsync`, lengkap dengan batas waktu dan error responder-tak-ada. |
| **Stream** | `StreamAsync` untuk consumer yang butuh thread sendiri. |
| **Menunggu** | `WaitForAsync` dan `SubscribeOnce`. |
| **Statistik** | `GetStatistics()`. |
| **Predikat** | `Subscribe<T>(topic, predicate, handler)`. |
| **Handle topik** | `Topic<T>(name)`, untuk melewati pencarian saat mem-publish di dalam loop. |
| **`INerveHub`** | Untuk injeksi lewat konstruktor. |

## Apa yang didapat

Diukur di mesin yang disebut pada [performance.md](performance.md), beban kerja yang sama lewat
keduanya:

| | v1 | v2 |
|---|---|---|
| Publish lewat nama topik | 70,8 ns | 32,4 ns |
| Alokasi selama 5.000.000 pesan | 267 MB | 376 B |
| Koleksi Gen0 | 66 | 0 |

Angka alokasinya yang menarik. v1 mem-box setiap pesan bertipe nilai, mengalokasikan state machine
per pemanggilan handler, dan menyalin daftar handler ke array baru pada setiap publish. v2 tidak
melakukan satu pun dari ketiganya.

## Cara meningkatkan versi

1. `dotnet add package Nerve` — v2 menyasar .NET 10.
2. Perbaiki setiap `Task t = PublishAsync(...)` menjadi `await` atau `.AsTask()`.
3. Perbaiki setiap handler berupa method group yang mengembalikan `Task`.
4. Sambungkan `HandlerError` ke logger Anda; sebelumnya Anda bergantung pada `Console`.
5. Periksa topik yang mengandung `+` atau `#` — sekarang melempar exception, bukan diam-diam tidak
   cocok.
