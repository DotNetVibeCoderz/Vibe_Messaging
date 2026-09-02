# Referensi API

Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.

Seluruh anggota publik, dalam satu halaman. Namespace `Nerve` kecuali disebutkan lain.

## NerveHub

`sealed class NerveHub : INerveHub, IDisposable`

### Konstruksi

| | |
|---|---|
| `NerveHub()` | Opsi bawaan. |
| `NerveHub(NerveOptions? options)` | `null` berarti pakai bawaan. |
| `static NerveHub Shared { get; }` | Hub untuk seluruh proses, bagi aplikasi yang enggan mengoper hub ke mana-mana. |

### Mem-publish

| | |
|---|---|
| `void Publish<T>(string topic, T message)` | Mengantar tanpa menunggu. Handler sinkron tetap sudah selesai saat ini kembali. |
| `ValueTask PublishAsync<T>(string topic, T message, CancellationToken = default)` | Selesai setelah semua subscriber selesai. Tidak mengalokasikan apa pun bila semuanya sinkron. |
| `ValueTask PublishRetainedAsync<T>(string topic, T message, CancellationToken = default)` | Mem-publish, dan menyimpan pesannya sebagai nilai retained topik itu. |
| `void PublishRetained<T>(string topic, T message)` | Bentuk tanpa-menunggu dari yang di atas. |
| `void ClearRetained<T>(string topic)` | Melupakan retained message sebuah topik. |
| `bool TryGetRetained<T>(string topic, out T message)` | Membacanya tanpa berlangganan. |

`topic` harus berupa topik konkret. Mem-publish ke topik yang mengandung `+` atau `#` melempar
`ArgumentException`.

### Berlangganan

Setiap overload mengembalikan `IDisposable`. Dispose untuk berhenti berlangganan; men-dispose dua
kali tidak berbahaya.

| | |
|---|---|
| `IDisposable Subscribe<T>(string topicFilter, Action<T> handler)` | Sinkron. Bentuk paling murah. |
| `IDisposable Subscribe<T>(string topicFilter, Func<T, ValueTask> handler)` | Asinkron. |
| `IDisposable Subscribe<T>(string topicFilter, Func<T, CancellationToken, ValueTask> handler)` | Asinkron, menerima token milik publisher. |
| `IDisposable Subscribe<T>(string topicFilter, Predicate<T> predicate, Action<T> handler)` | Predikatnya berjalan di dalam dispatch; pesan yang ditolaknya tidak pernah sampai ke handler. |
| `IDisposable SubscribeOnce<T>(string topicFilter, Action<T> handler)` | Menyala sekali, lalu berhenti berlangganan sendiri. |

`topicFilter` boleh memakai `+` dan `#`. Filter yang salah bentuk melempar `ArgumentException` saat
berlangganan.

### Request dan reply

| | |
|---|---|
| `IDisposable Respond<TRequest, TResponse>(string topicFilter, Func<TRequest, CancellationToken, ValueTask<TResponse>> responder)` | Responder asinkron. |
| `IDisposable Respond<TRequest, TResponse>(string topicFilter, Func<TRequest, TResponse> responder)` | Responder sinkron. |
| `Task<TResponse> RequestAsync<TRequest, TResponse>(string topic, TRequest request, TimeSpan? timeout = null, CancellationToken = default)` | Mengirim dan menunggu jawaban pertama. |

`RequestAsync` melempar `NerveNoResponderException` seketika bila tidak ada yang terdaftar,
`TimeoutException` bila tak ada yang menjawab tepat waktu, dan apa pun yang dilempar responder bila
ia gagal. `timeout` mengikuti `NerveOptions.DefaultRequestTimeout`; berikan `Timeout.InfiniteTimeSpan`
untuk menunggu tanpa batas.

### Stream dan menunggu

| | |
|---|---|
| `IAsyncEnumerable<T> StreamAsync<T>(string topicFilter, int capacity = 0, CancellationToken = default)` | Berbuffer, membuang yang terlama. `capacity` 0 berarti `NerveOptions.DefaultStreamCapacity`. |
| `Task<T> WaitForAsync<T>(string topicFilter, Predicate<T>? match = null, TimeSpan? timeout = null, CancellationToken = default)` | Pesan cocok berikutnya. `TimeoutException` bila tak ada yang tiba. |

Langganan milik stream didaftarkan pada `MoveNextAsync` pertama dan di-dispose saat enumerasinya
berakhir, di-`break`, atau melempar exception.

### Pemeriksaan

| | |
|---|---|
| `bool HasSubscribers<T>(string topic)` | Apakah pesan yang di-publish di sini akan sampai ke seseorang. |
| `int SubscriberCount<T>(string topic)` | Berapa banyak, termasuk wildcard. |
| `NerveTopic<T> Topic<T>(string topic)` | Handle yang sudah di-resolve, untuk mem-publish di dalam loop. |
| `NerveStatistics GetStatistics()` | Menjumlahkan penghitung per route. Menelusuri seluruh route, jadi ini panggilan diagnostik. |
| `event Action<NerveError>? HandlerError` | Dipicu untuk setiap kegagalan subscriber. Tidak pernah diizinkan melempar balik ke dispatch. |

### Masa hidup

`Dispose()` membuang seluruh route dan langganan. Handler yang sedang berjalan tidak diinterupsi;
tidak ada lagi yang di-dispatch setelahnya. Memakai hub yang sudah di-dispose melempar
`ObjectDisposedException`.

## NerveTopic&lt;T&gt;

`readonly struct NerveTopic<T>` — sebuah topik yang di-resolve sekali, dari `NerveHub.Topic<T>(topic)`.

| | |
|---|---|
| `string Name { get; }` | Topiknya. |
| `bool HasSubscribers { get; }` | Apakah ada yang mendengarkan. |
| `int SubscriberCount { get; }` | Berapa banyak. |
| `void Publish(T message)` | Tanpa menunggu. |
| `ValueTask PublishAsync(T message, CancellationToken = default)` | Dengan menunggu. |
| `ValueTask PublishRetainedAsync(T message, CancellationToken = default)` | Dan menyimpannya. |
| `IDisposable Subscribe(Action<T> handler)` | Ke topik ini persis. |
| `IDisposable Subscribe(Func<T, ValueTask> handler)` | Ke topik ini persis. |

## NerveOptions

`sealed class NerveOptions`

| | Bawaan | |
|---|---|---|
| `HandlerErrorBehavior ErrorBehavior` | `Isolate` | Apa yang terjadi saat subscriber melempar exception. |
| `bool CollectStatistics` | `true` | Apakah penghitung per route disimpan. Dimatikan berarti empat interlocked increment per publish hilang. |
| `Action<NerveError>? OnError` | `null` | Dipanggil untuk setiap kegagalan handler, berdampingan dengan event-nya. |
| `TimeSpan DefaultRequestTimeout` | 30 dtk | Dipakai bila `RequestAsync` tidak diberi timeout. |
| `int DefaultStreamCapacity` | 1024 | Jumlah pesan yang di-buffer stream sebelum yang terlama dibuang. |

### HandlerErrorBehavior

| | |
|---|---|
| `Isolate` | Laporkan kegagalannya lalu lanjutkan ke subscriber sisanya. |
| `Propagate` | Tinggalkan sisanya dan sampaikan ke yang menunggu publish-nya, sebagai `NerveHandlerException`. `Publish` tetap melapor lewat event, karena tak punya tempat untuk melempar. |

## NerveStatistics

`readonly record struct NerveStatistics`

| | |
|---|---|
| `long Published` | Pesan yang diserahkan ke `Publish` atau `PublishAsync`. |
| `long Delivered` | Pemanggilan handler yang selesai. Satu pesan ke delapan subscriber terhitung delapan. |
| `long Unrouted` | Pesan yang di-publish ke topik yang tidak didengarkan siapa pun. |
| `long Errors` | Pemanggilan handler yang melempar exception. |
| `long StreamDrops` | Pesan yang terbuang karena consumer stream tertinggal. |
| `int Routes` | Pasangan topik dan tipe pesan berbeda yang sudah di-resolve. |
| `int Subscriptions` | Langganan aktif di seluruh route. |
| `int Retained` | Topik yang sedang menyimpan retained message. |

`ToString()` memberi ringkasan satu baris yang cocok untuk log atau status bar.

## NerveError

`readonly record struct NerveError(string Topic, Type MessageType, string SubscriptionFilter, Exception Exception)`

`Topic` adalah topik konkret yang sedang di-publish; `SubscriptionFilter` adalah filter tempat
langganan yang gagal itu didaftarkan — untuk subscriber wildcard keduanya string yang berbeda.

## NerveRequest&lt;TRequest, TResponse&gt;

`sealed class NerveRequest<TRequest, TResponse>` — amplop yang membawa sebuah request. Anda hanya
melihatnya kalau berlangganan langsung ke topik request alih-alih memakai `Respond`.

| | |
|---|---|
| `TRequest Payload { get; }` | Apa yang dikirim pemanggil. |
| `CancellationToken CancellationToken { get; }` | Token milik pemanggil. |
| `bool IsAnswered { get; }` | Apakah sudah ada yang membalas atau menggagalkannya. |
| `bool Reply(TResponse response)` | Menjawab. `true` bila balasan inilah yang sampai ke pemanggil. |
| `bool Fail(Exception exception)` | Menggagalkan request. `true` bila kegagalan inilah yang sampai ke pemanggil. |

## Exception

| | |
|---|---|
| `NerveHandlerException` | Kegagalan subscriber, di bawah `HandlerErrorBehavior.Propagate`. Membawa `Topic`, `MessageType`, `SubscriptionFilter`, dan aslinya sebagai `InnerException`. |
| `NerveNoResponderException` | `RequestAsync` tidak menemukan siapa pun yang terdaftar menjawab. Membawa `Topic`. Sengaja bukan sebuah timeout. |

## TopicFilter

`static class TopicFilter`, namespace `Nerve.Routing` — pencocoknya, dipublikkan supaya bisa Anda
pakai ulang.

| | |
|---|---|
| `bool Matches(string filter, string topic)` | Apakah filter itu mencakup topiknya. |
| `bool Matches(ReadOnlySpan<char> filter, ReadOnlySpan<char> topic)` | Sama, tanpa alokasi. |
| `bool IsWildcard(string filter)` | Apakah mengandung `+` atau `#`. |
| `void ValidateFilter(string filter, string paramName = "topicFilter")` | Melempar exception untuk filter yang salah bentuk. |
| `void ValidateTopic(string topic, string paramName = "topic")` | Melempar exception untuk topik yang mengandung wildcard. |
| `const char Separator = '/'`, `SingleLevel = '+'`, `MultiLevel = '#'` | |

## INerveHub

Bagian yang layak dijadikan dependensi sebuah komponen: `Publish`, `PublishAsync`,
`PublishRetainedAsync`, kedua overload `Subscribe`, `SubscribeOnce`, kedua overload `Respond`,
`RequestAsync`, `StreamAsync`, `WaitForAsync`, `Topic`, `HasSubscribers`, dan `GetStatistics`.
