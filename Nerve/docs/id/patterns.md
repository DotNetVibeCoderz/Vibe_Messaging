# Pola pemakaian

Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.

Semua yang ada di sini dibangun di atas satu jalur dispatch yang dijelaskan di
[architecture.md](architecture.md). Tidak ada satu pun yang menjadi kasus khusus di dalam hub.

## Wildcard

Nerve memakai sintaks filter milik MQTT. Level dipisahkan `/`, `+` mewakili tepat satu level, dan
`#` mewakili sisa level dan hanya sah di posisi terakhir.

```csharp
nerve.Subscribe<double>("sensor/+/temperature", c => { });   // suhu tangki mana pun
nerve.Subscribe<double>("sensor/#", c => { });               // apa pun di bawah sensor, sedalam apa pun
nerve.Subscribe<double>("#", c => { });                      // setiap double, di mana pun
```

| Filter | Mencakup | Tidak mencakup |
|---|---|---|
| `sensor/tank-3/temp` | topik itu saja | selain itu |
| `sensor/+/temp` | `sensor/tank-3/temp` | `sensor/temp`, `sensor/a/b/temp` |
| `sensor/#` | `sensor`, `sensor/a`, `sensor/a/b` | `other/a` |
| `#` | semuanya | — |

Dua hal mengikuti dari cara ini diimplementasikan:

- **Wildcard tidak menambah biaya per pesan.** Pencocokan terjadi saat sebuah topik pertama kali
  di-publish, dan hasilnya disimpan pada route topik itu. Terukur: subscriber eksak 21,7 ns,
  subscriber wildcard pada topik yang sama 23,3 ns.
- **Subscriber eksak berjalan sebelum yang wildcard**, dan setiap langganan menerima pesannya tepat
  satu kali, walaupun beberapa filternya sama-sama cocok.

Mem-publish ke wildcard akan melempar exception — `+` dan `#` hanya bermakna di sisi subscribe:

```csharp
nerve.Publish("sensor/+/temp", 1.0);   // ArgumentException
```

## Retained message

Retained message adalah nilai terkini sebuah topik, disimpan lalu diberikan kepada siapa pun yang
berlangganan berikutnya.

```csharp
await nerve.PublishRetainedAsync("config/mode", "maintenance");

// Kapan pun sesudahnya, seberapa lama pun jaraknya:
using var telat = nerve.Subscribe<string>("config/mode",
    mode => Console.WriteLine(mode));       // langsung mencetak "maintenance"
```

Satu nilai per topik, tergantikan pada setiap retained publish. `ClearRetained<T>(topic)`
melupakannya, dan `TryGetRetained<T>(topic, out var value)` membacanya tanpa berlangganan.

Subscriber wildcard diberi nilai retained dari setiap topik yang cocok saat berlangganan — inilah
yang menjadikannya sebuah daftar personel:

```csharp
// Enam spesialis masing-masing menyimpan statusnya di agents/roster/{nama}.
using var roster = nerve.Subscribe<AgentStatus>("agents/roster/+", status => Tampilkan(status));
// Keenamnya tiba sekaligus, sebelum baris ini dijalankan.
```

Itulah sebabnya panel simulator bisa dibuka kapan saja dan langsung menampilkan enam terminal yang
terisi, bukan papan kosong yang baru terisi seiring pekerjaan berjalan.

## Request dan reply

```csharp
using var penjawab = nerve.Respond<string, int>("text/length", teks => teks.Length);

int panjang = await nerve.RequestAsync<string, int>("text/length", "gravicode");   // 9
```

Responder asinkron menerima cancellation token milik pemanggil:

```csharp
using var penjawab = nerve.Respond<int, string>("agents/+/ping", async (id, token) =>
{
    await Task.Delay(20, token);
    return $"agent {id} sudah bangun";
});

string jawaban = await nerve.RequestAsync<int, string>("agents/writer/ping", 4);
```

Perhatikan wildcard-nya: satu responder bisa menjawab untuk sekeluarga topik.

Tiga perilaku yang perlu diketahui:

- **Responder yang belum terdaftar dilaporkan seketika**, sebagai `NerveNoResponderException`, bukan
  setelah timeout. Menunggu tiga puluh detik hanya untuk tahu bahwa tidak ada yang pernah didaftarkan
  adalah cara termahal menemukan kesalahan perakitan.
- **Exception dari responder muncul di tempat pemanggilan**, bukan di `HandlerError`. `RequestAsync`
  melempar persis apa yang dilempar responder.
- **Balasan pertama yang menang.** Kalau ada dua responder mendengarkan, yang kedua diabaikan dan
  bukan dilempar sebagai error — perlombaan antar-responder adalah kesalahan perakitan yang tidak
  bisa diperbaiki oleh si pemanggil.

Timeout mengikuti `NerveOptions.DefaultRequestTimeout` (30 detik) dan bisa ditentukan per panggilan:

```csharp
await nerve.RequestAsync<int, int>("slow", 1, TimeSpan.FromSeconds(2));
await nerve.RequestAsync<int, int>("slow", 1, Timeout.InfiniteTimeSpan);   // menunggu selamanya
```

Sebuah request adalah pesan biasa yang membawa amplop `NerveRequest<TRequest, TResponse>`, jadi ia
ikut terhitung di statistik dan subscriber biasa pun bisa mengamati lalu lintasnya.

## Stream

Semua cara berlangganan yang lain menjalankan handler di thread milik publisher. Stream adalah
pengecualian yang disengaja: ia menyediakan buffer, dan consumer-lah yang menguras isinya sendiri.

```csharp
await foreach (Reading reading in nerve.StreamAsync<Reading>("sensor/#", cancellationToken: token))
{
    await TulisKeDatabase(reading);
}
```

- Langganannya hidup persis selama enumerasinya — didaftarkan pada `MoveNextAsync` pertama, dan
  di-dispose saat loop-nya berakhir, di-`break`, atau melempar exception.
- Buffer-nya **membuang yang terlama**, berkapasitas 1024 secara bawaan. Publisher tidak pernah
  ditahan oleh consumer yang lambat; yang terbuang dihitung di `NerveStatistics.StreamDrops`.
- Berikan `capacity:` untuk menyesuaikan dengan consumer yang Anda punya.

```csharp
nerve.StreamAsync<Reading>("sensor/#", capacity: 64, cancellationToken: token)
```

Inilah alat yang tepat setiap kali sebuah subscriber melakukan pekerjaan sungguhan — menulis berkas,
memanggil database, memperbarui UI — dan inilah yang dipakai para spesialis di simulator sehingga
keenamnya berjalan sekaligus.

## Menunggu satu pesan

```csharp
string siap = await nerve.WaitForAsync<string>("startup/ready", timeout: TimeSpan.FromSeconds(5));

// Atau menunggu pesan tertentu:
int besar = await nerve.WaitForAsync<int>("readings", v => v > 100, TimeSpan.FromSeconds(5));
```

Ia berlangganan, menunggu, lalu berhenti berlangganan — sehingga urutan start-up dan test tidak
perlu merakit sendiri `TaskCompletionSource` dan mengingat untuk membersihkannya. Kalau tidak ada
yang cocok sampai waktunya habis, ia melempar `TimeoutException`.

## Berlangganan sekali saja

```csharp
nerve.SubscribeOnce<Ready>("startup/ready", _ => Mulai());
```

Menyala pada pesan cocok yang pertama, lalu berhenti berlangganan sendiri. Men-dispose handle-nya
sebelum itu berarti membatalkannya.

## Handle topik yang sudah di-resolve

Ketika sebuah komponen mem-publish ke topik yang sama di dalam loop, resolve-lah sekali saja:

```csharp
private readonly NerveTopic<Reading> _readings = hub.Topic<Reading>("sensor/tank-3");

// ...
_readings.Publish(reading);
```

Itu menghilangkan pencarian di dictionary, satu-satunya biaya per pesan yang tersisa: 32,4 ns lewat
nama menjadi 21,0 ns lewat handle. `NerveTopic<T>` adalah struct yang membungkus dua referensi, jadi
menyimpannya di sebuah field tidak berbiaya apa pun.

Untuk apa pun yang tidak seketat loop, mem-publish lewat nama sudah cukup cepat — handle ini
optimasi, bukan cara normal memakai library-nya.
