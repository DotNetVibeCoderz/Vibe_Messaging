# Performa

Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.

Setiap angka di sini hasil pengukuran. Keluaran mentahnya:
[../benchmark-run.txt](../benchmark-run.txt) untuk harness beban berkelanjutan, dan
[../benchmark-micro.txt](../benchmark-micro.txt) untuk BenchmarkDotNet.

## Mesinnya

```
Intel Core i7-8650U, 1,90 GHz (Kaby Lake R), 4 core fisik / 8 core logis
Windows 11 25H2, .NET 10.0.11, X64 RyuJIT x86-64-v3
```

Sebuah laptop empat core, bukan server. Anggap rasionya sebagai temuannya, dan angka mutlaknya
sebagai batas bawah.

## Angka utama

Beban berkelanjutan, 5.000.000 pesan, satu topik, satu subscriber:

| | v1 | v2 |
|---|---|---|
| Publish lewat nama topik | 70,8 ns · 14,1 juta msg/dtk | **32,4 ns · 30,8 juta msg/dtk** |
| Publish lewat handle yang sudah di-resolve | — | **21,0 ns · 47,5 juta msg/dtk** |
| Alokasi selama run tersebut | 267,0 MB | **376 B** |
| Koleksi Gen0 | 66 | **0** |

376 byte itu milik harness-nya sendiri. Jalur publish-nya tidak mengalokasikan apa pun.

## Ke mana waktunya pergi

Dari run BenchmarkDotNet, mem-publish sebuah struct 16 byte:

| Subscriber | v1 | v2 lewat nama | v2 lewat nama, tanpa statistik | v2 lewat handle |
|---|---|---|---|---|
| 0 | 19,9 ns | 40,9 ns | 31,4 ns | **16,3 ns** |
| 1 | 68,4 ns | 43,2 ns | 41,8 ns | **22,3 ns** |
| 8 | 236,2 ns | 107,8 ns | **78,7 ns** | 85,2 ns |

Ada tiga hal yang tampak dari tabel itu, dan salah satunya tidak menguntungkan kami.

### Publish lewat nama membayar hashing string

`ByName` dikurangi `ByHandle` sekitar 22 ns di setiap jumlah subscriber. Itulah pencarian
dictionary-nya, dan sebagian besarnya adalah proses hashing string topik — `ChannelKey` menyimpan
hash-nya, tapi key baru dibangun pada setiap publish, jadi string-nya di-hash setiap kali.

Untuk itulah `Topic<T>(name)` ada. Resolve sekali, simpan handle-nya, dan biayanya hilang:

```csharp
private readonly NerveTopic<Reading> _readings = hub.Topic<Reading>("sensor/tank-3");
```

### Statistik berbiaya sekitar 3 ns per penghitung

`ByName` dikurangi `ByNameNoStatistics` adalah 9,6 ns pada nol subscriber, 1,4 ns pada satu, dan
29,1 ns pada delapan. Penghitungnya satu increment per publish ditambah satu per pengantaran, jadi
delapan subscriber berarti sembilan operasi interlocked.

Pada delapan subscriber, itu cukup untuk membuat `ByNameNoStatistics` (78,7 ns) mengungguli
`ByHandle` (85,2 ns) — mematikan statistik lebih hemat daripada melewati pencarian dictionary. Kalau
Anda melakukan fan-out lebar dan tidak butuh penghitungnya:

```csharp
new NerveHub(new NerveOptions { CollectStatistics = false })
```

### v2 lebih lambat dari v1 saat mem-publish ke ruang kosong

Dengan **tanpa subscriber**, v1 berada di 19,9 ns dan v2 lewat nama di 40,9 ns — v2 dua kali lebih
lambat.

v1 gagal menemukan entri di dictionary-nya lalu langsung kembali. v2 me-resolve sebuah route,
membuatnya bila topik ini baru, dan menghitung pesannya sebagai published sekaligus unrouted. Ia
dioptimalkan untuk pesan yang benar-benar diantar, dan mem-publish ke ruang kosong adalah satu-satunya
kasus di mana melakukan pembukuan dengan benar lebih mahal daripada tidak melakukannya.

Lewat handle angkanya 16,3 ns, lebih cepat daripada v1 bahkan di kasus ini. Kalau ada jalur panas
yang mem-publish ke topik yang biasanya kosong, simpanlah sebuah handle, atau periksa
`HasSubscribers<T>` lebih dulu.

## Fan-out

Satu publisher, satu topik, N subscriber, lewat handle:

| Subscriber | | |
|---|---|---|
| 1 | 21,2 ns | 47,2 juta msg/dtk |
| 2 | 33,0 ns | 30,3 juta msg/dtk |
| 8 | 83,9 ns | 11,9 juta msg/dtk |
| 32 | 300,2 ns | 3,3 juta msg/dtk |

Kira-kira 9 ns untuk setiap subscriber tambahan, dan tanpa alokasi pada lebar berapa pun. Fan-out
memang ada biayanya: pekerjaannya adalah memanggil handler-handler itu.

## Wildcard

Perbandingan yang adil adalah satu subscriber eksak melawan satu subscriber wildcard pada topik yang
sama, dan itulah yang diukur harness beban berkelanjutan:

| | | |
|---|---|---|
| subscriber eksak | 21,7 ns | 46,2 juta msg/dtk |
| satu subscriber wildcard | 23,3 ns | 42,9 juta msg/dtk |

Keduanya masih dalam rentang derau satu sama lain, dan sama-sama bebas alokasi. Pencocokannya sudah
terjadi saat route-nya di-resolve; ketika sebuah pesan di-publish, yang tersisa hanyalah menelusuri
sebuah array.

> Tabel `WildcardBenchmarks` di BenchmarkDotNet tampak lebih buruk — 40,9 ns lawan 49,6 ns — tetapi
> kasus itu punya **dua** subscriber wildcard yang cocok melawan satu subscriber eksak, jadi
> selisihnya adalah pengantaran kedua, bukan biaya wildcard. Angka harness di ataslah yang
> setara-untuk-setara.

Biaya sebuah wildcard muncul di tempat yang memang masuk akal, yaitu pada publish pertama ke setiap
topik baru:

| | | |
|---|---|---|
| 100.000 topik berbeda, dingin | 265,0 ns per topik | 5,3 MB total |

Itu sudah termasuk mengalokasikan tiap string topik, objek route-nya, dan menjalankan setiap filter
wildcard terhadapnya. Setelah pesan pertama, topik itu berharga 21 ns seperti topik lainnya.

`TopicFilter.Matches` sendirian berada di 37,2 ns untuk filter tiga level, tanpa alokasi.

## Konkurensi

Delapan publisher, masing-masing satu topik, di delapan core logis:

| | | |
|---|---|---|
| 8 publisher | 40,7 ns | 24,6 juta msg/dtk agregat |

Throughput agregatnya lebih rendah daripada 47 juta milik publisher tunggal, di laptop empat core
fisik dengan hyperthreading — core-nya memang dipakai bersama. Yang penting adalah tidak ada yang
menjadi antrean: tidak ada lock di jalur publish, dan penghitungnya tinggal per route sehingga dua
topik tidak pernah berbagi cache line.

## Pola-polanya

| | | |
|---|---|---|
| Satu putaran request/reply | 120,0 ns | 208 B |
| Subscribe, publish, unsubscribe | 211,2 ns | 256 B |

Request/reply mengalokasikan karena memang harus: satu amplop dan satu `TaskCompletionSource` per
panggilan. 120 ns untuk satu putaran penuh lewat mesin pub/sub adalah harga karena ia dibangun di
atas topik, bukan di sebelahnya — dan itu memberi wildcard, statistik, serta keterlihatan secara
cuma-cuma.

Angka subscribe/unsubscribe adalah biaya copy-on-write — dua penyalinan array. Itulah pertukaran
yang diambil desain ini: dispatch tidak mengambil lock, dan pendaftaranlah yang membayarnya. Cocok
untuk hub yang langganannya disusun sekali; keliru untuk beban kerja yang mengganti langganan
secepat ia mem-publish.

## Mengulanginya

```bash
dotnet run --project src/Nerve.Benchmarks -c Release -- --quick            # ~1 menit
dotnet run --project src/Nerve.Benchmarks -c Release -- --quick legacy     # satu tahap saja
dotnet run --project src/Nerve.Benchmarks -c Release -- --micro            # ~10 menit
dotnet run --project src/Nerve.Benchmarks -c Release -- --micro --job short
dotnet run --project src/Nerve.Benchmarks -c Release -- --filter "*Wildcard*"
```

Hub v1 disimpan apa adanya di `src/Nerve.Benchmarks/Baseline/LegacyHub.cs` supaya perbandingannya
melawan kode sungguhan, bukan perkiraan. Jangan dirapikan — biayanya justru intinya.

Kalau Anda mengubah sesuatu di jalur publish, jalankan ulang keduanya lalu perbarui halaman ini —
atau katakan terus terang bahwa angkanya berasal dari sebelum perubahan itu. Jangan menerka angka
benchmark.
