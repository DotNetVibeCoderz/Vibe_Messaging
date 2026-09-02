# Arsitektur

Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.

Bacalah halaman ini sebelum mengubah apa pun di dalam `src/Nerve/Routing/`.

## Bentuk keseluruhannya

```
NerveHub
  _routes      ConcurrentDictionary<ChannelKey, object>   topik + tipe  ->  Route<T>
  _registries  ConcurrentDictionary<Type, object>         tipe          ->  Registry<T>

Route<T>       satu topik konkret yang membawa satu tipe pesan
  _exact       Subscription<T>[]   subscriber yang menyebut topik ini persis
  _merged      Subscription<T>[]   yang di atas, plus setiap wildcard yang mencakupinya
  _retained    T                   nilai retained topik ini, kalau ada
  counters     long x4             published, delivered, unrouted, errors

Registry<T>    segala hal yang berlaku se-tipe
  _wildcards   Subscription<T>[]   langganan wildcard untuk tipe pesan ini
  _routes      Route<T>[]          setiap route bertipe ini, untuk pemindaian retained
```

`ChannelKey` adalah struct berisi string topik, `Type` pesan, dan sebuah hash yang dihitung sekali
saat konstruksi. Perbandingannya memeriksa hash, lalu tipe secara referensi, baru string secara
ordinal — sehingga sebagian besar ketidakcocokan sudah selesai sebelum perbandingan string berjalan.

## Apa yang terjadi saat publish

```csharp
public ValueTask PublishAsync<T>(string topic, T message, CancellationToken ct = default)
    => GetRoute<T>(topic).PublishAsync(message, ct);
```

Satu pencarian dictionary, lalu `Route<T>.PublishAsync`:

1. Baca array handler-nya (satu volatile read; lihat di bawah).
2. Naikkan `_published`, kalau statistik menyala.
3. Kalau array-nya kosong, naikkan `_unrouted` lalu kembalikan `default`.
4. Telusuri array-nya. Untuk setiap langganan yang masih hidup, panggil handler-nya. Kalau
   `ValueTask` yang dikembalikan sudah selesai, lanjut ke berikutnya.
5. Pada **handler pertama yang benar-benar tertunda**, serahkan sisanya ke sebuah kelanjutan
   `async`.

Langkah 5 adalah inti triknya. Metode yang mengembalikan `ValueTask` tetapi tidak pernah menunggu
tidak mengalokasikan apa pun, sehingga publish ke berapa pun subscriber sinkron tetap bebas alokasi.
Hanya handler yang sungguh-sungguh asinkron yang memunculkan state machine, dan hanya sejak handler
itu ke belakang.

### Mengapa tidak ada boxing

`_routes` dikunci dengan `typeof(T)` sebagai bagian dari `ChannelKey`, jadi nilai yang tersimpan di
sebuah key hanya mungkin berupa `Route<T>` untuk `T` yang sama. Pencariannya memakai
`Unsafe.As<Route<T>>` alih-alih cast, dan array handler-nya bertipe `Subscription<T>[]` — sebuah
pesan `struct` berjalan dari `PublishAsync<T>` sampai ke handler tanpa pernah menjadi `object`.

Itu juga sebabnya mem-publish `int` ke topik yang dilanggan sebagai `long` tidak sampai ke siapa
pun: keduanya key yang berbeda. Itulah harga dari desain ini, dan itu pertukaran yang disengaja.

### Mengapa tidak ada lock

Array handler bersifat immutable. Berlangganan membangun array baru dan menukarnya di bawah lock;
mem-publish membacanya dengan `Volatile.Read` dan tidak mengambil lock sama sekali. Langganan yang
datang di tengah publish akan terjaring oleh publish berikutnya, dan yang di-dispose di tengah
publish langsung berhenti — karena dispatch memeriksa `Subscription<T>.Active` pada setiap handler.

Penghitungnya tinggal di route, bukan di hub, sehingga dua thread yang mem-publish ke topik berbeda
tidak pernah menyentuh cache line yang sama. `GetStatistics()` menjumlahkannya saat diminta.

## Resolusi wildcard

Desain naifnya menyimpan seluruh langganan dalam satu daftar lalu mencocokkan filter pada setiap
publish. Nerve tidak begitu: pencocokan terjadi saat sebuah route pertama kali dimintai daftar
handler-nya, dan jawabannya disimpan.

- **Langganan eksak tinggal di route yang disebutnya.** Berlangganan ke `agents/task/writer`
  membatalkan cache satu route itu saja, bukan yang lain.
- **Langganan wildcard tinggal di `Registry<T>` milik tipenya** dan menaikkan `WildcardVersion`.
- Sebuah route hanya membangun ulang array gabungannya kalau daftar eksaknya sendiri berubah, atau
  `WildcardVersion` tipenya bergerak.
- **Selama `WildcardVersion` masih nol** — belum pernah ada wildcard didaftarkan untuk tipe pesan
  ini — route melewatkan penggabungan sepenuhnya dan langsung menyerahkan array eksaknya.

Pembangunan ulang membaca kedua penanda versi *sebelum* data yang mereka gambarkan. Perubahan yang
tiba di sela-sela itu membuat hasilnya tampak basi dan menyebabkan satu pembangunan ulang tambahan
nanti; ia tidak akan pernah kehilangan sebuah handler.

Akibatnya: subscriber wildcard gratis per pesan, dan biaya memilikinya muncul di tempat yang memang
masuk akal — pada publish pertama ke setiap topik baru, yaitu 265 ns termasuk alokasi string topiknya
sendiri.

## Retained message

`Route<T>` menyimpan field `_retained` yang bertipe, jadi nilai retained pun tidak di-box. Ia dibaca
dan ditulis di bawah lock milik route, karena menugaskan struct yang lebih besar dari satu word
bukan operasi atomik — dan operasi retained cukup jarang sehingga hal itu tidak jadi masalah.

Saat berlangganan:

- Langganan **eksak** ditawari nilai retained dari route-nya sendiri.
- Langganan **wildcard** menelusuri `Registry<T>.Routes` dan ditawari nilai retained dari setiap
  topik yang cocok.

Penelusuran itulah alasan registry menyimpan daftar route sama sekali — dictionary di jalur cepat
dirancang untuk pencarian, bukan penelusuran.

## Request/reply dan stream

Keduanya bukan kasus khusus di jalur dispatch.

**Request/reply** mem-publish `NerveRequest<TRequest, TResponse>` — sebuah class berisi payload dan
sebuah `TaskCompletionSource`. `Respond` adalah langganan terhadap tipe amplop itu; `RequestAsync`
mem-publish satu amplop lalu menunggu completion source-nya. Wildcard, statistik, dan pengamat biasa
semuanya bekerja di topik request secara cuma-cuma, karena semuanya memang cuma pub/sub.

**Stream** adalah langganan yang menulis ke `System.Threading.Channels.Channel<T>` berbatas dengan
`BoundedChannelFullMode.DropOldest`. `TryWrite` pada channel semacam itu tidak pernah memblokir dan
tidak pernah gagal karena penuh, sehingga publisher tidak pernah tertahan; consumer-lah yang
menguras reader-nya. Jumlah yang terbuang dicuplik di sisi masuk — pendekatan yang tidak presisi di
bawah konkurensi, tapi jujur soal consumer yang tertinggal.

## Penanganan error

`Route<T>` memasang `catch` di sekeliling setiap pemanggilan handler. Di bawah `Isolate` yang
menjadi bawaan, kegagalannya dihitung, dilaporkan lewat `NerveHub.ReportHandlerError`, lalu
penelusuran diteruskan. Di bawah `Propagate`, penelusurannya berhenti dan kegagalannya dikembalikan
sebagai `ValueTask` yang faulted membungkus `NerveHandlerException` — dikembalikan, bukan dilempar,
supaya kegagalan sinkron berperilaku sama dengan yang asinkron di tempat pemanggilan.

Pelapor error-nya sendiri dibungkus `catch` kosong. Penangan error yang melempar exception tidak
boleh ikut menjatuhkan publisher yang memicunya.

## Berapa biayanya

| | |
|---|---|
| Per publish | satu pencarian dictionary, satu volatile read, satu panggilan delegate per subscriber |
| Per publish, alokasi | nol, sampai ada handler yang tertunda |
| Per subscribe | satu penyalinan array di route bersangkutan, atau satu di registry untuk wildcard |
| Per topik baru | satu objek route, satu entri registry, dan pencocokan wildcard untuk tiap filter |

Desainnya disetel untuk hub yang langganannya disusun sekali lalu pesannya mengalir sesudah itu.
Beban kerja yang berlangganan dan berhenti berlangganan sesering ia mem-publish akan menghabiskan
sebagian besar waktunya menyalin array — itulah pertukaran yang diambil struktur copy-on-write, dan
di sini itu pertukaran yang tepat.
