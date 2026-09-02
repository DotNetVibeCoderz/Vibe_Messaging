# Simulator IoT Gateway

*BlackHole Messaging — Gravicode Studios, dipimpin oleh Kang Fadhil.*

Panel desktop Avalonia yang menjalankan gateway BlackHole sungguhan dan menyambungkan sebanyak apa
pun perangkat sensor simulasi. **Tidak ada bagian yang dipalsukan**: setiap perangkat adalah
`BlackHoleClient` di atas soket TCP-nya sendiri, menerbitkan ke topiknya sendiri, dan menjawab RPC
yang dikirim gateway balik lewat koneksi yang sama.

![Dua belas perangkat mengalirkan telemetri ke gateway](../images/gateway-panel.png)

## Menjalankannya

```bash
cd BlackHole
dotnet run --project src/BlackHole.IoTGateway              # panel kosong
dotnet run --project src/BlackHole.IoTGateway -- --demo 12 # 12 perangkat, langsung jalan
```

`--demo` memanggil persis perintah yang akan dipakai operator — gateway-nya benar-benar mendengarkan,
perangkatnya benar-benar menyambung. Mode ini ada untuk tangkapan layar dan untuk memperlihatkan
panelnya tanpa harus menjelaskan tombol satu per satu.

Gateway hanya mengikat **loopback**. Semua perangkat berjalan di proses yang sama, jadi tidak ada
alasan membuka port ke jaringan, dan tidak ada alasan membuat Windows bertanya soal firewall.

## Apa yang Anda lihat

**Pita lalu lintas** adalah inti panel ini: grafik perekam multi-kanal yang digambar sebagaimana
perekam kertas menggambarnya. Waktu berjalan dari kanan ke kiri, sampel terbaru berada tepat di bawah
ujung pena di tepi kanan, dan kisi-kisinya punya pembagian mayor dan minor sungguhan. Tiap perangkat
adalah satu pena, dan **warna pena itulah identitas perangkat di seluruh jendela** — penanda barisnya,
sparkline-nya, jejaknya. Pita ini adalah legenda bagi seluruh panel, bukan hiasan di atasnya.

**Lantai pabrik** mendaftar tiap perangkat: id dan areanya, apa yang diukurnya, sparkline 30 detik
terakhirnya, bacaan terkininya, kata peringatan ketika melewati ambang, dan berapa bacaan yang sudah
dikirimnya. Mengklik sebuah baris menebalkan pena perangkat itu di pita dan mengarahkan tombol
perintah kepadanya.

**Rel gateway** mencacah apa yang dilihat server — perangkat, topik, perintah yang dijawab, unggahan
firmware, bita terunggah, dan alarm.

**Aktivitas** mencatat koneksi, perintah, unggahan, dan kegagalan begitu terjadi, dengan penanda
warna per jenis.

## Jalur pustaka yang dijalankan tiap kontrol

| Kontrol | Jalur pustaka yang dijalankan |
|---|---|
| **Add device** / **Add ten** | Satu `BlackHoleClient` baru per perangkat, lewat soket sungguhan |
| **Identify** | RPC server-ke-client — gateway memanggil perangkat |
| **Calibrate** | RPC server-ke-client berargumen, mengubah keadaan perangkat |
| **Pause** | Menghentikan penerbitan tanpa memutus koneksi |
| **Excursion** | Mendorong nilainya ke ambang alarm, supaya Anda bisa melihat panelnya bereaksi |
| **Firmware** | Mengunggah 4 MiB sebagai stream BlackHole, terpotong-potong, dengan progres |
| **Sample rate** | 1–120 Hz per perangkat, diterapkan langsung |

![Unggahan firmware selesai sementara telemetri terus mengalir](../images/gateway-streaming.png)

Tangkapan layar di atas diambil di tengah demo: `room-3` mengunggah 4 MiB sebagai stream
(`UPLOADS 1`, `UPLOADED 4.0 MiB`) sementara dua belas perangkat tetap menerbitkan pada 93 pesan/detik
dan jejaknya tidak pernah terhenti. Itulah maksud panel ini — semua pola hidup berdampingan di satu
koneksi.

## Bagaimana ia dibangun

```
Simulation/
  SensorKind.cs        Enam profil sensor: rentang, hanyutan, derau, ambang
  Reading.cs           Satu bacaan, 20 bita, tata letak biner tetap
  SimulatedDevice.cs   BlackHoleClient sungguhan yang menerbitkan dan melayani RPC
  GatewayHost.cs       BlackHoleServer sungguhan yang menerima dan memerintah

Controls/
  TraceBuffer.cs       Ring bebas kunci: loop baca menulis, thread UI membaca
  StripChart.cs        Pita multi-kanal
  Sparkline.cs         Jejak yang sama pada skala baris

ViewModels/           MainViewModel, DeviceViewModel
Views/                MainWindow
Theme.axaml           Token warna dan tipografi
Styles.axaml          Permukaan kontrol
```

### Bacaan itu 20 bita, bukan JSON

```csharp
public readonly record struct Reading(long TimestampMs, double Value, int Sequence)
{
    public const int Size = 20;
}
```

Gateway yang menerima puluhan ribu bacaan per detik tidak sanggup menserialisasi satu objek per
sampel. Inilah bentuk yang memang dirancang untuk BlackHole: struct kecil berukuran tetap yang
ditulis langsung ke dalam frame. Cap waktunya memakai milidetik Unix supaya perangkat dan gateway di
mesin berbeda tetap sepakat tanpa berbagi tipe.

### Dua jam yang terpisah

Perangkat menerbitkan sampai 500 Hz masing-masing. Panel menggambar ulang 30 kali per detik.
Keduanya dipisahkan:

1. Loop baca gateway memanggil `TraceBuffer.Add` — satu kursor interlocked dan satu penulisan array,
   tanpa kunci, tanpa alokasi.
2. `DispatcherTimer` 33 ms menyegarkan baris, menghitung ulang laju, menguras log, dan menggerakkan
   grafik.

Biaya render jadi datar, entah 4 perangkat pada 2 Hz atau 40 pada 200 Hz. Mengikat langsung ke loop
baca akan membanjiri dispatcher dan membekukan jendela — inilah pola yang layak ditiru untuk UI
berlaju tinggi mana pun.

### Lalu lintas mengalir satu arah per jalur

Versi sebelumnya menyubscribe setiap koneksi ke `plant/#` supaya gateway "melihat semuanya". Akibatnya
broker menyebarkan tiap bacaan kembali ke dua belas perangkat — masing-masing lalu tertahan menulis
ke peer yang juga sedang tertahan menulis, dan seluruh lantai macet saat penyambungan.

Perbaikannya adalah aturan yang layak diingat: **telemetri naik, perintah turun, dan tidak ada jalur
yang membawa keduanya.** Gateway membaca bacaan dari router milik tiap koneksi; perangkat hanya
menyubscribe `control/all`.

## Desainnya

Panel ini dimodelkan sebagai **perekam grafik multi-kanal di dalam lemari instrumen baja**.

- **Warna adalah informasi, bukan hiasan.** Enam warna pena diambil dari set tinta yang dulu
  disertakan perekam semacam itu, dan sebuah pena menandai satu perangkat. Satu-satunya merah pekat
  di layar adalah keadaan alarm.
- **Bahnschrift** untuk label — turunan DIN 1451 dari Microsoft, standar huruf untuk mesin dan panel
  kendali Jerman. **Cascadia Mono** untuk semua angka, supaya digitnya tetap sejajar saat nilainya
  berubah.
- **Enam pena, disengaja.** Lebih dari enam jejak dalam satu grafik membuat semuanya tak terbaca;
  perangkat ketujuh memakai ulang pena pertama dan dibedakan lewat barisnya.
- **Keadaan kosong adalah undangan**, bukan permintaan maaf: "Start the gateway, then add a sensor."

## Menggunakan ulang polanya

`GatewayHost` adalah model ringkas untuk gateway sungguhan:

- Loopback atau endpoint tertentu, bukan membabi buta ke semua antarmuka
- Satu `SharedHeaderCache` untuk semua koneksi, karena perangkat berbagi kosakata topik
- Pencacah interlocked yang dibaca UI lewat timer — gateway tidak boleh melambat hanya karena diamati
- `StreamReceiver` per koneksi, supaya dua perangkat yang mengunggah `firmware.bin` tidak saling
  merusak
- Batas stream disetel, supaya perangkat bermasalah tidak bisa menghabiskan memori proses

---

*Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.*
