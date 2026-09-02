# Simulator koordinasi agent

Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.

```bash
dotnet run --project src/Nerve.AgentSim                 # kosong, menunggu Anda mengirim instruksi
dotnet run --project src/Nerve.AgentSim -- --demo 8     # memasukkan delapan instruksi
```

![Lima spesialis bekerja, impuls melaju dua arah](../images/agent-sim-flow.png)

## Apa ini

Sebuah orchestrator menerima instruksi. Ia menyusun rencana dari kata-kata di dalamnya, mengirim
potongan pekerjaannya ke enam spesialis, lalu melipat kembali jawaban mereka menjadi satu ringkasan.

Ini ada untuk menjawab pertanyaan yang tidak bisa dijawab benchmark: *seperti apa sebenarnya sistem
yang dibangun di atas Nerve?* Karena itu, seluruhnya dibangun dengan satu batasan: **tidak ada satu
agen pun yang memegang referensi ke agen lain.** Setiap panah di layar adalah sebuah topik.

## Topik-topiknya

| Topik | Membawa | Di-publish oleh | Dilanggan oleh |
|---|---|---|---|
| `agents/mission/inbox` | `Mission` | panelnya | orchestrator, sebagai stream |
| `agents/task/{specialty}` | `SubTask` | orchestrator | spesialis itu, sebagai stream |
| `agents/result/{specialty}` | `SubResult` | spesialis itu | orchestrator, lewat `agents/result/+` |
| `agents/mission/complete` | `MissionDigest` | orchestrator | panelnya |
| `agents/roster/{specialty}` | `AgentStatus` | spesialis itu, **retained** | panelnya, lewat `agents/roster/+` |
| `agents/{specialty}/capability` | request/reply | — | spesialis itu, lewat `agents/+/capability` |

Spesialis ketujuh akan langsung menerima pekerjaan begitu ia berlangganan ke topik tugasnya sendiri.
Tidak ada satu baris pun di orchestrator yang perlu berubah.

## Mengapa para spesialis memakai stream

Handler sebuah langganan berjalan di thread milik publisher. Kalau seorang spesialis tidur di
dalamnya, ia akan tidur di atas loop dispatch milik orchestrator, dan keenam agennya akan bergantian
alih-alih bekerja sekaligus.

Maka setiap spesialis mengambil pekerjaan lewat `StreamAsync`, yang memberinya buffer dan loop-nya
sendiri:

```csharp
await foreach (SubTask task in _hub.StreamAsync<SubTask>(Topics.TaskFor(Specialty), 256, token))
{
    SubResult result = await WorkAsync(task, token);
    await _hub.PublishAsync(_resultTopic, result, token);
}
```

Orchestrator melakukan kebalikannya untuk hasil, dan itu disengaja. Melipat satu jawaban ke dalam
dictionary hanya beberapa mikrodetik, jadi ia berjalan langsung di thread milik spesialis —
menyerahkannya ke thread lain justru lebih mahal daripada hematnya.

Kontras itulah inti dari contoh ini: **pakai stream ketika pekerjaannya lambat, pakai subscribe
ketika tidak.**

## Mengapa roster-nya retained

Para spesialis mem-publish statusnya dengan `PublishRetainedAsync`. Ketika panel berlangganan ke
`agents/roster/+`, keenam nilai retained-nya diantar sebelum `Subscribe` kembali — sehingga keenam
terminalnya terisi tepat saat jendelanya terbuka, bukan terisi perlahan seiring pekerjaan berjalan.

## Bagaimana orchestrator menyusun rencana

`Orchestrator.Plan` mencocokkan kata kunci di dalam instruksinya:

| Kata | Langkah yang ditambahkan |
|---|---|
| benchmark, latency, throughput, measure, profile, allocation | Analyst, Engineer |
| translate, bahasa, indonesian, localise | Translator |
| bug, crash, regression, fix, refactor, leak, race | Engineer |
| survey, compare, research, evaluate, prior art, sources | Researcher |
| draft, brief, announce, write, guide, readme, post, summary | Writer |
| *tidak ada yang cocok* | Researcher, Writer |

Setiap rencana lalu ditutup oleh Critic — satu-satunya langkah yang tidak bergantung pada
instruksinya.

Pencocokan kata kunci, dan itu disengaja. Inti simulasinya adalah koordinasinya, dan rencana yang
kelihatan berubah mengikuti pilihan kata membuat pengirimannya terbaca di layar. Orchestrator
sungguhan akan menaruh sebuah model di balik metode itu tanpa mengubah apa pun yang lain.

## Panelnya

![Satu misi selesai, diagregasi dari tiga spesialis](../images/agent-sim.png)

Panel ini adalah subscriber ketujuh. Ia tidak memegang referensi ke agen mana pun — lima langganan
wildcard adalah seluruh sambungannya ke simulasi:

```csharp
hub.Subscribe<Mission>(Topics.MissionInbox, _inbox.Enqueue);
hub.Subscribe<SubTask>("agents/task/+", _inbox.Enqueue);
hub.Subscribe<SubResult>("agents/result/+", _inbox.Enqueue);
hub.Subscribe<MissionDigest>(Topics.MissionComplete, _inbox.Enqueue);
hub.Subscribe<AgentStatus>("agents/roster/+", _inbox.Enqueue);
```

Handler-handler itu berjalan di thread para agen, jadi **tidak satu pun menyentuh koleksi yang
di-bind**. Mereka menjatuhkan pesannya ke sebuah `ConcurrentQueue`, dan timer 33 ms di thread UI
mengurasnya lalu menerapkan satu kumpulan per frame. Mem-bind koleksi langsung ke handler pesan
adalah kesalahan yang sama dengan memperbarui UI dari loop pembacaan socket.

### Membaca arbor-nya

Soma milik orchestrator ada di sebelah kiri, dengan dendrit menghadap ke antrean misi — sisi tempat
instruksi berdatangan. Enam akson memancar ke para spesialis.

- **Impuls ungu yang meninggalkan soma** adalah `SubTask` yang benar-benar di-publish.
- **Impuls berwarna khas seorang spesialis yang kembali** adalah `SubResult` miliknya.
- **Akson yang menyala** berarti spesialis itu sedang bekerja.
- **Garis-garis kecil di bawah terminal** adalah kedalaman antreannya.
- **Pita di sepanjang akson** adalah mielin, berjarak sama dalam parameter kurva, sehingga
  merapat di bagian kurva yang paling tajam.

Tidak ada apa pun yang dipancarkan oleh timer sekadar supaya gambarnya terlihat ramai. Kalau arbor-nya
sepi, hub-nya memang sedang sepi.

### Membaca antrean misi

Setiap kartu membawa satu pip per langkah rencana, dalam warna spesialisnya, meredup sampai mereka
menjawab. Pip itu menyebutkan *siapa* yang menangani misinya sekaligus sejauh apa kemajuannya —
sesuatu yang tidak bisa dilakukan progress bar.

## Tangkapan layar

Gambar-gambar di dokumentasi ini diambil dari panel yang sedang berjalan, bukan hasil mock-up:

```bash
dotnet run --project src/Nerve.AgentSim -- --screenshot docs/images/agent-sim.png --demo 8 --wait 5600
```

Ia me-render visual tree yang sedang hidup pada skala 2× menjadi PNG lalu keluar. Apa pun yang
kebetulan sedang dikerjakan para agen pada saat itulah yang masuk ke berkasnya.

## Berkas

```
src/Nerve.AgentSim/
  Agents/AgentMessages.cs    topik-topiknya dan tipe record yang berjalan di atasnya
  Agents/Orchestrator.cs     perencanaan, pengiriman, agregasi
  Agents/Specialist.cs       satu sub-agen, dan enam profilnya
  Agents/SimulationHost.cs   seluruh perakitannya: satu hub, satu orchestrator, enam spesialis
  Controls/Arbor.cs          peta sinyalnya
  Controls/ArborField.cs     apa yang sedang melaju, hanya dimajukan di thread UI
  ViewModels/MainViewModel.cs  lima langganannya dan penguras per frame
  Views/MainWindow.axaml     tiga kolom, mengikuti arah pekerjaan bergerak
  Theme.axaml                paletnya dan ketiga jenis hurufnya
```
