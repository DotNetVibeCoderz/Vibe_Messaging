# Dokumentasi Nerve

Dibuat oleh Gravicode Studios, dipimpin oleh Kang Fadhil.

## Halaman

| | |
|---|---|
| [getting-started.md](getting-started.md) | Instalasi, publish, subscribe, dan dua kesalahan yang sebaiknya dihindari |
| [patterns.md](patterns.md) | Wildcard, retained message, request/reply, stream, menunggu |
| [architecture.md](architecture.md) | Bagaimana sebuah publish menjadi panggilan handler, dan mengapa tanpa alokasi |
| [api-reference.md](api-reference.md) | Seluruh anggota publik |
| [performance.md](performance.md) | Apa yang diukur, di mesin apa, dan artinya |
| [agent-simulator.md](agent-simulator.md) | Bagaimana panel Avalonia-nya dirakit |
| [migration-v2.md](migration-v2.md) | Apa yang berubah dari v1 dan apa yang perlu dilakukan |

Versi bahasa Inggris: [../](../).

## Tangkapan layar

![Simulator koordinasi agent dengan lima spesialis sedang bekerja](../images/agent-sim-flow.png)

![Satu misi selesai, diagregasi dari tiga spesialis](../images/agent-sim.png)

## Data mentah

- [../benchmark-run.txt](../benchmark-run.txt) — keluaran uji beban berkelanjutan yang dikutip di
  [performance.md](performance.md)
- [../benchmark-micro.txt](../benchmark-micro.txt) — keluaran BenchmarkDotNet dari run yang sama
- [../legacy-readme.md](../legacy-readme.md) — README v1, disimpan sebagai rujukan

Setiap angka di [performance.md](performance.md) berasal dari kedua berkas itu. Kalau Anda mengubah
sesuatu di jalur publish, jalankan ulang harness-nya lalu perbarui keduanya — atau katakan terus
terang bahwa angkanya berasal dari sebelum perubahan tersebut.
