# Stream processing dan analitik realtime

[English](../en/streams-and-analytics.md) · [Bahasa Indonesia](streams-and-analytics.md)

BigPipe punya tiga lapisan untuk mengolah data saat data tiba. Ketiganya bisa dipakai bersamaan.

| Lapisan | Berjalan di | Dipakai untuk |
|---|---|---|
| **BigPipe Flow** | di dalam broker (Rust) | filter, masking, routing, mengubah bentuk data ([rincian](bpql-and-flows.md)) |
| **BigPipe.Streams** | di layanan .NET Anda | pemrosesan stateful: join, agregasi per window, tabel; model yang sama dengan Kafka Streams |
| **BigPipe.Analytics** (+ Scripting / ML / Torch / Gravicode) | di layanan .NET atau notebook | statistik, sketch, deteksi anomali, scoring ML dan deep learning, skrip dari pengguna |

## BigPipe.Streams

```csharp
var topology = new StreamsBuilder();
topology.Stream<string, Payment>("payments")
    .Filter((_, p) => p.Status == "SETTLED")
    .GroupBy((_, p) => p.MerchantId)                                  // repartisi berdasarkan key baru
    .WindowedBy(TumblingWindow.Of(TimeSpan.FromMinutes(1)).Grace(TimeSpan.FromMinutes(2)))
    .Aggregate(() => new MerchantTotals(),
               (_, p, acc) => acc with { Count = acc.Count + 1, Amount = acc.Amount + p.Amount },
               Stores.Persistent("merchant-totals"))                   // didukung topic changelog
    .ToStream()
    .To("merchant-totals-1m");

await using var app = new StreamsApp(topology, new StreamsConfig { ApplicationId = "merchant-aggregator", Bootstrap = "localhost:9092" });
await app.StartAsync();

var store = app.Store<Windowed<string>, MerchantTotals>("merchant-totals");   // interactive query
```

- **Operator:** `Filter`/`FilterNot`, `Map`, `MapValues`, `FlatMap`, `FlatMapValues`, `SelectKey`, `Peek`, `Branch`, `Merge`, `GroupByKey`/`GroupBy`, `Count`, `Reduce`, `Aggregate`, `Join`/`LeftJoin` stream–tabel (tabel dibuat dengan `builder.Table<K,V>(topic)`), `ToStream`, `To`.
- **Window:** `TumblingWindow`, `HoppingWindow`, `SlidingWindow`, dan `SessionWindow`, masing-masing dengan grace period untuk record yang terlambat.
- **State:** `Stores.Persistent` menyimpan state di memori dan menulis setiap perubahan ke `<app>-<store>-changelog`. Saat restart, store dibangun ulang dari topic tersebut. `Stores.InMemory` tidak memakai changelog.
- **Skalabilitas:** partisi dibagi ke semua instance dengan `ApplicationId` yang sama lewat consumer group biasa.

## BigPipe.Analytics

Semuanya berupa `IAsyncEnumerable<AnalyticsRecord>`, jadi kode analitik bisa digabung dengan C# biasa.

```csharp
using BigPipe.Analytics;

var source = BigPipeSource.Kafka("localhost:9092", "iot.telemetry", from: OffsetReset.Earliest);
// atau BigPipeSource.Http("http://localhost:8082", "iot.telemetry", filter: "this.site == \"JKT\"")

await foreach (var w in source.TumblingWindow(TimeSpan.FromSeconds(10)))
{
    var s = WindowSummary.Of(w, "temp_c");            // count, sum, mean, stddev, min, max, p50, p95
    Console.WriteLine($"{w.Start:T}  n={s.Count}  mean={s.Mean:0.0}  p95={s.P95:0.0}");
}
```

| Kelompok | Tipe |
|---|---|
| Sumber dan tujuan | `BigPipeSource.Kafka` / `.Http` / `.FromEnumerable`, `SinkToBigPipeAsync` |
| Operator | `WhereAsync`, `SelectAsync`, `TakeAsync`, `CollectAsync`, `TumblingWindow`, `SlidingWindow`, `CountWindow`, `GroupWindow` |
| Statistik streaming | `RunningStats` (Welford), `Ewma`, `QuantileSketch`, `RateMeter` |
| Sketch | `HyperLogLog` (hitung nilai unik), `CountMinSketch` (frekuensi), `TopK` (item terbanyak) |
| Detektor | `ZScoreDetector`, `EwmaDetector`, `MadDetector`; dipakai lewat `.Anomalies(value, detector)` atau `.Detect(...)` |

## Skrip dinamis: C#, Python, JavaScript (`BigPipe.Analytics.Scripting`)

Filter dan query bisa berasal dari pengguna, file konfigurasi, atau UI. Semuanya dikompilasi saat runtime oleh Roslyn (C#), IronPython, atau Jint (JavaScript).

```csharp
using var engine = ScriptEngines.Create(ScriptLanguage.Python);   // CSharp | Python | JavaScript

var big = source.WhereScript(engine, "r.num('total') > 500000 and r.header('region') == 'ID'");
await foreach (var (window, result) in big.TumblingWindow(TimeSpan.FromMinutes(1))
                   .QueryWindows(engine, "result = sum(x.num('items') for x in records) / len(records)"))
    Console.WriteLine($"{window.Start:T} avg items {result}");
```

| Bahasa | Filter | Query atas satu window |
|---|---|---|
| C# | `r.num("total") > 500000 && r.header("region") == "ID"` | `records.Average(x => x.num("items"))` |
| Python | `r.num('total') > 500000 and r.header('region') == 'ID'` | `result = sum(x.num('items') for x in records) / len(records)` |
| JavaScript | `r.num('total') > 500000 && r.header('region') === 'ID'` | `records.reduce((s, x) => s + x.num('items'), 0) / records.length` |

Semua bahasa memakai API record yang sama: `r.num(path)`, `r.str(path)`, `r.flag(path)`, `r.header(nama)`, `r.key`, dan `r.json`. Filter atau proyeksi dikompilasi sekali (`CompileFilter`, `CompileProjection`), lalu dipanggil untuk setiap record. Query di-cache berdasarkan teksnya. Skrip berjalan di dalam proses, jadi hanya terima skrip dari pengguna tepercaya.

## ML.NET (`BigPipe.Analytics.ML`)

| Tipe | Fungsi |
|---|---|
| `MLSpikeDetector` | deteksi lonjakan IID pada satu metrik, satu record demi satu record |
| `MLChangePointDetector` | mendeteksi pergeseran level |
| `SrCnnWindowDetector` | deteksi anomali SR-CNN atas satu window utuh |
| `SsaForecaster` / `SsaForecast` | peramalan Singular Spectrum Analysis dengan batas keyakinan |
| `StreamingBinaryClassifier` | belajar dari record berlabel yang mengalir dan dilatih ulang langsung |
| `MLModelScorer<TIn,TOut>` | memuat model ML.NET `.zip` yang tersimpan dan memberi skor pada record |

```csharp
var detector = new MLSpikeDetector(confidence: 99, pvalueHistoryLength: 60);
await foreach (var (rec, score) in source.Anomalies(r => r.Num("temp_c"), detector))
    Console.WriteLine($"{rec.Key} {rec.Num("temp_c")} °C  p={score.Score:0.000}");
```

![Deteksi anomali di Gallery](../images/gallery-anomaly.png)

## TorchSharp (`BigPipe.Analytics.Torch`)

| Tipe | Fungsi |
|---|---|
| `TorchAutoencoderDetector` | autoencoder online atas beberapa fitur. Ia mempelajari hubungan antarnilai dan menandai record yang menyimpang dari pola, meskipun setiap nilainya tampak normal |
| `TorchWindowDetector` | model yang sama atas sliding window dari satu deret |
| `TorchScriptScorer` | memberi skor pada record dengan model TorchScript hasil ekspor |

Tambahkan `TorchSharp-cpu` (atau paket `TorchSharp-cuda-*`) ke aplikasi Anda untuk runtime native.

## GravicodeScience, HF.Net, MediaPipe.NET (`BigPipe.Analytics.Gravicode`)

| Tipe | Library | Fungsi |
|---|---|---|
| `WindowFrames.ToDataFrame` | GraviFrame | mengubah window menjadi DataFrame ala pandas (`Describe()`, group-by, …) |
| `GraviKMeansSegmenter` | GraviLearn | K-Means yang dilatih pada satu window, lalu dipakai untuk menentukan segmen setiap record baru |
| `GraviOneClassSvmDetector` | GraviLearn | deteksi kebaruan (novelty) dengan one-class SVM |
| `GraviSentiment` | GraviText | sentimen berbasis leksikon, tanpa mengunduh model |
| `HfTextModel` / `SemanticFilter` | HF.Net (GraviTransformers) | memuat model Hugging Face (`HfTextModel.Load("repo/id")`) untuk klasifikasi, embedding, atau filter berdasarkan makna |
| `MediaPipeSentiment` / `MediaPipeLanguageDetector` | MediaPipe.NET | klasifikasi teks dan deteksi bahasa langsung di perangkat |

```csharp
using var sentiment = new MediaPipeSentiment();
using var language = new MediaPipeLanguageDetector();
await foreach (var r in BigPipeSource.Kafka(bootstrap, "reviews", from: OffsetReset.Earliest))
{
    var (label, score) = sentiment.Analyze(r.Str("text")!);
    var (lang, _) = language.Detect(r.Str("text")!);
    Console.WriteLine($"[{lang}] {label} {score:P0}");
}
```

Model berjalan lokal di ONNX Runtime dan diunduh ke cache pengguna saat pertama dipakai. `OnnxRuntimeNative.EnsureLoaded()` memastikan ONNX Runtime dari NuGet yang dimuat, bukan salinan lama yang ikut terpasang di System32 Windows.

![Sentimen di Gallery](../images/gallery-sentiment.png)

## Notebook

`samples/notebooks` berisi notebook .NET Interactive (Polyglot Notebooks di VS Code, atau Jupyter dengan kernel .NET). Setiap notebook ditulis dalam dua bahasa dan membuat data contohnya sendiri.

| Notebook | Topik |
|---|---|
| `01-streaming-statistics` | window, statistik Welford, kuantil, HyperLogLog, Count-Min, Top-K |
| `02-anomaly-detection-mlnet` | deteksi spike / change-point / SR-CNN dan peramalan SSA dengan ML.NET |
| `03-deep-learning-torchsharp` | autoencoder online untuk data sensor multivariat |
| `04-dynamic-scripting` | filter dan query yang sama dalam C#, Python, dan JavaScript |
| `05-gravicode-science` | DataFrame GraviFrame, K-Means GraviLearn, dan one-class SVM |
| `06-nlp-mediapipe-hfnet` | sentimen dan deteksi bahasa MediaPipe.NET, transformer HF.Net |
| `07-streams-and-flows` | agregasi per window dengan BigPipe.Streams dan sebuah BigPipe Flow |

Sebelum membuka notebook, jalankan `bigpiped` di port default. Notebook mereferensikan paket NuGet. Untuk mencobanya dengan build lokal, buat ulang notebook dengan `python tools/notebooks/build_notebooks.py --local-feed artifacts/nuget`. `tools/notebooks/run_notebooks.py` menjalankan semua notebook tanpa UI dengan `dotnet-repl` dan melaporkan sel yang error.

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
