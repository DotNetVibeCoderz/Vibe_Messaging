"""Generates the .NET Interactive (Polyglot) notebooks in samples/notebooks.

Each notebook is defined here as a list of markdown / C# cells so the notebooks stay reviewable
in diffs. Run:  python tools/notebooks/build_notebooks.py
Execute headlessly (needs a running bigpiped and the packages in artifacts/nuget):
    dotnet-repl --run samples/notebooks/01-streaming-statistics.ipynb --exit-after-run --working-dir samples/notebooks
"""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[2] / "samples" / "notebooks"

SETUP = """{feed}#r "nuget: {packages}"
using BigPipe.Client;
using BigPipe.Client.Admin;
using BigPipe.Analytics;
var bootstrap = Environment.GetEnvironmentVariable("BIGPIPE_BOOTSTRAP") ?? "localhost:9092";
var admin = new BigPipeAdminClient(Environment.GetEnvironmentVariable("BIGPIPE_ADMIN") ?? "http://localhost:9644");
var http = new BigPipeHttpClient(Environment.GetEnvironmentVariable("BIGPIPE_HTTP") ?? "http://localhost:8082");
string Unique(string p) => $"nb-{p}-{Guid.NewGuid().ToString("N")[..6]}";
var cluster = await admin.GetClusterAsync();
Console.WriteLine($"Connected to {cluster.ClusterId} (BigPipe {cluster.Version}) — {cluster.Credit}");"""

CREDIT = "\n\n*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*"


def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text.strip("\n").splitlines(keepends=True)}


def cs(code):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {"dotnet_interactive": {"language": "csharp"}, "polyglot_notebook": {"kernelName": "csharp"}},
        "outputs": [],
        "source": code.strip("\n").splitlines(keepends=True),
    }


def notebook(cells):
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": ".NET (C#)", "language": "C#", "name": ".net-csharp"},
            "language_info": {"name": "polyglot-notebook"},
            "polyglot_notebook": {"kernelInfo": {"defaultKernelName": "csharp", "items": [{"name": "csharp"}]}},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


LOCAL_FEED = None
FEED_HINT = (
    "// Using a local build instead of nuget.org? Uncomment and point to <repo>/artifacts/nuget (absolute path):\n"
    '// #i "nuget: C:/src/bigpipe/artifacts/nuget"\n'
)


def setup(*packages):
    refs = '"\n#r "nuget: '.join(f"{p}, 0.1.0" if not p.startswith("TorchSharp") else p for p in packages)
    feed = f'#i "nuget: {LOCAL_FEED}"\n' if LOCAL_FEED else FEED_HINT
    return cs(SETUP.replace("{packages}", refs).replace("{feed}", feed))


NOTEBOOKS = {
    "01-streaming-statistics.ipynb": [
        md("""# Streaming statistics on payments
**EN** — Compute live KPIs over a payment stream with bounded memory: running mean/deviation, percentiles, distinct customers (HyperLogLog), top merchants (Space-Saving) and tumbling-window summaries.

**ID** — Menghitung KPI secara langsung dari aliran pembayaran dengan memori terbatas: rata-rata/deviasi berjalan, persentil, jumlah pelanggan unik (HyperLogLog), merchant teratas (Space-Saving), dan ringkasan per jendela waktu.

Requires / Membutuhkan: `bigpiped --mode dev`.""" + CREDIT),
        setup("BigPipe.Analytics"),
        md("""## 1. Produce a realistic payment stream / Kirim aliran pembayaran
Payments in IDR from merchants across Indonesian cities, with event timestamps spread over five minutes."""),
        cs("""var topic = Unique("payments");
await admin.CreateTopicAsync(topic, 6);
var rng = new Random(42);
var merchants = new[] { "Warung Kopi", "Toko Batik", "Bakmi Jogja", "Sate Madura", "Apotek Sehat", "Gadget BDG" };
var cities = new[] { "Jakarta", "Bandung", "Surabaya", "Yogyakarta", "Medan" };
var t0 = DateTimeOffset.UtcNow.AddMinutes(-5);
double NextExponential(Random r) => -Math.Log(1 - r.NextDouble());
double NextLogNormal(Random r, double mu, double sigma) =>
    Math.Exp(mu + sigma * Math.Sqrt(-2 * Math.Log(1 - r.NextDouble())) * Math.Cos(2 * Math.PI * r.NextDouble()));
var records = Enumerable.Range(0, 3000).Select(i => new HttpProduceRecord
{
    Key = $"cust-{rng.Next(900)}",
    Value = new { merchant = merchants[(int)Math.Min(5, NextExponential(rng) * 1.5)], city = cities[rng.Next(cities.Length)], amount = Math.Round(NextLogNormal(rng, 11.5, 0.8)) },
    Timestamp = t0.AddMilliseconds(i * 100).ToUnixTimeMilliseconds(),
}).ToList();
foreach (var chunk in records.Chunk(500)) await http.ProduceAsync(topic, chunk);
Console.WriteLine($"produced {records.Count} payments to {topic}");"""),
        md("""## 2. One pass, bounded memory / Satu lintasan, memori terbatas"""),
        cs("""var stats = new RunningStats();
var q = new QuantileSketch();
var customers = new HyperLogLog();
var top = new TopK(20);
var n = 0;
await foreach (var r in BigPipeSource.Kafka(bootstrap, topic, from: OffsetReset.Earliest))
{
    var amount = r.Num("amount");
    stats.Add(amount);
    q.Add(amount);
    customers.Add(r.Key!);
    top.Add(r.Str("merchant")!);
    if (++n == records.Count) break;
}
Console.WriteLine($"payments        {stats.Count:N0}");
Console.WriteLine($"total           {stats.Sum:N0} IDR");
Console.WriteLine($"mean / sd       {stats.Mean:N0} / {stats.StdDev:N0} IDR");
Console.WriteLine($"p50 / p95 / p99 {q.Median:N0} / {q.P95:N0} / {q.P99:N0} IDR");
Console.WriteLine($"unique customers ≈ {customers.Estimate():N0}");
top.Top(5)"""),
        md("""## 3. One-minute tumbling windows / Jendela satu menit"""),
        cs("""var windows = await BigPipeSource.Kafka(bootstrap, topic, from: OffsetReset.Earliest)
    .TakeAsync(records.Count)
    .TumblingWindow(TimeSpan.FromMinutes(1))
    .CollectAsync();
windows.Select(w => WindowSummary.Of(w, "amount")).Select(w => new { start = w.Start.ToString("HH:mm"), w.Count, total = w.Sum.ToString("N0"), p95 = w.P95.ToString("N0") })"""),
    ],
    "02-anomaly-detection-mlnet.ipynb": [
        md("""# Realtime anomaly detection with ML.NET
**EN** — Monitor cold-chain truck temperatures. Compare ML.NET's IID spike detector with a z-score baseline, run SR-CNN over a whole window, and forecast with SSA.

**ID** — Memantau suhu truk rantai dingin. Bandingkan detektor spike IID ML.NET dengan baseline z-score, jalankan SR-CNN pada satu jendela, dan prakirakan dengan SSA.""" + CREDIT),
        setup("BigPipe.Analytics.ML"),
        cs("""using BigPipe.Analytics.ML;
var topic = Unique("telemetry");
await admin.CreateTopicAsync(topic, 1, StorageMode.Diskless);   // telemetry is cheap on object storage
var rng = new Random(7);
var spikes = new HashSet<int> { 180, 300, 420 };
var readings = Enumerable.Range(0, 500).Select(i => 5 + Math.Sin(i / 24.0) + rng.NextDouble() * 0.4 + (spikes.Contains(i) ? 8 : 0)).ToList();
await http.ProduceAsync(topic, readings.Select((t, i) => new HttpProduceRecord { Key = "truck-07", Value = new { temp_c = Math.Round(t, 3), i } }));
Console.WriteLine($"produced {readings.Count} readings with faults at {string.Join(", ", spikes)}");"""),
        md("""## Streaming detectors / Detektor streaming"""),
        cs("""var ml = new MLSpikeDetector(confidence: 99, pvalueHistoryLength: 60);
var z = new ZScoreDetector(threshold: 4);
var alerts = new List<object>();
var i = 0;
await foreach (var r in BigPipeSource.Kafka(bootstrap, topic, from: OffsetReset.Earliest))
{
    var t = r.Num("temp_c");
    var a = ml.Observe(t);
    var b = z.Observe(t);
    if (a.IsAnomaly || b.IsAnomaly) alerts.Add(new { reading = i, temp = t, mlSpike = a.IsAnomaly, zscore = Math.Round(b.Score, 1) });
    if (++i == readings.Count) break;
}
alerts"""),
        md("""## SR-CNN over a window and SSA forecast / SR-CNN pada jendela dan prakiraan SSA"""),
        cs("""var sr = new SrCnnWindowDetector(threshold: 0.3).Detect(readings);
Console.WriteLine("SR-CNN anomalies at: " + string.Join(", ", sr.Where(p => p.IsAnomaly).Select(p => p.Index)));
var ssa = new SsaForecaster(windowSize: 24, seriesLength: 96, horizon: 6);
ssa.Train(readings.Take(160));
var f = ssa.ObserveAndForecast(readings[160]);
f.ForecastedValues.Select((v, k) => new { step = k + 1, forecast = Math.Round(v, 2), low = Math.Round(f.LowerBound[k], 2), high = Math.Round(f.UpperBound[k], 2) })"""),
    ],
    "03-deep-learning-torchsharp.ipynb": [
        md("""# Multivariate anomalies with TorchSharp
**EN** — An online autoencoder learns how speed, temperature and door state move together and flags readings that break the pattern.

**ID** — Autoencoder online mempelajari hubungan kecepatan, suhu, dan status pintu, lalu menandai pembacaan yang melanggar pola tersebut.""" + CREDIT),
        setup("BigPipe.Analytics.Torch", "TorchSharp-cpu, 0.107.0"),
        cs("""using BigPipe.Analytics.Torch;
var topic = Unique("fleet");
await admin.CreateTopicAsync(topic, 1);
var rng = new Random(9);
var rows = new List<HttpProduceRecord>();
for (var i = 0; i < 1500; i++)
{
    var moving = rng.NextDouble() > 0.3;
    var door = moving ? 0 : rng.Next(2);
    if (i is 1300 or 1420) door = 1;   // door open while driving
    rows.Add(new HttpProduceRecord { Key = $"truck-{i % 5}", Value = new { speed = moving ? 40 + rng.NextDouble() * 40 : 0, temp_c = 4 + rng.NextDouble() + door * 2, door } });
}
foreach (var chunk in rows.Chunk(500)) await http.ProduceAsync(topic, chunk);
var detector = new TorchAutoencoderDetector(features: 3, k: 5, warmup: 300);
var found = new List<object>();
var n = 0;
await foreach (var r in BigPipeSource.Kafka(bootstrap, topic, from: OffsetReset.Earliest))
{
    var s = detector.Observe([(float)r.Num("speed"), (float)r.Num("temp_c"), (float)r.Num("door")]);
    if (s.IsAnomaly) found.Add(new { reading = n, r.Value, error = Math.Round(s.Value, 4) });
    if (++n == rows.Count) break;
}
found"""),
    ],
    "04-dynamic-scripting.ipynb": [
        md("""# Dynamic scripting: C#, Python and JavaScript
**EN** — Let analysts filter and query live data with scripts compiled at runtime.

**ID** — Analis dapat memfilter dan melakukan query data langsung dengan skrip yang dikompilasi saat runtime.""" + CREDIT),
        setup("BigPipe.Analytics.Scripting"),
        cs("""using BigPipe.Analytics.Scripting;
var topic = Unique("orders");
await admin.CreateTopicAsync(topic, 3);
var rng = new Random(3);
var channels = new[] { "app", "web", "pos" };
await http.ProduceAsync(topic, Enumerable.Range(0, 600).Select(i => new HttpProduceRecord
{
    Key = $"order-{i}",
    Value = new { total = rng.Next(10, 900) * 1000, items = rng.Next(1, 8), channel = channels[i % 3] },
    Headers = new Dictionary<string, string> { ["region"] = i % 4 == 0 ? "SG" : "ID" },
}));
var window = await BigPipeSource.Kafka(bootstrap, topic, from: OffsetReset.Earliest).CollectAsync(600);
Console.WriteLine($"{window.Count} orders in the window");"""),
        md("""In an application, `ScriptEngines.Create(ScriptLanguage.CSharp)` compiles C# filters with Roslyn. Inside this notebook C# *is* the kernel language, so the C# version is simply a lambda; Python and JavaScript use the scripting engines.

Di aplikasi, `ScriptEngines.Create(ScriptLanguage.CSharp)` mengompilasi filter C# dengan Roslyn. Di notebook ini C# sudah menjadi bahasa kernel, jadi versi C#-nya cukup berupa lambda; Python dan JavaScript memakai mesin skrip."""),
        cs("""Func<AnalyticsRecord, bool> csharpFilter = r => r.Num("total") > 500000 && r.Header("region") == "ID";
var csMatches = window.Where(csharpFilter).ToList();
var results = new List<object> { new { language = "C# (lambda)", matches = csMatches.Count, avgItems = Math.Round(csMatches.Average(x => x.Num("items")), 3) } };
foreach (var (lang, filter, query) in new[]
{
    (ScriptLanguage.Python, "r.num('total') > 500000 and r.header('region') == 'ID'", "result = sum(x.num('items') for x in records) / len(records)"),
    (ScriptLanguage.JavaScript, "r.num('total') > 500000 && r.header('region') === 'ID'", "records.reduce((s, x) => s + x.num('items'), 0) / records.length"),
})
{
    using var engine = ScriptEngines.Create(lang);
    var matches = window.Where(engine.CompileFilter(filter)).ToList();
    results.Add(new { language = lang.ToString(), matches = matches.Count, avgItems = Math.Round(Convert.ToDouble(engine.Query(query, matches)), 3) });
}
results"""),
        md("""Server-side, the broker itself evaluates **bpql** filters — the same idea without any client code:

`GET /v1/topics/{topic}/stream?filter=this.total > 500000 && header("region") == "ID"`"""),
    ],
    "05-gravicode-science.ipynb": [
        md("""# GravicodeScience on streams: DataFrames, clustering, novelty
**EN** — Turn a window of checkouts into a GraviFrame DataFrame, segment customers with GraviLearn K-Means and catch unusual checkouts with a one-class SVM.

**ID** — Ubah jendela transaksi menjadi DataFrame GraviFrame, segmentasikan pelanggan dengan K-Means GraviLearn, dan tangkap transaksi tidak wajar dengan one-class SVM.""" + CREDIT),
        setup("BigPipe.Analytics.Gravicode"),
        cs("""using BigPipe.Analytics.Gravicode;
var topic = Unique("checkouts");
await admin.CreateTopicAsync(topic, 1);
var rng = new Random(4);
var cities = new[] { "Jakarta", "Bandung", "Surabaya" };
await http.ProduceAsync(topic, Enumerable.Range(0, 600).Select(i =>
{
    var kind = i % 3;
    var basket = kind switch { 0 => rng.Next(20, 80), 1 => rng.Next(150, 400), _ => rng.Next(1500, 4000) } * 1000;
    var items = kind == 1 ? rng.Next(4, 9) : rng.Next(1, 3);
    return new HttpProduceRecord { Key = $"cust-{i}", Value = new { basket, items, city = cities[rng.Next(3)] } };
}));
var window = await BigPipeSource.Kafka(bootstrap, topic, from: OffsetReset.Earliest).CollectAsync(600);
var frame = window.ToDataFrame(["basket", "items"], ["city"]);
frame.Describe().ToString()"""),
        cs("""frame.GroupBy("city").Mean("basket", "items").ToString()"""),
        cs("""var seg = new GraviKMeansSegmenter(clusters: 3);
var rows = window.Select(r => new[] { r.Num("basket") / 100_000, r.Num("items") }).ToList();
seg.Fit(rows);
seg.AssignAll(rows).Select((label, i) => (label, i)).GroupBy(x => x.label)
   .Select(g => new { segment = g.Key, customers = g.Count(), avgBasket = g.Average(x => window[x.i].Num("basket")).ToString("N0") })"""),
        cs("""var novelty = new GraviOneClassSvmDetector(nu: 0.02);
novelty.Fit(rows);
new[] { new[] { 0.5, 1.0 }, new[] { 2.5, 6.0 }, new[] { 120.0, 40.0 } }
    .Select(x => new { basket = x[0] * 100_000, items = x[1], unusual = novelty.Observe(x).IsAnomaly })"""),
    ],
    "06-nlp-mediapipe-hfnet.ipynb": [
        md("""# Text analytics: MediaPipe.NET, GraviText and HF.Net
**EN** — Score reviews as they stream: MediaPipe.NET sentiment and language detection, GraviText lexicon sentiment, and (optionally) any Hugging Face classifier through HF.Net.

**ID** — Menilai ulasan secara langsung: sentimen dan deteksi bahasa dengan MediaPipe.NET, sentimen leksikon GraviText, dan (opsional) classifier Hugging Face apa pun lewat HF.Net.""" + CREDIT),
        setup("BigPipe.Analytics.Gravicode"),
        cs("""using BigPipe.Analytics.Gravicode;
var topic = Unique("reviews");
await admin.CreateTopicAsync(topic, 1);
string[] reviews =
[
    "Absolutely love this batik shirt, the quality is excellent!",
    "Terrible experience, the package arrived broken.",
    "Kopinya enak sekali dan pengirimannya cepat, pasti beli lagi!",
    "Great value for the price, my kids are happy with it.",
    "Worst purchase ever, a complete waste of money.",
];
await http.ProduceAsync(topic, reviews.Select((t, i) => new HttpProduceRecord { Key = $"r{i}", Value = new { text = t } }));
var sentiment = new MediaPipeSentiment();     // IDisposable: dispose when done
var language = new MediaPipeLanguageDetector();
var lexicon = new GraviSentiment();
var scored = new List<object>();
await foreach (var r in BigPipeSource.Kafka(bootstrap, topic, from: OffsetReset.Earliest))
{
    var text = r.Str("text")!;
    var (label, score) = sentiment.Analyze(text);
    scored.Add(new { lang = language.Detect(text).Language, mediapipe = label, score = Math.Round(score, 2), gravitext = lexicon.Analyze(text).Label, text });
    if (scored.Count == reviews.Length) break;
}
scored"""),
        md("""### Optional: a Hugging Face model via HF.Net / Opsional: model Hugging Face lewat HF.Net
Downloads the model once to the local cache. Set `RUN_HF=1` to run this cell."""),
        cs("""if (Environment.GetEnvironmentVariable("RUN_HF") == "1")
{
    using var hf = HfTextModel.Load("distilbert-base-uncased-finetuned-sst-2-english");
    display(reviews.Select(t => new { t, prediction = hf.Classify(t) }).ToList());
}
else
{
    display("Skipped (set RUN_HF=1 to download and run a Hugging Face classifier).");
}"""),
    ],
    "07-streams-and-flows.ipynb": [
        md("""# BigPipe.Streams and Flows
**EN** — Build a windowed aggregation with BigPipe.Streams and query its state interactively; then deploy an in-broker Flow that filters, masks and routes records.

**ID** — Bangun agregasi berjendela dengan BigPipe.Streams dan query state-nya secara interaktif; lalu deploy Flow di dalam broker yang memfilter, menyamarkan, dan merutekan record.""" + CREDIT),
        setup("BigPipe.Streams", "BigPipe.Analytics"),
        cs("""using BigPipe.Streams;
public record Payment(string MerchantId, decimal Amount, string Status);
public record MerchantTotals(long Count = 0, decimal Amount = 0);

var input = Unique("payments");
await admin.CreateTopicAsync(input, 3);
var topology = new StreamsBuilder();
topology.Stream<string, Payment>(input)
    .Filter((_, p) => p.Status == "SETTLED")
    .GroupBy((_, p) => p.MerchantId)
    .WindowedBy(TumblingWindow.Of(TimeSpan.FromMinutes(1)).Grace(TimeSpan.FromMinutes(2)))
    .Aggregate(() => new MerchantTotals(), (_, p, acc) => acc with { Count = acc.Count + 1, Amount = acc.Amount + p.Amount }, Stores.Persistent("totals"));
var app = new StreamsApp(topology, new StreamsConfig { ApplicationId = Unique("agg"), Bootstrap = bootstrap });
await app.StartAsync();

await using (var p = new ProducerBuilder<string, Payment>().WithBootstrap(bootstrap).Build())
{
    var merchants = new[] { "Warung Kopi", "Toko Batik", "Bakmi Jogja" };
    var t0 = DateTimeOffset.UtcNow;
    await Task.WhenAll(Enumerable.Range(0, 300).Select(i => p.SendAsync(input, new Message<string, Payment>
    {
        Key = $"tx-{i}", Value = new Payment(merchants[i % 3], 10_000m * (1 + i % 7), i % 10 == 0 ? "PENDING" : "SETTLED"), Timestamp = t0.AddSeconds(i / 3.0),
    })));
}
var store = app.Store<Windowed<string>, MerchantTotals>("totals");
for (var k = 0; k < 100 && store.All().Sum(x => x.Value.Count) < 270; k++) await Task.Delay(200);
store.All().OrderBy(x => x.Key.Start).Select(x => new { window = $"{x.Key.StartTime:HH:mm}", merchant = x.Key.Key, x.Value.Count, amount = x.Value.Amount })"""),
        cs("""await app.DisposeAsync();
var flow = Unique("big-payments");
await admin.DeployFlowAsync(new FlowSpec
{
    Name = flow, Input = input, Output = input + ".big",
    Processors = [FlowProcessor.FilterBy("this.amount > 50000"), FlowProcessor.MapWith("root = this\\nroot.flagged = true")],
});
await Task.Delay(2000);
(await admin.ListFlowsAsync()).Where(f => f.Name == flow).Select(f => f.Stats)"""),
    ],
}


def main():
    """--local-feed <abs dir> --out <dir>: write copies that restore from a local feed (for testing)."""
    import sys
    global LOCAL_FEED, OUT
    if "--local-feed" in sys.argv:
        LOCAL_FEED = Path(sys.argv[sys.argv.index("--local-feed") + 1]).resolve().as_posix()
    if "--out" in sys.argv:
        OUT = Path(sys.argv[sys.argv.index("--out") + 1])
    OUT.mkdir(parents=True, exist_ok=True)
    for name, cells in NOTEBOOKS.items():
        if LOCAL_FEED:
            for c in cells:
                src = "".join(c["source"])
                if FEED_HINT in src:
                    c["source"] = src.replace(FEED_HINT, f'#i "nuget: {LOCAL_FEED}"\n').splitlines(keepends=True)
        (OUT / name).write_text(json.dumps(notebook(cells), indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
        print("wrote", OUT / name)


if __name__ == "__main__":
    main()
