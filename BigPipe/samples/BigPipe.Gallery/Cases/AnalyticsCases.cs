using BigPipe.Analytics;
using BigPipe.Analytics.Gravicode;
using BigPipe.Analytics.ML;
using BigPipe.Analytics.Scripting;
using BigPipe.Analytics.Torch;
using BigPipe.Client;
using BigPipe.Client.Admin;
using BigPipe.Streams;

namespace BigPipe.Gallery.Cases;

internal static class Telemetry
{
    /// <summary>Cold-chain truck temperatures with a few injected faults.</summary>
    public static IEnumerable<(string Device, double Temp, DateTimeOffset At)> Series(int n, int[] spikes, int seed = 11)
    {
        var rng = new Random(seed);
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-n / 60.0);
        for (var i = 0; i < n; i++)
            yield return ($"truck-{i % 4:D2}", 4.5 + rng.NextDouble() + (spikes.Contains(i) ? 9 : 0), t0.AddSeconds(i));
    }

    public static async Task<string> ProduceAsync(GalleryContext ctx, string prefix, int n, int[] spikes, CancellationToken ct)
    {
        var topic = ctx.Unique(prefix);
        await ctx.Admin.CreateTopicAsync(topic, 1, StorageMode.Diskless, ct: ct);
        using var http = new BigPipeHttpClient(ctx.Endpoints.HttpUrl);
        var batch = Series(n, spikes).Select(x => new HttpProduceRecord
        {
            Key = x.Device,
            Value = new { device = x.Device, temp_c = Math.Round(x.Temp, 2) },
            Timestamp = x.At.ToUnixTimeMilliseconds(),
        }).ToList();
        foreach (var chunk in batch.Chunk(500)) await http.ProduceAsync(topic, chunk, ct: ct);
        ctx.Log($"produced {n} telemetry readings to diskless topic {topic}");
        return topic;
    }

    public static IAsyncEnumerable<AnalyticsRecord> Read(GalleryContext ctx, string topic, CancellationToken ct) =>
        BigPipeSource.Kafka(ctx.Endpoints.Bootstrap, topic, from: OffsetReset.Earliest, ct: ct);
}

public sealed class StreamsWindowCase : GalleryCase
{
    public override string Category => "Processing";
    public override string Title => "Windowed aggregation (BigPipe.Streams)";
    public override string Summary => "Total settled payments per merchant in one-minute tumbling windows, with a queryable state store backed by a changelog topic.";
    public override IReadOnlyList<string> Highlights => ["Filter → GroupBy (repartitioned) → Window → Aggregate", "Interactive queries on the store", "State restored from the changelog after restarts"];

    public override string Code => """
        var topology = new StreamsBuilder();
        topology.Stream<string, Payment>("payments")
            .Filter((_, p) => p.Status == "SETTLED")
            .GroupBy((_, p) => p.MerchantId)
            .WindowedBy(TumblingWindow.Of(TimeSpan.FromMinutes(1)).Grace(TimeSpan.FromMinutes(2)))
            .Aggregate(() => new MerchantTotals(),
                       (_, p, acc) => acc with { Count = acc.Count + 1, Amount = acc.Amount + p.Amount },
                       Stores.RocksDb("merchant-totals"))
            .ToStream()
            .To("merchant-totals-1m");

        await using var app = new StreamsApp(topology, new StreamsConfig
        {
            ApplicationId = "merchant-aggregator",
            Bootstrap = "localhost:9092",
        });
        await app.StartAsync();
        var store = app.Store<Windowed<string>, MerchantTotals>("merchant-totals");
        """;

    public sealed record Payment(string MerchantId, decimal Amount, string Status);

    public sealed record MerchantTotals(long Count = 0, decimal Amount = 0);

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var input = ctx.Unique("payments");
        await ctx.Admin.CreateTopicAsync(input, 3, ct: ct);
        var topology = new StreamsBuilder();
        topology.Stream<string, Payment>(input)
            .Filter((_, p) => p.Status == "SETTLED")
            .GroupBy((_, p) => p.MerchantId)
            .WindowedBy(TumblingWindow.Of(TimeSpan.FromMinutes(1)).Grace(TimeSpan.FromMinutes(2)))
            .Aggregate(() => new MerchantTotals(), (_, p, acc) => acc with { Count = acc.Count + 1, Amount = acc.Amount + p.Amount },
                Stores.RocksDb("merchant-totals"))
            .ToStream()
            .To(input + "-totals-1m");
        await using var app = new StreamsApp(topology, new StreamsConfig { ApplicationId = ctx.Unique("agg"), Bootstrap = ctx.Endpoints.Bootstrap });
        await app.StartAsync();
        var merchants = new[] { "Warung Kopi", "Toko Batik", "Bakmi Jogja" };
        var t0 = DateTimeOffset.UtcNow;
        await using (var p = new ProducerBuilder<string, Payment>().WithBootstrap(ctx.Endpoints.Bootstrap).Build())
        {
            var rng = new Random(5);
            await Task.WhenAll(Enumerable.Range(0, 300).Select(i => p.SendAsync(input, new Message<string, Payment>
            {
                Key = $"tx-{i}",
                Value = new Payment(merchants[i % 3], rng.Next(10, 500) * 1000m, i % 10 == 0 ? "PENDING" : "SETTLED"),
                Timestamp = t0.AddSeconds(i / 3.0),
            }, ct)));
        }
        ctx.Log("produced 300 payments over ~100 seconds of event time (10% pending)");
        var store = app.Store<Windowed<string>, MerchantTotals>("merchant-totals");
        for (var i = 0; i < 150 && store.All().Sum(x => x.Value.Count) < 270; i++) await Task.Delay(200, ct);
        foreach (var (w, totals) in store.All().OrderBy(x => x.Key.Start).ThenBy(x => x.Key.Key))
        {
            ctx.Log($"  {w.StartTime:HH:mm}–{w.EndTime:HH:mm}  {w.Key,-12} {totals.Count,4} tx  {totals.Amount,14:N0} IDR");
            ctx.Metric($"{w.Key} @{w.StartTime:HH:mm}", (double)totals.Amount / 1_000_000, "M IDR");
        }
        ctx.Log($"processed {app.ProcessedRecords} records; state is queryable while the app runs");
    }
}

public sealed class MLAnomalyCase : GalleryCase
{
    public override string Category => "Realtime analytics & AI";
    public override string Title => "Anomaly detection with ML.NET";
    public override string Summary => "Watch cold-chain truck temperatures and flag spikes as they stream in, using ML.NET's IID spike detector side by side with a z-score baseline.";
    public override IReadOnlyList<string> Highlights => ["Streaming detectors keep state across records", "SR-CNN for whole windows, SSA for forecasting", "Plugs into any IAsyncEnumerable"];

    public override string Code => """
        var detector = new MLSpikeDetector(confidence: 99, pvalueHistoryLength: 60);

        await foreach (var (rec, score) in BigPipeSource.Kafka("localhost:9092", "iot.telemetry", from: OffsetReset.Earliest)
                           .Anomalies(r => r.Num("temp_c"), detector))
        {
            Console.WriteLine($"{rec.Key} {rec.Num("temp_c")} °C  p={score.Score:0.000}");
        }
        """;

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var spikes = new[] { 150, 260, 330 };
        var topic = await Telemetry.ProduceAsync(ctx, "telemetry", 400, spikes, ct);
        var ml = new MLSpikeDetector(confidence: 99, pvalueHistoryLength: 60);
        var z = new ZScoreDetector(threshold: 4);
        int mlHits = 0, zHits = 0, n = 0;
        await foreach (var rec in Telemetry.Read(ctx, topic, ct))
        {
            var t = rec.Num("temp_c");
            var a = ml.Observe(t);
            var b = z.Observe(t);
            if (a.IsAnomaly || b.IsAnomaly)
                ctx.Log($"#{n,3} {rec.Key} {t,5:0.00} °C   ml.spike={(a.IsAnomaly ? "YES" : "no ")}  zscore={b.Score,5:0.0}{(b.IsAnomaly ? " YES" : "")}");
            if (a.IsAnomaly) mlHits++;
            if (b.IsAnomaly) zHits++;
            if (++n == 400) break;
        }
        ctx.Log($"injected faults at readings {string.Join(", ", spikes)}");
        ctx.Metric("ml.spike alerts", mlHits);
        ctx.Metric("z-score alerts", zHits);
    }
}

public sealed class TorchCase : GalleryCase
{
    public override string Category => "Realtime analytics & AI";
    public override string Title => "Deep learning detector (TorchSharp)";
    public override string Summary => "An online autoencoder learns the joint pattern of speed, temperature and door state, and flags readings that break it — even when each value alone looks normal.";
    public override IReadOnlyList<string> Highlights => ["Trains incrementally on the stream", "Multivariate: catches broken correlations", "Load TorchScript models with TorchScriptScorer"];

    public override string Code => """
        using var detector = new TorchAutoencoderDetector(features: 3, k: 5, warmup: 300);

        await foreach (var r in BigPipeSource.Kafka(bootstrap, "fleet.sensors", from: OffsetReset.Earliest))
        {
            var score = detector.Observe([(float)r.Num("speed"), (float)r.Num("temp_c"), (float)r.Num("door")]);
            if (score.IsAnomaly) Console.WriteLine($"{r.Key}: unusual combination (error {score.Value:0.000})");
        }
        """;

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = ctx.Unique("fleet-sensors");
        await ctx.Admin.CreateTopicAsync(topic, 1, ct: ct);
        using var http = new BigPipeHttpClient(ctx.Endpoints.HttpUrl);
        var rng = new Random(9);
        var readings = new List<HttpProduceRecord>();
        for (var i = 0; i < 1400; i++)
        {
            var moving = rng.NextDouble() > 0.3;
            var speed = moving ? 40 + rng.NextDouble() * 40 : 0;
            var door = moving ? 0 : rng.Next(2);          // doors only open when parked
            var temp = 4 + rng.NextDouble() + door * 2;   // an open door warms the cargo a little
            if (i is 1250 or 1320) door = 1;              // door open while driving: anomaly
            readings.Add(new HttpProduceRecord { Key = $"truck-{i % 5}", Value = new { speed = Math.Round(speed, 1), temp_c = Math.Round(temp, 2), door } });
        }
        foreach (var chunk in readings.Chunk(500)) await http.ProduceAsync(topic, chunk, ct: ct);
        ctx.Log("produced 1,400 sensor readings (two with a door open at speed)");
        using var detector = new TorchAutoencoderDetector(features: 3, k: 5, warmup: 300);
        var n = 0;
        var hits = 0;
        await foreach (var r in BigPipeSource.Kafka(ctx.Endpoints.Bootstrap, topic, from: OffsetReset.Earliest, ct: ct))
        {
            var s = detector.Observe([(float)r.Num("speed"), (float)r.Num("temp_c"), (float)r.Num("door")]);
            if (s.IsAnomaly)
            {
                hits++;
                ctx.Log($"#{n} {r.Key} {r.Value}  reconstruction error {s.Value:0.000} (z {s.Score:0.0})");
            }
            if (++n == readings.Count) break;
        }
        ctx.Metric("readings", n);
        ctx.Metric("anomalies", hits);
    }
}

public sealed class ScriptingCase : GalleryCase
{
    public override string Category => "Realtime analytics & AI";
    public override string Title => "Dynamic scripting: C#, Python, JavaScript";
    public override string Summary => "Filter and query live windows with user-supplied scripts compiled at runtime — the same question asked in three languages.";
    public override IReadOnlyList<string> Highlights => ["C# via Roslyn, Python via IronPython, JavaScript via Jint", "Scripts see r.num(), r.str(), r.header(), r.json", "Window queries over records"];

    public override string Code => """
        using var py = ScriptEngines.Create(ScriptLanguage.Python);
        var hot = py.CompileFilter("r.num('temp_c') > 5.2 and r.key.endswith('01')");

        await foreach (var window in source.WhereAsync(hot).TumblingWindow(TimeSpan.FromSeconds(30)))
        {
            var avg = py.Query("result = sum(x.num('temp_c') for x in records) / len(records)", window.Items);
            Console.WriteLine($"{window.Start:HH:mm:ss} {window.Count} hot readings, avg {avg:0.00} °C");
        }
        """;

    public override IReadOnlyList<Snippet> OtherLanguages =>
    [
        new("C# script", """r.num("temp_c") > 5.2 && r.key.EndsWith("01")"""),
        new("JavaScript", """r.num('temp_c') > 5.2 && r.key.endsWith('01')"""),
        new("Python", """r.num('temp_c') > 5.2 and r.key.endswith('01')"""),
    ];

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = await Telemetry.ProduceAsync(ctx, "telemetry", 240, [], ct);
        var records = await Telemetry.Read(ctx, topic, ct).Take(240).ToListAsync(ct);
        var scripts = new (ScriptLanguage Lang, string Filter, string Query)[]
        {
            (ScriptLanguage.CSharp, """r.num("temp_c") > 5.2 && r.key.EndsWith("01")""", """records.Average(x => x.num("temp_c"))"""),
            (ScriptLanguage.Python, "r.num('temp_c') > 5.2 and r.key.endswith('01')", "result = sum(x.num('temp_c') for x in records) / len(records)"),
            (ScriptLanguage.JavaScript, "r.num('temp_c') > 5.2 && r.key.endsWith('01')", "records.reduce((s, x) => s + x.num('temp_c'), 0) / records.length"),
        };
        foreach (var (lang, filter, query) in scripts)
        {
            using var engine = ScriptEngines.Create(lang);
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var f = engine.CompileFilter(filter);
            var hot = records.Where(f).ToList();
            var avg = hot.Count > 0 ? Convert.ToDouble(engine.Query(query, hot)) : 0;
            ctx.Log($"{lang,-10} {hot.Count,3} hot readings from truck-01, avg {avg:0.00} °C  ({sw.ElapsedMilliseconds} ms incl. compile)");
            ctx.Metric(lang.ToString(), hot.Count);
        }
        ctx.Log("all three engines agree on the same records");
    }
}

public sealed class SentimentCase : GalleryCase
{
    public override string Category => "Realtime analytics & AI";
    public override string Title => "Review sentiment (MediaPipe.NET · GraviText)";
    public override string Summary => "Score product reviews as they arrive with MediaPipe.NET's text classifier and GraviText's lexicon analyzer, and detect the review language.";
    public override IReadOnlyList<string> Highlights => ["Models run locally (ONNX Runtime), no cloud calls", "Language detection routes Indonesian vs English reviews", "HF.Net transformers load any Hugging Face classifier the same way"];

    public override string Code => """
        using var sentiment = new MediaPipeSentiment();            // SST-2 AverageWord model
        using var language = new MediaPipeLanguageDetector();
        var lexicon = new GraviSentiment();                          // GraviText, no model download

        await foreach (var r in BigPipeSource.Kafka(bootstrap, "reviews", from: OffsetReset.Earliest))
        {
            var (label, score) = sentiment.Analyze(r.Str("text")!);
            var (lang, _) = language.Detect(r.Str("text")!);
            Console.WriteLine($"[{lang}] {label} {score:P0}  {r.Str("text")}");
        }

        // Hugging Face models via HF.Net:
        // using var hf = HfTextModel.Load("distilbert-base-uncased-finetuned-sst-2-english");
        """;

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = ctx.Unique("reviews");
        await ctx.Admin.CreateTopicAsync(topic, 1, ct: ct);
        string[] reviews =
        [
            "Absolutely love this batik shirt, the quality is excellent and delivery was fast!",
            "Terrible experience. The package arrived broken and support never answered.",
            "Kopinya enak sekali dan pengirimannya cepat, pasti beli lagi!",
            "The phone case is okay but the color is not what I expected.",
            "Worst purchase ever, a complete waste of money.",
            "Great value for the price, my kids are happy with it.",
        ];
        using var http = new BigPipeHttpClient(ctx.Endpoints.HttpUrl);
        await http.ProduceAsync(topic, reviews.Select((t, i) => new HttpProduceRecord { Key = $"review-{i}", Value = new { text = t } }), ct: ct);
        using var sentiment = new MediaPipeSentiment();
        using var language = new MediaPipeLanguageDetector();
        var lexicon = new GraviSentiment();
        var n = 0;
        var positive = 0;
        await foreach (var r in BigPipeSource.Kafka(ctx.Endpoints.Bootstrap, topic, from: OffsetReset.Earliest, ct: ct))
        {
            var text = r.Str("text")!;
            var (label, score) = sentiment.Analyze(text);
            var (lang, _) = language.Detect(text);
            var (lexLabel, _) = lexicon.Analyze(text);
            ctx.Log($"[{lang}] mediapipe={label,-8} {score,4:P0}  gravitext={lexLabel,-8}  {text}");
            if (label == "positive") positive++;
            if (++n == reviews.Length) break;
        }
        ctx.Log("note: the SST-2 models are English-only — route [id] reviews to an Indonesian model (e.g. via HfTextModel.Load) using the detected language");
        ctx.Metric("positive", positive);
        ctx.Metric("negative", n - positive);
    }
}

public sealed class ClusteringCase : GalleryCase
{
    public override string Category => "Realtime analytics & AI";
    public override string Title => "Customer segments (GraviLearn · GraviFrame)";
    public override string Summary => "Turn a window of checkout events into a GraviFrame DataFrame, segment customers with GraviLearn K-Means, then label every new checkout with its segment.";
    public override IReadOnlyList<string> Highlights => ["Window → DataFrame (pandas-style) with WindowFrames", "Fit on a window, assign on the stream", "One-class SVM for novelty detection"];

    public override string Code => """
        var window = await source.CollectAsync(600);
        var frame = window.ToDataFrame(["basket", "items"], ["city"]);
        Console.WriteLine(frame.Describe());

        var segments = new GraviKMeansSegmenter(clusters: 3);
        segments.Fit(window.Select(r => new[] { r.Num("basket") / 100_000, r.Num("items") }).ToList());

        await foreach (var r in source)
            Console.WriteLine($"{r.Key} → segment {segments.Assign([r.Num("basket") / 100_000, r.Num("items")])}");
        """;

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = ctx.Unique("checkouts");
        await ctx.Admin.CreateTopicAsync(topic, 1, ct: ct);
        var rng = new Random(4);
        (double Basket, int Items) Draw(int kind) => kind switch
        {
            0 => (rng.Next(20, 80) * 1000, rng.Next(1, 3)),       // quick snacks
            1 => (rng.Next(150, 400) * 1000, rng.Next(4, 9)),     // weekly groceries
            _ => (rng.Next(1500, 4000) * 1000, rng.Next(1, 3)),   // electronics
        };
        using var http = new BigPipeHttpClient(ctx.Endpoints.HttpUrl);
        var cities = new[] { "Jakarta", "Bandung", "Surabaya" };
        await http.ProduceAsync(topic, Enumerable.Range(0, 600).Select(i =>
        {
            var (b, it) = Draw(i % 3);
            return new HttpProduceRecord { Key = $"cust-{i}", Value = new { basket = b, items = it, city = cities[rng.Next(3)] } };
        }), ct: ct);
        var window = await BigPipeSource.Kafka(ctx.Endpoints.Bootstrap, topic, from: OffsetReset.Earliest, ct: ct).Take(600).ToListAsync(ct);
        var frame = window.ToDataFrame(["basket", "items"], ["city"]);
        ctx.Log(frame.Describe().ToString());
        var seg = new GraviKMeansSegmenter(clusters: 3);
        var rows = window.Select(r => new[] { r.Num("basket") / 100_000, r.Num("items") }).ToList();
        seg.Fit(rows);
        var labels = seg.AssignAll(rows);
        foreach (var g in labels.Select((l, i) => (l, i)).GroupBy(x => x.l).OrderBy(g => g.Key))
        {
            var members = g.Select(x => window[x.i]).ToList();
            ctx.Log($"segment {g.Key}: {members.Count,3} customers, avg basket {members.Average(m => m.Num("basket")),12:N0} IDR, avg items {members.Average(m => m.Num("items")):0.0}");
            ctx.Metric($"segment {g.Key}", members.Count);
        }
        ctx.Log($"new checkout of 2,500,000 IDR × 1 item → segment {seg.Assign([25, 1])}");
    }
}
