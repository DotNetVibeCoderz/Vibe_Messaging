using BigPipe.Analytics;
using BigPipe.Analytics.Gravicode;
using BigPipe.Analytics.ML;
using BigPipe.Analytics.Scripting;
using BigPipe.Analytics.Torch;

namespace BigPipe.Client.Tests;

public class AnalyticsTests
{
    private static List<double> SeriesWithSpike(int n = 300, int spikeAt = 250, double spike = 60)
    {
        var rng = new Random(3);
        return Enumerable.Range(0, n).Select(i => 10 + rng.NextDouble() * 2 + (i == spikeAt ? spike : 0)).ToList();
    }

    [Fact]
    public void Running_stats_and_sketches()
    {
        var s = new RunningStats();
        foreach (var x in new[] { 2.0, 4, 4, 4, 5, 5, 7, 9 }) s.Add(x);
        Assert.Equal(5, s.Mean, 6);
        Assert.Equal(2.138, s.StdDev, 3);

        var hll = new HyperLogLog();
        for (var i = 0; i < 50_000; i++) hll.Add($"user-{i % 20_000}");
        Assert.InRange(hll.Estimate(), 19_000, 21_000);

        var q = new QuantileSketch();
        for (var i = 1; i <= 10_000; i++) q.Add(i);
        Assert.InRange(q.Quantile(0.5), 4_700, 5_300);

        var top = new TopK(10);
        for (var i = 0; i < 1000; i++) top.Add(i % 10 == 0 ? "hot" : $"cold-{i}");
        Assert.Equal("hot", top.Top(1)[0].Item);

        var cms = new CountMinSketch();
        for (var i = 0; i < 500; i++) cms.Add("ID");
        Assert.InRange(cms.Estimate("ID"), 500, 510);
    }

    [Theory]
    [InlineData("zscore")]
    [InlineData("ewma")]
    [InlineData("mad")]
    [InlineData("ml.spike")]
    [InlineData("torch.window")]
    public void Detectors_find_injected_spike(string name)
    {
        IAnomalyDetector d = name switch
        {
            "zscore" => new ZScoreDetector(),
            "ewma" => new EwmaDetector(),
            "mad" => new MadDetector(),
            "ml.spike" => new MLSpikeDetector(confidence: 99, pvalueHistoryLength: 60),
            _ => new TorchWindowDetector(window: 8, k: 6, warmup: 150),
        };
        var flagged = SeriesWithSpike().Select((v, i) => (i, d.Observe(v))).Where(x => x.Item2.IsAnomaly).Select(x => x.i).ToList();
        Assert.Contains(flagged, i => i >= 250 && i <= 257);
        Assert.True(flagged.Count(i => i < 240) <= 3, $"{name} false positives: {string.Join(",", flagged)}");
    }

    [Fact]
    public void SrCnn_and_ssa_forecast()
    {
        var series = Enumerable.Range(0, 200).Select(i => 20 + 5 * Math.Sin(i * 2 * Math.PI / 24)).ToList();
        series[150] += 40;
        var points = new SrCnnWindowDetector().Detect(series);
        Assert.Contains(points, p => p.IsAnomaly && p.Index == 150);

        var f = new SsaForecaster(windowSize: 24, seriesLength: 72, horizon: 3);
        f.Train(series.Take(120));
        var forecast = f.ObserveAndForecast(series[120]);
        Assert.Equal(3, forecast.ForecastedValues.Length);
        Assert.InRange(forecast.ForecastedValues[0], 10, 30);
    }

    [Fact]
    public void Streaming_classifier_learns_online()
    {
        var clf = new StreamingBinaryClassifier(features: 2, retrainEvery: 200);
        var rng = new Random(1);
        for (var i = 0; i < 1000; i++)
        {
            var amount = (float)rng.NextDouble() * 100;
            var hour = (float)rng.Next(24);
            clf.AddExample([amount, hour], amount > 70 && hour < 6);
        }
        Assert.True(clf.IsTrained);
        Assert.True(clf.PredictProbability([95, 2]) > 0.5f);
        Assert.True(clf.PredictProbability([10, 14]) < 0.5f);
    }

    [Fact]
    public void Torch_autoencoder_flags_multivariate_outlier()
    {
        using var ae = new TorchAutoencoderDetector(features: 3, k: 5, warmup: 300);
        var rng = new Random(5);
        for (var i = 0; i < 1500; i++)
        {
            var a = (float)rng.NextDouble();
            ae.Observe([a, 2 * a, 1 - a]); // strongly correlated features
        }
        var normal = ae.Observe([0.5f, 1.0f, 0.5f]);
        var odd = ae.Observe([0.5f, -1.0f, 1.5f]); // breaks the correlation
        Assert.False(normal.IsAnomaly);
        Assert.True(odd.IsAnomaly, odd.ToString());
    }

    [Theory]
    [InlineData(ScriptLanguage.CSharp, "r.num(\"amount\") > 1000 && r.header(\"region\") == \"ID\"", "r.num(\"amount\") * 2", "records.Sum(x => x.num(\"amount\"))")]
    [InlineData(ScriptLanguage.Python, "r.num('amount') > 1000 and r.header('region') == 'ID'", "r.json['amount'] * 2", "result = sum(x.num('amount') for x in records)")]
    [InlineData(ScriptLanguage.JavaScript, "r.num('amount') > 1000 && r.header('region') === 'ID'", "r.json.amount * 2", "records.reduce((s, x) => s + x.num('amount'), 0)")]
    public void Scripting_filter_project_query(ScriptLanguage lang, string filter, string projection, string query)
    {
        using var engine = ScriptEngines.Create(lang);
        var records = new[] { 500.0, 1500, 2500 }.Select(a => new AnalyticsRecord
        {
            Value = $"{{\"amount\":{a}}}",
            Headers = new Dictionary<string, string?> { ["region"] = a > 2000 ? "SG" : "ID" },
        }).ToList();
        var f = engine.CompileFilter(filter);
        Assert.Equal([false, true, false], records.Select(f));
        var p = engine.CompileProjection(projection);
        Assert.Equal(3000.0, Convert.ToDouble(p(records[1])));
        Assert.Equal(4500.0, Convert.ToDouble(engine.Query(query, records)));
    }

    [Fact]
    public void Script_errors_are_reported()
    {
        using var engine = ScriptEngines.Create(ScriptLanguage.CSharp);
        Assert.Throws<ScriptException>(() => engine.CompileFilter("r.nope("));
    }

    [Fact]
    public async Task Windows_and_summaries()
    {
        var t0 = new DateTimeOffset(2026, 9, 29, 0, 0, 0, TimeSpan.Zero);
        var records = Enumerable.Range(0, 100).Select(i => AnalyticsRecord.Of(new { amount = i, merchant = i % 2 == 0 ? "A" : "B" }, timestamp: t0.AddSeconds(i)));
        var windows = await BigPipeSource.FromEnumerable(records).TumblingWindow(TimeSpan.FromSeconds(30)).ToListAsync();
        Assert.Equal(4, windows.Count);
        Assert.Equal(30, windows[0].Count);
        var summary = WindowSummary.Of(windows[0], "amount");
        Assert.Equal(14.5, summary.Mean, 6);
        var keyed = await BigPipeSource.FromEnumerable(records).TumblingWindow(TimeSpan.FromSeconds(30)).GroupWindow(r => r.Str("merchant")!).ToListAsync();
        Assert.Equal(8, keyed.Count);
    }

    [Fact]
    public void Gravicode_frames_clustering_ocsvm_sentiment()
    {
        var records = Enumerable.Range(0, 60).Select(i => AnalyticsRecord.Of(new { amount = i % 3 == 0 ? 1000 + i : 10 + i % 5, city = i % 2 == 0 ? "Bandung" : "Jakarta" })).ToList();
        var frame = records.ToDataFrame(["amount"], ["city"]);
        Assert.True(frame.HasColumn("amount") && frame.HasColumn("city"));
        Assert.Equal(60, frame.Numeric("amount").Length);

        var seg = new GraviKMeansSegmenter(clusters: 2);
        var rows = records.Select(r => new[] { r.Num("amount") }).ToList();
        seg.Fit(rows);
        Assert.NotEqual(seg.Assign([5]), seg.Assign([1050]));

        var svm = new GraviOneClassSvmDetector(nu: 0.05);
        var rng = new Random(2);
        svm.Fit(Enumerable.Range(0, 200).Select(_ => new[] { rng.NextDouble(), rng.NextDouble() }).ToList());
        Assert.True(svm.Observe([8, -7]).IsAnomaly);
        Assert.False(svm.Observe([0.5, 0.5]).IsAnomaly);

        var sentiment = new GraviSentiment();
        Assert.Equal("positive", sentiment.Analyze("great service, love it, excellent and happy").Label);
        Assert.Equal("negative", sentiment.Analyze("terrible, awful, broken and bad experience").Label);
    }

    [Fact]
    public void MediaPipe_text_sentiment_and_language()
    {
        using var s = new MediaPipeSentiment();
        Assert.Equal("positive", s.Analyze("What a wonderful, delightful product. I love it!").Label);
        Assert.Equal("negative", s.Analyze("This is the worst, most disappointing purchase ever.").Label);
    }
}
