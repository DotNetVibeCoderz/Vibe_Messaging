using Gravicode.HFNet.GraviTransformers;
using Gravicode.Science.GraviFrame;
using Gravicode.Science.GraviLearn.Anomaly;
using Gravicode.Science.GraviLearn.Clustering;
using Gravicode.Science.GraviNum;
using Gravicode.Science.GraviText.Tasks;
using MpLanguageDetector = MediaPipeNet.Tasks.Text.LanguageDetector;
using MpTextClassifier = MediaPipeNet.Tasks.Text.TextClassifier;
using MediaPipeNet.Tasks.Text;

namespace BigPipe.Analytics.Gravicode;

/// <summary>Turns windows of records into GraviFrame DataFrames for pandas-style analysis.</summary>
public static class WindowFrames
{
    /// <summary>
    /// Builds a DataFrame with the given numeric (dotted JSON paths) and text columns.
    /// Adds <c>offset</c> and <c>timestamp</c> (ms) columns.
    /// </summary>
    public static DataFrame ToDataFrame(this IReadOnlyList<AnalyticsRecord> records, IReadOnlyList<string> numeric, IReadOnlyList<string>? text = null)
    {
        var columns = new List<Series>
        {
            new NumericSeries("offset", records.Select(r => (double)r.Offset).ToArray()),
            new NumericSeries("timestamp", records.Select(r => (double)r.Timestamp.ToUnixTimeMilliseconds()).ToArray()),
        };
        foreach (var n in numeric)
            columns.Add(new NumericSeries(n, records.Select(r => r.Num(n)).ToArray()));
        foreach (var t in text ?? [])
            columns.Add(new TextSeries(t, records.Select(r => r.Str(t) ?? "").ToArray()));
        return new DataFrame(columns);
    }

    public static DataFrame ToDataFrame(this Window<AnalyticsRecord> window, IReadOnlyList<string> numeric, IReadOnlyList<string>? text = null) =>
        window.Items.ToDataFrame(numeric, text);

    /// <summary>Feature matrix (rows = records) from numeric JSON paths.</summary>
    public static NdArray ToMatrix(this IReadOnlyList<AnalyticsRecord> records, IReadOnlyList<string> features) =>
        NdArray.FromRows(records.Select(r => features.Select(r.Num).ToArray()).ToList());
}

/// <summary>
/// Segments a stream with GraviLearn K-Means: fit on a window of feature vectors, then assign
/// each new record to its nearest centroid (refit periodically).
/// </summary>
public sealed class GraviKMeansSegmenter(int clusters = 4, int seed = 42)
{
    private KMeans? _model;

    public bool IsFitted => _model is not null;
    public NdArray? Centroids => _model?.Centroids;
    public double Inertia => _model?.Inertia ?? double.NaN;

    public void Fit(IReadOnlyList<double[]> rows)
    {
        var km = new KMeans(clusters, seed: seed);
        km.Fit(NdArray.FromRows(rows));
        _model = km;
    }

    public int Assign(double[] features)
    {
        var model = _model ?? throw new InvalidOperationException("call Fit first");
        return (int)model.Predict(NdArray.FromRows([features])).ToArray()[0];
    }

    public int[] AssignAll(IReadOnlyList<double[]> rows)
    {
        var model = _model ?? throw new InvalidOperationException("call Fit first");
        return model.Predict(NdArray.FromRows(rows)).ToArray().Select(x => (int)x).ToArray();
    }
}

/// <summary>
/// Novelty detection with GraviLearn's one-class SVM: learn the shape of normal traffic from a
/// baseline window, then flag records outside it (decision function &lt; 0).
/// </summary>
public sealed class GraviOneClassSvmDetector(double nu = 0.05, double? gamma = null)
{
    private OneClassSvm? _svm;
    private double[] _mean = [];
    private double[] _scale = [];

    public bool IsFitted => _svm is not null;

    public void Fit(IReadOnlyList<double[]> baseline)
    {
        var d = baseline[0].Length;
        _mean = Enumerable.Range(0, d).Select(j => baseline.Average(r => r[j])).ToArray();
        _scale = Enumerable.Range(0, d).Select(j =>
        {
            var sd = Math.Sqrt(baseline.Average(r => Math.Pow(r[j] - _mean[j], 2)));
            return sd > 0 ? sd : 1;
        }).ToArray();
        // A smooth kernel on standardized features (0.2/d) avoids holes between support vectors.
        _svm = new OneClassSvm(nu, SvmKernel.Rbf, gamma ?? 0.2 / d).Fit(NdArray.FromRows(baseline.Select(Standardize).ToList()));
    }

    private double[] Standardize(double[] x) => x.Select((v, j) => (v - _mean[j]) / _scale[j]).ToArray();

    public AnomalyScore Observe(double[] features)
    {
        var svm = _svm ?? throw new InvalidOperationException("call Fit first");
        var score = svm.DecisionFunction(NdArray.FromRows([Standardize(features)])).ToArray()[0];
        return new AnomalyScore(features.Length > 0 ? features[0] : 0, -score, score < 0, "gravi.ocsvm");
    }
}

/// <summary>Sentiment of a text field with GraviText's lexicon analyzer (no model download).</summary>
public sealed class GraviSentiment
{
    private readonly SentimentAnalyzer _analyzer = new();
    private readonly object _lock = new();

    /// <summary>Optionally trains on labelled examples to go beyond the lexicon.</summary>
    public GraviSentiment Train(IReadOnlyList<string> documents, IReadOnlyList<string> labels)
    {
        lock (_lock) _analyzer.Train(documents, labels);
        return this;
    }

    public (string Label, double Confidence) Analyze(string text)
    {
        lock (_lock)
        {
            var p = _analyzer.IsTrained ? _analyzer.Analyze(text) : _analyzer.AnalyzeWithLexicon(text);
            return (p.Label, p.Confidence);
        }
    }
}

/// <summary>
/// Hugging Face transformer models through HF.Net (GraviTransformers). Loads a model from the
/// Hub (cached locally) — e.g. a sentiment classifier or a sentence-embedding model.
/// </summary>
public sealed class HfTextModel : IDisposable
{
    private readonly TransformerModel _model;
    private readonly object _lock = new();

    private HfTextModel(TransformerModel model) => _model = model;

    /// <summary>Loads <paramref name="repoId"/>, e.g. <c>distilbert-base-uncased-finetuned-sst-2-english</c>.</summary>
    public static HfTextModel Load(string repoId, string revision = "main") => new(TransformerModel.Load(repoId, revision));

    public string RepoId => _model.RepoId;
    public bool CanClassify => _model.HasClassificationHead;
    public IReadOnlyList<string> Labels => _model.Labels;

    public (string Label, double Score) Classify(string text)
    {
        lock (_lock)
        {
            var top = _model.Predict(text, 1)[0];
            return (top.Label, top.Score);
        }
    }

    public double[] Embed(string text)
    {
        lock (_lock) return _model.Embed(text).ToArray();
    }

    public double Similarity(string a, string b)
    {
        lock (_lock) return _model.Similarity(a, b);
    }

    public void Dispose() => _model.Dispose();
}

/// <summary>Keeps records whose text is semantically close to a reference description.</summary>
public sealed class SemanticFilter
{
    private readonly HfTextModel _model;
    private readonly double[] _reference;
    private readonly double _threshold;

    public SemanticFilter(HfTextModel model, string reference, double threshold = 0.6)
    {
        _model = model;
        _reference = model.Embed(reference);
        _threshold = threshold;
    }

    public double Score(string text) => Cosine(_model.Embed(text), _reference);

    public bool Matches(string text) => Score(text) >= _threshold;

    private static double Cosine(double[] a, double[] b)
    {
        double dot = 0, na = 0, nb = 0;
        for (var i = 0; i < Math.Min(a.Length, b.Length); i++)
        {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        return na > 0 && nb > 0 ? dot / Math.Sqrt(na * nb) : 0;
    }
}

/// <summary>MediaPipe.NET text sentiment (SST-2) — AverageWord (tiny, fast) or MobileBERT (accurate).</summary>
public sealed class MediaPipeSentiment : IDisposable
{
    private readonly MpTextClassifier _classifier;

    public MediaPipeSentiment(bool accurate = false)
    {
        OnnxRuntimeNative.EnsureLoaded();
        _classifier = MpTextClassifier.Create(new TextClassifierOptions { Model = accurate ? TextClassifierModel.Bert : TextClassifierModel.AverageWord });
    }

    /// <summary>Returns "positive"/"negative" with a score.</summary>
    public (string Label, float Score) Analyze(string text)
    {
        var top = _classifier.Classify(text).TopCategory;
        var name = top?.CategoryName ?? "";
        var label = name switch
        {
            "1" => "positive",
            "0" => "negative",
            _ => name.ToLowerInvariant(),
        };
        return (label, top?.Score ?? 0);
    }

    public void Dispose() => _classifier.Dispose();
}

/// <summary>MediaPipe.NET language detection (route or filter streams by language).</summary>
public sealed class MediaPipeLanguageDetector : IDisposable
{
    private readonly MpLanguageDetector _detector;

    public MediaPipeLanguageDetector()
    {
        OnnxRuntimeNative.EnsureLoaded();
        _detector = MpLanguageDetector.Create();
    }

    public (string Language, float Probability) Detect(string text)
    {
        var top = _detector.Detect(text).TopLanguage;
        return top is { } t ? (t.LanguageCode, t.Probability) : ("und", 0);
    }

    public void Dispose() => _detector.Dispose();
}
