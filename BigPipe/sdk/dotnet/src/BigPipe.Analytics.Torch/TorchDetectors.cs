using TorchSharp;
using static TorchSharp.torch;
using static TorchSharp.torch.nn;

namespace BigPipe.Analytics.Torch;

/// <summary>
/// Online autoencoder for multivariate anomaly detection. Each observation is scored by its
/// reconstruction error; normal-looking observations are also used for a gradient step, so the
/// model adapts to the stream. After a warm-up, errors beyond <c>mean + k·sd</c> of recent
/// errors are anomalies.
/// </summary>
public sealed class TorchAutoencoderDetector : IDisposable
{
    private readonly Module<Tensor, Tensor> _model;
    private readonly optim.Optimizer _optimizer;
    // Recent reconstruction error level: EWMA so early training errors do not inflate the baseline.
    private readonly Ewma _errors = new(0.02);
    private readonly RunningStats[] _featureStats;
    private readonly double _k;
    private readonly int _warmup;
    private readonly object _lock = new();

    public TorchAutoencoderDetector(int features, int hidden = 0, double learningRate = 0.01, double k = 4.0, int warmup = 200, int seed = 7)
    {
        torch.manual_seed(seed);
        var h = hidden > 0 ? hidden : Math.Max(2, features / 2);
        var bottleneck = Math.Max(1, h / 2);
        _model = Sequential(
            ("enc1", Linear(features, h)), ("act1", Tanh()),
            ("enc2", Linear(h, bottleneck)), ("act2", Tanh()),
            ("dec1", Linear(bottleneck, h)), ("act3", Tanh()),
            ("dec2", Linear(h, features)));
        _optimizer = optim.Adam(_model.parameters(), learningRate);
        _featureStats = Enumerable.Range(0, features).Select(_ => new RunningStats()).ToArray();
        _k = k;
        _warmup = warmup;
        Features = features;
    }

    public int Features { get; }
    public long Observations { get; private set; }

    /// <summary>Scores and (if normal) learns one feature vector.</summary>
    public AnomalyScore Observe(float[] x)
    {
        if (x.Length != Features) throw new ArgumentException($"expected {Features} features, got {x.Length}");
        lock (_lock)
        {
            Observations++;
            // Standardize with running statistics so features on different scales train well.
            var z = new float[x.Length];
            for (var i = 0; i < x.Length; i++)
            {
                var s = _featureStats[i];
                z[i] = s.Count > 1 && s.StdDev > 0 ? (float)((x[i] - s.Mean) / s.StdDev) : 0f;
            }
            using var scope = NewDisposeScope();
            var input = tensor(z).reshape(1, Features);
            var recon = _model.forward(input);
            var loss = functional.mse_loss(recon, input);
            var error = loss.item<float>();
            var sd = _errors.StdDev;
            var score = _errors.Count > 1 && sd > 0 ? (error - _errors.Value) / sd : 0;
            var anomalous = Observations > _warmup && score > _k;
            if (!anomalous)
            {
                _optimizer.zero_grad();
                loss.backward();
                _optimizer.step();
                _errors.Add(error);
                for (var i = 0; i < x.Length; i++) _featureStats[i].Add(x[i]);
            }
            return new AnomalyScore(error, score, anomalous, "torch.autoencoder");
        }
    }

    public void Save(string path) => _model.save(path);

    public void Dispose()
    {
        _optimizer.Dispose();
        _model.Dispose();
    }
}

/// <summary>
/// Univariate adapter: turns the last <c>window</c> values into a feature vector for the
/// autoencoder, so shapes (not just levels) that look unusual are detected.
/// </summary>
public sealed class TorchWindowDetector(int window = 16, double k = 4.0, int warmup = 200) : IAnomalyDetector, IDisposable
{
    private readonly TorchAutoencoderDetector _ae = new(window, k: k, warmup: warmup);
    private readonly Queue<float> _recent = new();

    public string Name => "torch.window";

    public AnomalyScore Observe(double value)
    {
        _recent.Enqueue((float)value);
        if (_recent.Count > window) _recent.Dequeue();
        if (_recent.Count < window) return new AnomalyScore(value, 0, false, Name);
        var s = _ae.Observe([.. _recent]);
        return s with { Value = value, Detector = Name };
    }

    public void Dispose() => _ae.Dispose();
}

/// <summary>Scores feature vectors with a TorchScript model exported from PyTorch (<c>torch.jit.save</c>).</summary>
public sealed class TorchScriptScorer : IDisposable
{
    private readonly jit.ScriptModule<Tensor, Tensor> _module;
    private readonly object _lock = new();

    public TorchScriptScorer(string path)
    {
        _module = jit.load<Tensor, Tensor>(path);
        _module.eval();
    }

    public float[] Score(float[] features)
    {
        lock (_lock)
        {
            using var scope = NewDisposeScope();
            using var _ = no_grad();
            var output = _module.forward(tensor(features).reshape(1, features.Length));
            return output.data<float>().ToArray();
        }
    }

    public void Dispose() => _module.Dispose();
}
