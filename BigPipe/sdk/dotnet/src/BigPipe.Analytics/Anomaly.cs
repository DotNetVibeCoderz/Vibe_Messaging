namespace BigPipe.Analytics;

/// <summary>Result of scoring one observation.</summary>
public readonly record struct AnomalyScore(double Value, double Score, bool IsAnomaly, string Detector)
{
    public override string ToString() => $"{Detector}: value={Value:0.###} score={Score:0.###}{(IsAnomaly ? " ANOMALY" : "")}";
}

/// <summary>Scores a stream of numbers one at a time, learning as it goes.</summary>
public interface IAnomalyDetector
{
    string Name { get; }
    AnomalyScore Observe(double value);
}

/// <summary>Flags values more than <paramref name="threshold"/> standard deviations from the running mean.</summary>
public sealed class ZScoreDetector(double threshold = 3.0, int warmup = 30) : IAnomalyDetector
{
    private readonly RunningStats _stats = new();

    public string Name => "zscore";

    public AnomalyScore Observe(double value)
    {
        var z = _stats.ZScore(value);
        var anomalous = _stats.Count >= warmup && Math.Abs(z) > threshold;
        // Anomalies are not learned, so a burst does not shift the baseline.
        if (!anomalous) _stats.Add(value);
        return new AnomalyScore(value, z, anomalous, Name);
    }
}

/// <summary>EWMA control chart: adapts to drift; flags deviations beyond k sigma of the smoothed level.</summary>
public sealed class EwmaDetector(double alpha = 0.1, double k = 3.0, int warmup = 20) : IAnomalyDetector
{
    private readonly Ewma _ewma = new(alpha);

    public string Name => "ewma";

    public AnomalyScore Observe(double value)
    {
        var level = _ewma.Value;
        var sd = _ewma.StdDev;
        var score = sd > 0 && !double.IsNaN(level) ? (value - level) / sd : 0;
        var anomalous = _ewma.Count >= warmup && Math.Abs(score) > k;
        if (!anomalous) _ewma.Add(value);
        return new AnomalyScore(value, score, anomalous, Name);
    }
}

/// <summary>Robust detector: median absolute deviation over a sliding window of recent values.</summary>
public sealed class MadDetector(int window = 200, double threshold = 3.5) : IAnomalyDetector
{
    private readonly Queue<double> _recent = new();

    public string Name => "mad";

    public AnomalyScore Observe(double value)
    {
        var score = 0.0;
        var anomalous = false;
        if (_recent.Count >= 10)
        {
            var sorted = _recent.OrderBy(x => x).ToArray();
            var median = sorted[sorted.Length / 2];
            var mad = sorted.Select(x => Math.Abs(x - median)).OrderBy(x => x).ElementAt(sorted.Length / 2);
            score = mad > 0 ? 0.6745 * (value - median) / mad : 0;
            anomalous = Math.Abs(score) > threshold;
        }
        _recent.Enqueue(value);
        if (_recent.Count > window) _recent.Dequeue();
        return new AnomalyScore(value, score, anomalous, Name);
    }
}
