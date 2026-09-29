using Microsoft.ML;
using Microsoft.ML.Data;
using Microsoft.ML.TimeSeries;
using Microsoft.ML.Transforms.TimeSeries;

namespace BigPipe.Analytics.ML;

internal sealed class ValuePoint
{
    public float Value { get; set; }
}

internal sealed class DetectionOutput
{
    // [alert, raw score, p-value] (+ martingale for change points)
    [VectorType]
    public double[] Prediction { get; set; } = [];
}

/// <summary>
/// ML.NET IID spike detector as a streaming <see cref="IAnomalyDetector"/>: a stateful
/// prediction engine keeps the p-value history, so each call scores one new observation.
/// </summary>
public sealed class MLSpikeDetector : IAnomalyDetector
{
    private readonly TimeSeriesPredictionEngine<ValuePoint, DetectionOutput> _engine;

    public MLSpikeDetector(double confidence = 95, int pvalueHistoryLength = 30, AnomalySide side = AnomalySide.TwoSided, int seed = 1)
    {
        var ml = new MLContext(seed);
        var empty = ml.Data.LoadFromEnumerable(new List<ValuePoint>());
        var pipeline = ml.Transforms.DetectIidSpike(nameof(DetectionOutput.Prediction), nameof(ValuePoint.Value), confidence, pvalueHistoryLength, side);
        _engine = pipeline.Fit(empty).CreateTimeSeriesEngine<ValuePoint, DetectionOutput>(ml);
    }

    public string Name => "ml.spike";

    public AnomalyScore Observe(double value)
    {
        var p = _engine.Predict(new ValuePoint { Value = (float)value }).Prediction;
        return new AnomalyScore(value, p.Length > 2 ? 1 - p[2] : 0, p.Length > 0 && p[0] > 0, Name);
    }
}

/// <summary>ML.NET IID change-point detector (martingale): flags level shifts rather than single spikes.</summary>
public sealed class MLChangePointDetector : IAnomalyDetector
{
    private readonly TimeSeriesPredictionEngine<ValuePoint, DetectionOutput> _engine;

    public MLChangePointDetector(double confidence = 95, int changeHistoryLength = 20, int seed = 1)
    {
        var ml = new MLContext(seed);
        var empty = ml.Data.LoadFromEnumerable(new List<ValuePoint>());
        var pipeline = ml.Transforms.DetectIidChangePoint(nameof(DetectionOutput.Prediction), nameof(ValuePoint.Value), confidence, changeHistoryLength);
        _engine = pipeline.Fit(empty).CreateTimeSeriesEngine<ValuePoint, DetectionOutput>(ml);
    }

    public string Name => "ml.changepoint";

    public AnomalyScore Observe(double value)
    {
        var p = _engine.Predict(new ValuePoint { Value = (float)value }).Prediction;
        return new AnomalyScore(value, p.Length > 3 ? p[3] : 0, p.Length > 0 && p[0] > 0, Name);
    }
}

/// <summary>A point flagged by SR-CNN.</summary>
public readonly record struct SrCnnPoint(int Index, double Value, bool IsAnomaly, double Score, double ExpectedValue);

/// <summary>
/// Microsoft's Spectral Residual + CNN detector (the algorithm behind Azure Anomaly Detector),
/// run over a whole window at once. Good for periodic series with a batch per window.
/// </summary>
public sealed class SrCnnWindowDetector(double threshold = 0.3, int batchSize = -1, double sensitivity = 64, SrCnnDetectMode mode = SrCnnDetectMode.AnomalyAndExpectedValue)
{
    private readonly MLContext _ml = new(1);

    private sealed class SrCnnOutput
    {
        [VectorType]
        public double[] Prediction { get; set; } = [];
    }

    public IReadOnlyList<SrCnnPoint> Detect(IReadOnlyList<double> values)
    {
        if (values.Count < 12) return values.Select((v, i) => new SrCnnPoint(i, v, false, 0, v)).ToList();
        var data = _ml.Data.LoadFromEnumerable(values.Select(v => new SrCnnInput { Value = v }));
        var output = _ml.AnomalyDetection.DetectEntireAnomalyBySrCnn(data, nameof(SrCnnOutput.Prediction), nameof(SrCnnInput.Value),
            threshold, batchSize, sensitivity, mode);
        return _ml.Data.CreateEnumerable<SrCnnOutput>(output, reuseRowObject: false)
            .Select((o, i) => new SrCnnPoint(i, values[i], o.Prediction[0] > 0, o.Prediction[1], o.Prediction.Length > 3 ? o.Prediction[3] : values[i]))
            .ToList();
    }

    private sealed class SrCnnInput
    {
        public double Value { get; set; }
    }
}

public sealed class SsaForecast
{
    public float[] ForecastedValues { get; set; } = [];
    public float[] LowerBound { get; set; } = [];
    public float[] UpperBound { get; set; } = [];
}

/// <summary>
/// Singular Spectrum Analysis forecaster. Train on history, then feed each new observation
/// with <see cref="ObserveAndForecast"/> to get an updated forecast with confidence bounds.
/// </summary>
public sealed class SsaForecaster
{
    private readonly MLContext _ml = new(1);
    private readonly int _windowSize;
    private readonly int _seriesLength;
    private readonly int _horizon;
    private readonly float _confidence;
    private TimeSeriesPredictionEngine<ValuePoint, SsaForecast>? _engine;

    public SsaForecaster(int windowSize = 12, int seriesLength = 48, int horizon = 5, float confidence = 0.95f)
    {
        _windowSize = windowSize;
        _seriesLength = seriesLength;
        _horizon = horizon;
        _confidence = confidence;
    }

    public bool IsTrained => _engine is not null;

    /// <summary>Fits the model; needs at least <c>2 * windowSize</c> points.</summary>
    public void Train(IEnumerable<double> history)
    {
        var points = history.Select(v => new ValuePoint { Value = (float)v }).ToList();
        if (points.Count < 2 * _windowSize)
            throw new ArgumentException($"SSA needs at least {2 * _windowSize} points to train, got {points.Count}");
        var pipeline = _ml.Forecasting.ForecastBySsa(nameof(SsaForecast.ForecastedValues), nameof(ValuePoint.Value), _windowSize,
            Math.Max(_seriesLength, _windowSize + 1), points.Count, _horizon, confidenceLevel: _confidence,
            confidenceLowerBoundColumn: nameof(SsaForecast.LowerBound), confidenceUpperBoundColumn: nameof(SsaForecast.UpperBound));
        var model = pipeline.Fit(_ml.Data.LoadFromEnumerable(points));
        _engine = model.CreateTimeSeriesEngine<ValuePoint, SsaForecast>(_ml);
    }

    /// <summary>Adds an observation to the model state and returns the next <c>horizon</c> values.</summary>
    public SsaForecast ObserveAndForecast(double value)
    {
        if (_engine is null) throw new InvalidOperationException("call Train first");
        return _engine.Predict(new ValuePoint { Value = (float)value });
    }

    /// <summary>Forecast from the current state without adding an observation.</summary>
    public SsaForecast Forecast() => (_engine ?? throw new InvalidOperationException("call Train first")).Predict();
}
