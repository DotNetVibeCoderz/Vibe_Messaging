using Microsoft.ML;
using Microsoft.ML.Data;

namespace BigPipe.Analytics.ML;

/// <summary>
/// Binary classifier that learns from the stream: add labelled examples as they arrive,
/// retrain every <c>retrainEvery</c> examples (SDCA logistic regression over the most recent
/// <c>maxExamples</c>), and score new feature vectors in between.
/// </summary>
public sealed class StreamingBinaryClassifier
{
    private readonly MLContext _ml = new(1);
    private readonly int _features;
    private readonly int _maxExamples;
    private readonly int _retrainEvery;
    private readonly Queue<Example> _examples = new();
    private readonly object _lock = new();
    private PredictionEngine<Example, Output>? _engine;
    private int _sinceTrain;

    private sealed class Example
    {
        public bool Label { get; set; }
        public float[] Features { get; set; } = [];
    }

    private sealed class Output
    {
        public bool PredictedLabel { get; set; }
        public float Probability { get; set; }
        public float Score { get; set; }
    }

    public StreamingBinaryClassifier(int features, int maxExamples = 10_000, int retrainEvery = 500)
    {
        _features = features;
        _maxExamples = maxExamples;
        _retrainEvery = retrainEvery;
    }

    public bool IsTrained => _engine is not null;
    public int Examples => _examples.Count;
    public int Retrains { get; private set; }

    public void AddExample(float[] features, bool label)
    {
        if (features.Length != _features) throw new ArgumentException($"expected {_features} features, got {features.Length}");
        lock (_lock)
        {
            _examples.Enqueue(new Example { Label = label, Features = features });
            while (_examples.Count > _maxExamples) _examples.Dequeue();
            if (++_sinceTrain >= _retrainEvery) Retrain();
        }
    }

    /// <summary>Trains on the buffered examples now (requires both classes).</summary>
    public void Retrain()
    {
        lock (_lock)
        {
            _sinceTrain = 0;
            var data = _examples.ToList();
            if (data.Select(e => e.Label).Distinct().Count() < 2) return;
            var schema = SchemaDefinition.Create(typeof(Example));
            schema[nameof(Example.Features)].ColumnType = new VectorDataViewType(NumberDataViewType.Single, _features);
            var view = _ml.Data.LoadFromEnumerable(data, schema);
            var pipeline = _ml.Transforms.NormalizeMinMax(nameof(Example.Features))
                .Append(_ml.BinaryClassification.Trainers.SdcaLogisticRegression(nameof(Example.Label), nameof(Example.Features)));
            var model = pipeline.Fit(view);
            _engine = _ml.Model.CreatePredictionEngine<Example, Output>(model, inputSchemaDefinition: schema);
            Retrains++;
        }
    }

    /// <summary>Probability of the positive class (0.5 until the first training).</summary>
    public float PredictProbability(float[] features)
    {
        lock (_lock)
        {
            return _engine?.Predict(new Example { Features = features }).Probability ?? 0.5f;
        }
    }
}

/// <summary>Scores records with a model saved by ML.NET (<c>mlContext.Model.Save</c>).</summary>
public sealed class MLModelScorer<TInput, TOutput> : IDisposable
    where TInput : class
    where TOutput : class, new()
{
    private readonly PredictionEngine<TInput, TOutput> _engine;
    private readonly object _lock = new();

    public MLModelScorer(string modelPath)
    {
        var ml = new MLContext();
        var model = ml.Model.Load(modelPath, out _);
        _engine = ml.Model.CreatePredictionEngine<TInput, TOutput>(model);
    }

    public MLModelScorer(MLContext ml, ITransformer model) => _engine = ml.Model.CreatePredictionEngine<TInput, TOutput>(model);

    public TOutput Score(TInput input)
    {
        lock (_lock) return _engine.Predict(input);
    }

    public void Dispose() => _engine.Dispose();
}
