// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using BenchmarkDotNet.Attributes;
using Nerve.Benchmarks.Baseline;

namespace Nerve.Benchmarks;

/// <summary>
/// The publish path, message by message. Every case here publishes a <c>struct</c>, because that is
/// where v1's boxing shows up and where v2's typed routes earn their keep.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class DispatchBenchmarks
{
    private readonly NerveHub _hub = new();
    private readonly NerveHub _hubNoStats = new(new NerveOptions { CollectStatistics = false });
    private readonly LegacyHub _legacy = new();

    private NerveTopic<Reading> _handle;
    private int _sink;

    /// <summary>How many subscribers are listening on the topic being published to.</summary>
    [Params(0, 1, 8)]
    public int Subscribers { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        for (int i = 0; i < Subscribers; i++)
        {
            _hub.Subscribe<Reading>("sensor/tank-3/temperature", r => _sink += r.Value);
            _hubNoStats.Subscribe<Reading>("sensor/tank-3/temperature", r => _sink += r.Value);
            _legacy.Subscribe<Reading>("sensor/tank-3/temperature", r => _sink += r.Value);
        }

        _handle = _hub.Topic<Reading>("sensor/tank-3/temperature");
    }

    /// <summary>v1: locks, copies the handler list, boxes the message, allocates a state machine.</summary>
    [Benchmark(Baseline = true)]
    public void Legacy() => _legacy.Publish("sensor/tank-3/temperature", new Reading(1));

    /// <summary>v2 by topic name: one dictionary lookup, then a direct call.</summary>
    [Benchmark]
    public void ByName() => _hub.Publish("sensor/tank-3/temperature", new Reading(1));

    /// <summary>v2 with statistics off: four interlocked increments removed.</summary>
    [Benchmark]
    public void ByNameNoStatistics() => _hubNoStats.Publish("sensor/tank-3/temperature", new Reading(1));

    /// <summary>v2 through a pre-resolved handle: the dictionary lookup is gone too.</summary>
    [Benchmark]
    public void ByHandle() => _handle.Publish(new Reading(1));

    /// <summary>A 12-byte struct - the shape most in-process messages actually have.</summary>
    public readonly record struct Reading(int Value)
    {
        public long Timestamp { get; init; }
    }
}

/// <summary>
/// What a wildcard subscription costs. The answer should be "nothing per message": matching happens
/// when a route is first resolved, not on the publish path.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class WildcardBenchmarks
{
    private readonly NerveHub _exact = new();
    private readonly NerveHub _wildcard = new();
    private int _sink;

    [GlobalSetup]
    public void Setup()
    {
        _exact.Subscribe<int>("agents/task/writer", v => _sink += v);

        // Deliberately more filters than a real application would carry, to show that the count
        // does not reach the publish path.
        _wildcard.Subscribe<int>("agents/task/+", v => _sink += v);
        _wildcard.Subscribe<int>("agents/#", v => _sink += v);
        for (int i = 0; i < 8; i++) _wildcard.Subscribe<int>($"other/{i}/#", v => _sink += v);

        _exact.Publish("agents/task/writer", 1);
        _wildcard.Publish("agents/task/writer", 1);
    }

    [Benchmark(Baseline = true)]
    public void ExactSubscriber() => _exact.Publish("agents/task/writer", 1);

    [Benchmark]
    public void TwoWildcardSubscribers() => _wildcard.Publish("agents/task/writer", 1);

    /// <summary>The matcher on its own, for the cost of resolving a route the first time.</summary>
    [Benchmark]
    public bool MatchOnly() => Routing.TopicFilter.Matches("agents/+/writer", "agents/task/writer");
}

/// <summary>
/// Request/reply and the pieces built on top of pub/sub, so their overhead is visible rather than
/// assumed.
/// </summary>
[MemoryDiagnoser]
[HideColumns("Error", "StdDev", "Median", "RatioSD")]
public class PatternBenchmarks
{
    private readonly NerveHub _hub = new();
    private IDisposable? _responder;

    [GlobalSetup]
    public void Setup() => _responder = _hub.Respond<int, int>("double", v => v * 2);

    [GlobalCleanup]
    public void Cleanup() => _responder?.Dispose();

    [Benchmark]
    public Task<int> RequestReply() => _hub.RequestAsync<int, int>("double", 21);

    [Benchmark]
    public async Task SubscribeAndUnsubscribe()
    {
        using IDisposable subscription = _hub.Subscribe<long>("churn", _ => { });
        await _hub.PublishAsync("churn", 1L);
    }
}
