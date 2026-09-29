using BigPipe.Client.Admin;

namespace BigPipe.Console.Services;

public sealed class ConsoleOptions
{
    public string AdminUrl { get; set; } = "http://localhost:9644";
    public string HttpUrl { get; set; } = "http://localhost:8082";
    public string RegistryUrl { get; set; } = "http://localhost:8081";
    public string? ApiKey { get; set; }
    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromSeconds(2);
}

/// <summary>A point-in-time rate sample computed from counter deltas.</summary>
public readonly record struct RateSample(DateTimeOffset At, double RecordsIn, double BytesIn, double BytesOut);

/// <summary>
/// One shared poller for all browser sessions: refreshes cluster, topics, groups and flows,
/// and derives throughput rates. Components subscribe to <see cref="Changed"/>.
/// </summary>
public sealed class ClusterState(ConsoleOptions options, ILogger<ClusterState> log) : BackgroundService
{
    private readonly BigPipeAdminClient _admin = new(options.AdminUrl, options.ApiKey);
    private readonly LinkedList<RateSample> _rates = new();
    private MetricsSnapshot? _lastMetrics;
    private DateTimeOffset _lastAt;
    private readonly Dictionary<string, long> _lastTopicRecords = new();

    public ClusterInfo? Cluster { get; private set; }
    public MetricsSnapshot? Metrics { get; private set; }
    public IReadOnlyList<TopicInfo> Topics { get; private set; } = [];
    public IReadOnlyList<GroupInfo> Groups { get; private set; } = [];
    public IReadOnlyList<FlowStatus> Flows { get; private set; } = [];
    public IReadOnlyDictionary<string, double> TopicRates { get; private set; } = new Dictionary<string, double>();
    public string? Error { get; private set; }
    public DateTimeOffset? LastUpdated { get; private set; }
    public bool Online => Error is null && Cluster is not null;

    public IReadOnlyList<RateSample> Rates
    {
        get
        {
            lock (_rates) return _rates.ToList();
        }
    }

    public RateSample Current
    {
        get
        {
            lock (_rates) return _rates.Last?.Value ?? default;
        }
    }

    public BigPipeAdminClient Admin => _admin;

    public event Action? Changed;

    /// <summary>Refreshes immediately (after a change made from the UI).</summary>
    public async Task RefreshNowAsync()
    {
        await PollAsync(CancellationToken.None);
        Changed?.Invoke();
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(options.RefreshInterval);
        do
        {
            await PollAsync(ct);
            Changed?.Invoke();
        } while (await timer.WaitForNextTickAsync(ct));
    }

    private async Task PollAsync(CancellationToken ct)
    {
        try
        {
            var cluster = _admin.GetClusterAsync(ct);
            var metrics = _admin.GetMetricsAsync(ct);
            var topics = _admin.ListTopicsAsync(ct);
            var groups = _admin.ListGroupsAsync(ct);
            var flows = _admin.ListFlowsAsync(ct);
            await Task.WhenAll(cluster, metrics, topics, groups, flows);
            var now = DateTimeOffset.UtcNow;
            var m = metrics.Result;
            if (_lastMetrics is { } prev)
            {
                var dt = Math.Max(0.001, (now - _lastAt).TotalSeconds);
                var sample = new RateSample(now,
                    Math.Max(0, (m.ProduceRecordsTotal - prev.ProduceRecordsTotal) / dt),
                    Math.Max(0, (m.ProduceBytesTotal - prev.ProduceBytesTotal) / dt),
                    Math.Max(0, (m.FetchBytesTotal - prev.FetchBytesTotal) / dt));
                lock (_rates)
                {
                    _rates.AddLast(sample);
                    while (_rates.Count > 90) _rates.RemoveFirst();
                }
                var perTopic = new Dictionary<string, double>();
                foreach (var t in topics.Result)
                {
                    if (_lastTopicRecords.TryGetValue(t.Name, out var before))
                        perTopic[t.Name] = Math.Max(0, (t.Records - before) / dt);
                    _lastTopicRecords[t.Name] = t.Records;
                }
                TopicRates = perTopic;
            }
            else
            {
                foreach (var t in topics.Result) _lastTopicRecords[t.Name] = t.Records;
            }
            _lastMetrics = m;
            _lastAt = now;
            Cluster = cluster.Result;
            Metrics = m;
            Topics = topics.Result.OrderBy(t => t.Internal).ThenBy(t => t.Name).ToList();
            Groups = groups.Result;
            Flows = flows.Result;
            Error = null;
            LastUpdated = now;
        }
        catch (Exception e) when (!ct.IsCancellationRequested)
        {
            if (Error is null) log.LogWarning("BigPipe admin API unreachable at {Url}: {Message}", options.AdminUrl, e.Message);
            Error = e.Message;
        }
    }
}
