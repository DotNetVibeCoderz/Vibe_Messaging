using System.Diagnostics;
using System.Text.Json;
using BigPipe.Client;
using BigPipe.Client.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BigPipe.Streams;

public enum ProcessingGuarantee
{
    /// <summary>Outputs and state changes are flushed before input offsets are committed.</summary>
    AtLeastOnce,

    /// <summary>
    /// Requested exactly-once. BigPipe transactions are on the roadmap; until then the app runs
    /// at-least-once with an idempotent producer and logs a warning.
    /// </summary>
    ExactlyOnce,
}

public sealed class StreamsConfig
{
    /// <summary>Consumer group id and prefix for internal (repartition / changelog) topics.</summary>
    public required string ApplicationId { get; set; }
    public string Bootstrap { get; set; } = "localhost:9092";
    public ProcessingGuarantee ProcessingGuarantee { get; set; } = ProcessingGuarantee.AtLeastOnce;
    public TimeSpan CommitInterval { get; set; } = TimeSpan.FromSeconds(1);
    public OffsetReset AutoOffsetReset { get; set; } = OffsetReset.Earliest;
    public CompressionType Compression { get; set; } = CompressionType.Lz4;
}

public enum StreamsState
{
    Created,
    Restoring,
    Running,
    Stopped,
    Failed,
}

internal sealed class StreamsRuntime(Producer<byte[]?, byte[]?> producer)
{
    public List<Task> Pending { get; } = [];

    public void Emit(string topic, byte[]? key, byte[]? value, long timestamp) =>
        Pending.Add(producer.SendAsync(topic, new Message<byte[]?, byte[]?>
        {
            Key = key,
            Value = value,
            Timestamp = timestamp > 0 ? DateTimeOffset.FromUnixTimeMilliseconds(timestamp) : null,
        }));
}

/// <summary>
/// Runs a <see cref="StreamsBuilder"/> topology: consumes the source topics as consumer group
/// <see cref="StreamsConfig.ApplicationId"/>, processes records through the graph, writes
/// outputs and store changelogs, then commits input offsets (at-least-once).
/// </summary>
public sealed class StreamsApp : IAsyncDisposable
{
    private readonly StreamsBuilder _topology;
    private readonly StreamsConfig _cfg;
    private readonly ILogger _log;
    private readonly CancellationTokenSource _cts = new();
    private Task? _run;
    private long _processed;

    public StreamsApp(StreamsBuilder topology, StreamsConfig config, ILogger? logger = null)
    {
        _topology = topology;
        _cfg = config;
        _log = logger ?? NullLogger.Instance;
        foreach (var (sink, source, suffix) in topology.Repartitions)
        {
            sink.Topic = $"{config.ApplicationId}-{suffix}";
            source.Topic = sink.Topic;
        }
    }

    public StreamsState State { get; private set; } = StreamsState.Created;

    public long ProcessedRecords => Interlocked.Read(ref _processed);

    public Exception? Error { get; private set; }

    /// <summary>Topics consumed by the topology (including internal repartition topics).</summary>
    public IReadOnlyList<string> SourceTopics => _topology.Sources.Select(s => s.Topic).Distinct().ToList();

    /// <summary>Read-only view of a state store (interactive queries).</summary>
    public IReadOnlyStore<TKey, TValue> Store<TKey, TValue>(string name) =>
        _topology.Stores.TryGetValue(name, out var s)
            ? new TypedStoreView<TKey, TValue>(s)
            : throw new KeyNotFoundException($"no store named '{name}'");

    private string ChangelogTopic(KeyValueStore s) => $"{_cfg.ApplicationId}-{s.Name}-changelog";

    /// <summary>Starts processing in the background.</summary>
    public Task StartAsync()
    {
        _run ??= Task.Run(() => RunCoreAsync(_cts.Token));
        return Task.CompletedTask;
    }

    /// <summary>Runs until cancelled.</summary>
    public async Task RunAsync(CancellationToken ct = default)
    {
        await StartAsync().ConfigureAwait(false);
        using var reg = ct.Register(() => _cts.Cancel());
        await _run!.ConfigureAwait(false);
    }

    private async Task RunCoreAsync(CancellationToken ct)
    {
        if (_cfg.ProcessingGuarantee == ProcessingGuarantee.ExactlyOnce)
            _log.LogWarning("BigPipe.Streams: exactly-once is not available yet; running at-least-once with an idempotent producer");
        try
        {
            await EnsureChangelogTopicsAsync(ct).ConfigureAwait(false);
            await using var producer = new ProducerBuilder<byte[]?, byte[]?>(new ProducerConfig
            {
                Bootstrap = _cfg.Bootstrap,
                ClientId = _cfg.ApplicationId + "-producer",
                Compression = _cfg.Compression,
                Linger = TimeSpan.FromMilliseconds(10),
            }).Build();
            var runtime = new StreamsRuntime(producer);
            var ctx = new ProcessorContext { Runtime = runtime };
            var needRestore = 1;
            await using var consumer = new ConsumerBuilder<byte[]?, byte[]?>(new ConsumerConfig
            {
                Bootstrap = _cfg.Bootstrap,
                GroupId = _cfg.ApplicationId,
                ClientId = _cfg.ApplicationId + "-consumer",
                EnableAutoCommit = false,
                AutoOffsetReset = _cfg.AutoOffsetReset,
            }).Build();
            consumer.PartitionsAssigned += _ => Interlocked.Exchange(ref needRestore, 1);
            var byTopic = _topology.Sources.GroupBy(s => s.Topic).ToDictionary(g => g.Key, g => g.ToList());
            var allNodes = new List<Node>();
            foreach (var s in _topology.Sources) Collect(s, allNodes);
            var punctuated = allNodes.Where(n => n is WindowAggregateNode).Distinct().ToList();
            consumer.Subscribe(byTopic.Keys);
            State = StreamsState.Running;
            _log.LogInformation("BigPipe.Streams {App} started: sources {Topics}", _cfg.ApplicationId, string.Join(", ", byTopic.Keys));

            var lastCommit = Stopwatch.GetTimestamp();
            var uncommitted = 0;
            await foreach (var rec in consumer.ConsumeAsync(ct).ConfigureAwait(false))
            {
                if (Interlocked.Exchange(ref needRestore, 0) == 1)
                {
                    State = StreamsState.Restoring;
                    await RestoreAsync(ct).ConfigureAwait(false);
                    State = StreamsState.Running;
                }
                ctx.Record.Topic = rec.Topic;
                ctx.Record.Partition = rec.Partition;
                ctx.Record.Offset = rec.Offset;
                var ts = rec.Timestamp.ToUnixTimeMilliseconds();
                ctx.Record.Timestamp = ts;
                var advanced = ts > ctx.StreamTime;
                if (advanced) ctx.StreamTime = ts;
                foreach (var src in byTopic[rec.Topic])
                {
                    var key = src.KeySerde.Deserialize(rec.Key, rec.Topic, true);
                    var value = src.ValueSerde.Deserialize(rec.Value, rec.Topic, false);
                    src.Process(key, value, ts, ctx);
                }
                if (advanced)
                    foreach (var n in punctuated) n.Punctuate(ctx);
                Interlocked.Increment(ref _processed);
                uncommitted++;
                if (Stopwatch.GetElapsedTime(lastCommit) >= _cfg.CommitInterval || uncommitted >= 10_000)
                {
                    await CommitAsync(runtime, producer, consumer, ct).ConfigureAwait(false);
                    lastCommit = Stopwatch.GetTimestamp();
                    uncommitted = 0;
                }
            }
            await CommitAsync(runtime, producer, consumer, CancellationToken.None).ConfigureAwait(false);
            State = StreamsState.Stopped;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            State = StreamsState.Stopped;
        }
        catch (Exception e)
        {
            Error = e;
            State = StreamsState.Failed;
            _log.LogError(e, "BigPipe.Streams {App} failed", _cfg.ApplicationId);
            throw;
        }
    }

    private static void Collect(Node n, List<Node> acc)
    {
        if (acc.Contains(n)) return;
        acc.Add(n);
        foreach (var c in n.Children) Collect(c, acc);
    }

    /// <summary>
    /// Creates store changelogs as compacted topics (as Kafka Streams does), so they keep the
    /// latest value per key forever instead of expiring with time retention. Existing topics are
    /// left as they are.
    /// </summary>
    private async Task EnsureChangelogTopicsAsync(CancellationToken ct)
    {
        var compacted = new Dictionary<string, string> { ["cleanup.policy"] = "compact" };
        var topics = _topology.Stores.Values.Where(s => s.Spec.Changelog)
            .Select(s => new NewTopic(ChangelogTopic(s), Config: compacted)).ToList();
        if (topics.Count == 0) return;
        try
        {
            await using var admin = new KafkaAdminClient(_cfg.Bootstrap, _cfg.ApplicationId + "-admin");
            await admin.EnsureTopicsAsync(topics, ct).ConfigureAwait(false);
        }
        catch (BigPipeException e)
        {
            _log.LogWarning(e, "BigPipe.Streams could not create compacted changelog topics; they will be auto-created with broker defaults");
        }
    }

    /// <summary>Flush outputs and changelogs, then commit consumed offsets.</summary>
    private async Task CommitAsync(StreamsRuntime runtime, Producer<byte[]?, byte[]?> producer, Consumer<byte[]?, byte[]?> consumer, CancellationToken ct)
    {
        foreach (var store in _topology.Stores.Values.Where(s => s.Spec.Changelog))
        {
            var topic = ChangelogTopic(store);
            foreach (var (k, v) in store.DrainDirty())
            {
                runtime.Pending.Add(producer.SendAsync(topic, JsonSerializer.SerializeToUtf8Bytes(k, store.KeyType, BigPipeJson.Options),
                    v is null ? null : JsonSerializer.SerializeToUtf8Bytes(v, store.ValueType, BigPipeJson.Options), ct: ct));
            }
        }
        if (runtime.Pending.Count > 0)
        {
            await producer.FlushAsync(ct).ConfigureAwait(false);
            await Task.WhenAll(runtime.Pending).ConfigureAwait(false);
            runtime.Pending.Clear();
        }
        try
        {
            await consumer.CommitAsync(ct).ConfigureAwait(false);
        }
        catch (BigPipeException e) when (e.Code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration or ErrorCode.UnknownMemberId)
        {
            _log.LogInformation("BigPipe.Streams commit skipped during rebalance: {Message}", e.Message);
        }
    }

    /// <summary>Rebuilds changelog-backed stores by replaying their changelog topics.</summary>
    private async Task RestoreAsync(CancellationToken ct)
    {
        var stores = _topology.Stores.Values.Where(s => s.Spec.Changelog).ToList();
        if (stores.Count == 0) return;
        var sw = Stopwatch.StartNew();
        long restored = 0;
        foreach (var store in stores)
        {
            var topic = ChangelogTopic(store);
            await using var reader = new ConsumerBuilder<byte[]?, byte[]?>(new ConsumerConfig
            {
                Bootstrap = _cfg.Bootstrap,
                ClientId = _cfg.ApplicationId + "-restore",
                EnableAutoCommit = false,
                AutoOffsetReset = OffsetReset.Earliest,
                FetchMaxWait = TimeSpan.FromMilliseconds(100),
            }).Build();
            var partitions = await reader.GetPartitionCountAsync(topic, ct).ConfigureAwait(false);
            var ends = new Dictionary<int, long>();
            for (var p = 0; p < partitions; p++)
                ends[p] = await reader.ListOffsetAsync(new TopicPartition(topic, p), -1, ct).ConfigureAwait(false);
            var todo = ends.Where(e => e.Value > 0).Select(e => e.Key).ToHashSet();
            if (todo.Count == 0) continue;
            store.Clear();
            reader.Assign(todo.Select(p => new TopicPartitionOffset(topic, p, 0)));
            await foreach (var r in reader.ConsumeAsync(ct).ConfigureAwait(false))
            {
                if (r.Key is { } kb)
                {
                    var key = JsonSerializer.Deserialize(kb, store.KeyType, BigPipeJson.Options)!;
                    var value = r.Value is { } vb ? JsonSerializer.Deserialize(vb, store.ValueType, BigPipeJson.Options) : null;
                    store.RestorePut(key, value);
                    restored++;
                }
                if (r.Offset + 1 >= ends[r.Partition]) todo.Remove(r.Partition);
                if (todo.Count == 0) break;
            }
        }
        _log.LogInformation("BigPipe.Streams {App} restored {Count} changelog entries in {Ms} ms", _cfg.ApplicationId, restored, sw.ElapsedMilliseconds);
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_run is not null)
        {
            try
            {
                await _run.ConfigureAwait(false);
            }
            catch
            {
                // surfaced through State/Error
            }
        }
        _cts.Dispose();
    }
}
