using System.Diagnostics;
using BigPipe.Client.Network;
using BigPipe.Client.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BigPipe.Client;

/// <summary>How many replicas must acknowledge a write.</summary>
public enum Acks : short
{
    None = 0,
    Leader = 1,
    All = -1,
}

public sealed class ProducerConfig : ClientConfig
{
    public Acks Acks { get; set; } = Acks.All;
    public CompressionType Compression { get; set; } = CompressionType.None;

    /// <summary>How long a partition batch waits for more records before it is sent.</summary>
    public TimeSpan Linger { get; set; } = TimeSpan.FromMilliseconds(5);

    /// <summary>Target batch size in bytes (uncompressed).</summary>
    public int BatchSize { get; set; } = 256 * 1024;

    /// <summary>Idempotent writes: retries never duplicate or reorder records.</summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>Upper bound for a record to be acknowledged, including retries.</summary>
    public TimeSpan DeliveryTimeout { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>Maximum records buffered but not yet acknowledged (backpressure).</summary>
    public int MaxBufferedRecords { get; set; } = 1_000_000;
}

/// <summary>Where a record was written.</summary>
public readonly record struct DeliveryResult(string Topic, int Partition, long Offset);

/// <summary>A record to produce.</summary>
public sealed class Message<TKey, TValue>
{
    public TKey Key { get; set; } = default!;
    public TValue Value { get; set; } = default!;
    public List<Header>? Headers { get; set; }
    public DateTimeOffset? Timestamp { get; set; }
    public int? Partition { get; set; }
}

/// <summary>
/// Kafka-protocol producer. Records are accumulated per partition and sent in batches;
/// each partition has at most one batch in flight, so ordering holds even across retries.
/// Thread-safe: share one instance per application.
/// </summary>
public sealed class Producer<TKey, TValue> : IAsyncDisposable
{
    private readonly ProducerConfig _cfg;
    private readonly Cluster _cluster;
    private readonly ISerializer<TKey> _keySer;
    private readonly ISerializer<TValue> _valueSer;
    private readonly ILogger _log;
    private readonly object _lock = new();
    private readonly Dictionary<(string Topic, int Partition), PartitionQueue> _queues = new();
    private readonly SemaphoreSlim _wake = new(0, int.MaxValue);
    private readonly SemaphoreSlim _buffered;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _sender;
    private readonly SemaphoreSlim _pidLock = new(1, 1);
    private long _producerId = -1;
    private short _epoch = -1;
    private int _roundRobin;
    private volatile bool _flushRequested;

    private sealed class Batch
    {
        public required RecordBatchBuilder Builder;
        public required TaskCompletionSource<long> Completion;
        public byte[]? Encoded;
        public int Count;
        public long CreatedTicks;
    }

    private sealed class PartitionQueue
    {
        public Batch? Open;
        public readonly LinkedList<Batch> Sealed = new();
        public bool InFlight;
        public int NextSequence;
    }

    internal Producer(ProducerConfig cfg, ISerializer<TKey> keySer, ISerializer<TValue> valueSer, ILogger? log)
    {
        _cfg = cfg;
        _cluster = new Cluster(cfg);
        _keySer = keySer;
        _valueSer = valueSer;
        _log = log ?? NullLogger.Instance;
        _buffered = new SemaphoreSlim(cfg.MaxBufferedRecords, cfg.MaxBufferedRecords);
        _sender = Task.Run(SenderLoopAsync);
    }

    public ProducerConfig Config => _cfg;

    /// <summary>Sends a record and completes when the broker acknowledges it.</summary>
    public Task<DeliveryResult> SendAsync(string topic, TKey key, TValue value, IEnumerable<Header>? headers = null, int? partition = null, CancellationToken ct = default)
        => SendAsync(topic, new Message<TKey, TValue> { Key = key, Value = value, Headers = headers?.ToList(), Partition = partition }, ct);

    /// <summary>Sends a record with string headers.</summary>
    public Task<DeliveryResult> SendAsync(string topic, TKey key, TValue value, IDictionary<string, string> headers, CancellationToken ct = default)
        => SendAsync(topic, key, value, headers.Select(kv => Header.Of(kv.Key, kv.Value)), null, ct);

    public async Task<DeliveryResult> SendAsync(string topic, Message<TKey, TValue> message, CancellationToken ct = default)
    {
        var keyBytes = _keySer.Serialize(message.Key, new SerializationContext(topic, true));
        var valueBytes = _valueSer.Serialize(message.Value, new SerializationContext(topic, false));
        var meta = await _cluster.TopicAsync(topic, ct).ConfigureAwait(false);
        var n = meta.Partitions.Count;
        var partition = message.Partition
            ?? (keyBytes is not null ? Murmur2.Partition(keyBytes, n) : (int)((uint)Interlocked.Increment(ref _roundRobin) % (uint)n));
        if (partition < 0 || partition >= n)
            throw new BigPipeException(ErrorCode.UnknownTopicOrPartition, $"partition {partition} does not exist in '{topic}' ({n} partitions)");
        var ts = (message.Timestamp ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds();

        await _buffered.WaitAsync(ct).ConfigureAwait(false);
        Batch batch;
        int index;
        lock (_lock)
        {
            if (!_queues.TryGetValue((topic, partition), out var q))
                _queues[(topic, partition)] = q = new PartitionQueue();
            if (q.Open is null)
            {
                q.Open = new Batch
                {
                    Builder = new RecordBatchBuilder(_cfg.Compression),
                    Completion = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously),
                    CreatedTicks = Stopwatch.GetTimestamp(),
                };
            }
            batch = q.Open;
            index = batch.Count;
            batch.Builder.Add(keyBytes, keyBytes is null, valueBytes, valueBytes is null, message.Headers, ts);
            batch.Count++;
            if (batch.Builder.EstimatedSize >= _cfg.BatchSize)
            {
                q.Sealed.AddLast(batch);
                q.Open = null;
                _wake.Release();
            }
        }
        if (_cfg.Linger <= TimeSpan.Zero) _wake.Release();
        try
        {
            var baseOffset = await batch.Completion.Task.ConfigureAwait(false);
            return new DeliveryResult(topic, partition, baseOffset < 0 ? -1 : baseOffset + index);
        }
        finally
        {
            _buffered.Release();
        }
    }

    /// <summary>Fire-and-forget send; errors go to <paramref name="onError"/> (or the logger).</summary>
    public void Produce(string topic, TKey key, TValue value, Action<Exception>? onError = null)
    {
        _ = SendAsync(topic, key, value).ContinueWith(t =>
        {
            if (t.Exception is { } e)
            {
                if (onError is not null) onError(e.InnerException ?? e);
                else _log.LogError(e, "BigPipe produce to {Topic} failed", topic);
            }
        }, TaskContinuationOptions.OnlyOnFaulted);
    }

    /// <summary>Sends every buffered record now and waits until all are acknowledged.</summary>
    public async Task FlushAsync(CancellationToken ct = default)
    {
        List<Task> pending;
        lock (_lock)
        {
            pending = _queues.Values.SelectMany(q => q.Sealed.Select(b => (Task)b.Completion.Task)
                .Concat(q.Open is { } o ? [o.Completion.Task] : [])).ToList();
        }
        _flushRequested = true;
        _wake.Release();
        try
        {
            await Task.WhenAll(pending).WaitAsync(ct).ConfigureAwait(false);
        }
        catch (BigPipeException)
        {
            // Individual SendAsync callers observe their own failures.
        }
    }

    private async Task EnsureProducerIdAsync(CancellationToken ct)
    {
        if (!_cfg.EnableIdempotence || _producerId >= 0) return;
        await _pidLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_producerId >= 0) return;
            var conn = await _cluster.AnyAsync(ct).ConfigureAwait(false);
            (_producerId, _epoch) = await Requests.InitProducerIdAsync(conn, _cluster.Timeout, ct).ConfigureAwait(false);
        }
        finally
        {
            _pidLock.Release();
        }
    }

    private async Task SenderLoopAsync()
    {
        var ct = _cts.Token;
        var lingerTicks = (long)(_cfg.Linger.TotalSeconds * Stopwatch.Frequency);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await _wake.WaitAsync(_cfg.Linger > TimeSpan.Zero ? _cfg.Linger : TimeSpan.FromMilliseconds(50), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            try
            {
                await EnsureProducerIdAsync(ct).ConfigureAwait(false);
            }
            catch (Exception e) when (!ct.IsCancellationRequested)
            {
                _log.LogWarning(e, "BigPipe producer could not get a producer id; retrying");
                await Task.Delay(500, ct).ConfigureAwait(false);
                _wake.Release();
                continue;
            }
            var ready = new List<(string Topic, int Partition, Batch Batch)>();
            var now = Stopwatch.GetTimestamp();
            var flush = _flushRequested;
            _flushRequested = false;
            lock (_lock)
            {
                foreach (var (tp, q) in _queues)
                {
                    if (q.Open is { } o && (flush || now - o.CreatedTicks >= lingerTicks || _cfg.Linger <= TimeSpan.Zero))
                    {
                        q.Sealed.AddLast(o);
                        q.Open = null;
                    }
                    if (q.InFlight || q.Sealed.First is null) continue;
                    var b = q.Sealed.First.Value;
                    q.Sealed.RemoveFirst();
                    if (b.Encoded is null)
                    {
                        if (_cfg.EnableIdempotence)
                        {
                            b.Builder.ProducerId = _producerId;
                            b.Builder.ProducerEpoch = _epoch;
                            b.Builder.BaseSequence = q.NextSequence;
                            q.NextSequence += b.Count;
                        }
                        b.Encoded = b.Builder.Build();
                        b.Builder.Dispose();
                    }
                    q.InFlight = true;
                    ready.Add((tp.Topic, tp.Partition, b));
                }
            }
            if (ready.Count == 0) continue;
            // One Produce request per leader broker.
            var byLeader = new Dictionary<int, List<(string, int, Batch)>>();
            foreach (var r in ready)
            {
                int leader;
                try
                {
                    var meta = await _cluster.TopicAsync(r.Topic, ct).ConfigureAwait(false);
                    leader = meta.Partitions[r.Partition].Leader;
                }
                catch (Exception e)
                {
                    Complete(r.Topic, r.Partition, r.Batch, null, e);
                    continue;
                }
                if (!byLeader.TryGetValue(leader, out var list)) byLeader[leader] = list = [];
                list.Add(r);
            }
            foreach (var (leader, list) in byLeader)
                _ = SendToLeaderAsync(leader, list, ct);
        }
    }

    private async Task SendToLeaderAsync(int leader, List<(string Topic, int Partition, Batch Batch)> list, CancellationToken ct)
    {
        try
        {
            var conn = await _cluster.BrokerAsync(leader, ct).ConfigureAwait(false);
            var results = await Requests.ProduceAsync(conn, (short)_cfg.Acks, (int)_cluster.Timeout.TotalMilliseconds,
                list.Select(x => (x.Topic, x.Partition, x.Batch.Encoded!)).ToList(), _cluster.Timeout, ct).ConfigureAwait(false);
            foreach (var (topic, res) in results)
            {
                var item = list.First(x => x.Topic == topic && x.Partition == res.Partition);
                if (res.Error == 0 || res.Error == (short)ErrorCode.DuplicateSequenceNumber)
                    Complete(topic, res.Partition, item.Batch, res.BaseOffset, null);
                else
                    Complete(topic, res.Partition, item.Batch, null, new BigPipeException((ErrorCode)res.Error, $"produce to {topic}-{res.Partition} failed: {(ErrorCode)res.Error}"));
            }
        }
        catch (Exception e)
        {
            foreach (var x in list) Complete(x.Topic, x.Partition, x.Batch, null, e);
        }
    }

    private void Complete(string topic, int partition, Batch b, long? baseOffset, Exception? error)
    {
        var retry = false;
        if (error is not null)
        {
            var expired = Stopwatch.GetElapsedTime(b.CreatedTicks) > _cfg.DeliveryTimeout;
            retry = !expired && !_cts.IsCancellationRequested && error is BigPipeException { IsRetriable: true };
            if (retry)
            {
                _log.LogDebug("BigPipe produce to {Topic}-{Partition} will retry: {Error}", topic, partition, error.Message);
                _ = _cluster.RefreshAsync([topic], CancellationToken.None);
            }
        }
        lock (_lock)
        {
            var q = _queues[(topic, partition)];
            q.InFlight = false;
            if (retry) q.Sealed.AddFirst(b);
        }
        if (retry)
        {
            _ = Task.Delay(100).ContinueWith(_ => _wake.Release());
            return;
        }
        if (error is not null) b.Completion.TrySetException(error);
        else b.Completion.TrySetResult(baseOffset ?? -1);
        _wake.Release();
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await FlushAsync(new CancellationTokenSource(_cfg.RequestTimeout).Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // best effort
        }
        _cts.Cancel();
        try
        {
            await _sender.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        lock (_lock)
        {
            foreach (var q in _queues.Values)
            {
                foreach (var b in q.Sealed) b.Completion.TrySetException(new ObjectDisposedException("producer"));
                q.Open?.Completion.TrySetException(new ObjectDisposedException("producer"));
            }
        }
        await _cluster.DisposeAsync().ConfigureAwait(false);
        _cts.Dispose();
    }
}

/// <summary>Fluent builder, e.g. <c>new ProducerBuilder&lt;string, Order&gt;().WithBootstrap("localhost:9092").Build()</c>.</summary>
public sealed class ProducerBuilder<TKey, TValue>
{
    private readonly ProducerConfig _cfg;
    private ISerializer<TKey>? _key;
    private ISerializer<TValue>? _value;
    private ILogger? _log;

    public ProducerBuilder(ProducerConfig? config = null) => _cfg = config ?? new ProducerConfig();

    public ProducerBuilder<TKey, TValue> WithBootstrap(string bootstrap) { _cfg.Bootstrap = bootstrap; return this; }
    public ProducerBuilder<TKey, TValue> WithClientId(string clientId) { _cfg.ClientId = clientId; return this; }
    public ProducerBuilder<TKey, TValue> WithAcks(Acks acks) { _cfg.Acks = acks; return this; }
    public ProducerBuilder<TKey, TValue> WithCompression(CompressionType c) { _cfg.Compression = c; return this; }
    public ProducerBuilder<TKey, TValue> WithIdempotence(bool enabled = true) { _cfg.EnableIdempotence = enabled; return this; }
    public ProducerBuilder<TKey, TValue> WithLinger(TimeSpan linger) { _cfg.Linger = linger; return this; }
    public ProducerBuilder<TKey, TValue> WithBatchSize(int bytes) { _cfg.BatchSize = bytes; return this; }
    public ProducerBuilder<TKey, TValue> WithKeySerializer(ISerializer<TKey> s) { _key = s; return this; }
    public ProducerBuilder<TKey, TValue> WithValueSerializer(ISerializer<TValue> s) { _value = s; return this; }
    public ProducerBuilder<TKey, TValue> WithLogger(ILogger log) { _log = log; return this; }

    public Producer<TKey, TValue> Build() =>
        new(_cfg, _key ?? Serializers.For<TKey>(), _value ?? Serializers.For<TValue>(), _log);
}
