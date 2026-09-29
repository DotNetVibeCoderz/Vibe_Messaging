using System.Runtime.CompilerServices;
using System.Text;
using BigPipe.Client.Admin;

namespace BigPipe.Client;

/// <summary>
/// A record delivered by a share group. Exactly one of <see cref="Accept"/>, <see cref="Release"/>
/// or <see cref="Reject"/> should be called; unacknowledged records are redelivered after the lock expires.
/// </summary>
public sealed class ShareRecord<TKey, TValue>
{
    private readonly ShareConsumer<TKey, TValue> _owner;

    internal ShareRecord(ShareConsumer<TKey, TValue> owner) => _owner = owner;

    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required TKey Key { get; init; }
    public required TValue Value { get; init; }
    public required IReadOnlyDictionary<string, string?> Headers { get; init; }

    /// <summary>How many times this record has been delivered (1 on first delivery).</summary>
    public int DeliveryCount { get; init; }

    public bool IsAcknowledged { get; private set; }

    /// <summary>Processing succeeded.</summary>
    public void Accept() => Ack(ShareAckAction.Accept);

    /// <summary>Give the record back for redelivery (to any member).</summary>
    public void Release() => Ack(ShareAckAction.Release);

    /// <summary>Give up on the record; it goes to the dead-letter topic when configured.</summary>
    public void Reject() => Ack(ShareAckAction.Reject);

    private void Ack(ShareAckAction action)
    {
        if (IsAcknowledged) return;
        IsAcknowledged = true;
        _owner.Enqueue(new ShareAck(Topic, Partition, Offset, action));
    }
}

public sealed class ShareConsumerConfig
{
    /// <summary>BigPipe HTTP gateway URL.</summary>
    public string HttpEndpoint { get; set; } = "http://localhost:8082";
    public string ShareGroup { get; set; } = "workers";
    public string MemberId { get; set; } = $"{Environment.MachineName}-{Guid.NewGuid():N}"[..40];
    public int MaxRecords { get; set; } = 100;
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliveryAttempts { get; set; } = 5;
    public string? DeadLetterTopic { get; set; }
    /// <summary>Optional bpql filter evaluated by the broker; non-matching records are skipped.</summary>
    public string? Filter { get; set; }
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(2);
}

/// <summary>
/// Queue-style consumer on a BigPipe share group: many workers read the same partitions,
/// each record is locked to one worker, acknowledged individually and dead-lettered after
/// too many attempts.
/// </summary>
public sealed class ShareConsumer<TKey, TValue> : IAsyncDisposable
{
    private readonly ShareConsumerConfig _cfg;
    private readonly BigPipeHttpClient _http;
    private readonly IDeserializer<TKey> _keyDes;
    private readonly IDeserializer<TValue> _valueDes;
    private readonly List<ShareAck> _pendingAcks = [];
    private readonly object _lock = new();
    private List<string> _topics = [];

    internal ShareConsumer(ShareConsumerConfig cfg, IDeserializer<TKey> keyDes, IDeserializer<TValue> valueDes)
    {
        _cfg = cfg;
        _http = new BigPipeHttpClient(cfg.HttpEndpoint);
        _keyDes = keyDes;
        _valueDes = valueDes;
    }

    public ShareConsumerConfig Config => _cfg;

    public void Subscribe(params string[] topics) => _topics = topics.Distinct().ToList();

    internal void Enqueue(ShareAck ack)
    {
        lock (_lock) _pendingAcks.Add(ack);
    }

    /// <summary>Sends pending acknowledgements now (also done automatically before each poll).</summary>
    public async Task FlushAcksAsync(CancellationToken ct = default)
    {
        List<ShareAck> acks;
        lock (_lock)
        {
            if (_pendingAcks.Count == 0) return;
            acks = [.. _pendingAcks];
            _pendingAcks.Clear();
        }
        await _http.ShareAckAsync(_cfg.ShareGroup, _cfg.MemberId, acks, ct).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<ShareRecord<TKey, TValue>> ConsumeAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_topics.Count == 0) throw new InvalidOperationException("call Subscribe before ConsumeAsync");
        while (!ct.IsCancellationRequested)
        {
            List<HttpRecord> batch;
            try
            {
                await FlushAcksAsync(ct).ConfigureAwait(false);
                batch = await _http.SharePollAsync(_cfg.ShareGroup, _cfg.MemberId, _topics, _cfg.MaxRecords, _cfg.LockDuration,
                    _cfg.MaxDeliveryAttempts, _cfg.DeadLetterTopic, _cfg.Filter, (int)_cfg.PollTimeout.TotalMilliseconds, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
            foreach (var r in batch)
            {
                yield return new ShareRecord<TKey, TValue>(this)
                {
                    Topic = r.Topic,
                    Partition = r.Partition,
                    Offset = r.Offset,
                    Timestamp = r.Time,
                    Key = _keyDes.Deserialize(r.KeyBytes, new SerializationContext(r.Topic, true)),
                    Value = _valueDes.Deserialize(r.ValueBytes, new SerializationContext(r.Topic, false)),
                    Headers = r.Headers,
                    DeliveryCount = r.DeliveryCount ?? 1,
                };
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await FlushAcksAsync().ConfigureAwait(false);
        }
        catch
        {
            // locks expire and records are redelivered
        }
        _http.Dispose();
    }
}

/// <summary>Fluent builder matching the design: <c>new ShareConsumerBuilder&lt;string, EmailJob&gt;().WithShareGroup("email-workers")</c>.</summary>
public sealed class ShareConsumerBuilder<TKey, TValue>
{
    private readonly ShareConsumerConfig _cfg = new();
    private IDeserializer<TKey>? _key;
    private IDeserializer<TValue>? _value;

    /// <summary>HTTP gateway URL (share groups are served over HTTP).</summary>
    public ShareConsumerBuilder<TKey, TValue> WithHttpEndpoint(string url) { _cfg.HttpEndpoint = url; return this; }

    /// <summary>Accepts a Kafka bootstrap (host:9092) and maps it to the gateway on port 8082.</summary>
    public ShareConsumerBuilder<TKey, TValue> WithBootstrap(string bootstrap)
    {
        var host = bootstrap.Split(',')[0].Split(':')[0];
        _cfg.HttpEndpoint = $"http://{host}:8082";
        return this;
    }

    public ShareConsumerBuilder<TKey, TValue> WithShareGroup(string group) { _cfg.ShareGroup = group; return this; }
    public ShareConsumerBuilder<TKey, TValue> WithMemberId(string id) { _cfg.MemberId = id; return this; }
    public ShareConsumerBuilder<TKey, TValue> WithMaxDeliveryAttempts(int n) { _cfg.MaxDeliveryAttempts = n; return this; }
    public ShareConsumerBuilder<TKey, TValue> WithDeadLetterTopic(string topic) { _cfg.DeadLetterTopic = topic; return this; }
    public ShareConsumerBuilder<TKey, TValue> WithLockDuration(TimeSpan d) { _cfg.LockDuration = d; return this; }
    public ShareConsumerBuilder<TKey, TValue> WithMaxRecords(int n) { _cfg.MaxRecords = n; return this; }
    public ShareConsumerBuilder<TKey, TValue> WithFilter(string bpql) { _cfg.Filter = bpql; return this; }
    public ShareConsumerBuilder<TKey, TValue> WithKeyDeserializer(IDeserializer<TKey> d) { _key = d; return this; }
    public ShareConsumerBuilder<TKey, TValue> WithValueDeserializer(IDeserializer<TValue> d) { _value = d; return this; }

    public ShareConsumer<TKey, TValue> Build() =>
        new(_cfg, _key ?? Deserializers.For<TKey>(), _value ?? Deserializers.For<TValue>());
}

internal static class TextHelpers
{
    public static string Utf8(ReadOnlyMemory<byte> b) => Encoding.UTF8.GetString(b.Span);
}
