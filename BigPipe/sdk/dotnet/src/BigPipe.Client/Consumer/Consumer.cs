using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using BigPipe.Client.Network;
using BigPipe.Client.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BigPipe.Client;

public enum OffsetReset
{
    Earliest,
    Latest,
    Error,
}

public enum AssignmentStrategy
{
    Range,
    RoundRobin,
}

public sealed class ConsumerConfig : ClientConfig
{
    /// <summary>Consumer group id. Required for <c>Subscribe</c> and for committing offsets.</summary>
    public string? GroupId { get; set; }

    public OffsetReset AutoOffsetReset { get; set; } = OffsetReset.Latest;
    public bool EnableAutoCommit { get; set; } = true;
    public TimeSpan AutoCommitInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan SessionTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>How long the group waits for members to rejoin during a rebalance.</summary>
    public TimeSpan RebalanceTimeout { get; set; } = TimeSpan.FromSeconds(60);

    public TimeSpan FetchMaxWait { get; set; } = TimeSpan.FromMilliseconds(500);
    public int FetchMinBytes { get; set; } = 1;
    public int FetchMaxBytes { get; set; } = 50 * 1024 * 1024;
    public int MaxPartitionFetchBytes { get; set; } = 1024 * 1024;

    /// <summary>Records buffered ahead of the application.</summary>
    public int QueueCapacity { get; set; } = 10_000;

    public AssignmentStrategy AssignmentStrategy { get; set; } = AssignmentStrategy.Range;
}

/// <summary>A consumed record.</summary>
public sealed class ConsumeResult<TKey, TValue>
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required TKey Key { get; init; }
    public required TValue Value { get; init; }
    public IReadOnlyList<Header> Headers { get; init; } = [];
    internal int Epoch { get; init; }

    public TopicPartition TopicPartition => new(Topic, Partition);
    public TopicPartitionOffset TopicPartitionOffset => new(Topic, Partition, Offset);

    /// <summary>First header with this name, as UTF-8 text.</summary>
    public string? GetHeader(string name)
    {
        foreach (var h in Headers)
            if (h.Key == name) return h.GetString();
        return null;
    }
}

/// <summary>Group identity for transactional/offset APIs and diagnostics.</summary>
public sealed record GroupMetadata(string GroupId, int GenerationId, string MemberId);

/// <summary>
/// Kafka-protocol consumer with consumer-group support (range / round-robin assignment,
/// heartbeats, auto-commit, cooperative rejoin on rebalance). Records are fetched ahead into a
/// bounded queue; <see cref="ConsumeAsync"/> yields them with at-least-once semantics.
/// </summary>
public sealed class Consumer<TKey, TValue> : IAsyncDisposable
{
    private readonly ConsumerConfig _cfg;
    private readonly Cluster _cluster;
    private readonly IDeserializer<TKey> _keyDes;
    private readonly IDeserializer<TValue> _valueDes;
    private readonly ILogger _log;
    private readonly Channel<ConsumeResult<TKey, TValue>> _queue;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _state = new();
    private readonly Dictionary<TopicPartition, long> _fetchPos = new();
    private readonly ConcurrentDictionary<TopicPartition, long> _consumed = new();
    private Task? _runner;
    private Task? _heartbeat;
    private List<string> _subscription = [];
    private bool _groupMode;
    private volatile TopicPartition[] _assigned = [];
    private volatile int _epoch;
    private string _memberId = "";
    private volatile int _generation = -1;
    private volatile bool _rejoin;
    private BrokerConnection? _coordinator;
    private DateTime _lastAutoCommit = DateTime.UtcNow;

    /// <summary>Raised after the group assigns partitions to this consumer.</summary>
    public event Action<IReadOnlyList<TopicPartition>>? PartitionsAssigned;

    /// <summary>Raised before partitions are taken away during a rebalance.</summary>
    public event Action<IReadOnlyList<TopicPartition>>? PartitionsRevoked;

    internal Consumer(ConsumerConfig cfg, IDeserializer<TKey> keyDes, IDeserializer<TValue> valueDes, ILogger? log)
    {
        _cfg = cfg;
        _cluster = new Cluster(cfg);
        _keyDes = keyDes;
        _valueDes = valueDes;
        _log = log ?? NullLogger.Instance;
        _queue = Channel.CreateBounded<ConsumeResult<TKey, TValue>>(new BoundedChannelOptions(Math.Max(16, cfg.QueueCapacity))
        {
            SingleWriter = false,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    public ConsumerConfig Config => _cfg;

    public IReadOnlyList<TopicPartition> Assignment => _assigned;

    public GroupMetadata? GroupMetadata => _cfg.GroupId is { } g ? new GroupMetadata(g, _generation, _memberId) : null;

    /// <summary>Joins the consumer group and subscribes to the topics.</summary>
    public void Subscribe(params string[] topics) => Subscribe((IEnumerable<string>)topics);

    public void Subscribe(IEnumerable<string> topics)
    {
        if (string.IsNullOrEmpty(_cfg.GroupId))
            throw new InvalidOperationException("Subscribe requires ConsumerConfig.GroupId; use Assign for group-less consumption.");
        lock (_state)
        {
            _subscription = topics.Distinct().ToList();
            _groupMode = true;
            _rejoin = true;
        }
        Start();
    }

    /// <summary>Consumes explicit partitions without group management. Offset &lt; 0 = committed or reset policy.</summary>
    public void Assign(IEnumerable<TopicPartitionOffset> partitions)
    {
        var list = partitions.ToList();
        lock (_state)
        {
            _groupMode = false;
            _pendingManual = list;
        }
        Start();
    }

    public void Assign(params TopicPartitionOffset[] partitions) => Assign((IEnumerable<TopicPartitionOffset>)partitions);

    public void Assign(params TopicPartition[] partitions) => Assign(partitions.Select(p => new TopicPartitionOffset(p.Topic, p.Partition, -1)));

    private List<TopicPartitionOffset>? _pendingManual;

    private void Start()
    {
        lock (_state)
        {
            _runner ??= Task.Run(RunAsync);
            if (_groupMode) _heartbeat ??= Task.Run(HeartbeatLoopAsync);
        }
    }

    /// <summary>Streams records until cancelled.</summary>
    public async IAsyncEnumerable<ConsumeResult<TKey, TValue>> ConsumeAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        while (true)
        {
            ConsumeResult<TKey, TValue> r;
            try
            {
                r = await _queue.Reader.ReadAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
            catch (ChannelClosedException)
            {
                yield break;
            }
            if (r.Epoch != _epoch) continue; // fetched before a rebalance/seek
            _consumed[r.TopicPartition] = r.Offset + 1;
            yield return r;
        }
    }

    /// <summary>Returns the next record, or null when none arrives within <paramref name="timeout"/>.</summary>
    public async Task<ConsumeResult<TKey, TValue>?> ConsumeOneAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try
        {
            while (true)
            {
                var r = await _queue.Reader.ReadAsync(cts.Token).ConfigureAwait(false);
                if (r.Epoch != _epoch) continue;
                _consumed[r.TopicPartition] = r.Offset + 1;
                return r;
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return null;
        }
    }

    /// <summary>Commits the position after <paramref name="record"/>.</summary>
    public Task CommitAsync(ConsumeResult<TKey, TValue> record, CancellationToken ct = default) =>
        CommitAsync([new TopicPartitionOffset(record.Topic, record.Partition, record.Offset + 1)], ct);

    /// <summary>Commits the positions of every record returned so far.</summary>
    public Task CommitAsync(CancellationToken ct = default) =>
        CommitAsync(_consumed.Select(kv => new TopicPartitionOffset(kv.Key.Topic, kv.Key.Partition, kv.Value)).ToList(), ct);

    public async Task CommitAsync(IReadOnlyCollection<TopicPartitionOffset> offsets, CancellationToken ct = default)
    {
        if (offsets.Count == 0) return;
        var group = _cfg.GroupId ?? throw new InvalidOperationException("committing offsets requires ConsumerConfig.GroupId");
        _coordinator ??= await _cluster.CoordinatorAsync(group, ct).ConfigureAwait(false);
        var generation = _groupMode ? _generation : -1;
        var member = _groupMode ? _memberId : "";
        var res = await Requests.OffsetCommitAsync(_coordinator, group, generation, member,
            offsets.Select(o => (o.Topic, o.Partition, o.Offset)).ToList(), _cluster.Timeout, ct).ConfigureAwait(false);
        foreach (var (topic, partition, error) in res)
        {
            if (error == 0) continue;
            var code = (ErrorCode)error;
            if (code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration or ErrorCode.UnknownMemberId)
                _rejoin = true;
            throw new BigPipeException(code, $"commit {topic}-{partition} failed: {code}");
        }
    }

    /// <summary>Moves the next fetch position of a partition.</summary>
    public void Seek(TopicPartition tp, long offset)
    {
        lock (_state)
        {
            ResetBufferLocked();
            _fetchPos[tp] = offset;
            _consumed[tp] = offset;
        }
    }

    /// <summary>Next offset that <see cref="ConsumeAsync"/> will return for a partition.</summary>
    public long? Position(TopicPartition tp) => _consumed.TryGetValue(tp, out var o) ? o : null;

    /// <summary>Drops records fetched ahead and rewinds fetch positions to what the app consumed.</summary>
    private void ResetBufferLocked()
    {
        Interlocked.Increment(ref _epoch);
        foreach (var tp in _fetchPos.Keys.ToList())
            if (_consumed.TryGetValue(tp, out var c)) _fetchPos[tp] = c;
    }

    // ----------------------------------------------------------------------------- runner ------

    private async Task RunAsync()
    {
        var ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_groupMode && (_rejoin || _generation < 0))
                {
                    await RebalanceAsync(ct).ConfigureAwait(false);
                    continue;
                }
                if (!_groupMode && _pendingManual is { } manual)
                {
                    _pendingManual = null;
                    await ApplyManualAssignmentAsync(manual, ct).ConfigureAwait(false);
                }
                var assigned = _assigned;
                if (assigned.Length == 0)
                {
                    await Task.Delay(100, ct).ConfigureAwait(false);
                    continue;
                }
                await FetchRoundAsync(assigned, ct).ConfigureAwait(false);
                if (_cfg.EnableAutoCommit && _cfg.GroupId is not null && DateTime.UtcNow - _lastAutoCommit >= _cfg.AutoCommitInterval)
                {
                    _lastAutoCommit = DateTime.UtcNow;
                    try
                    {
                        await CommitAsync(ct).ConfigureAwait(false);
                    }
                    catch (BigPipeException e)
                    {
                        _log.LogDebug("BigPipe auto-commit skipped: {Message}", e.Message);
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (BigPipeException e)
            {
                _log.LogWarning("BigPipe consumer: {Message}", e.Message);
                if (e.Code is ErrorCode.NotCoordinator or ErrorCode.CoordinatorNotAvailable or ErrorCode.ConnectionFailed)
                    _coordinator = null;
                if (e.Code is ErrorCode.UnknownMemberId) _memberId = "";
                if (_groupMode && e.Code is ErrorCode.RebalanceInProgress or ErrorCode.IllegalGeneration or ErrorCode.UnknownMemberId
                        or ErrorCode.NotCoordinator or ErrorCode.CoordinatorNotAvailable)
                    _rejoin = true;
                await SafeDelay(300, ct).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _log.LogError(e, "BigPipe consumer loop error");
                await SafeDelay(500, ct).ConfigureAwait(false);
            }
        }
    }

    private static async Task SafeDelay(int ms, CancellationToken ct)
    {
        try
        {
            await Task.Delay(ms, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task HeartbeatLoopAsync()
    {
        var ct = _cts.Token;
        while (!ct.IsCancellationRequested)
        {
            await SafeDelay((int)_cfg.HeartbeatInterval.TotalMilliseconds, ct).ConfigureAwait(false);
            if (ct.IsCancellationRequested || _rejoin || _generation < 0 || _coordinator is null) continue;
            try
            {
                var code = (ErrorCode)await Requests.HeartbeatAsync(_coordinator, _cfg.GroupId!, _generation, _memberId, _cluster.Timeout, ct).ConfigureAwait(false);
                switch (code)
                {
                    case ErrorCode.None:
                        break;
                    case ErrorCode.UnknownMemberId:
                        _memberId = "";
                        _rejoin = true;
                        break;
                    case ErrorCode.NotCoordinator or ErrorCode.CoordinatorNotAvailable:
                        _coordinator = null;
                        _rejoin = true;
                        break;
                    default:
                        _rejoin = true;
                        break;
                }
            }
            catch (Exception e) when (!ct.IsCancellationRequested)
            {
                _log.LogDebug("BigPipe heartbeat failed: {Message}", e.Message);
                _coordinator = null;
            }
        }
    }

    private async Task RebalanceAsync(CancellationToken ct)
    {
        var group = _cfg.GroupId!;
        // Commit progress before giving partitions away.
        if (_cfg.EnableAutoCommit && _generation >= 0 && _assigned.Length > 0)
        {
            try
            {
                await CommitAsync(ct).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _log.LogDebug("BigPipe pre-rebalance commit failed: {Message}", e.Message);
            }
        }
        var old = _assigned;
        if (old.Length > 0)
        {
            InvokeSafely(PartitionsRevoked, old);
            lock (_state)
            {
                _assigned = [];
                ResetBufferLocked();
            }
        }
        _coordinator ??= await _cluster.CoordinatorAsync(group, ct).ConfigureAwait(false);
        var subscription = ConsumerProtocol.EncodeSubscription(_subscription);
        var protocols = _cfg.AssignmentStrategy == AssignmentStrategy.RoundRobin
            ? new List<(string, byte[])> { ("roundrobin", subscription), ("range", subscription) }
            : [("range", subscription), ("roundrobin", subscription)];

        _rejoin = false;
        var joinTimeout = _cfg.RebalanceTimeout + _cfg.RequestTimeout;
        JoinResult join;
        while (true)
        {
            join = await Requests.JoinGroupAsync(_coordinator, group, (int)_cfg.SessionTimeout.TotalMilliseconds,
                (int)_cfg.RebalanceTimeout.TotalMilliseconds, _memberId, protocols, joinTimeout, ct).ConfigureAwait(false);
            if (join.Error == (short)ErrorCode.MemberIdRequired)
            {
                _memberId = join.MemberId;
                continue;
            }
            if (join.Error == (short)ErrorCode.UnknownMemberId && _memberId != "")
            {
                _memberId = "";
                continue;
            }
            BigPipeException.ThrowIfError(join.Error, $"join group '{group}'");
            break;
        }
        _memberId = join.MemberId;
        _generation = join.Generation;

        var assignments = new List<(string, byte[])>();
        if (join.Leader == _memberId)
        {
            var subs = join.Members.ToDictionary(m => m.MemberId, m => ConsumerProtocol.DecodeSubscription(m.Metadata));
            var counts = new Dictionary<string, int>();
            foreach (var topic in subs.Values.SelectMany(t => t).Distinct())
            {
                try
                {
                    counts[topic] = (await _cluster.TopicAsync(topic, ct).ConfigureAwait(false)).Partitions.Count;
                }
                catch (BigPipeException e) when (e.Code == ErrorCode.UnknownTopicOrPartition)
                {
                    _log.LogWarning("BigPipe: subscribed topic {Topic} does not exist", topic);
                }
            }
            var plan = join.Protocol == "roundrobin" ? ConsumerProtocol.RoundRobin(subs, counts) : ConsumerProtocol.Range(subs, counts);
            assignments = plan.Select(kv => (kv.Key, ConsumerProtocol.EncodeAssignment(kv.Value))).ToList();
        }
        var (err, bytes) = await Requests.SyncGroupAsync(_coordinator, group, _generation, _memberId, assignments, joinTimeout, ct).ConfigureAwait(false);
        if (err == (short)ErrorCode.RebalanceInProgress)
        {
            _rejoin = true;
            return;
        }
        BigPipeException.ThrowIfError(err, $"sync group '{group}'");
        var parts = ConsumerProtocol.DecodeAssignment(bytes);
        var committed = parts.Count == 0
            ? new Dictionary<(string, int), long>()
            : await Requests.OffsetFetchAsync(_coordinator, group, parts.Select(p => (p.Topic, p.Partition)).ToList(), _cluster.Timeout, ct).ConfigureAwait(false);
        var positions = new Dictionary<TopicPartition, long>();
        foreach (var p in parts)
            positions[p] = committed.TryGetValue((p.Topic, p.Partition), out var o) && o >= 0 ? o : await ResetOffsetAsync(p, ct).ConfigureAwait(false);
        lock (_state)
        {
            _fetchPos.Clear();
            _consumed.Clear();
            foreach (var (tp, off) in positions)
            {
                _fetchPos[tp] = off;
                _consumed[tp] = off;
            }
            Interlocked.Increment(ref _epoch);
            _assigned = parts.ToArray();
        }
        _log.LogInformation("BigPipe consumer joined group {Group} generation {Generation}: {Count} partition(s)", group, _generation, parts.Count);
        InvokeSafely(PartitionsAssigned, parts);
    }

    private async Task ApplyManualAssignmentAsync(List<TopicPartitionOffset> manual, CancellationToken ct)
    {
        Dictionary<(string, int), long> committed = new();
        if (_cfg.GroupId is { } g && manual.Any(m => m.Offset < 0))
        {
            _coordinator ??= await _cluster.CoordinatorAsync(g, ct).ConfigureAwait(false);
            committed = await Requests.OffsetFetchAsync(_coordinator, g, manual.Select(m => (m.Topic, m.Partition)).ToList(), _cluster.Timeout, ct).ConfigureAwait(false);
        }
        var positions = new Dictionary<TopicPartition, long>();
        foreach (var m in manual)
        {
            var tp = m.TopicPartition;
            positions[tp] = m.Offset >= 0 ? m.Offset
                : committed.TryGetValue((m.Topic, m.Partition), out var o) && o >= 0 ? o
                : await ResetOffsetAsync(tp, ct).ConfigureAwait(false);
        }
        lock (_state)
        {
            _fetchPos.Clear();
            _consumed.Clear();
            foreach (var (tp, off) in positions)
            {
                _fetchPos[tp] = off;
                _consumed[tp] = off;
            }
            Interlocked.Increment(ref _epoch);
            _assigned = positions.Keys.ToArray();
        }
        InvokeSafely(PartitionsAssigned, _assigned);
    }

    private void InvokeSafely(Action<IReadOnlyList<TopicPartition>>? handler, IReadOnlyList<TopicPartition> parts)
    {
        try
        {
            handler?.Invoke(parts);
        }
        catch (Exception e)
        {
            _log.LogError(e, "BigPipe rebalance callback threw");
        }
    }

    private async Task<long> ResetOffsetAsync(TopicPartition tp, CancellationToken ct)
    {
        if (_cfg.AutoOffsetReset == OffsetReset.Error)
            throw new BigPipeException(ErrorCode.OffsetOutOfRange, $"no committed offset for {tp} and AutoOffsetReset=Error");
        return await ListOffsetAsync(tp, _cfg.AutoOffsetReset == OffsetReset.Earliest ? -2 : -1, ct).ConfigureAwait(false);
    }

    /// <summary>Offset lookup: -1 latest, -2 earliest, or first offset at/after a timestamp (ms).</summary>
    public async Task<long> ListOffsetAsync(TopicPartition tp, long timestamp, CancellationToken ct = default)
    {
        var meta = await _cluster.TopicAsync(tp.Topic, ct).ConfigureAwait(false);
        var conn = await _cluster.BrokerAsync(meta.Partitions[tp.Partition].Leader, ct).ConfigureAwait(false);
        var res = await Requests.ListOffsetsAsync(conn, tp.Topic, [(tp.Partition, timestamp)], _cluster.Timeout, ct).ConfigureAwait(false);
        var (err, off) = res[tp.Partition];
        BigPipeException.ThrowIfError(err, $"list offsets {tp}");
        return off;
    }

    /// <summary>Number of partitions of a topic (creates it when auto-creation is allowed).</summary>
    public async Task<int> GetPartitionCountAsync(string topic, CancellationToken ct = default) =>
        (await _cluster.TopicAsync(topic, ct).ConfigureAwait(false)).Partitions.Count;

    /// <summary>(earliest, latest) offsets of a partition.</summary>
    public async Task<(long Low, long High)> GetWatermarksAsync(TopicPartition tp, CancellationToken ct = default) =>
        (await ListOffsetAsync(tp, -2, ct).ConfigureAwait(false), await ListOffsetAsync(tp, -1, ct).ConfigureAwait(false));

    private async Task FetchRoundAsync(TopicPartition[] assigned, CancellationToken ct)
    {
        var epoch = _epoch;
        var byLeader = new Dictionary<int, List<(string, int, long, int)>>();
        lock (_state)
        {
            foreach (var tp in assigned)
            {
                if (!_fetchPos.TryGetValue(tp, out var pos)) continue;
                var leader = _cluster.KnownTopics.FirstOrDefault(t => t.Name == tp.Topic)?.Partitions.ElementAtOrDefault(tp.Partition)?.Leader;
                var key = leader ?? -1;
                if (!byLeader.TryGetValue(key, out var list)) byLeader[key] = list = [];
                list.Add((tp.Topic, tp.Partition, pos, _cfg.MaxPartitionFetchBytes));
            }
        }
        if (byLeader.ContainsKey(-1))
        {
            await _cluster.RefreshAsync(assigned.Select(a => a.Topic).Distinct().ToList(), ct).ConfigureAwait(false);
            return;
        }
        await Task.WhenAll(byLeader.Select(kv => FetchFromLeaderAsync(kv.Key, kv.Value, epoch, ct))).ConfigureAwait(false);
    }

    private async Task FetchFromLeaderAsync(int leader, List<(string Topic, int Partition, long Offset, int MaxBytes)> parts, int epoch, CancellationToken ct)
    {
        var conn = await _cluster.BrokerAsync(leader, ct).ConfigureAwait(false);
        var results = await Requests.FetchAsync(conn, (int)_cfg.FetchMaxWait.TotalMilliseconds, _cfg.FetchMinBytes, _cfg.FetchMaxBytes,
            parts, _cfg.FetchMaxWait + _cfg.RequestTimeout, ct).ConfigureAwait(false);
        var raw = new List<RawRecord>();
        foreach (var r in results)
        {
            var tp = new TopicPartition(r.Topic, r.Partition);
            var requested = parts.First(p => p.Topic == r.Topic && p.Partition == r.Partition).Offset;
            switch ((ErrorCode)r.Error)
            {
                case ErrorCode.None:
                    break;
                case ErrorCode.OffsetOutOfRange:
                {
                    var reset = await ResetOffsetAsync(tp, ct).ConfigureAwait(false);
                    _log.LogWarning("BigPipe: offset {Offset} out of range for {TopicPartition}; resetting to {Reset}", requested, tp, reset);
                    lock (_state)
                    {
                        if (epoch == _epoch)
                        {
                            _fetchPos[tp] = reset;
                            _consumed[tp] = reset;
                        }
                    }
                    continue;
                }
                default:
                    _ = _cluster.RefreshAsync([r.Topic], ct);
                    continue;
            }
            if (r.Records.IsEmpty) continue;
            raw.Clear();
            RecordBatchReader.Decode(r.Records, requested, raw, out var next);
            foreach (var rec in raw)
            {
                var result = new ConsumeResult<TKey, TValue>
                {
                    Topic = r.Topic,
                    Partition = r.Partition,
                    Offset = rec.Offset,
                    Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(rec.Timestamp),
                    Key = _keyDes.Deserialize(rec.Key, new SerializationContext(r.Topic, true)),
                    Value = _valueDes.Deserialize(rec.Value, new SerializationContext(r.Topic, false)),
                    Headers = rec.Headers,
                    Epoch = epoch,
                };
                await _queue.Writer.WriteAsync(result, ct).ConfigureAwait(false);
                if (epoch != _epoch) return; // rebalance/seek happened while we were blocked
            }
            lock (_state)
            {
                if (epoch == _epoch && next > requested) _fetchPos[tp] = next;
            }
        }
    }

    /// <summary>Leaves the group (after a final commit when auto-commit is on) and closes connections.</summary>
    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        foreach (var t in new[] { _runner, _heartbeat })
        {
            if (t is null) continue;
            try
            {
                await t.ConfigureAwait(false);
            }
            catch
            {
                // shutting down
            }
        }
        using var closeCts = new CancellationTokenSource(_cfg.RequestTimeout);
        if (_groupMode && _cfg.GroupId is { } g && _generation >= 0)
        {
            try
            {
                if (_cfg.EnableAutoCommit) await CommitAsync(closeCts.Token).ConfigureAwait(false);
                if (_coordinator is not null) await Requests.LeaveGroupAsync(_coordinator, g, _memberId, _cluster.Timeout, closeCts.Token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _log.LogDebug("BigPipe consumer close: {Message}", e.Message);
            }
        }
        _queue.Writer.TryComplete();
        await _cluster.DisposeAsync().ConfigureAwait(false);
        _cts.Dispose();
    }
}

/// <summary>Fluent builder for <see cref="Consumer{TKey,TValue}"/>.</summary>
public sealed class ConsumerBuilder<TKey, TValue>
{
    private readonly ConsumerConfig _cfg;
    private IDeserializer<TKey>? _key;
    private IDeserializer<TValue>? _value;
    private ILogger? _log;

    public ConsumerBuilder(ConsumerConfig? config = null) => _cfg = config ?? new ConsumerConfig();

    public ConsumerBuilder<TKey, TValue> WithBootstrap(string bootstrap) { _cfg.Bootstrap = bootstrap; return this; }
    public ConsumerBuilder<TKey, TValue> WithClientId(string clientId) { _cfg.ClientId = clientId; return this; }
    public ConsumerBuilder<TKey, TValue> WithGroup(string groupId) { _cfg.GroupId = groupId; return this; }
    public ConsumerBuilder<TKey, TValue> WithAutoOffsetReset(OffsetReset reset) { _cfg.AutoOffsetReset = reset; return this; }
    public ConsumerBuilder<TKey, TValue> WithAutoCommit(bool enabled) { _cfg.EnableAutoCommit = enabled; return this; }
    public ConsumerBuilder<TKey, TValue> WithAssignmentStrategy(AssignmentStrategy s) { _cfg.AssignmentStrategy = s; return this; }
    public ConsumerBuilder<TKey, TValue> WithKeyDeserializer(IDeserializer<TKey> d) { _key = d; return this; }
    public ConsumerBuilder<TKey, TValue> WithValueDeserializer(IDeserializer<TValue> d) { _value = d; return this; }
    public ConsumerBuilder<TKey, TValue> WithLogger(ILogger log) { _log = log; return this; }

    public Consumer<TKey, TValue> Build() =>
        new(_cfg, _key ?? Deserializers.For<TKey>(), _value ?? Deserializers.For<TValue>(), _log);
}
