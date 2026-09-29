using System.Collections.Concurrent;
using BigPipe.Client.Protocol;

namespace BigPipe.Client.Network;

/// <summary>Connection settings shared by producers, consumers and admin clients.</summary>
public class ClientConfig
{
    /// <summary>Comma-separated <c>host:port</c> list, e.g. <c>localhost:9092</c>.</summary>
    public string Bootstrap { get; set; } = "localhost:9092";

    public string ClientId { get; set; } = "bigpipe-dotnet";

    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How often topic metadata is refreshed in the background.</summary>
    public TimeSpan MetadataMaxAge { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Let the broker create unknown topics on first use (BigPipe and Kafka default).</summary>
    public bool AllowAutoCreateTopics { get; set; } = true;

    internal IEnumerable<(string Host, int Port)> BootstrapEndpoints()
    {
        foreach (var part in Bootstrap.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var s = part.Contains("://") ? part[(part.IndexOf("://", StringComparison.Ordinal) + 3)..] : part;
            var i = s.LastIndexOf(':');
            yield return i > 0 ? (s[..i], int.Parse(s[(i + 1)..])) : (s, 9092);
        }
    }
}

/// <summary>
/// Cluster view: bootstrap, metadata cache and one pipelined connection per broker
/// (plus a dedicated coordinator connection so heartbeats never queue behind long-poll fetches).
/// </summary>
internal sealed class Cluster : IAsyncDisposable
{
    private readonly ClientConfig _cfg;
    private readonly ConcurrentDictionary<int, BrokerNode> _brokers = new();
    private readonly ConcurrentDictionary<string, BrokerConnection> _connections = new();
    private readonly SemaphoreSlim _connectLock = new(1, 1);
    private readonly SemaphoreSlim _metadataLock = new(1, 1);
    private volatile Dictionary<string, TopicMeta> _topics = new();
    private DateTime _lastRefresh = DateTime.MinValue;

    public Cluster(ClientConfig cfg) => _cfg = cfg;

    public ClientConfig Config => _cfg;

    public TimeSpan Timeout => _cfg.RequestTimeout;

    private async Task<BrokerConnection> ConnectAsync(string key, string host, int port, CancellationToken ct)
    {
        if (_connections.TryGetValue(key, out var c) && c.IsHealthy) return c;
        await _connectLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_connections.TryGetValue(key, out c) && c.IsHealthy) return c;
            if (c is not null) await c.DisposeAsync().ConfigureAwait(false);
            c = await BrokerConnection.ConnectAsync(host, port, _cfg.ClientId, _cfg.ConnectTimeout, ct).ConfigureAwait(false);
            _connections[key] = c;
            return c;
        }
        finally
        {
            _connectLock.Release();
        }
    }

    /// <summary>Any live broker (bootstrap servers first).</summary>
    public async Task<BrokerConnection> AnyAsync(CancellationToken ct)
    {
        foreach (var kv in _connections)
            if (kv.Value.IsHealthy && !kv.Key.StartsWith("coord:", StringComparison.Ordinal)) return kv.Value;
        Exception? last = null;
        foreach (var b in _brokers.Values)
        {
            try { return await ConnectAsync($"node:{b.NodeId}", b.Host, b.Port, ct).ConfigureAwait(false); }
            catch (Exception e) { last = e; }
        }
        foreach (var (host, port) in _cfg.BootstrapEndpoints())
        {
            try { return await ConnectAsync($"boot:{host}:{port}", host, port, ct).ConfigureAwait(false); }
            catch (Exception e) { last = e; }
        }
        throw new BigPipeException(ErrorCode.ConnectionFailed, $"no broker reachable (bootstrap: {_cfg.Bootstrap})", last);
    }

    public async Task<BrokerConnection> BrokerAsync(int nodeId, CancellationToken ct)
    {
        if (!_brokers.TryGetValue(nodeId, out var b))
        {
            await RefreshAsync(null, ct).ConfigureAwait(false);
            if (!_brokers.TryGetValue(nodeId, out b))
                throw new BigPipeException(ErrorCode.LeaderNotAvailable, $"broker {nodeId} is not in the cluster metadata");
        }
        return await ConnectAsync($"node:{nodeId}", b.Host, b.Port, ct).ConfigureAwait(false);
    }

    public async Task<BrokerConnection> CoordinatorAsync(string group, CancellationToken ct)
    {
        var any = await AnyAsync(ct).ConfigureAwait(false);
        var node = await Requests.FindCoordinatorAsync(any, group, Timeout, ct).ConfigureAwait(false);
        return await ConnectAsync($"coord:{node.NodeId}", node.Host, node.Port, ct).ConfigureAwait(false);
    }

    /// <summary>Refreshes metadata for the given topics (null = all known + requested).</summary>
    public async Task RefreshAsync(IReadOnlyCollection<string>? topics, CancellationToken ct)
    {
        await _metadataLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var want = topics is null ? (_topics.Count == 0 ? null : _topics.Keys.ToList()) : topics.Union(_topics.Keys).ToList();
            var conn = await AnyAsync(ct).ConfigureAwait(false);
            var md = await Requests.MetadataAsync(conn, want, _cfg.AllowAutoCreateTopics, Timeout, ct).ConfigureAwait(false);
            foreach (var b in md.Brokers) _brokers[b.NodeId] = b;
            var next = new Dictionary<string, TopicMeta>(_topics);
            foreach (var t in md.Topics)
            {
                if (t.Error == 0) next[t.Name] = t;
                else next.Remove(t.Name);
            }
            _topics = next;
            _lastRefresh = DateTime.UtcNow;
        }
        finally
        {
            _metadataLock.Release();
        }
    }

    /// <summary>Topic metadata, fetching it (and auto-creating the topic when allowed) on first use.</summary>
    public async Task<TopicMeta> TopicAsync(string topic, CancellationToken ct)
    {
        if (_topics.TryGetValue(topic, out var t) && DateTime.UtcNow - _lastRefresh < _cfg.MetadataMaxAge) return t;
        for (var attempt = 0; attempt < 10; attempt++)
        {
            await RefreshAsync([topic], ct).ConfigureAwait(false);
            if (_topics.TryGetValue(topic, out t) && t.Partitions.Count > 0) return t;
            await Task.Delay(100 * (attempt + 1), ct).ConfigureAwait(false);
        }
        throw new BigPipeException(ErrorCode.UnknownTopicOrPartition, $"topic '{topic}' does not exist");
    }

    public IReadOnlyCollection<TopicMeta> KnownTopics => _topics.Values;

    public async ValueTask DisposeAsync()
    {
        foreach (var c in _connections.Values) await c.DisposeAsync().ConfigureAwait(false);
        _connections.Clear();
        _connectLock.Dispose();
        _metadataLock.Dispose();
    }
}
