using BigPipe.Client.Network;
using BigPipe.Client.Protocol;

namespace BigPipe.Client.Admin;

/// <summary>A topic to create with <see cref="KafkaAdminClient"/>.</summary>
/// <param name="Name">Topic name.</param>
/// <param name="Partitions">Partition count; 0 or less uses the broker default.</param>
/// <param name="Config">Topic configuration, e.g. <c>cleanup.policy=compact</c>.</param>
public sealed record NewTopic(string Name, int Partitions = -1, IReadOnlyDictionary<string, string>? Config = null);

/// <summary>
/// Topic administration over the Kafka protocol (port 9092), so it works with only a bootstrap
/// address, against BigPipe or Apache Kafka. For the full BigPipe admin API (storage modes,
/// migration, flows, compaction stats) use <see cref="BigPipeAdminClient"/>.
/// </summary>
public sealed class KafkaAdminClient : IAsyncDisposable
{
    private readonly Cluster _cluster;

    public KafkaAdminClient(string bootstrap = "localhost:9092", string clientId = "bigpipe-admin") =>
        _cluster = new Cluster(new ClientConfig { Bootstrap = bootstrap, ClientId = clientId });

    /// <summary>Creates topics. Throws on the first failure, including an existing topic.</summary>
    public async Task CreateTopicsAsync(IEnumerable<NewTopic> topics, CancellationToken ct = default)
    {
        foreach (var (name, error, message) in await SendAsync(topics, ct).ConfigureAwait(false))
            if (error != 0)
                throw new BigPipeException((ErrorCode)error, message ?? $"cannot create topic '{name}' (error {error})");
    }

    /// <summary>Creates the topics that do not exist yet; existing topics are left unchanged.</summary>
    public async Task EnsureTopicsAsync(IEnumerable<NewTopic> topics, CancellationToken ct = default)
    {
        foreach (var (name, error, message) in await SendAsync(topics, ct).ConfigureAwait(false))
            if (error != 0 && error != (short)ErrorCode.TopicAlreadyExists)
                throw new BigPipeException((ErrorCode)error, message ?? $"cannot create topic '{name}' (error {error})");
    }

    private async Task<List<(string Name, short Error, string? Message)>> SendAsync(IEnumerable<NewTopic> topics, CancellationToken ct)
    {
        var list = topics.Select(t => (t.Name, t.Partitions, t.Config)).ToList();
        if (list.Count == 0) return [];
        var conn = await _cluster.AnyAsync(ct).ConfigureAwait(false);
        return await Requests.CreateTopicsAsync(conn, list, _cluster.Timeout, ct).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => _cluster.DisposeAsync();
}
