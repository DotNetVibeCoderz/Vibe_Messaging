using BigPipe.Client.Network;

namespace BigPipe.Client.Protocol;

// API versions chosen to work against BigPipe and Apache Kafka 2.1 through 4.x.
internal static class ApiKey
{
    public const short Produce = 0, Fetch = 1, ListOffsets = 2, Metadata = 3, OffsetCommit = 8, OffsetFetch = 9,
        FindCoordinator = 10, JoinGroup = 11, Heartbeat = 12, LeaveGroup = 13, SyncGroup = 14,
        CreateTopics = 19, DeleteTopics = 20, InitProducerId = 22;
}

internal sealed record BrokerNode(int NodeId, string Host, int Port);

internal sealed record PartitionMeta(int Partition, short Error, int Leader);

internal sealed record TopicMeta(string Name, short Error, IReadOnlyList<PartitionMeta> Partitions);

internal sealed record MetadataResult(IReadOnlyList<BrokerNode> Brokers, string? ClusterId, int ControllerId, IReadOnlyList<TopicMeta> Topics);

internal sealed record ProducePartitionResult(int Partition, short Error, long BaseOffset, string? Message);

internal sealed record FetchPartitionResult(string Topic, int Partition, short Error, long HighWatermark, long LogStart, ReadOnlyMemory<byte> Records);

internal sealed record JoinResult(short Error, int Generation, string Protocol, string Leader, string MemberId, IReadOnlyList<(string MemberId, ReadOnlyMemory<byte> Metadata)> Members);

internal static class Requests
{
    public static async Task<MetadataResult> MetadataAsync(BrokerConnection c, IReadOnlyCollection<string>? topics, bool allowAutoCreate, TimeSpan timeout, CancellationToken ct)
    {
        const short v = 5;
        var body = await c.SendAsync(ApiKey.Metadata, v, w =>
        {
            if (topics is null) w.ArrayLength(-1);
            else
            {
                w.ArrayLength(topics.Count);
                foreach (var t in topics) w.String(t);
            }
            w.Bool(allowAutoCreate);
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32(); // throttle
        var brokers = new List<BrokerNode>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
        {
            var id = r.Int32();
            var host = r.String();
            var port = r.Int32();
            r.NullableString();
            brokers.Add(new BrokerNode(id, host, port));
        }
        var cluster = r.NullableString();
        var controller = r.Int32();
        var list = new List<TopicMeta>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
        {
            var err = r.Int16();
            var name = r.String();
            r.Bool();
            var parts = new List<PartitionMeta>();
            for (int j = 0, m = r.ArrayLength(); j < m; j++)
            {
                var perr = r.Int16();
                var idx = r.Int32();
                var leader = r.Int32();
                for (int k = 0, q = r.ArrayLength(); k < q; k++) r.Int32();
                for (int k = 0, q = r.ArrayLength(); k < q; k++) r.Int32();
                for (int k = 0, q = r.ArrayLength(); k < q; k++) r.Int32();
                parts.Add(new PartitionMeta(idx, perr, leader));
            }
            parts.Sort((a, b) => a.Partition.CompareTo(b.Partition));
            list.Add(new TopicMeta(name, err, parts));
        }
        return new MetadataResult(brokers, cluster, controller, list);
    }

    public static async Task<List<(string Topic, ProducePartitionResult Result)>> ProduceAsync(
        BrokerConnection c, short acks, int timeoutMs, IReadOnlyList<(string Topic, int Partition, byte[] Batch)> batches, TimeSpan timeout, CancellationToken ct)
    {
        const short v = 7;
        var byTopic = batches.GroupBy(b => b.Topic).ToList();
        var body = await c.SendAsync(ApiKey.Produce, v, w =>
        {
            w.String(null);
            w.Int16(acks);
            w.Int32(timeoutMs);
            w.ArrayLength(byTopic.Count);
            foreach (var g in byTopic)
            {
                w.String(g.Key);
                var parts = g.ToList();
                w.ArrayLength(parts.Count);
                foreach (var p in parts)
                {
                    w.Int32(p.Partition);
                    w.Bytes(p.Batch);
                }
            }
        }, timeout, ct, expectResponse: acks != 0).ConfigureAwait(false);
        var results = new List<(string, ProducePartitionResult)>();
        if (acks == 0)
        {
            foreach (var b in batches) results.Add((b.Topic, new ProducePartitionResult(b.Partition, 0, -1, null)));
            return results;
        }
        var r = new KafkaReader(body);
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
        {
            var topic = r.String();
            for (int j = 0, m = r.ArrayLength(); j < m; j++)
            {
                var p = r.Int32();
                var err = r.Int16();
                var baseOffset = r.Int64();
                r.Int64(); // log append time
                r.Int64(); // log start offset
                results.Add((topic, new ProducePartitionResult(p, err, baseOffset, null)));
            }
        }
        return results;
    }

    public static async Task<List<FetchPartitionResult>> FetchAsync(
        BrokerConnection c, int maxWaitMs, int minBytes, int maxBytes, IReadOnlyList<(string Topic, int Partition, long Offset, int MaxBytes)> parts, TimeSpan timeout, CancellationToken ct)
    {
        const short v = 8;
        var byTopic = parts.GroupBy(p => p.Topic).ToList();
        var body = await c.SendAsync(ApiKey.Fetch, v, w =>
        {
            w.Int32(-1);
            w.Int32(maxWaitMs);
            w.Int32(minBytes);
            w.Int32(maxBytes);
            w.Int8(0); // read_uncommitted
            w.Int32(0); // session id: full fetch
            w.Int32(-1); // session epoch: no session
            w.ArrayLength(byTopic.Count);
            foreach (var g in byTopic)
            {
                w.String(g.Key);
                var ps = g.ToList();
                w.ArrayLength(ps.Count);
                foreach (var p in ps)
                {
                    w.Int32(p.Partition);
                    w.Int64(p.Offset);
                    w.Int64(-1);
                    w.Int32(p.MaxBytes);
                }
            }
            w.ArrayLength(0); // forgotten topics
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32(); // throttle
        var top = r.Int16();
        r.Int32(); // session id
        BigPipeException.ThrowIfError(top, "fetch");
        var results = new List<FetchPartitionResult>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
        {
            var topic = r.String();
            for (int j = 0, m = r.ArrayLength(); j < m; j++)
            {
                var p = r.Int32();
                var err = r.Int16();
                var hw = r.Int64();
                r.Int64(); // last stable offset
                var logStart = r.Int64();
                var aborted = r.ArrayLength();
                for (var k = 0; k < aborted; k++)
                {
                    r.Int64();
                    r.Int64();
                }
                var records = r.NullableBytes() ?? ReadOnlyMemory<byte>.Empty;
                results.Add(new FetchPartitionResult(topic, p, err, hw, logStart, records));
            }
        }
        return results;
    }

    /// <summary>timestamp: -1 latest, -2 earliest, otherwise ms since epoch.</summary>
    public static async Task<Dictionary<int, (short Error, long Offset)>> ListOffsetsAsync(BrokerConnection c, string topic, IReadOnlyList<(int Partition, long Timestamp)> parts, TimeSpan timeout, CancellationToken ct)
    {
        const short v = 2;
        var body = await c.SendAsync(ApiKey.ListOffsets, v, w =>
        {
            w.Int32(-1);
            w.Int8(0);
            w.ArrayLength(1);
            w.String(topic);
            w.ArrayLength(parts.Count);
            foreach (var p in parts)
            {
                w.Int32(p.Partition);
                w.Int64(p.Timestamp);
            }
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        var result = new Dictionary<int, (short, long)>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
        {
            r.String();
            for (int j = 0, m = r.ArrayLength(); j < m; j++)
            {
                var p = r.Int32();
                var err = r.Int16();
                r.Int64();
                var off = r.Int64();
                result[p] = (err, off);
            }
        }
        return result;
    }

    public static async Task<BrokerNode> FindCoordinatorAsync(BrokerConnection c, string group, TimeSpan timeout, CancellationToken ct)
    {
        var body = await c.SendAsync(ApiKey.FindCoordinator, 2, w =>
        {
            w.String(group);
            w.Int8(0);
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        var err = r.Int16();
        var msg = r.NullableString();
        var node = r.Int32();
        var host = r.String();
        var port = r.Int32();
        if (err != 0) throw new BigPipeException((ErrorCode)err, $"find coordinator for '{group}': {msg ?? ((ErrorCode)err).ToString()}");
        return new BrokerNode(node, host, port);
    }

    public static async Task<JoinResult> JoinGroupAsync(BrokerConnection c, string group, int sessionTimeoutMs, int rebalanceTimeoutMs, string memberId,
        IReadOnlyList<(string Name, byte[] Metadata)> protocols, TimeSpan timeout, CancellationToken ct)
    {
        var body = await c.SendAsync(ApiKey.JoinGroup, 4, w =>
        {
            w.String(group);
            w.Int32(sessionTimeoutMs);
            w.Int32(rebalanceTimeoutMs);
            w.String(memberId);
            w.String("consumer");
            w.ArrayLength(protocols.Count);
            foreach (var p in protocols)
            {
                w.String(p.Name);
                w.Bytes(p.Metadata);
            }
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        var err = r.Int16();
        var gen = r.Int32();
        var proto = r.String();
        var leader = r.String();
        var member = r.String();
        var members = new List<(string, ReadOnlyMemory<byte>)>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
            members.Add((r.String(), r.Bytes()));
        return new JoinResult(err, gen, proto, leader, member, members);
    }

    public static async Task<(short Error, ReadOnlyMemory<byte> Assignment)> SyncGroupAsync(BrokerConnection c, string group, int generation, string memberId,
        IReadOnlyList<(string MemberId, byte[] Assignment)> assignments, TimeSpan timeout, CancellationToken ct)
    {
        var body = await c.SendAsync(ApiKey.SyncGroup, 2, w =>
        {
            w.String(group);
            w.Int32(generation);
            w.String(memberId);
            w.ArrayLength(assignments.Count);
            foreach (var a in assignments)
            {
                w.String(a.MemberId);
                w.Bytes(a.Assignment);
            }
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        var err = r.Int16();
        return (err, r.Bytes());
    }

    public static async Task<short> HeartbeatAsync(BrokerConnection c, string group, int generation, string memberId, TimeSpan timeout, CancellationToken ct)
    {
        var body = await c.SendAsync(ApiKey.Heartbeat, 2, w =>
        {
            w.String(group);
            w.Int32(generation);
            w.String(memberId);
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        return r.Int16();
    }

    public static async Task LeaveGroupAsync(BrokerConnection c, string group, string memberId, TimeSpan timeout, CancellationToken ct)
    {
        await c.SendAsync(ApiKey.LeaveGroup, 2, w =>
        {
            w.String(group);
            w.String(memberId);
        }, timeout, ct).ConfigureAwait(false);
    }

    public static async Task<List<(string Topic, int Partition, short Error)>> OffsetCommitAsync(BrokerConnection c, string group, int generation, string memberId,
        IReadOnlyList<(string Topic, int Partition, long Offset)> offsets, TimeSpan timeout, CancellationToken ct)
    {
        var byTopic = offsets.GroupBy(o => o.Topic).ToList();
        var body = await c.SendAsync(ApiKey.OffsetCommit, 5, w =>
        {
            w.String(group);
            w.Int32(generation);
            w.String(memberId);
            w.ArrayLength(byTopic.Count);
            foreach (var g in byTopic)
            {
                w.String(g.Key);
                var ps = g.ToList();
                w.ArrayLength(ps.Count);
                foreach (var p in ps)
                {
                    w.Int32(p.Partition);
                    w.Int64(p.Offset);
                    w.String(null);
                }
            }
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        var result = new List<(string, int, short)>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
        {
            var t = r.String();
            for (int j = 0, m = r.ArrayLength(); j < m; j++)
                result.Add((t, r.Int32(), r.Int16()));
        }
        return result;
    }

    public static async Task<Dictionary<(string Topic, int Partition), long>> OffsetFetchAsync(BrokerConnection c, string group,
        IReadOnlyList<(string Topic, int Partition)> parts, TimeSpan timeout, CancellationToken ct)
    {
        var byTopic = parts.GroupBy(p => p.Topic).ToList();
        var body = await c.SendAsync(ApiKey.OffsetFetch, 5, w =>
        {
            w.String(group);
            w.ArrayLength(byTopic.Count);
            foreach (var g in byTopic)
            {
                w.String(g.Key);
                var ps = g.ToList();
                w.ArrayLength(ps.Count);
                foreach (var p in ps) w.Int32(p.Partition);
            }
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        var result = new Dictionary<(string, int), long>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
        {
            var t = r.String();
            for (int j = 0, m = r.ArrayLength(); j < m; j++)
            {
                var p = r.Int32();
                var off = r.Int64();
                r.Int32(); // leader epoch
                r.NullableString();
                var err = r.Int16();
                if (err == 0) result[(t, p)] = off;
            }
        }
        BigPipeException.ThrowIfError(r.Int16(), $"offset fetch for group '{group}'");
        return result;
    }

    public static async Task<(long ProducerId, short Epoch)> InitProducerIdAsync(BrokerConnection c, TimeSpan timeout, CancellationToken ct)
    {
        var body = await c.SendAsync(ApiKey.InitProducerId, 1, w =>
        {
            w.String(null);
            w.Int32(60_000);
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        BigPipeException.ThrowIfError(r.Int16(), "init producer id");
        return (r.Int64(), r.Int16());
    }

    public static async Task<List<(string Name, short Error, string? Message)>> CreateTopicsAsync(BrokerConnection c,
        IReadOnlyList<(string Name, int Partitions, IReadOnlyDictionary<string, string>? Config)> topics, TimeSpan timeout, CancellationToken ct)
    {
        var body = await c.SendAsync(ApiKey.CreateTopics, 3, w =>
        {
            w.ArrayLength(topics.Count);
            foreach (var t in topics)
            {
                w.String(t.Name);
                w.Int32(t.Partitions);
                w.Int16(1);
                w.ArrayLength(0);
                var cfg = t.Config ?? new Dictionary<string, string>();
                w.ArrayLength(cfg.Count);
                foreach (var kv in cfg)
                {
                    w.String(kv.Key);
                    w.String(kv.Value);
                }
            }
            w.Int32((int)timeout.TotalMilliseconds);
            w.Bool(false);
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        var result = new List<(string, short, string?)>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
            result.Add((r.String(), r.Int16(), r.NullableString()));
        return result;
    }

    public static async Task<List<(string Name, short Error)>> DeleteTopicsAsync(BrokerConnection c, IReadOnlyList<string> names, TimeSpan timeout, CancellationToken ct)
    {
        var body = await c.SendAsync(ApiKey.DeleteTopics, 3, w =>
        {
            w.ArrayLength(names.Count);
            foreach (var n in names) w.String(n);
            w.Int32((int)timeout.TotalMilliseconds);
        }, timeout, ct).ConfigureAwait(false);
        var r = new KafkaReader(body);
        r.Int32();
        var result = new List<(string, short)>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
            result.Add((r.String(), r.Int16()));
        return result;
    }
}
