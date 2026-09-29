using BigPipe.Client.Protocol;

namespace BigPipe.Client;

/// <summary>A topic partition.</summary>
public readonly record struct TopicPartition(string Topic, int Partition)
{
    public override string ToString() => $"{Topic}-{Partition}";
}

/// <summary>A topic partition with an offset (the next offset to read).</summary>
public readonly record struct TopicPartitionOffset(string Topic, int Partition, long Offset)
{
    public TopicPartition TopicPartition => new(Topic, Partition);
}

/// <summary>Kafka "consumer" embedded protocol: subscription and assignment encoding, plus assignors.</summary>
internal static class ConsumerProtocol
{
    public static byte[] EncodeSubscription(IReadOnlyCollection<string> topics)
    {
        using var w = new KafkaWriter(64);
        w.Int16(0);
        w.ArrayLength(topics.Count);
        foreach (var t in topics.OrderBy(t => t, StringComparer.Ordinal)) w.String(t);
        w.Int32(-1); // user data
        return w.WrittenSpan.ToArray();
    }

    public static List<string> DecodeSubscription(ReadOnlyMemory<byte> data)
    {
        var r = new KafkaReader(data);
        r.Int16();
        var topics = new List<string>();
        for (int i = 0, n = r.ArrayLength(); i < n; i++) topics.Add(r.String());
        return topics;
    }

    public static byte[] EncodeAssignment(IEnumerable<TopicPartition> parts)
    {
        var byTopic = parts.GroupBy(p => p.Topic).OrderBy(g => g.Key, StringComparer.Ordinal).ToList();
        using var w = new KafkaWriter(64);
        w.Int16(0);
        w.ArrayLength(byTopic.Count);
        foreach (var g in byTopic)
        {
            w.String(g.Key);
            var ps = g.Select(p => p.Partition).OrderBy(p => p).ToList();
            w.ArrayLength(ps.Count);
            foreach (var p in ps) w.Int32(p);
        }
        w.Int32(-1);
        return w.WrittenSpan.ToArray();
    }

    public static List<TopicPartition> DecodeAssignment(ReadOnlyMemory<byte> data)
    {
        var result = new List<TopicPartition>();
        if (data.Length < 6) return result;
        var r = new KafkaReader(data);
        r.Int16();
        for (int i = 0, n = r.ArrayLength(); i < n; i++)
        {
            var t = r.String();
            for (int j = 0, m = r.ArrayLength(); j < m; j++) result.Add(new TopicPartition(t, r.Int32()));
        }
        return result;
    }

    /// <summary>Kafka's RangeAssignor: contiguous partition ranges per topic.</summary>
    public static Dictionary<string, List<TopicPartition>> Range(IReadOnlyDictionary<string, List<string>> subscriptions, IReadOnlyDictionary<string, int> partitionCounts)
    {
        var result = subscriptions.Keys.ToDictionary(m => m, _ => new List<TopicPartition>());
        foreach (var (topic, count) in partitionCounts)
        {
            var members = subscriptions.Where(s => s.Value.Contains(topic)).Select(s => s.Key).OrderBy(m => m, StringComparer.Ordinal).ToList();
            if (members.Count == 0) continue;
            var per = count / members.Count;
            var extra = count % members.Count;
            var p = 0;
            for (var i = 0; i < members.Count; i++)
            {
                var n = per + (i < extra ? 1 : 0);
                for (var k = 0; k < n; k++) result[members[i]].Add(new TopicPartition(topic, p++));
            }
        }
        return result;
    }

    /// <summary>Kafka's RoundRobinAssignor across all subscribed partitions.</summary>
    public static Dictionary<string, List<TopicPartition>> RoundRobin(IReadOnlyDictionary<string, List<string>> subscriptions, IReadOnlyDictionary<string, int> partitionCounts)
    {
        var result = subscriptions.Keys.ToDictionary(m => m, _ => new List<TopicPartition>());
        var members = subscriptions.Keys.OrderBy(m => m, StringComparer.Ordinal).ToList();
        var i = 0;
        foreach (var (topic, count) in partitionCounts.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            for (var p = 0; p < count; p++)
            {
                for (var tries = 0; tries < members.Count; tries++)
                {
                    var m = members[i++ % members.Count];
                    if (!subscriptions[m].Contains(topic)) continue;
                    result[m].Add(new TopicPartition(topic, p));
                    break;
                }
            }
        }
        return result;
    }
}
