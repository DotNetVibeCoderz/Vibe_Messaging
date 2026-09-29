using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace BigPipe.Client.Admin;

/// <summary>Where new data of a topic is stored.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<StorageMode>))]
public enum StorageMode
{
    [JsonStringEnumMemberName("local")] Local,
    [JsonStringEnumMemberName("tiered")] Tiered,
    [JsonStringEnumMemberName("diskless")] Diskless,
}

public sealed record ClusterListeners(string Kafka, string Http, string Admin, string Metrics);

public sealed record ClusterInfo(
    string ClusterId,
    int NodeId,
    string Version,
    long UptimeSeconds,
    int Shards,
    string ObjectStore,
    ClusterListeners Listeners,
    int Topics,
    int Groups,
    string Credit);

public sealed record TopicBytes(long Local, long Remote, long Diskless)
{
    public long Total => Local + Remote + Diskless;
}

public sealed record PartitionDetail(
    int Partition,
    long LogStartOffset,
    long HighWatermark,
    long LocalBytes,
    long RemoteBytes,
    long DisklessBytes,
    int Segments,
    int RemoteSegments,
    int DisklessExtents,
    long CacheBytes,
    long RecordsIn,
    long BytesIn);

public sealed record TopicInfo(
    string Name,
    int Partitions,
    StorageMode Mode,
    bool Internal,
    long CreatedMs,
    long Records,
    TopicBytes Bytes,
    IReadOnlyDictionary<string, string> Config,
    IReadOnlyDictionary<string, string> EffectiveConfig,
    IReadOnlyList<PartitionDetail>? PartitionDetails);

public sealed record MigrationResult(string Topic, StorageMode From, StorageMode To, IReadOnlyDictionary<string, long> SwitchOffsets);

public sealed record AssignedPartition(string Topic, int Partition);

public sealed record GroupMember(string MemberId, string ClientId, string ClientHost, IReadOnlyList<AssignedPartition> Assignment);

public sealed record GroupSummary(string GroupId, string Kind, string State, string ProtocolType, string Protocol, int Generation, IReadOnlyList<GroupMember> Members);

public sealed record GroupOffset(string Topic, int Partition, long Committed, long HighWatermark, long Lag, long CommitMs);

public sealed record GroupInfo(string GroupId, GroupSummary? Summary, IReadOnlyList<GroupOffset>? Offsets, long Lag, int? Partitions)
{
    /// <summary>Topics with committed offsets (list view only).</summary>
    public IReadOnlyList<string>? Topics { get; init; }
}

public sealed record MetricsSnapshot(
    long ConnectionsOpen,
    long RequestsTotal,
    long RequestErrorsTotal,
    long ProduceRecordsTotal,
    long ProduceBytesTotal,
    long FetchBytesTotal,
    long HttpRequestsTotal,
    long DisklessFilesTotal,
    long DisklessBytesTotal,
    long TieredUploadsTotal,
    long RebalancesTotal,
    double ProduceP50Ms,
    double ProduceP99Ms,
    double FetchP99Ms,
    double DisklessPutP99Ms,
    long ObjectCacheBytes,
    long DisklessFilesReferenced);

/// <summary>One flow processor. Set exactly one of the properties.</summary>
public sealed record FlowProcessor
{
    /// <summary>Keep records where this bpql expression is true.</summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public string? Filter { get; init; }

    /// <summary>Rewrite the value with a bpql mapping (<c>root.x = this.y * 2</c>).</summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public string? Mapping { get; init; }

    /// <summary>Choose the output topic per record.</summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public string? Route { get; init; }

    public static FlowProcessor FilterBy(string expr) => new() { Filter = expr };
    public static FlowProcessor MapWith(string mapping) => new() { Mapping = mapping };
    public static FlowProcessor RouteBy(string expr) => new() { Route = expr };
}

public sealed record FlowSpec
{
    public required string Name { get; init; }
    public required string Input { get; init; }
    public required string Output { get; init; }
    public IReadOnlyList<FlowProcessor> Processors { get; init; } = [];
    public string StartFrom { get; init; } = "earliest";
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public string? Dlq { get; init; }
    public bool Paused { get; init; }
}

public sealed record FlowStats(long RecordsIn, long RecordsOut, long RecordsFiltered, long Errors, string? LastError, bool Running);

public sealed record FlowStatus(string Name, string Input, string Output, IReadOnlyList<FlowProcessor> Processors, string StartFrom, string? Dlq, bool Paused, FlowStats Stats);

/// <summary>A record as returned by the HTTP gateway and admin message browser.</summary>
public sealed record HttpRecord
{
    public string Topic { get; init; } = "";
    public int Partition { get; init; }
    public long Offset { get; init; }
    public long Timestamp { get; init; }
    public string? Key { get; init; }
    public string KeyEncoding { get; init; } = "utf8";
    public string? Value { get; init; }
    public string ValueEncoding { get; init; } = "utf8";
    public IReadOnlyDictionary<string, string?> Headers { get; init; } = new Dictionary<string, string?>();
    public int? DeliveryCount { get; init; }

    public DateTimeOffset Time => DateTimeOffset.FromUnixTimeMilliseconds(Timestamp);

    public byte[]? ValueBytes => Value is null ? null : ValueEncoding == "base64" ? Convert.FromBase64String(Value) : Encoding.UTF8.GetBytes(Value);

    public byte[]? KeyBytes => Key is null ? null : KeyEncoding == "base64" ? Convert.FromBase64String(Key) : Encoding.UTF8.GetBytes(Key);

    /// <summary>Deserializes a JSON value.</summary>
    public T? ValueAs<T>(JsonSerializerOptions? options = null) =>
        Value is null ? default : JsonSerializer.Deserialize<T>(ValueBytes!, options ?? BigPipeJson.Options);
}

/// <summary>A record to send through the HTTP gateway.</summary>
public sealed record HttpProduceRecord
{
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public string? Key { get; init; }
    public object? Value { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public IDictionary<string, string>? Headers { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public int? Partition { get; init; }
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] public long? Timestamp { get; init; }
}

public sealed record HttpOffset(int Partition, long Offset);

internal static class AdminJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DictionaryKeyPolicy = null,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };
}
