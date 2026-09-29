using System.Text.Json;
using System.Text.Json.Serialization;
using BigPipe.Client;
using BigPipe.Client.Admin;

namespace BigPipe.SchemaRegistry;

public sealed class RegistryOptions
{
    public string Bootstrap { get; set; } = "localhost:9092";
    public string AdminUrl { get; set; } = "http://localhost:9644";
    public string Topic { get; set; } = "__bp_schemas";
    public string DefaultCompatibility { get; set; } = "BACKWARD";
}

public sealed record SchemaVersion(string Subject, int Version, int Id, string Schema, SchemaFormat Format, bool Deleted = false);

/// <summary>Log entry in the schemas topic. Replaying the topic rebuilds the registry.</summary>
internal sealed record LogEntry
{
    [JsonPropertyName("kind")] public required string Kind { get; init; } // schema | config | delete
    [JsonPropertyName("subject")] public string? Subject { get; init; }
    [JsonPropertyName("version")] public int Version { get; init; }
    [JsonPropertyName("id")] public int Id { get; init; }
    [JsonPropertyName("schema")] public string? Schema { get; init; }
    [JsonPropertyName("schemaType")] public string? SchemaType { get; init; }
    [JsonPropertyName("compatibility")] public string? Compatibility { get; init; }
}

/// <summary>
/// In-memory registry state backed by an append-only topic. The service is stateless: on
/// start it replays the topic; every change is written to the topic before it is applied.
/// Run a single writer instance (additional instances may serve reads).
/// </summary>
public sealed class RegistryStore : IAsyncDisposable
{
    private readonly RegistryOptions _opt;
    private readonly ILogger<RegistryStore> _log;
    private readonly SemaphoreSlim _write = new(1, 1);
    private readonly object _lock = new();
    private readonly Dictionary<string, List<SchemaVersion>> _subjects = new();
    private readonly Dictionary<int, (string Schema, SchemaFormat Format)> _byId = new();
    private readonly Dictionary<string, int> _idByCanonical = new();
    private readonly Dictionary<string, string> _subjectConfig = new();
    private string _globalCompatibility;
    private Producer<string, string>? _producer;
    private int _nextId = 1;

    public RegistryStore(RegistryOptions opt, ILogger<RegistryStore> log)
    {
        _opt = opt;
        _log = log;
        _globalCompatibility = opt.DefaultCompatibility;
    }

    public bool Ready { get; private set; }

    public async Task StartAsync(CancellationToken ct)
    {
        using (var admin = new BigPipeAdminClient(_opt.AdminUrl))
            await admin.EnsureTopicAsync(_opt.Topic, 1, StorageMode.Local, new Dictionary<string, string> { ["retention.ms"] = "-1" }, ct);
        _producer = new ProducerBuilder<string, string>().WithBootstrap(_opt.Bootstrap).WithClientId("schema-registry").WithLinger(TimeSpan.Zero).Build();
        await using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            Bootstrap = _opt.Bootstrap,
            ClientId = "schema-registry-replay",
            EnableAutoCommit = false,
            FetchMaxWait = TimeSpan.FromMilliseconds(100),
        }).Build();
        var end = await consumer.ListOffsetAsync(new TopicPartition(_opt.Topic, 0), -1, ct);
        var n = 0;
        if (end > 0)
        {
            consumer.Assign(new TopicPartitionOffset(_opt.Topic, 0, 0));
            await foreach (var r in consumer.ConsumeAsync(ct))
            {
                if (r.Value is not null) Apply(JsonSerializer.Deserialize<LogEntry>(r.Value)!);
                n++;
                if (r.Offset + 1 >= end) break;
            }
        }
        Ready = true;
        _log.LogInformation("Schema registry ready: {Subjects} subjects, {Schemas} schemas replayed from {Records} records", _subjects.Count, _byId.Count, n);
    }

    private void Apply(LogEntry e)
    {
        lock (_lock)
        {
            switch (e.Kind)
            {
                case "schema":
                {
                    var fmt = Compatibility.ParseFormat(e.SchemaType);
                    _byId[e.Id] = (e.Schema!, fmt);
                    _idByCanonical[$"{fmt}:{Compatibility.Canonicalize(fmt, e.Schema!)}"] = e.Id;
                    _nextId = Math.Max(_nextId, e.Id + 1);
                    if (!_subjects.TryGetValue(e.Subject!, out var list)) _subjects[e.Subject!] = list = [];
                    list.RemoveAll(v => v.Version == e.Version);
                    list.Add(new SchemaVersion(e.Subject!, e.Version, e.Id, e.Schema!, fmt));
                    list.Sort((a, b) => a.Version.CompareTo(b.Version));
                    break;
                }
                case "delete":
                    if (_subjects.TryGetValue(e.Subject!, out var versions))
                    {
                        if (e.Version <= 0) _subjects.Remove(e.Subject!);
                        else versions.RemoveAll(v => v.Version == e.Version);
                    }
                    break;
                case "config":
                    if (e.Subject is null) _globalCompatibility = e.Compatibility!;
                    else _subjectConfig[e.Subject] = e.Compatibility!;
                    break;
            }
        }
    }

    private async Task AppendAsync(LogEntry e, CancellationToken ct)
    {
        await _producer!.SendAsync(_opt.Topic, e.Subject ?? "_config", JsonSerializer.Serialize(e), partition: 0, ct: ct);
        Apply(e);
    }

    public IReadOnlyList<string> Subjects()
    {
        lock (_lock) return _subjects.Where(kv => kv.Value.Count > 0).Select(kv => kv.Key).OrderBy(s => s).ToList();
    }

    public IReadOnlyList<SchemaVersion> Versions(string subject)
    {
        lock (_lock)
        {
            return _subjects.TryGetValue(subject, out var list) && list.Count > 0
                ? list.ToList()
                : throw new RegistryException(40401, $"Subject '{subject}' not found.");
        }
    }

    public SchemaVersion Version(string subject, string version)
    {
        var list = Versions(subject);
        if (version == "latest" || version == "-1") return list[^1];
        if (!int.TryParse(version, out var v)) throw new RegistryException(42202, $"invalid version '{version}'");
        return list.FirstOrDefault(x => x.Version == v) ?? throw new RegistryException(40402, $"Version {v} not found.");
    }

    public (string Schema, SchemaFormat Format) ById(int id)
    {
        lock (_lock) return _byId.TryGetValue(id, out var s) ? s : throw new RegistryException(40403, $"Schema {id} not found");
    }

    public IReadOnlyList<(string Subject, int Version)> SubjectsForId(int id)
    {
        lock (_lock) return _subjects.Values.SelectMany(l => l).Where(v => v.Id == id).Select(v => (v.Subject, v.Version)).ToList();
    }

    public string CompatibilityFor(string? subject)
    {
        lock (_lock) return subject is not null && _subjectConfig.TryGetValue(subject, out var c) ? c : _globalCompatibility;
    }

    public async Task SetCompatibilityAsync(string? subject, string level, CancellationToken ct)
    {
        var valid = new[] { "NONE", "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE" };
        level = level.ToUpperInvariant();
        if (!valid.Contains(level)) throw new RegistryException(42203, $"Invalid compatibility level '{level}'");
        await AppendAsync(new LogEntry { Kind = "config", Subject = subject, Compatibility = level }, ct);
    }

    /// <summary>Returns the version if this exact schema is registered under the subject.</summary>
    public SchemaVersion? Lookup(string subject, string schema, SchemaFormat format)
    {
        var canonical = Compatibility.Canonicalize(format, schema);
        lock (_lock)
        {
            if (!_idByCanonical.TryGetValue($"{format}:{canonical}", out var id)) return null;
            return _subjects.TryGetValue(subject, out var list) ? list.LastOrDefault(v => v.Id == id) : null;
        }
    }

    public List<string> CheckCompatibility(string subject, string schema, SchemaFormat format, string? version = null)
    {
        List<SchemaVersion> previous;
        lock (_lock) previous = _subjects.TryGetValue(subject, out var l) ? l.ToList() : [];
        if (version is not null && version != "latest")
            previous = previous.Where(v => v.Version == int.Parse(version)).ToList();
        if (previous.Any(p => p.Format != format))
            return [$"schema type changed from {previous[^1].Format} to {format}"];
        Compatibility.IsCompatible(format, version is null ? CompatibilityFor(subject) : CompatibilityFor(subject).Replace("_TRANSITIVE", ""),
            schema, previous.Select(p => p.Schema).ToList(), out var messages);
        return messages;
    }

    public async Task<int> RegisterAsync(string subject, string schema, SchemaFormat format, CancellationToken ct)
    {
        await _write.WaitAsync(ct);
        try
        {
            var canonical = Compatibility.Canonicalize(format, schema);
            if (Lookup(subject, schema, format) is { } existing) return existing.Id;
            var problems = CheckCompatibility(subject, schema, format);
            if (problems.Count > 0)
                throw new RegistryException(409, $"Schema being registered is incompatible with an earlier schema for subject \"{subject}\": {string.Join("; ", problems)}");
            int id;
            int version;
            lock (_lock)
            {
                id = _idByCanonical.TryGetValue($"{format}:{canonical}", out var known) ? known : _nextId;
                version = _subjects.TryGetValue(subject, out var list) && list.Count > 0 ? list[^1].Version + 1 : 1;
            }
            await AppendAsync(new LogEntry
            {
                Kind = "schema",
                Subject = subject,
                Version = version,
                Id = id,
                Schema = schema,
                SchemaType = format.ToString().ToUpperInvariant(),
            }, ct);
            return id;
        }
        finally
        {
            _write.Release();
        }
    }

    public async Task<IReadOnlyList<int>> DeleteSubjectAsync(string subject, CancellationToken ct)
    {
        var versions = Versions(subject).Select(v => v.Version).ToList();
        await AppendAsync(new LogEntry { Kind = "delete", Subject = subject, Version = 0 }, ct);
        return versions;
    }

    public async Task<int> DeleteVersionAsync(string subject, string version, CancellationToken ct)
    {
        var v = Version(subject, version);
        await AppendAsync(new LogEntry { Kind = "delete", Subject = subject, Version = v.Version }, ct);
        return v.Version;
    }

    public async ValueTask DisposeAsync()
    {
        if (_producer is not null) await _producer.DisposeAsync();
    }
}

internal sealed class RegistryStartup(RegistryStore store, ILogger<RegistryStartup> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await store.StartAsync(stoppingToken);
                return;
            }
            catch (Exception e) when (!stoppingToken.IsCancellationRequested)
            {
                log.LogWarning("Schema registry waiting for BigPipe: {Message}", e.Message);
                await Task.Delay(2000, stoppingToken);
            }
        }
    }
}
