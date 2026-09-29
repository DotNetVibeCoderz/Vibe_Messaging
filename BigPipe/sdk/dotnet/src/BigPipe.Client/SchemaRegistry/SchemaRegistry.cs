using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Schema;

namespace BigPipe.Client;

/// <summary>Schema formats the .NET serializers can produce.</summary>
public enum SchemaFormat
{
    /// <summary>JSON Schema generated from the .NET type; payload is JSON.</summary>
    Json,
}

/// <summary>Client for the BigPipe (Confluent-compatible) Schema Registry.</summary>
public sealed class SchemaRegistryClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly ConcurrentDictionary<int, string> _byId = new();

    public SchemaRegistryClient(string url = "http://localhost:8081") =>
        _http = new HttpClient { BaseAddress = new Uri(url.TrimEnd('/') + "/") };

    private static async Task<JsonObject> ReadAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        var node = JsonNode.Parse(string.IsNullOrEmpty(body) ? "{}" : body) as JsonObject ?? [];
        if (!resp.IsSuccessStatusCode)
            throw new BigPipeException(ErrorCode.HttpError, $"schema registry {(int)resp.StatusCode}: {node["message"]}");
        return node;
    }

    /// <summary>Registers (or finds) a schema under a subject and returns its id.</summary>
    public async Task<int> RegisterAsync(string subject, string schema, string schemaType = "JSON", CancellationToken ct = default)
    {
        var resp = await _http.PostAsJsonAsync($"subjects/{Uri.EscapeDataString(subject)}/versions", new { schema, schemaType }, ct).ConfigureAwait(false);
        return (await ReadAsync(resp, ct).ConfigureAwait(false))["id"]!.GetValue<int>();
    }

    public async Task<string> GetSchemaAsync(int id, CancellationToken ct = default)
    {
        if (_byId.TryGetValue(id, out var s)) return s;
        var node = await ReadAsync(await _http.GetAsync($"schemas/ids/{id}", ct).ConfigureAwait(false), ct).ConfigureAwait(false);
        return _byId[id] = node["schema"]!.ToString();
    }

    public async Task<IReadOnlyList<string>> SubjectsAsync(CancellationToken ct = default) =>
        (await _http.GetFromJsonAsync<List<string>>("subjects", ct).ConfigureAwait(false))!;

    public async Task<bool> IsCompatibleAsync(string subject, string schema, string schemaType = "JSON", CancellationToken ct = default)
    {
        var resp = await _http.PostAsJsonAsync($"compatibility/subjects/{Uri.EscapeDataString(subject)}/versions/latest", new { schema, schemaType }, ct).ConfigureAwait(false);
        if (resp.StatusCode == System.Net.HttpStatusCode.NotFound) return true;
        return (await ReadAsync(resp, ct).ConfigureAwait(false))["is_compatible"]!.GetValue<bool>();
    }

    public void Dispose() => _http.Dispose();
}

/// <summary>
/// Serializes values as JSON in the Confluent wire format (<c>0x00</c> + 4-byte schema id +
/// payload). The JSON Schema is generated from <typeparamref name="T"/> and registered under
/// <c>&lt;topic&gt;-value</c> (or <c>-key</c>) on first use.
/// </summary>
public sealed class SchemaRegistrySerializer<T> : ISerializer<T>
{
    private readonly SchemaRegistryClient _client;
    private readonly JsonSerializerOptions _json;
    private readonly ConcurrentDictionary<string, int> _ids = new();

    public SchemaRegistrySerializer(string registryUrl, SchemaFormat format = SchemaFormat.Json, JsonSerializerOptions? json = null)
    {
        _client = new SchemaRegistryClient(registryUrl);
        _json = json ?? BigPipeJson.Options;
        Format = format;
    }

    public SchemaFormat Format { get; }

    /// <summary>The generated JSON Schema for <typeparamref name="T"/>.</summary>
    public string Schema => JsonSchemaExporter.GetJsonSchemaAsNode(_json, typeof(T)).ToJsonString();

    public byte[]? Serialize(T value, SerializationContext context)
    {
        if (value is null) return null;
        var subject = $"{context.Topic}-{(context.IsKey ? "key" : "value")}";
        var id = _ids.GetOrAdd(subject, s => _client.RegisterAsync(s, Schema).GetAwaiter().GetResult());
        var payload = JsonSerializer.SerializeToUtf8Bytes(value, _json);
        var buf = new byte[5 + payload.Length];
        BinaryPrimitives.WriteInt32BigEndian(buf.AsSpan(1), id);
        payload.CopyTo(buf, 5);
        return buf;
    }
}

/// <summary>Reads Confluent wire-format JSON values (and plain JSON without the header).</summary>
public sealed class SchemaRegistryDeserializer<T>(string registryUrl, JsonSerializerOptions? json = null) : IDeserializer<T>
{
    private readonly SchemaRegistryClient _client = new(registryUrl);
    private readonly JsonSerializerOptions _json = json ?? BigPipeJson.Options;

    /// <summary>Schema id of the last value read (0 for plain JSON).</summary>
    public int LastSchemaId { get; private set; }

    public T Deserialize(ReadOnlyMemory<byte>? data, SerializationContext context)
    {
        if (data is not { Length: > 0 } d) return default!;
        var span = d.Span;
        if (span.Length > 5 && span[0] == 0)
        {
            LastSchemaId = BinaryPrimitives.ReadInt32BigEndian(span[1..]);
            _ = _client; // the schema can be fetched with GetSchemaAsync when evolution handling is needed
            span = span[5..];
        }
        return JsonSerializer.Deserialize<T>(span, _json)!;
    }
}
