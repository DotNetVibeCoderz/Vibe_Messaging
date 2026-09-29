using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using BigPipe.Client;
using BigPipe.Client.Admin;

namespace BigPipe.Analytics;

/// <summary>
/// A record as seen by analytics: text key/value plus lazily parsed JSON, with helpers for
/// dotted-path field access (<c>record.Num("payment.amount")</c>).
/// </summary>
public sealed class AnalyticsRecord
{
    private JsonNode? _json;
    private bool _parsed;

    public string Topic { get; init; } = "";
    public int Partition { get; init; }
    public long Offset { get; init; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
    public string? Key { get; init; }
    public string? Value { get; init; }
    public IReadOnlyDictionary<string, string?> Headers { get; init; } = new Dictionary<string, string?>();

    /// <summary>The value parsed as JSON (null when it is not JSON).</summary>
    public JsonNode? Json
    {
        get
        {
            if (_parsed) return _json;
            _parsed = true;
            if (string.IsNullOrWhiteSpace(Value)) return null;
            try
            {
                _json = JsonNode.Parse(Value);
            }
            catch (JsonException)
            {
                _json = null;
            }
            return _json;
        }
    }

    /// <summary>Field at a dotted path (<c>a.b.0.c</c>), or null.</summary>
    public JsonNode? Field(string path)
    {
        var cur = Json;
        foreach (var part in path.Split('.', StringSplitOptions.RemoveEmptyEntries))
        {
            cur = cur switch
            {
                JsonObject o => o[part],
                JsonArray a when int.TryParse(part, out var i) && i >= 0 && i < a.Count => a[i],
                _ => null,
            };
            if (cur is null) return null;
        }
        return cur;
    }

    /// <summary>Numeric field (numbers or numeric strings); NaN when missing.</summary>
    public double Num(string path)
    {
        var n = Field(path);
        if (n is JsonValue v)
        {
            if (v.TryGetValue<double>(out var d)) return d;
            if (v.TryGetValue<decimal>(out var m)) return (double)m;
            if (v.TryGetValue<long>(out var l)) return l;
            if (v.TryGetValue<string>(out var s) && double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var p)) return p;
        }
        return double.NaN;
    }

    /// <summary>String field; null when missing.</summary>
    public string? Str(string path)
    {
        var n = Field(path);
        return n switch
        {
            null => null,
            JsonValue v when v.TryGetValue<string>(out var s) => s,
            _ => n.ToJsonString(),
        };
    }

    public bool Bool(string path) => Field(path) is JsonValue v && v.TryGetValue<bool>(out var b) && b;

    public string? Header(string name) => Headers.TryGetValue(name, out var v) ? v : null;

    /// <summary>Deserializes the value.</summary>
    public T? As<T>(JsonSerializerOptions? options = null) =>
        Value is null ? default : JsonSerializer.Deserialize<T>(Value, options ?? BigPipeJson.Options);

    /// <summary>Converts JSON into plain .NET objects (Dictionary / List / double / string / bool) for scripting engines.</summary>
    public static object? ToPlain(JsonNode? node) => node switch
    {
        null => null,
        JsonObject o => o.ToDictionary(kv => kv.Key, kv => ToPlain(kv.Value)),
        JsonArray a => a.Select(ToPlain).ToList(),
        JsonValue v when v.TryGetValue<bool>(out var b) => b,
        JsonValue v when v.TryGetValue<double>(out var d) => d,
        JsonValue v when v.TryGetValue<string>(out var s) => s,
        _ => node.ToJsonString(),
    };

    public override string ToString() => $"{Topic}-{Partition}@{Offset} {Key ?? "-"} {Value}";

    public static AnalyticsRecord From<TKey, TValue>(ConsumeResult<TKey, TValue> r) => new()
    {
        Topic = r.Topic,
        Partition = r.Partition,
        Offset = r.Offset,
        Timestamp = r.Timestamp,
        Key = r.Key?.ToString(),
        Value = r.Value switch
        {
            null => null,
            string s => s,
            byte[] b => Encoding.UTF8.GetString(b),
            var o => JsonSerializer.Serialize(o, BigPipeJson.Options),
        },
        Headers = r.Headers.GroupBy(h => h.Key).ToDictionary(g => g.Key, g => g.Last().GetString()),
    };

    public static AnalyticsRecord From(HttpRecord r) => new()
    {
        Topic = r.Topic,
        Partition = r.Partition,
        Offset = r.Offset,
        Timestamp = r.Time,
        Key = r.Key,
        Value = r.ValueEncoding == "base64" && r.ValueBytes is { } vb ? Convert.ToBase64String(vb) : r.Value,
        Headers = r.Headers,
    };

    /// <summary>Creates a record from any object (serialized to JSON) — handy for tests and notebooks.</summary>
    public static AnalyticsRecord Of(object value, string? key = null, DateTimeOffset? timestamp = null, string topic = "local") => new()
    {
        Topic = topic,
        Key = key,
        Value = value as string ?? JsonSerializer.Serialize(value, BigPipeJson.Options),
        Timestamp = timestamp ?? DateTimeOffset.UtcNow,
    };
}
