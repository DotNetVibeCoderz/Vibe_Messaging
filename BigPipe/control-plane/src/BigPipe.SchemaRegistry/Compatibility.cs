using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace BigPipe.SchemaRegistry;

public enum SchemaFormat
{
    Avro,
    Json,
    Protobuf,
}

/// <summary>
/// Schema validation and compatibility rules for Avro, JSON Schema and Protobuf.
/// <c>CanRead(reader, writer)</c> answers: can consumers using <c>reader</c> decode data written with <c>writer</c>?
/// </summary>
public static partial class Compatibility
{
    public static SchemaFormat ParseFormat(string? schemaType) => (schemaType ?? "AVRO").ToUpperInvariant() switch
    {
        "AVRO" => SchemaFormat.Avro,
        "JSON" => SchemaFormat.Json,
        "PROTOBUF" => SchemaFormat.Protobuf,
        var t => throw new RegistryException(42201, $"unsupported schema type '{t}'"),
    };

    /// <summary>Validates and returns a canonical form used for identity (whitespace-insensitive for JSON formats).</summary>
    public static string Canonicalize(SchemaFormat format, string schema)
    {
        switch (format)
        {
            case SchemaFormat.Avro:
            case SchemaFormat.Json:
                try
                {
                    var node = JsonNode.Parse(schema) ?? throw new RegistryException(42201, "schema is empty");
                    if (format == SchemaFormat.Avro) ValidateAvro(node);
                    return node.ToJsonString();
                }
                catch (System.Text.Json.JsonException e)
                {
                    throw new RegistryException(42201, $"invalid schema: {e.Message}");
                }
            case SchemaFormat.Protobuf:
                if (!MessageRegex().IsMatch(schema)) throw new RegistryException(42201, "invalid Protobuf schema: no message definition found");
                return string.Join('\n', schema.Split('\n').Select(l => l.Trim()).Where(l => l.Length > 0));
            default:
                throw new ArgumentOutOfRangeException(nameof(format));
        }
    }

    private static void ValidateAvro(JsonNode node)
    {
        switch (node)
        {
            case JsonValue v when v.TryGetValue<string>(out _):
                return;
            case JsonArray union:
                foreach (var branch in union) ValidateAvro(branch!);
                return;
            case JsonObject o:
                var type = o["type"]?.ToString() ?? throw new RegistryException(42201, "invalid Avro schema: missing \"type\"");
                if (type == "record" || type == "error")
                {
                    if (o["name"] is null) throw new RegistryException(42201, "invalid Avro schema: record needs a name");
                    if (o["fields"] is not JsonArray fields) throw new RegistryException(42201, "invalid Avro schema: record needs fields");
                    foreach (var f in fields)
                    {
                        if (f?["name"] is null || f["type"] is null) throw new RegistryException(42201, "invalid Avro schema: every field needs name and type");
                        ValidateAvro(f["type"]!);
                    }
                }
                else if (type == "enum" && o["symbols"] is not JsonArray)
                    throw new RegistryException(42201, "invalid Avro schema: enum needs symbols");
                else if (type == "array") ValidateAvro(o["items"] ?? throw new RegistryException(42201, "invalid Avro schema: array needs items"));
                else if (type == "map") ValidateAvro(o["values"] ?? throw new RegistryException(42201, "invalid Avro schema: map needs values"));
                return;
            default:
                throw new RegistryException(42201, "invalid Avro schema");
        }
    }

    public static bool IsCompatible(SchemaFormat format, string level, string candidate, IReadOnlyList<string> previous, out List<string> messages)
    {
        messages = [];
        level = level.ToUpperInvariant();
        if (level == "NONE" || previous.Count == 0) return true;
        var against = level.EndsWith("_TRANSITIVE") ? previous : [previous[^1]];
        var backward = level.StartsWith("BACKWARD") || level.StartsWith("FULL");
        var forward = level.StartsWith("FORWARD") || level.StartsWith("FULL");
        foreach (var old in against)
        {
            if (backward) messages.AddRange(CanRead(format, candidate, old).Select(m => "backward: " + m));
            if (forward) messages.AddRange(CanRead(format, old, candidate).Select(m => "forward: " + m));
        }
        return messages.Count == 0;
    }

    /// <summary>Problems preventing <paramref name="reader"/> from reading data written with <paramref name="writer"/>.</summary>
    public static List<string> CanRead(SchemaFormat format, string reader, string writer)
    {
        var problems = new List<string>();
        switch (format)
        {
            case SchemaFormat.Avro:
                AvroCanRead(JsonNode.Parse(reader)!, JsonNode.Parse(writer)!, "", problems);
                break;
            case SchemaFormat.Json:
                JsonCanRead(JsonNode.Parse(reader)!, JsonNode.Parse(writer)!, "", problems);
                break;
            case SchemaFormat.Protobuf:
                ProtoCanRead(reader, writer, problems);
                break;
        }
        return problems;
    }

    // ------------------------------------------------------------------------------ Avro ------

    private static string AvroType(JsonNode n) => n switch
    {
        JsonValue v => v.ToString(),
        JsonArray => "union",
        JsonObject o => o["type"]?.ToString() ?? "unknown",
        _ => "unknown",
    };

    private static readonly Dictionary<string, string[]> Promotions = new()
    {
        ["int"] = ["long", "float", "double"],
        ["long"] = ["float", "double"],
        ["float"] = ["double"],
        ["string"] = ["bytes"],
        ["bytes"] = ["string"],
    };

    private static void AvroCanRead(JsonNode reader, JsonNode writer, string path, List<string> problems)
    {
        var rt = AvroType(reader);
        var wt = AvroType(writer);
        if (rt == "union")
        {
            var branches = ((JsonArray)reader).Select(b => b!).ToList();
            if (wt == "union")
            {
                foreach (var w in (JsonArray)writer)
                    if (!branches.Any(b => { var p = new List<string>(); AvroCanRead(b, w!, path, p); return p.Count == 0; }))
                        problems.Add($"{Loc(path)} union has no branch for writer type {AvroType(w!)}");
                return;
            }
            if (!branches.Any(b => { var p = new List<string>(); AvroCanRead(b, writer, path, p); return p.Count == 0; }))
                problems.Add($"{Loc(path)} union has no branch for writer type {wt}");
            return;
        }
        if (wt == "union")
        {
            foreach (var w in (JsonArray)writer) AvroCanRead(reader, w!, path, problems);
            return;
        }
        if (rt != wt && !(Promotions.TryGetValue(wt, out var to) && to.Contains(rt)))
        {
            problems.Add($"{Loc(path)} type changed from {wt} to {rt}");
            return;
        }
        switch (rt)
        {
            case "record":
            {
                var wf = ((JsonArray)writer["fields"]!).ToDictionary(f => f!["name"]!.ToString(), f => f!);
                foreach (var f in (JsonArray)reader["fields"]!)
                {
                    var name = f!["name"]!.ToString();
                    if (wf.TryGetValue(name, out var w)) AvroCanRead(f["type"]!, w["type"]!, $"{path}.{name}", problems);
                    else if (f["default"] is null && !(f as JsonObject)!.ContainsKey("default"))
                        problems.Add($"{Loc(path + "." + name)} added without a default");
                }
                break;
            }
            case "enum":
            {
                var rs = ((JsonArray)reader["symbols"]!).Select(s => s!.ToString()).ToHashSet();
                var missing = ((JsonArray)writer["symbols"]!).Select(s => s!.ToString()).Where(s => !rs.Contains(s)).ToList();
                if (missing.Count > 0 && reader["default"] is null) problems.Add($"{Loc(path)} enum lost symbols {string.Join(", ", missing)}");
                break;
            }
            case "array":
                AvroCanRead(reader["items"]!, writer["items"]!, path + "[]", problems);
                break;
            case "map":
                AvroCanRead(reader["values"]!, writer["values"]!, path + "{}", problems);
                break;
        }
    }

    // ------------------------------------------------------------------------------ JSON ------

    private static void JsonCanRead(JsonNode reader, JsonNode writer, string path, List<string> problems)
    {
        var rt = reader["type"]?.ToString();
        var wt = writer["type"]?.ToString();
        if (rt is not null && wt is not null && rt != wt && !(rt == "number" && wt == "integer"))
        {
            problems.Add($"{Loc(path)} type changed from {wt} to {rt}");
            return;
        }
        if (rt != "object" && reader["properties"] is null) return;
        var rp = reader["properties"] as JsonObject ?? [];
        var wp = writer["properties"] as JsonObject ?? [];
        var wRequired = (writer["required"] as JsonArray)?.Select(x => x!.ToString()).ToHashSet() ?? [];
        foreach (var req in (reader["required"] as JsonArray)?.Select(x => x!.ToString()) ?? [])
            if (!wRequired.Contains(req)) problems.Add($"{Loc(path + "." + req)} is required by the reader but optional or absent for the writer");
        foreach (var (name, schema) in rp)
            if (wp[name] is { } w) JsonCanRead(schema!, w, $"{path}.{name}", problems);
        var closed = reader["additionalProperties"] is JsonValue ap && ap.TryGetValue<bool>(out var open) && !open;
        if (closed)
            foreach (var (name, _) in wp)
                if (!rp.ContainsKey(name)) problems.Add($"{Loc(path + "." + name)} is written but the reader forbids additional properties");
    }

    // ------------------------------------------------------------------------------ Protobuf --

    [GeneratedRegex(@"\bmessage\s+\w+\s*\{")]
    private static partial Regex MessageRegex();

    [GeneratedRegex(@"(?:(?:optional|required|repeated)\s+)?([\w.]+(?:\s*<[^>]+>)?)\s+(\w+)\s*=\s*(\d+)")]
    private static partial Regex FieldRegex();

    private static Dictionary<int, (string Type, string Name)> ProtoFields(string schema)
    {
        var fields = new Dictionary<int, (string, string)>();
        foreach (Match m in FieldRegex().Matches(schema))
        {
            var type = m.Groups[1].Value;
            if (type is "syntax" or "package" or "option") continue;
            fields[int.Parse(m.Groups[3].Value)] = (type, m.Groups[2].Value);
        }
        return fields;
    }

    private static void ProtoCanRead(string reader, string writer, List<string> problems)
    {
        var r = ProtoFields(reader);
        foreach (var (num, (type, name)) in ProtoFields(writer))
            if (r.TryGetValue(num, out var rf) && rf.Type != type)
                problems.Add($"field {num} ({name}) changed type from {type} to {rf.Type}");
    }

    private static string Loc(string path) => path.Length == 0 ? "root" : $"field '{path.TrimStart('.')}'";
}

public sealed class RegistryException(int errorCode, string message) : Exception(message)
{
    public int ErrorCode { get; } = errorCode;
    /// <summary>HTTP status: Confluent codes are the status followed by two digits (40401 → 404).</summary>
    public int Status => ErrorCode is >= 400 and < 600 ? ErrorCode : ErrorCode / 100;
}
