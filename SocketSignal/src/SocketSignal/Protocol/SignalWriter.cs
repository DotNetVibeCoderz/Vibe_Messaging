// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;
using System.Text.Json;

namespace SocketSignal.Protocol;

/// <summary>
/// Encodes frames straight into a caller-owned buffer.
/// </summary>
/// <remarks>
/// Every method here writes through a <see cref="Utf8JsonWriter"/> into an
/// <see cref="IBufferWriter{T}"/> the connection already owns. There is no intermediate
/// <see cref="string"/> and no per-frame <c>byte[]</c>: the writer instance is reset and reused,
/// so a warmed-up connection encodes frames without allocating.
/// </remarks>
internal static class SignalWriter
{
    /// <summary>The protocol revision announced in the welcome frame.</summary>
    public const int ProtocolVersion = 2;

    public static void WriteWelcome(Utf8JsonWriter w, string clientId, string serverName)
    {
        w.WriteStartObject();
        w.WriteString(ProtocolNames.Type, ProtocolNames.Welcome);
        w.WriteString(ProtocolNames.Id, clientId);
        w.WriteNumber(ProtocolNames.Protocol, ProtocolVersion);
        w.WriteString(ProtocolNames.Server, serverName);
        w.WriteEndObject();
        w.Flush();
    }

    /// <summary>An invoke carrying a positional argument list.</summary>
    public static void WriteInvoke(
        Utf8JsonWriter w, long callId, bool expectReturn, string method,
        object?[] args, JsonSerializerOptions options)
    {
        w.WriteStartObject();
        w.WriteString(ProtocolNames.Type, ProtocolNames.Invoke);
        WriteId(w, callId);
        w.WriteString(ProtocolNames.Method, method);
        w.WritePropertyName(ProtocolNames.Args);
        w.WriteStartArray();
        for (int i = 0; i < args.Length; i++)
            JsonSerializer.Serialize(w, args[i], options);
        w.WriteEndArray();
        if (expectReturn)
            w.WriteBoolean(ProtocolNames.ExpectReturn, true);
        w.WriteEndObject();
        w.Flush();
    }

    /// <summary>
    /// An invoke with a single strongly typed argument - the allocation-free call path. Named apart
    /// from <see cref="WriteInvoke"/> on purpose: as an overload it wins against an object?[] and
    /// quietly nests the whole argument list inside one argument.
    /// </summary>
    public static void WriteInvokeSingle<T>(
        Utf8JsonWriter w, long callId, bool expectReturn, string method,
        T arg, JsonSerializerOptions options)
    {
        w.WriteStartObject();
        w.WriteString(ProtocolNames.Type, ProtocolNames.Invoke);
        WriteId(w, callId);
        w.WriteString(ProtocolNames.Method, method);
        w.WritePropertyName(ProtocolNames.Args);
        w.WriteStartArray();
        JsonSerializer.Serialize(w, arg, options);
        w.WriteEndArray();
        if (expectReturn)
            w.WriteBoolean(ProtocolNames.ExpectReturn, true);
        w.WriteEndObject();
        w.Flush();
    }

    /// <summary>A successful reply. <paramref name="id"/> is echoed verbatim from the request.</summary>
    public static void WriteResult(Utf8JsonWriter w, ReadOnlySpan<byte> id, object? value, JsonSerializerOptions options)
    {
        w.WriteStartObject();
        w.WriteString(ProtocolNames.Type, ProtocolNames.Result);
        w.WriteString(ProtocolNames.Id, id);
        w.WritePropertyName(ProtocolNames.Result);
        JsonSerializer.Serialize(w, value, options);
        w.WriteEndObject();
        w.Flush();
    }

    /// <summary>A failed reply. The message is the only thing that crosses the wire - no stack traces.</summary>
    public static void WriteError(Utf8JsonWriter w, ReadOnlySpan<byte> id, string message)
    {
        w.WriteStartObject();
        w.WriteString(ProtocolNames.Type, ProtocolNames.Result);
        w.WriteString(ProtocolNames.Id, id);
        w.WriteString(ProtocolNames.Error, message);
        w.WriteEndObject();
        w.Flush();
    }

    public static void WritePing(Utf8JsonWriter w, long id)
    {
        w.WriteStartObject();
        w.WriteString(ProtocolNames.Type, ProtocolNames.Ping);
        WriteId(w, id);
        w.WriteEndObject();
        w.Flush();
    }

    public static void WritePong(Utf8JsonWriter w, ReadOnlySpan<byte> id)
    {
        w.WriteStartObject();
        w.WriteString(ProtocolNames.Type, ProtocolNames.Pong);
        w.WriteString(ProtocolNames.Id, id);
        w.WriteEndObject();
        w.Flush();
    }

    /// <summary>
    /// Writes the correlation id as a JSON string without formatting a <see cref="string"/> first:
    /// ids are monotonic longs, so the digits go straight into a stack buffer.
    /// </summary>
    private static void WriteId(Utf8JsonWriter w, long id)
    {
        Span<byte> digits = stackalloc byte[20];
        System.Buffers.Text.Utf8Formatter.TryFormat(id, digits, out int written);
        w.WriteString(ProtocolNames.Id, digits[..written]);
    }
}
