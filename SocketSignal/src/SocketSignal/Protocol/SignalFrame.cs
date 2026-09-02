// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Text.Json;

namespace SocketSignal.Protocol;

/// <summary>
/// A decoded view over one received frame. Every member is a slice of the receive buffer, so
/// parsing a frame allocates nothing at all - but the frame is only valid until the connection
/// reads the next message. Anything that outlives the dispatch call must be copied.
/// </summary>
public readonly ref struct SignalFrame
{
    /// <summary>The <c>type</c> field.</summary>
    public MessageType Type { get; init; }

    /// <summary>The raw <c>id</c> string, unquoted. Empty when the frame carries no id.</summary>
    public ReadOnlySpan<byte> Id { get; init; }

    /// <summary>The raw <c>method</c> string, unquoted. Empty on non-invoke frames.</summary>
    public ReadOnlySpan<byte> Method { get; init; }

    /// <summary>The <c>args</c> array as raw JSON, brackets included. Empty when absent.</summary>
    public ReadOnlySpan<byte> Args { get; init; }

    /// <summary>The <c>result</c> value as raw JSON. Empty when absent.</summary>
    public ReadOnlySpan<byte> Result { get; init; }

    /// <summary>The <c>error</c> string, unquoted. Empty when the call succeeded.</summary>
    public ReadOnlySpan<byte> Error { get; init; }

    /// <summary>The <c>expectReturn</c> flag.</summary>
    public bool ExpectReturn { get; init; }

    /// <summary>True when <see cref="Error"/> is present.</summary>
    public bool HasError => !Error.IsEmpty;

    /// <summary>
    /// Decodes one UTF-8 JSON frame. Returns false for anything that is not a JSON object;
    /// unknown properties are skipped so the protocol can grow without breaking old peers.
    /// </summary>
    public static bool TryParse(ReadOnlySpan<byte> utf8Json, out SignalFrame frame)
    {
        frame = default;
        var reader = new Utf8JsonReader(utf8Json, isFinalBlock: true, state: default);

        try
        {
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                return false;

            MessageType type = MessageType.Unknown;
            ReadOnlySpan<byte> id = default, method = default, args = default, result = default, error = default;
            bool expectReturn = false;

            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                if (reader.ValueTextEquals(ProtocolNames.Type))
                {
                    reader.Read();
                    type = ParseType(reader.ValueSpan);
                }
                else if (reader.ValueTextEquals(ProtocolNames.Id))
                {
                    reader.Read();
                    // Ids are strings on the wire, but a hand-rolled client may send a number.
                    id = Raw(utf8Json, ref reader);
                }
                else if (reader.ValueTextEquals(ProtocolNames.Method))
                {
                    reader.Read();
                    method = reader.TokenType == JsonTokenType.String ? reader.ValueSpan : default;
                }
                else if (reader.ValueTextEquals(ProtocolNames.Args))
                {
                    reader.Read();
                    args = Slice(utf8Json, ref reader);
                }
                else if (reader.ValueTextEquals(ProtocolNames.ExpectReturn))
                {
                    reader.Read();
                    expectReturn = reader.TokenType == JsonTokenType.True;
                }
                else if (reader.ValueTextEquals(ProtocolNames.Result))
                {
                    reader.Read();
                    result = Slice(utf8Json, ref reader);
                }
                else if (reader.ValueTextEquals(ProtocolNames.Error))
                {
                    reader.Read();
                    error = reader.TokenType == JsonTokenType.String ? reader.ValueSpan : default;
                }
                else
                {
                    reader.Read();
                    reader.Skip();
                }
            }

            frame = new SignalFrame
            {
                Type = type,
                Id = id,
                Method = method,
                Args = args,
                Result = result,
                Error = error,
                ExpectReturn = expectReturn,
            };
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>An id token as raw bytes: the contents for a string, the digits for a number.</summary>
    private static ReadOnlySpan<byte> Raw(ReadOnlySpan<byte> json, scoped ref Utf8JsonReader reader) =>
        reader.TokenType switch
        {
            JsonTokenType.String => reader.ValueSpan,
            JsonTokenType.Number => Slice(json, ref reader),
            _ => default,
        };

    /// <summary>The exact JSON text of the token the reader is sitting on, children included.</summary>
    private static ReadOnlySpan<byte> Slice(ReadOnlySpan<byte> json, scoped ref Utf8JsonReader reader)
    {
        if (reader.TokenType is JsonTokenType.None or JsonTokenType.Null)
            return default;

        int start = (int)reader.TokenStartIndex;
        reader.Skip();
        return json[start..(int)reader.BytesConsumed];
    }

    private static MessageType ParseType(ReadOnlySpan<byte> value)
    {
        // Ordered by how often each type crosses the wire.
        if (value.SequenceEqual(ProtocolNames.Invoke)) return MessageType.Invoke;
        if (value.SequenceEqual(ProtocolNames.Result)) return MessageType.Result;
        if (value.SequenceEqual(ProtocolNames.Ping)) return MessageType.Ping;
        if (value.SequenceEqual(ProtocolNames.Pong)) return MessageType.Pong;
        if (value.SequenceEqual(ProtocolNames.Welcome)) return MessageType.Welcome;
        return MessageType.Unknown;
    }
}

/// <summary>UTF-8 literals for every name in the protocol, so the codec never touches a string.</summary>
internal static class ProtocolNames
{
    public static ReadOnlySpan<byte> Type => "type"u8;
    public static ReadOnlySpan<byte> Id => "id"u8;
    public static ReadOnlySpan<byte> Method => "method"u8;
    public static ReadOnlySpan<byte> Args => "args"u8;
    public static ReadOnlySpan<byte> ExpectReturn => "expectReturn"u8;
    public static ReadOnlySpan<byte> Result => "result"u8;
    public static ReadOnlySpan<byte> Error => "error"u8;
    public static ReadOnlySpan<byte> Protocol => "protocol"u8;
    public static ReadOnlySpan<byte> Server => "server"u8;

    public static ReadOnlySpan<byte> Welcome => "welcome"u8;
    public static ReadOnlySpan<byte> Invoke => "invoke"u8;
    public static ReadOnlySpan<byte> Ping => "ping"u8;
    public static ReadOnlySpan<byte> Pong => "pong"u8;
}
