// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Text;

namespace BlackHole.Protocol;

/// <summary>
/// The single unit that crosses the wire.
/// </summary>
/// <remarks>
/// <para>
/// This is a readonly struct on purpose: the hot path moves millions of these and a class would
/// mean one gen-0 allocation per message. It is 40 bytes, so pass it with <c>in</c> where the
/// signature allows.
/// </para>
/// <para>
/// <b>Payload lifetime.</b> On the receive path <see cref="Payload"/> points into a buffer owned by
/// the transport. It is valid only until the dispatch <see cref="ValueTask"/> for that message
/// completes. Handlers that keep the bytes must copy - use <see cref="ToOwned"/>.
/// </para>
/// </remarks>
public readonly struct BlackHoleMessage
{
    public BlackHoleMessage(
        MessageType type,
        string? header = null,
        ReadOnlyMemory<byte> payload = default,
        long correlationId = 0,
        MessageFlags flags = MessageFlags.None)
    {
        Type = type;
        Header = header ?? string.Empty;
        Payload = payload;
        CorrelationId = correlationId;
        Flags = flags;
    }

    /// <summary>What this message is.</summary>
    public MessageType Type { get; init; }

    /// <summary>Per-message bit flags.</summary>
    public MessageFlags Flags { get; init; }

    /// <summary>
    /// Correlates a reply with its request. Reused as the chunk index on <see cref="MessageType.StreamChunk"/>
    /// and as the inner-message count on <see cref="MessageType.Batch"/>.
    /// </summary>
    public long CorrelationId { get; init; }

    /// <summary>
    /// Routing key, UTF-8 on the wire: RPC method name, Pub/Sub topic, or stream id depending on
    /// <see cref="Type"/>. Never null.
    /// </summary>
    public string Header { get; init; }

    /// <summary>The body. See the payload-lifetime note on the type.</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>True when the peer reported a failure instead of a result.</summary>
    public bool IsError => (Flags & MessageFlags.Error) != 0;

    /// <summary>Decodes the payload as UTF-8. Allocates.</summary>
    public string PayloadAsString() => Payload.IsEmpty ? string.Empty : Encoding.UTF8.GetString(Payload.Span);

    /// <summary>
    /// Copies the payload into a private array so the message outlives the transport buffer that
    /// produced it. Cheap no-op when the payload is already empty.
    /// </summary>
    public BlackHoleMessage ToOwned() =>
        Payload.IsEmpty ? this : this with { Payload = Payload.ToArray() };

    public static BlackHoleMessage Text(MessageType type, string header, string payload) =>
        new(type, header, Encoding.UTF8.GetBytes(payload));

    public override string ToString() =>
        $"{Type}{(Flags == MessageFlags.None ? "" : $"[{Flags}]")} '{Header}' {Payload.Length}B #{CorrelationId}";
}
