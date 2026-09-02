// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
namespace BlackHole.Protocol;

/// <summary>
/// Identifies what a <see cref="BlackHoleMessage"/> means on the wire. One byte, so the
/// numeric values are part of the protocol contract and must never be reused.
/// </summary>
public enum MessageType : byte
{
    /// <summary>Reserved / uninitialised.</summary>
    None = 0x00,

    // --- Request / response -------------------------------------------------
    /// <summary>RPC call. <c>Header</c> is the method name, <c>CorrelationId</c> matches the response.</summary>
    RpcRequest = 0x01,
    /// <summary>RPC reply. Carries <see cref="MessageFlags.Error"/> when the handler failed.</summary>
    RpcResponse = 0x02,

    // --- Publish / subscribe ------------------------------------------------
    /// <summary>Message published to the topic in <c>Header</c>.</summary>
    Publish = 0x03,
    /// <summary>Request to receive the topic in <c>Header</c>.</summary>
    Subscribe = 0x04,
    /// <summary>Generic acknowledgement.</summary>
    Ack = 0x05,
    /// <summary>Stop receiving the topic in <c>Header</c>.</summary>
    Unsubscribe = 0x06,

    // --- Streaming ----------------------------------------------------------
    /// <summary>Opens a stream. <c>Header</c> is the stream id; payload holds an optional <see cref="StreamDescriptor"/>.</summary>
    StreamStart = 0x10,
    /// <summary>One chunk of an open stream. <c>CorrelationId</c> is the zero-based chunk index.</summary>
    StreamChunk = 0x11,
    /// <summary>Closes a stream successfully.</summary>
    StreamEnd = 0x12,
    /// <summary>Closes a stream with an error; payload is a UTF-8 reason.</summary>
    StreamAbort = 0x13,

    // --- Batching -----------------------------------------------------------
    /// <summary>Envelope holding several inner messages. <c>CorrelationId</c> is the inner count.</summary>
    Batch = 0x20,

    // --- Connection liveness ------------------------------------------------
    /// <summary>Keepalive probe. Answered by the transport itself, never routed.</summary>
    Ping = 0x30,
    /// <summary>Keepalive answer. Consumed by the transport itself, never routed.</summary>
    Pong = 0x31,
}

/// <summary>Per-message bit flags. Occupies one byte on the wire.</summary>
[Flags]
public enum MessageFlags : byte
{
    None = 0,
    /// <summary>The payload is an error description rather than a result.</summary>
    Error = 1 << 0,
    /// <summary>Reserved: payload is compressed. Not produced by this version.</summary>
    Compressed = 1 << 1,
    /// <summary>Sender does not expect (and will discard) a reply.</summary>
    NoReply = 1 << 2,
}
