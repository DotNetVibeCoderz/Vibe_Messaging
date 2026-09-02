// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
namespace SocketSignal.Protocol;

/// <summary>The <c>type</c> discriminator carried by every frame.</summary>
public enum MessageType : byte
{
    /// <summary>Unrecognised or absent <c>type</c>. Frames of this kind are dropped.</summary>
    Unknown = 0,

    /// <summary>Server to client, once, on connect. Carries the assigned client id and protocol version.</summary>
    Welcome,

    /// <summary>Either direction. A method call; <c>expectReturn</c> decides whether a <see cref="Result"/> is owed.</summary>
    Invoke,

    /// <summary>Either direction. The reply to an <see cref="Invoke"/>, carrying <c>result</c> or <c>error</c>.</summary>
    Result,

    /// <summary>Keepalive probe. The receiver answers with <see cref="Pong"/> and nothing else.</summary>
    Ping,

    /// <summary>The answer to a <see cref="Ping"/>, echoing its id.</summary>
    Pong,
}
