// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
namespace BlackHole.Protocol;

/// <summary>
/// Thrown when bytes on the wire cannot form a valid frame. Always fatal for the connection that
/// produced it: once framing is lost there is no way to resynchronise.
/// </summary>
public sealed class BlackHoleProtocolException : Exception
{
    public BlackHoleProtocolException(string message) : base(message) { }
    public BlackHoleProtocolException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Thrown on the calling side when a remote method is unknown, fails, or times out.</summary>
public sealed class RpcException : Exception
{
    public RpcException(string method, string message) : base(message) => Method = method;

    /// <summary>The method name that was called.</summary>
    public string Method { get; }
}
