// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
namespace SocketSignal;

/// <summary>Base class for every failure the library raises deliberately.</summary>
public class SocketSignalException : Exception
{
    public SocketSignalException(string message) : base(message) { }
    public SocketSignalException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// The remote peer ran the method and it threw. <see cref="Exception.Message"/> is the message the
/// remote side chose to publish - stack traces never cross the wire.
/// </summary>
public sealed class SignalInvocationException(string method, string remoteMessage)
    : SocketSignalException($"Remote method '{method}' failed: {remoteMessage}")
{
    /// <summary>The method that failed.</summary>
    public string Method { get; } = method;

    /// <summary>The raw message from the remote side, without this exception's framing.</summary>
    public string RemoteMessage { get; } = remoteMessage;
}

/// <summary>The peer has no handler registered under that name.</summary>
public sealed class MethodNotFoundException(string method)
    : SocketSignalException($"Method '{method}' is not registered on the remote peer.")
{
    public string Method { get; } = method;
}

/// <summary>
/// The connection went away with calls still in flight. Every pending call fails with this rather
/// than hanging - the v1 behaviour where a dropped reply parked the caller forever.
/// </summary>
public sealed class SignalConnectionClosedException(string reason)
    : SocketSignalException($"The connection closed before the call completed: {reason}");

/// <summary>The reply did not arrive inside <see cref="SocketSignalOptions.CallTimeout"/>.</summary>
public sealed class SignalTimeoutException(string method, TimeSpan timeout)
    : SocketSignalException($"Remote method '{method}' did not answer within {timeout.TotalSeconds:0.###}s.")
{
    public string Method { get; } = method;
    public TimeSpan Timeout { get; } = timeout;
}
