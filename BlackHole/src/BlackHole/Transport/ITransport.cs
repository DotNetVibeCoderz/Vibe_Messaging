// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using BlackHole.Diagnostics;
using BlackHole.Protocol;

namespace BlackHole.Transport;

/// <summary>
/// Handles one received message. The returned task must complete before the transport reclaims the
/// buffer behind <see cref="BlackHoleMessage.Payload"/>.
/// </summary>
public delegate ValueTask MessageDispatch(ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken);

/// <summary>
/// A duplex message channel with one peer. The two directions are independent: the receive loop is
/// owned by the transport, sends are serialised behind a single writer lock.
/// </summary>
/// <remarks>
/// <para>
/// v2 exposed a multicast <c>OnMessageReceived</c> event, which meant every pattern object attached
/// to a connection saw every message and none of them could apply backpressure. v3 has exactly one
/// <see cref="Dispatcher"/>; fan-out to several handlers is the job of
/// <see cref="BlackHole.Hosting.MessageRouter"/>, which can await each of them.
/// </para>
/// </remarks>
public interface ITransport : IAsyncDisposable
{
    /// <summary>Stable id for this connection, useful in logs.</summary>
    string Id { get; }

    /// <summary>False once the peer went away or <see cref="ITransport"/> was disposed.</summary>
    bool IsConnected { get; }

    /// <summary>Remote endpoint as text, or "(disconnected)".</summary>
    string RemoteEndPoint { get; }

    /// <summary>
    /// Where received messages go. Set it before starting the transport; a null dispatcher drops
    /// everything except the keepalive traffic the transport answers itself.
    /// </summary>
    MessageDispatch? Dispatcher { get; set; }

    /// <summary>Live counters for this connection.</summary>
    TransportStatistics Statistics { get; }

    /// <summary>Writes a message and flushes it to the socket.</summary>
    ValueTask SendAsync(BlackHoleMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes a message without flushing. Use it to coalesce a burst into one socket write, then
    /// call <see cref="FlushAsync"/> once. Nothing reaches the peer until you do.
    /// </summary>
    ValueTask WriteAsync(BlackHoleMessage message, CancellationToken cancellationToken = default);

    /// <summary>Pushes everything buffered by <see cref="WriteAsync"/> to the socket.</summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>Raised once, when the connection ends. The exception is null on a clean close.</summary>
    event Action<ITransport, Exception?>? Closed;
}
