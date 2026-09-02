// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
namespace BlackHole.Transport;

/// <summary>
/// Accepts inbound connections, whatever they arrive over.
/// </summary>
/// <remarks>
/// This is the seam that lets <see cref="Hosting.BlackHoleServer"/> serve TCP, Unix domain sockets,
/// named pipes and shared memory without knowing which it has. A listener's only job is to produce
/// started-but-unwired transports and say when they go away; everything above it is identical.
/// </remarks>
public interface IListenerHost : IAsyncDisposable
{
    /// <summary>Where this listener accepts, as text: a port, a socket path, a pipe or segment name.</summary>
    string Endpoint { get; }

    /// <summary>Connections currently open.</summary>
    int ConnectionCount { get; }

    /// <summary>Refuse new connections past this count.</summary>
    int MaxConnections { get; set; }

    /// <summary>Starts accepting. Returns as soon as the accept loop is running.</summary>
    /// <param name="backlog">Pending-connection queue depth, where the transport has one.</param>
    void Start(int backlog = 512);

    /// <summary>
    /// Raised for each new connection, before its first message is dispatched. Install the
    /// dispatcher here: the transport is not yet reading.
    /// </summary>
    event Action<ITransport>? TransportConnected;

    /// <summary>Raised after a connection ends, with the failure if there was one.</summary>
    event Action<ITransport, Exception?>? TransportDisconnected;
}
