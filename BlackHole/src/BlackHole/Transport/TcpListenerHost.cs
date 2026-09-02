// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace BlackHole.Transport;

/// <summary>
/// Accepts TCP connections and hands each one back as a started <see cref="TcpTransport"/>.
/// </summary>
/// <remarks>
/// The accept loop does nothing but accept: wiring a connection to patterns happens in the
/// <see cref="ClientConnected"/> handler, which runs synchronously so the caller can install a
/// dispatcher before the first frame is delivered. Connections are tracked so shutdown can close
/// them, and <see cref="MaxConnections"/> gives the host a hard ceiling instead of accepting until
/// the process runs out of handles.
/// </remarks>
public sealed class TcpListenerHost : IListenerHost
{
    private readonly Socket _listener;
    private readonly TransportOptions _options;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ConcurrentDictionary<string, TcpTransport> _connections = new();
    private Task? _acceptLoop;

    public TcpListenerHost(int port, TransportOptions? options = null)
        : this(new IPEndPoint(IPAddress.Any, port), options) { }

    public TcpListenerHost(IPEndPoint endPoint, TransportOptions? options = null)
    {
        _options = options ?? new TransportOptions();
        _listener = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _listener.Bind(endPoint);
        EndPoint = (IPEndPoint)_listener.LocalEndPoint!;
    }

    /// <summary>Where the listener is bound. Read it after construction to resolve port 0.</summary>
    public IPEndPoint EndPoint { get; }

    /// <summary>Refuse new connections past this count. Default 10,000.</summary>
    public int MaxConnections { get; set; } = 10_000;

    /// <summary>Live connections.</summary>
    public int ConnectionCount => _connections.Count;

    /// <summary>Snapshot of the live connections.</summary>
    public IReadOnlyCollection<TcpTransport> Connections => _connections.Values.ToArray();

    /// <summary>
    /// Raised on the accept loop for each new connection, before its first message is dispatched.
    /// Install the dispatcher here.
    /// </summary>
    public event Action<TcpTransport>? ClientConnected;

    /// <summary>Raised after a connection ends, with the failure if there was one.</summary>
    public event Action<TcpTransport, Exception?>? ClientDisconnected;

    /// <inheritdoc />
    public event Action<ITransport>? TransportConnected;

    /// <inheritdoc />
    public event Action<ITransport, Exception?>? TransportDisconnected;

    /// <inheritdoc />
    /// <remarks>The bound endpoint as text; read it after Start to resolve port 0.</remarks>
    public string Endpoint => EndPoint.ToString();

    /// <summary>Starts listening. Returns as soon as the accept loop is running.</summary>
    public void Start(int backlog = 512)
    {
        _listener.Listen(backlog);
        _acceptLoop = Task.Run(() => AcceptLoopAsync(_shutdown.Token));
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            Socket socket;
            try
            {
                socket = await _listener.AcceptAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (SocketException ex)
            {
                _options.ErrorHandler?.Invoke(ex);
                continue;
            }

            if (_connections.Count >= MaxConnections)
            {
                // Better an immediate refusal the client can retry than a silent queue.
                socket.Dispose();
                continue;
            }

            // Created without its receive loop: the handler below installs the dispatcher, and only
            // then does the transport begin delivering. Starting first would let a client that
            // sends immediately on connect - a subscribe, say - have that message dropped.
            TcpTransport transport = TcpTransport.ForAcceptedSocket(socket, _options, startReceiving: false);
            _connections[transport.Id] = transport;
            transport.Closed += OnTransportClosed;

            try
            {
                ClientConnected?.Invoke(transport);
                TransportConnected?.Invoke(transport);
                transport.Start();
            }
            catch (Exception ex)
            {
                _options.ErrorHandler?.Invoke(ex);
                await transport.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private void OnTransportClosed(ITransport transport, Exception? failure)
    {
        if (!_connections.TryRemove(transport.Id, out TcpTransport? removed))
            return;

        ClientDisconnected?.Invoke(removed, failure);
        TransportDisconnected?.Invoke(removed, failure);
    }

    /// <summary>Broadcasts one message to every live connection.</summary>
    public async ValueTask BroadcastAsync(Protocol.BlackHoleMessage message, CancellationToken cancellationToken = default)
    {
        foreach (TcpTransport transport in _connections.Values)
        {
            if (!transport.IsConnected) continue;
            try
            {
                await transport.SendAsync(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _options.ErrorHandler?.Invoke(ex);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        try { _shutdown.Cancel(); } catch (ObjectDisposedException) { }
        _listener.Dispose();

        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); }
            catch (Exception) { }
        }

        foreach (TcpTransport transport in _connections.Values)
            await transport.DisposeAsync().ConfigureAwait(false);

        _connections.Clear();
        _shutdown.Dispose();
    }
}
