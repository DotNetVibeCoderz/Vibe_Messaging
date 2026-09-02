// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using BlackHole.Patterns;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Hosting;

/// <summary>
/// One accepted connection with its own router and per-connection pattern objects.
/// </summary>
public sealed class BlackHoleConnection
{
    internal BlackHoleConnection(ITransport transport, MessageRouter router, StreamReceiver streams, BatchReceiver batches)
    {
        Transport = transport;
        Router = router;
        Streams = streams;
        Batches = batches;
    }

    /// <summary>The connection itself.</summary>
    public ITransport Transport { get; }

    /// <summary>Where its messages are routed. Add handlers here for connection-specific behaviour.</summary>
    public MessageRouter Router { get; }

    /// <summary>Inbound stream reassembly for this connection only, so ids cannot collide across clients.</summary>
    public StreamReceiver Streams { get; }

    /// <summary>Unpacks batch envelopes back into <see cref="Router"/>.</summary>
    public BatchReceiver Batches { get; }

    /// <summary>Anything the application wants to hang off the connection - a device id, a session.</summary>
    public object? State { get; set; }

    /// <summary>Sends one message to this client.</summary>
    public ValueTask SendAsync(BlackHoleMessage message, CancellationToken cancellationToken = default) =>
        Transport.SendAsync(message, cancellationToken);
}

/// <summary>
/// A ready-made server: listener, router, RPC dispatch, topic broker, streaming and batching, wired
/// together per connection.
/// </summary>
/// <remarks>
/// v2's demo had to hand-wire all of this inside its accept handler and got the lifetimes wrong -
/// subscribers were never removed when a client vanished, and every pattern object was shared
/// across connections. Here RPC methods and topic subscriptions are server-wide (they should be),
/// while stream reassembly and batch unpacking are per connection (they must be).
/// </remarks>
public sealed class BlackHoleServer : IAsyncDisposable
{
    private readonly IListenerHost _listener;
    private readonly TransportOptions _options;
    private readonly bool _ownsListener;
    private readonly ConcurrentDictionary<string, BlackHoleConnection> _connections = new();

    /// <summary>Listens on every TCP interface. Use an endpoint overload to restrict that.</summary>
    public BlackHoleServer(int port, TransportOptions? options = null)
        : this(new System.Net.IPEndPoint(System.Net.IPAddress.Any, port), options) { }

    /// <summary>
    /// Listens on one TCP endpoint. Pass <see cref="System.Net.IPAddress.Loopback"/> for a server
    /// that only ever talks to this machine - it keeps the port off the network and, on Windows,
    /// avoids the firewall prompt that binding to Any triggers.
    /// </summary>
    public BlackHoleServer(System.Net.IPEndPoint endPoint, TransportOptions? options = null)
        : this(new TcpListenerHost(endPoint, options ?? new TransportOptions()), options, ownsListener: true) { }

    /// <summary>
    /// Serves over any listener: TCP, a Unix domain socket, a named pipe, or shared memory.
    /// </summary>
    /// <remarks>
    /// Everything above the transport is identical whichever you pick - the same RPC methods, the
    /// same broker, the same streams - because a listener's only job is to produce connections.
    /// Choosing a same-machine transport is a deployment decision, not an application one.
    /// </remarks>
    /// <param name="listener">The listener to serve. Disposed with this server unless you say otherwise.</param>
    /// <param name="options">Transport settings, for the parts of the server that need them.</param>
    /// <param name="ownsListener">False to keep the listener alive after this server is disposed.</param>
    public BlackHoleServer(IListenerHost listener, TransportOptions? options = null, bool ownsListener = true)
    {
        ArgumentNullException.ThrowIfNull(listener);

        _options = options ?? new TransportOptions();
        _listener = listener;
        _ownsListener = ownsListener;
        _listener.TransportConnected += OnClientConnected;
        _listener.TransportDisconnected += OnClientDisconnected;
    }

    /// <summary>Methods every client can call.</summary>
    public RpcServer Rpc { get; } = new();

    /// <summary>Topic broker shared by every client.</summary>
    public PubSubBroker PubSub { get; } = new();

    /// <summary>
    /// Where the listener is bound, as text: a TCP endpoint, a socket path, a pipe or segment name.
    /// </summary>
    public string Endpoint => _listener.Endpoint;

    /// <summary>
    /// Where the TCP listener is bound; read it after construction to resolve port 0.
    /// </summary>
    /// <exception cref="InvalidOperationException">This server is not serving TCP.</exception>
    public System.Net.IPEndPoint EndPoint => _listener is TcpListenerHost tcp
        ? tcp.EndPoint
        : throw new InvalidOperationException(
            $"This server listens on {_listener.Endpoint}, which has no IP endpoint. Use Endpoint instead.");

    /// <summary>Live connections.</summary>
    public IReadOnlyCollection<BlackHoleConnection> Connections => _connections.Values.ToArray();

    /// <summary>Live connection count.</summary>
    public int ConnectionCount => _connections.Count;

    /// <summary>Refuse new connections past this count.</summary>
    public int MaxConnections { get => _listener.MaxConnections; set => _listener.MaxConnections = value; }

    /// <summary>Raised on accept, before the first message is dispatched. Hook stream events here.</summary>
    public event Action<BlackHoleConnection>? ClientConnected;

    /// <summary>Raised after a client goes away, with the failure if there was one.</summary>
    public event Action<BlackHoleConnection, Exception?>? ClientDisconnected;

    /// <summary>Raised when a handler throws anywhere in the pipeline.</summary>
    public event Action<BlackHoleMessage, Exception>? HandlerFaulted;

    /// <summary>Starts accepting.</summary>
    public BlackHoleServer Start(int backlog = 512)
    {
        _listener.Start(backlog);
        return this;
    }

    /// <summary>Publishes to every subscriber of a topic, as if a client had published it.</summary>
    public ValueTask PublishAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default) =>
        PubSub.PublishAsync(topic, payload, publisher: null, cancellationToken);

    private void OnClientConnected(ITransport transport)
    {
        var router = new MessageRouter();
        var streams = new StreamReceiver();
        var batches = new BatchReceiver(router.Dispatch, HeaderCacheOf(transport));

        router.HandlerFaulted += (message, ex) => HandlerFaulted?.Invoke(message, ex);
        Rpc.AttachTo(router);
        PubSub.AttachTo(router);
        streams.AttachTo(router);
        batches.AttachTo(router);

        var connection = new BlackHoleConnection(transport, router, streams, batches);
        _connections[transport.Id] = connection;

        // Set the dispatcher last: until now the transport had nowhere to deliver, and the receive
        // loop is already running.
        transport.Dispatcher = router.Dispatch;

        ClientConnected?.Invoke(connection);
    }

    private void OnClientDisconnected(ITransport transport, Exception? failure)
    {
        PubSub.RemoveSubscriber(transport);
        if (_connections.TryRemove(transport.Id, out BlackHoleConnection? connection))
        {
            connection.Streams.Dispose();
            ClientDisconnected?.Invoke(connection, failure);
        }
    }

    /// <summary>The header cache a transport exposes, or null to let the receiver make its own.</summary>
    private static Protocol.HeaderCache? HeaderCacheOf(ITransport transport) => transport switch
    {
        StreamTransport stream => stream.HeaderCache,
        TcpTransport tcp => tcp.HeaderCache,
        _ => null,
    };

    public async ValueTask DisposeAsync()
    {
        if (_ownsListener)
            await _listener.DisposeAsync().ConfigureAwait(false);
        foreach (BlackHoleConnection connection in _connections.Values)
            connection.Streams.Dispose();
        _connections.Clear();
    }
}
