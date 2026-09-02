// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using BlackHole.Diagnostics;
using BlackHole.Patterns;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Hosting;

/// <summary>
/// A connected client with every pattern already wired: call RPC, serve RPC, publish, subscribe,
/// stream, and batch over one socket.
/// </summary>
/// <remarks>
/// <see cref="Handlers"/> makes the connection genuinely bidirectional - the server can call methods
/// on the client over the same socket, which is how the IoT gateway pushes a command to a device
/// that sits behind NAT.
/// </remarks>
public sealed class BlackHoleClient : IAsyncDisposable
{
    private readonly ITransport _transport;

    private BlackHoleClient(ITransport transport, Action<BlackHoleClient>? configure)
    {
        _transport = transport;
        Router = new MessageRouter();

        Rpc = new RpcClient(transport).AttachTo(Router);
        Handlers = new RpcServer().AttachTo(Router);
        PubSub = new PubSubClient(transport).AttachTo(Router);
        OutgoingStreams = new StreamSender(transport);
        IncomingStreams = new StreamReceiver().AttachTo(Router);
        Batch = new BatchSender(transport).Start();
        Batches = new BatchReceiver(Router.Dispatch, HeaderCacheOf(transport)).AttachTo(Router);

        Router.HandlerFaulted += (message, ex) => HandlerFaulted?.Invoke(message, ex);
        transport.Closed += (_, failure) => Closed?.Invoke(failure);
        transport.Dispatcher = Router.Dispatch;

        // The caller's own handlers go on before the first frame is delivered. Attaching them after
        // ConnectAsync returns would be a race: a server that pushes on accept can beat the
        // subscription, and the message would be routed to an event with no listeners.
        configure?.Invoke(this);

        // Everything is wired, so it is now safe to let messages in. The transport was handed over
        // unstarted precisely so nothing could arrive before this point.
        Start(transport);
    }

    /// <summary>The header cache a transport exposes, or null to let the receiver make its own.</summary>
    private static Protocol.HeaderCache? HeaderCacheOf(ITransport transport) => transport switch
    {
        StreamTransport stream => stream.HeaderCache,
        TcpTransport tcp => tcp.HeaderCache,
        _ => null,
    };

    /// <summary>Starts a transport that was handed over unstarted.</summary>
    private static void Start(ITransport transport)
    {
        switch (transport)
        {
            case StreamTransport stream: stream.Start(); break;
            case TcpTransport tcp: tcp.Start(); break;
            // A transport from somewhere else is assumed to be reading already; there is no
            // interface member to call, and starting twice would be the only alternative.
        }
    }

    /// <summary>
    /// Wraps a transport this client did not create - a Unix socket, a named pipe, shared memory,
    /// or anything else implementing <see cref="ITransport"/>.
    /// </summary>
    /// <remarks>
    /// Hand it over unstarted (every built-in factory takes <c>startReceiving: false</c>) so
    /// <paramref name="configure"/> can register handlers before the first frame is delivered. This
    /// method starts it.
    /// </remarks>
    /// <param name="transport">A connected transport, ideally not yet receiving.</param>
    /// <param name="configure">Runs before any message is delivered.</param>
    public static BlackHoleClient Over(ITransport transport, Action<BlackHoleClient>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(transport);
        return new BlackHoleClient(transport, configure);
    }

    /// <summary>Connects over a Unix domain socket.</summary>
    /// <param name="path">Filesystem path of the socket.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <param name="configure">Runs before any message is delivered.</param>
    public static async Task<BlackHoleClient> ConnectUnixAsync(
        string path,
        TransportOptions? options = null,
        CancellationToken cancellationToken = default,
        Action<BlackHoleClient>? configure = null)
    {
        StreamTransport transport = await UnixSocketTransport
            .ConnectAsync(path, options, cancellationToken, startReceiving: false)
            .ConfigureAwait(false);
        return new BlackHoleClient(transport, configure);
    }

    /// <summary>Connects over a named pipe.</summary>
    /// <param name="pipeName">Pipe name, without the machine-local pipe prefix.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="timeout">How long to wait for the pipe to exist. Default 10 seconds.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <param name="configure">Runs before any message is delivered.</param>
    public static async Task<BlackHoleClient> ConnectPipeAsync(
        string pipeName,
        TransportOptions? options = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default,
        Action<BlackHoleClient>? configure = null)
    {
        StreamTransport transport = await NamedPipeTransport
            .ConnectAsync(pipeName, options, timeout, cancellationToken, startReceiving: false)
            .ConfigureAwait(false);
        return new BlackHoleClient(transport, configure);
    }

    /// <summary>Connects over shared memory, claiming a free slot from a listener's pool.</summary>
    /// <param name="name">The listener's base segment name.</param>
    /// <param name="slots">Pool size the listener was created with. Default 8.</param>
    /// <param name="timeout">How long to keep retrying the pool. Default 10 seconds.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="shared">Ring capacity and waiting strategy.</param>
    /// <param name="cancellationToken">Cancels the attempt.</param>
    /// <param name="configure">Runs before any message is delivered.</param>
    public static async Task<BlackHoleClient> ConnectSharedMemoryAsync(
        string name,
        int slots = 8,
        TimeSpan? timeout = null,
        TransportOptions? options = null,
        SharedMemoryOptions? shared = null,
        CancellationToken cancellationToken = default,
        Action<BlackHoleClient>? configure = null)
    {
        StreamTransport transport = await SharedMemoryTransport
            .ConnectAsync(name, slots, timeout, options, shared, cancellationToken, startReceiving: false)
            .ConfigureAwait(false);
        return new BlackHoleClient(transport, configure);
    }

    /// <summary>Connects and returns a ready client.</summary>
    /// <param name="host">Host name or address.</param>
    /// <param name="port">Port to dial.</param>
    /// <param name="options">Transport settings; defaults are used when omitted.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <param name="configure">
    /// Runs once the client is built but before any message is delivered. Subscribe to
    /// <see cref="PubSubClient.Received"/>, register <see cref="Handlers"/>, or add router entries
    /// here when the peer may send something the instant it accepts - handlers attached after this
    /// method returns can miss that first message.
    /// </param>
    public static async Task<BlackHoleClient> ConnectAsync(
        string host,
        int port,
        TransportOptions? options = null,
        CancellationToken cancellationToken = default,
        Action<BlackHoleClient>? configure = null)
    {
        TcpTransport transport = await TcpTransport
            .ConnectAsync(host, port, options, cancellationToken, startReceiving: false)
            .ConfigureAwait(false);
        return new BlackHoleClient(transport, configure);
    }

    /// <summary>Connects with exponential backoff, for a client that may start before its server.</summary>
    /// <param name="host">Host name or address.</param>
    /// <param name="port">Port to dial.</param>
    /// <param name="attempts">How many times to try before giving up.</param>
    /// <param name="initialDelay">Delay before the second attempt; doubles up to 5 seconds.</param>
    /// <param name="options">Transport settings; defaults are used when omitted.</param>
    /// <param name="cancellationToken">Cancels the connect attempts.</param>
    /// <param name="configure">Runs before any message is delivered. See <see cref="ConnectAsync"/>.</param>
    public static async Task<BlackHoleClient> ConnectWithRetryAsync(
        string host,
        int port,
        int attempts = 5,
        TimeSpan? initialDelay = null,
        TransportOptions? options = null,
        CancellationToken cancellationToken = default,
        Action<BlackHoleClient>? configure = null)
    {
        TcpTransport transport = await TcpTransport
            .ConnectWithRetryAsync(host, port, attempts, initialDelay, options, cancellationToken, startReceiving: false)
            .ConfigureAwait(false);
        return new BlackHoleClient(transport, configure);
    }

    /// <summary>The underlying connection.</summary>
    public ITransport Transport => _transport;

    /// <summary>Routing table for inbound messages. Add your own handlers here.</summary>
    public MessageRouter Router { get; }

    /// <summary>Calls methods on the server.</summary>
    public RpcClient Rpc { get; }

    /// <summary>Methods the server may call on this client.</summary>
    public RpcServer Handlers { get; }

    /// <summary>Publish and subscribe.</summary>
    public PubSubClient PubSub { get; }

    /// <summary>Sends large bodies in chunks.</summary>
    public StreamSender OutgoingStreams { get; }

    /// <summary>Reassembles large bodies pushed from the server.</summary>
    public StreamReceiver IncomingStreams { get; }

    /// <summary>Coalesces small messages into envelopes.</summary>
    public BatchSender Batch { get; }

    /// <summary>Unpacks inbound envelopes back into <see cref="Router"/>.</summary>
    public BatchReceiver Batches { get; }

    /// <summary>Counters for this connection.</summary>
    public TransportStatistics Statistics => _transport.Statistics;

    /// <summary>Which transport this client is running over: "tcp", "uds", "pipe" or "shm".</summary>
    public string TransportKind => _transport switch
    {
        StreamTransport stream => stream.Kind,
        TcpTransport => "tcp",
        _ => "custom",
    };

    /// <summary>False once the connection ends.</summary>
    public bool IsConnected => _transport.IsConnected;

    /// <summary>Raised when the connection ends; the argument is null on a clean close.</summary>
    public event Action<Exception?>? Closed;

    /// <summary>Raised when any handler throws.</summary>
    public event Action<BlackHoleMessage, Exception>? HandlerFaulted;

    /// <summary>Sends a raw message.</summary>
    public ValueTask SendAsync(BlackHoleMessage message, CancellationToken cancellationToken = default) =>
        _transport.SendAsync(message, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        await Batch.DisposeAsync().ConfigureAwait(false);
        Rpc.Dispose();
        IncomingStreams.Dispose();
        await _transport.DisposeAsync().ConfigureAwait(false);
    }
}
