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
    private readonly TcpTransport _transport;

    private BlackHoleClient(TcpTransport transport, Action<BlackHoleClient>? configure)
    {
        _transport = transport;
        Router = new MessageRouter();

        Rpc = new RpcClient(transport).AttachTo(Router);
        Handlers = new RpcServer().AttachTo(Router);
        PubSub = new PubSubClient(transport).AttachTo(Router);
        OutgoingStreams = new StreamSender(transport);
        IncomingStreams = new StreamReceiver().AttachTo(Router);
        Batch = new BatchSender(transport).Start();
        Batches = new BatchReceiver(Router.Dispatch, transport.HeaderCache).AttachTo(Router);

        Router.HandlerFaulted += (message, ex) => HandlerFaulted?.Invoke(message, ex);
        transport.Closed += (_, failure) => Closed?.Invoke(failure);
        transport.Dispatcher = Router.Dispatch;

        // The caller's own handlers go on before the first frame is delivered. Attaching them after
        // ConnectAsync returns would be a race: a server that pushes on accept can beat the
        // subscription, and the message would be routed to an event with no listeners.
        configure?.Invoke(this);

        // Everything is wired, so it is now safe to let messages in. The transport was handed over
        // unstarted precisely so nothing could arrive before this point.
        transport.Start();
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
