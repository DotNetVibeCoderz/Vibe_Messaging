// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.Text;
using BlackHole.Hosting;
using BlackHole.Patterns;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.IoTGateway.Simulation;

/// <summary>One line in the activity log.</summary>
public readonly record struct GatewayEvent(DateTimeOffset At, GatewayEventKind Kind, string Source, string Detail);

/// <summary>What kind of thing happened, which decides the colour of the marker in the log.</summary>
public enum GatewayEventKind
{
    Connection,
    Telemetry,
    Command,
    Stream,
    Alarm,
    Fault,
}

/// <summary>
/// The gateway side: a real <see cref="BlackHoleServer"/> that accepts device connections, receives
/// telemetry on wildcard topics, answers device RPC, and takes firmware uploads as streams.
/// </summary>
/// <remarks>
/// The counters here are deliberately cheap - interlocked longs read by the UI on a timer - because
/// the gateway must not slow down to be watched. Nothing on the receive path allocates per reading.
/// </remarks>
public sealed class GatewayHost : IAsyncDisposable
{
    private readonly ConcurrentQueue<GatewayEvent> _events = new();
    private readonly ConcurrentDictionary<string, DeviceTelemetry> _telemetry = new(StringComparer.Ordinal);
    private BlackHoleServer? _server;
    private long _readingsReceived;
    private long _commandsHandled;
    private long _streamsReceived;
    private long _streamBytes;
    private long _alarms;

    /// <summary>Live figures for one device as the gateway sees them.</summary>
    public sealed class DeviceTelemetry
    {
        public required string Topic { get; init; }
        public double LastValue;
        public long Count;
        public long LastSeenTicks;
    }

    /// <summary>Port the gateway listens on. 0 lets the OS choose; read <see cref="Port"/> after starting.</summary>
    public int RequestedPort { get; set; } = 5100;

    /// <summary>Port actually bound.</summary>
    public int Port { get; private set; }

    /// <summary>True once the listener is accepting.</summary>
    public bool IsRunning => _server is not null;

    /// <summary>Devices connected right now.</summary>
    public int ConnectionCount => _server?.ConnectionCount ?? 0;

    /// <summary>Topic subscriptions the broker is tracking.</summary>
    public int TopicCount => _server?.PubSub.TopicCount ?? 0;

    /// <summary>Readings received since start.</summary>
    public long ReadingsReceived => Interlocked.Read(ref _readingsReceived);

    /// <summary>RPC calls the gateway answered.</summary>
    public long CommandsHandled => Interlocked.Read(ref _commandsHandled);

    /// <summary>Firmware streams completed.</summary>
    public long StreamsReceived => Interlocked.Read(ref _streamsReceived);

    /// <summary>Bytes received through streams.</summary>
    public long StreamBytes => Interlocked.Read(ref _streamBytes);

    /// <summary>Readings that arrived above their sensor's alarm threshold.</summary>
    public long Alarms => Interlocked.Read(ref _alarms);

    /// <summary>Bytes received across every connection.</summary>
    public long BytesReceived
    {
        get
        {
            long total = 0;
            foreach (BlackHoleConnection connection in _server?.Connections ?? [])
                total += connection.Transport.Statistics.BytesReceived;
            return total;
        }
    }

    /// <summary>Messages received across every connection, telemetry and control alike.</summary>
    public long MessagesReceived
    {
        get
        {
            long total = 0;
            foreach (BlackHoleConnection connection in _server?.Connections ?? [])
                total += connection.Transport.Statistics.MessagesReceived;
            return total;
        }
    }

    /// <summary>Raised for every reading, on the receive loop, so the chart can sample it.</summary>
    public event Action<string, Reading>? ReadingReceived;

    /// <summary>Starts listening.</summary>
    public void Start()
    {
        if (_server is not null)
            return;

        var options = new TransportOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(20),
            // Hundreds of devices publish from the same small topic vocabulary, so one shared cache
            // across every connection is both smaller and warmer than one cache each.
            SharedHeaderCache = new HeaderCache(2048),
            ErrorHandler = ex => Log(GatewayEventKind.Fault, "transport", ex.Message),
        };

        // Loopback only: every device in this simulator runs in this process, so there is no
        // reason to expose the port to the network - or to make Windows ask about the firewall.
        _server = new BlackHoleServer(
            new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, RequestedPort), options);

        _server.Rpc
            .RegisterText("gateway/ping", _ =>
            {
                Interlocked.Increment(ref _commandsHandled);
                return "pong";
            })
            .RegisterText("gateway/time", _ =>
            {
                Interlocked.Increment(ref _commandsHandled);
                return DateTimeOffset.UtcNow.ToString("O");
            })
            .RegisterText("gateway/register", identity =>
            {
                Interlocked.Increment(ref _commandsHandled);
                Log(GatewayEventKind.Connection, identity.Split('|').FirstOrDefault() ?? "device", "registered");
                return "accepted";
            });

        _server.ClientConnected += OnClientConnected;
        _server.ClientDisconnected += (connection, failure) => Log(
            failure is null ? GatewayEventKind.Connection : GatewayEventKind.Fault,
            connection.Transport.Id,
            failure is null ? "disconnected" : $"dropped: {failure.Message}");

        _server.HandlerFaulted += (message, ex) =>
            Log(GatewayEventKind.Fault, message.Header, $"{ex.GetType().Name}: {ex.Message}");

        _server.Start();
        Port = _server.EndPoint.Port;
        Log(GatewayEventKind.Connection, "gateway", $"listening on port {Port}");
    }

    private void OnClientConnected(BlackHoleConnection connection)
    {
        // Telemetry is read straight off this connection's router. Subscribing the connection to
        // "plant/#" would look tempting, but it would make the broker fan every reading back out to
        // all the devices - each one then blocked writing to peers that are themselves blocked
        // writing, which stalls the whole floor. Devices publish up; commands go down over RPC or
        // the control topic. Traffic only ever flows one way per path.
        connection.Router.On(MessageType.Publish, (_, message) =>
        {
            if (!Reading.TryParse(message.Payload.Span, out Reading reading))
                return;

            Interlocked.Increment(ref _readingsReceived);

            DeviceTelemetry telemetry = _telemetry.GetOrAdd(message.Header,
                static topic => new DeviceTelemetry { Topic = topic });
            Volatile.Write(ref telemetry.LastValue, reading.Value);
            Interlocked.Increment(ref telemetry.Count);
            Volatile.Write(ref telemetry.LastSeenTicks, Environment.TickCount64);

            ReadingReceived?.Invoke(message.Header, reading);
        });

        connection.Streams.Started += (id, descriptor) =>
            Log(GatewayEventKind.Stream, id, $"upload started, {descriptor.TotalLength / 1024:N0} KiB");

        connection.Streams.Completed += (_, e) =>
        {
            Interlocked.Increment(ref _streamsReceived);
            Interlocked.Add(ref _streamBytes, e.Length);
            Log(GatewayEventKind.Stream, e.StreamId, $"upload complete, {e.Length / 1024:N0} KiB received");
        };

        connection.Streams.Aborted += (id, reason) => Log(GatewayEventKind.Fault, id, $"upload aborted: {reason}");

        Log(GatewayEventKind.Connection, connection.Transport.Id, $"device connected from {connection.Transport.RemoteEndPoint}");
    }

    /// <summary>
    /// Calls a method on one connected device. This is the direction that makes the gateway useful:
    /// the device dialled out, and the gateway still commands it over the same socket.
    /// </summary>
    public async Task<string> SendCommandAsync(
        string method, string argument, CancellationToken cancellationToken = default)
    {
        if (_server is null || _server.ConnectionCount == 0)
            return "No devices are connected.";

        BlackHoleConnection connection = _server.Connections.First();
        var caller = new RpcClient(connection.Transport) { DefaultTimeout = TimeSpan.FromSeconds(5) };
        connection.Router.On(MessageType.RpcResponse, caller.HandleAsync);

        try
        {
            string reply = await caller.CallTextAsync(method, argument, cancellationToken: cancellationToken);
            Interlocked.Increment(ref _commandsHandled);
            Log(GatewayEventKind.Command, method, reply);
            return reply;
        }
        catch (RpcException ex)
        {
            Log(GatewayEventKind.Fault, method, ex.Message);
            return ex.Message;
        }
        finally
        {
            caller.Dispose();
            connection.Router.Clear(MessageType.RpcResponse);
        }
    }

    /// <summary>Pushes a broadcast to every device subscribed to the control topic.</summary>
    public async Task BroadcastAsync(string topic, string payload, CancellationToken cancellationToken = default)
    {
        if (_server is null)
            return;
        await _server.PublishAsync(topic, Encoding.UTF8.GetBytes(payload), cancellationToken);
        Log(GatewayEventKind.Command, topic, $"broadcast: {payload}");
    }

    /// <summary>Records an alarm, so the panel can show how many fired without recounting readings.</summary>
    public void RecordAlarm(string deviceId, string detail)
    {
        Interlocked.Increment(ref _alarms);
        Log(GatewayEventKind.Alarm, deviceId, detail);
    }

    /// <summary>Adds a line to the activity log, trimming the oldest.</summary>
    public void Log(GatewayEventKind kind, string source, string detail)
    {
        _events.Enqueue(new GatewayEvent(DateTimeOffset.Now, kind, source, detail));
        while (_events.Count > 500 && _events.TryDequeue(out _)) { }
    }

    /// <summary>Drains everything logged since the last call. The UI polls this on its repaint timer.</summary>
    public List<GatewayEvent> DrainEvents()
    {
        var drained = new List<GatewayEvent>();
        while (_events.TryDequeue(out GatewayEvent item))
            drained.Add(item);
        return drained;
    }

    /// <summary>Latest value the gateway saw on a topic, or null if it has seen none.</summary>
    public double? LastValueFor(string topic) =>
        _telemetry.TryGetValue(topic, out DeviceTelemetry? telemetry) ? Volatile.Read(ref telemetry.LastValue) : null;

    /// <summary>Stops accepting and closes every connection.</summary>
    public async Task StopAsync()
    {
        if (_server is null)
            return;

        BlackHoleServer server = _server;
        _server = null;
        await server.DisposeAsync();
        Log(GatewayEventKind.Connection, "gateway", "stopped");
    }

    public async ValueTask DisposeAsync() => await StopAsync();
}
