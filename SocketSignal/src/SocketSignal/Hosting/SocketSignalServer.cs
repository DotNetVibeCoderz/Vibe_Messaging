// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text.Json;
using SocketSignal.Diagnostics;
using SocketSignal.Dispatch;

using SocketSignal.Hosting;

namespace SocketSignal;

/// <summary>
/// Accepts WebSocket clients and routes calls between them.
/// </summary>
/// <example>
/// <code>
/// var server = new SocketSignalServer("http://localhost:8080/ws/");
/// server.Register("sum", (ClientConnection c, int a, int b) =&gt; ValueTask.FromResult(a + b));
/// _ = server.StartAsync();
/// </code>
/// </example>
public sealed class SocketSignalServer : IAsyncDisposable
{
    private readonly HttpListener _listener = new();
    private readonly SocketSignalOptions _options;
    private readonly Utf8HandlerTable _handlers = new();

    private readonly ConcurrentDictionary<string, ClientConnection> _clients = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _groups = new(StringComparer.Ordinal);

    private readonly CancellationTokenSource _shutdown = new();
    private Task? _keepAlive;
    private int _disposed;

    /// <param name="urlPrefix">
    /// An <see cref="HttpListener"/> prefix - note it is <c>http://</c>, while clients dial the
    /// matching <c>ws://</c> address. Anything other than localhost needs a URL ACL on Windows.
    /// </param>
    /// <param name="options">Tuning. The defaults are sensible for a service on a LAN.</param>
    public SocketSignalServer(string urlPrefix, SocketSignalOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(urlPrefix);
        _options = options ?? new SocketSignalOptions();
        _options.Validate();
        _listener.Prefixes.Add(urlPrefix);
        UrlPrefix = urlPrefix;
    }

    /// <summary>The prefix this server listens on.</summary>
    public string UrlPrefix { get; }

    /// <summary>Name announced to clients in the welcome frame.</summary>
    public string Name { get; set; } = "SocketSignal";

    /// <summary>
    /// Counters for every connection this server has accepted, live ones included. Rolled up on
    /// read, so it is a snapshot rather than a live object - grab it once per report, not per frame.
    /// </summary>
    public SignalStatistics Statistics
    {
        get
        {
            var total = new SignalStatistics();
            total.Absorb(_closed);
            foreach (ClientConnection client in _clients.Values)
                total.Absorb(client.Statistics);
            return total;
        }
    }

    /// <summary>What disconnected connections contributed, kept so their traffic is not forgotten.</summary>
    private readonly SignalStatistics _closed = new();

    /// <summary>How many clients are connected right now. Cheaper than counting <see cref="Clients"/>.</summary>
    public int ClientCount => _clients.Count;

    /// <summary>A snapshot of the connected clients. Allocates - prefer <see cref="ClientCount"/> in a loop.</summary>
    public IReadOnlyCollection<ClientConnection> Clients => _clients.Values.ToArray();

    /// <summary>Every group that currently has at least one member.</summary>
    public IReadOnlyCollection<string> GroupNames => _groups.Keys.ToArray();

    /// <summary>
    /// Vets an incoming upgrade before it becomes a connection. Return false to reject with 403 -
    /// the hook to check a token, an origin, or a cookie.
    /// </summary>
    public Func<HttpListenerContext, ValueTask<bool>>? Authenticate { get; set; }

    public event Action<ClientConnection>? ClientConnected;
    public event Action<ClientConnection, string>? ClientDisconnected;

    // =========================================================================================
    // Registration
    // =========================================================================================

    /// <summary>Registers a method, taking its arguments as raw <see cref="JsonElement"/>s.</summary>
    public void Register(string method, Func<ClientConnection, JsonElement[], Task<object?>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new DynamicHandler((sender, args) => handler((ClientConnection)sender!, args)));
    }

    /// <summary>Registers a method that takes no arguments.</summary>
    public void Register<TResult>(string method, Func<ClientConnection, ValueTask<TResult>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new TypedHandler<TResult>(sender => handler((ClientConnection)sender!)));
    }

    /// <summary>Registers a method whose argument is deserialised straight into <typeparamref name="T1"/>.</summary>
    public void Register<T1, TResult>(string method, Func<ClientConnection, T1?, ValueTask<TResult>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new TypedHandler<T1, TResult>((sender, a1) => handler((ClientConnection)sender!, a1)));
    }

    /// <inheritdoc cref="Register{T1, TResult}"/>
    public void Register<T1, T2, TResult>(string method, Func<ClientConnection, T1?, T2?, ValueTask<TResult>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new TypedHandler<T1, T2, TResult>((sender, a1, a2) => handler((ClientConnection)sender!, a1, a2)));
    }

    /// <inheritdoc cref="Register{T1, TResult}"/>
    public void Register<T1, T2, T3, TResult>(string method, Func<ClientConnection, T1?, T2?, T3?, ValueTask<TResult>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new TypedHandler<T1, T2, T3, TResult>((sender, a1, a2, a3) => handler((ClientConnection)sender!, a1, a2, a3)));
    }

    /// <summary>Removes a registration. Later calls to it answer with a method-not-found error.</summary>
    public bool Unregister(string method) => _handlers.Remove(method);

    /// <summary>Every method name currently registered.</summary>
    public IReadOnlyCollection<string> Methods => _handlers.Methods;

    // =========================================================================================
    // Lifetime
    // =========================================================================================

    /// <summary>Starts listening and accepts clients until the token fires.</summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
        CancellationToken token = linked.Token;

        _listener.Start();
        _keepAlive ??= Task.Run(() => KeepAliveLoopAsync(token), CancellationToken.None);

        try
        {
            while (!token.IsCancellationRequested)
            {
                HttpListenerContext context;
                try
                {
                    context = await _listener.GetContextAsync().WaitAsync(token).ConfigureAwait(false);
                }
                catch (Exception) when (token.IsCancellationRequested)
                {
                    break;
                }

                _ = AcceptAsync(context, token);
            }
        }
        finally
        {
            if (_listener.IsListening) _listener.Stop();
        }
    }

    private async Task AcceptAsync(HttpListenerContext context, CancellationToken token)
    {
        if (!context.Request.IsWebSocketRequest)
        {
            context.Response.StatusCode = 400;
            context.Response.Close();
            return;
        }

        if (Authenticate is { } authenticate && !await authenticate(context).ConfigureAwait(false))
        {
            context.Response.StatusCode = 403;
            context.Response.Close();
            return;
        }

        WebSocketContext wsContext;
        try
        {
            // Native keepalive is off: SocketSignal pings at the protocol level instead, so every
            // SDK - browser included - sees the same liveness signal.
            wsContext = await context.AcceptWebSocketAsync(subProtocol: null, keepAliveInterval: TimeSpan.Zero)
                                     .ConfigureAwait(false);
        }
        catch (Exception)
        {
            return;
        }

        string id = Guid.NewGuid().ToString("N");
        var connection = new SignalConnection(wsContext.WebSocket, _options, _handlers, sender: null);
        var client = new ClientConnection(id, connection, this, context.Request.RemoteEndPoint);
        connection.SetSender(client);

        _clients[id] = client;
        connection.Closed += reason => Detach(client, reason);

        try
        {
            await connection.SendWelcomeAsync(id, Name, token).ConfigureAwait(false);
            ClientConnected?.Invoke(client);
            await connection.RunAsync(token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // RunAsync already reports the reason through Closed.
        }
        finally
        {
            _closed.Absorb(connection.Statistics);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void Detach(ClientConnection client, string reason)
    {
        if (!_clients.TryRemove(client.Id, out _))
            return;

        foreach (ConcurrentDictionary<string, byte> members in _groups.Values)
            members.TryRemove(client.Id, out _);

        ClientDisconnected?.Invoke(client, reason);
    }

    /// <summary>
    /// Pings connections that have gone quiet and drops the ones that stopped answering. One loop
    /// for the whole server rather than a timer per connection.
    /// </summary>
    private async Task KeepAliveLoopAsync(CancellationToken token)
    {
        if (_options.KeepAliveInterval == Timeout.InfiniteTimeSpan)
            return;

        using var timer = new PeriodicTimer(_options.KeepAliveInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                DateTime now = DateTime.UtcNow;
                foreach (ClientConnection client in _clients.Values)
                {
                    TimeSpan idle = now - client.Connection.LastActivityUtc;
                    try
                    {
                        if (idle > _options.IdleTimeout)
                            await client.CloseAsync("idle timeout").ConfigureAwait(false);
                        else if (idle >= _options.KeepAliveInterval)
                            await client.Connection.SendPingAsync(token).ConfigureAwait(false);
                    }
                    catch
                    {
                        // A connection dying mid-sweep is exactly what the sweep is for.
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
    }

    // =========================================================================================
    // Fan-out
    // =========================================================================================

    /// <summary>Calls a method on every connected client.</summary>
    public async Task BroadcastAsync(string method, params object?[] args)
    {
        int count = _clients.Count;
        if (count == 0) return;

        var pending = new List<Task>(count);
        foreach (ClientConnection client in _clients.Values)
            pending.Add(SafeSend(client, method, args));

        await Task.WhenAll(pending).ConfigureAwait(false);
    }

    /// <summary>Calls a method on one client by id. Silently does nothing if it has gone.</summary>
    public async Task SendToClientAsync(string clientId, string method, params object?[] args)
    {
        if (_clients.TryGetValue(clientId, out ClientConnection? client))
            await SafeSend(client, method, args).ConfigureAwait(false);
    }

    /// <summary>Calls a method on every member of a group.</summary>
    public async Task SendToGroupAsync(string groupName, string method, params object?[] args)
    {
        if (!_groups.TryGetValue(groupName, out ConcurrentDictionary<string, byte>? members))
            return;

        var pending = new List<Task>(members.Count);
        foreach (string clientId in members.Keys)
        {
            if (_clients.TryGetValue(clientId, out ClientConnection? client))
                pending.Add(SafeSend(client, method, args));
        }

        await Task.WhenAll(pending).ConfigureAwait(false);
    }

    /// <summary>Calls a method on one client and waits for its return value.</summary>
    public ValueTask<TResult?> CallClientAsync<TResult>(string clientId, string method, params object?[] args)
    {
        if (!_clients.TryGetValue(clientId, out ClientConnection? client))
            throw new SocketSignalException($"No client with id '{clientId}' is connected.");
        return client.CallAsync<TResult>(method, args);
    }

    /// <summary>
    /// A send that cannot bring the fan-out down with it: one dead client must not fail a broadcast
    /// to a thousand healthy ones.
    /// </summary>
    private static async Task SafeSend(ClientConnection client, string method, object?[] args)
    {
        try
        {
            await client.SendAsync(method, args).ConfigureAwait(false);
        }
        catch
        {
            // The keepalive sweep will collect it.
        }
    }

    // =========================================================================================
    // Groups
    // =========================================================================================

    /// <summary>Adds a client to a group, creating the group if needed.</summary>
    public void AddToGroup(string groupName, string clientId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupName);
        _groups.GetOrAdd(groupName, static _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal))[clientId] = 0;
    }

    /// <summary>Removes a client from a group.</summary>
    public void RemoveFromGroup(string groupName, string clientId)
    {
        if (_groups.TryGetValue(groupName, out ConcurrentDictionary<string, byte>? members))
            members.TryRemove(clientId, out _);
    }

    /// <summary>Members of a group, by client id.</summary>
    public IReadOnlyCollection<string> GroupMembers(string groupName) =>
        _groups.TryGetValue(groupName, out ConcurrentDictionary<string, byte>? members)
            ? members.Keys.ToArray()
            : [];

    /// <summary>Every group a client belongs to.</summary>
    public IReadOnlyCollection<string> GroupsOf(string clientId)
    {
        List<string>? found = null;
        foreach (KeyValuePair<string, ConcurrentDictionary<string, byte>> group in _groups)
        {
            if (group.Value.ContainsKey(clientId))
                (found ??= []).Add(group.Key);
        }
        return found ?? (IReadOnlyCollection<string>)[];
    }

    // =========================================================================================
    // Shutdown
    // =========================================================================================

    /// <summary>Stops accepting, then closes every live connection.</summary>
    public async Task StopAsync()
    {
        await _shutdown.CancelAsync().ConfigureAwait(false);

        foreach (ClientConnection client in _clients.Values)
        {
            try { await client.CloseAsync("server stopping").ConfigureAwait(false); }
            catch { /* already gone */ }
        }

        if (_listener.IsListening) _listener.Stop();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await StopAsync().ConfigureAwait(false);

        if (_keepAlive is not null)
        {
            try { await _keepAlive.ConfigureAwait(false); }
            catch { /* cancelled */ }
        }

        _shutdown.Dispose();
        _listener.Close();
    }
}
