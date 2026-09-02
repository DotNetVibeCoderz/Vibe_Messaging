// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Net.WebSockets;
using System.Text.Json;
using SocketSignal.Diagnostics;
using SocketSignal.Dispatch;

using SocketSignal.Hosting;

namespace SocketSignal;

/// <summary>
/// Connects to a <see cref="SocketSignalServer"/> and exchanges calls with it.
/// </summary>
/// <example>
/// <code>
/// var client = new SocketSignalClient();
/// client.On("serverHello", (string? text) =&gt; { Console.WriteLine(text); return ValueTask.FromResult(true); });
/// await client.ConnectAsync(new Uri("ws://localhost:8080/ws/"));
/// int total = await client.CallAsync&lt;int&gt;("sum", 5, 7);
/// </code>
/// </example>
public sealed class SocketSignalClient : IAsyncDisposable
{
    private readonly SocketSignalOptions _options;
    private readonly Utf8HandlerTable _handlers = new();
    private readonly SemaphoreSlim _connectLock = new(1, 1);

    private SignalConnection? _connection;
    private Task? _pump;
    private Uri? _serverUri;
    private CancellationTokenSource? _lifetime;
    private int _disposed;

    public SocketSignalClient(SocketSignalOptions? options = null)
    {
        _options = options ?? new SocketSignalOptions();
        _options.Validate();
    }

    /// <summary>The id the server handed out in its welcome frame. Null until connected.</summary>
    public string? ClientId { get; private set; }

    /// <summary>True while the socket is open.</summary>
    public bool IsConnected => _connection?.IsOpen == true;

    /// <summary>Counters for the current connection. Reset when a reconnect replaces the socket.</summary>
    public SignalStatistics Statistics => _connection?.Statistics ?? new SignalStatistics();

    /// <summary>
    /// Reconnect automatically when the socket drops. Off by default, because a client that silently
    /// reconnects also silently loses whatever state the server was keeping for it.
    /// </summary>
    public bool AutoReconnect { get; set; }

    /// <summary>First reconnect delay. Doubles up to <see cref="MaxReconnectDelay"/>.</summary>
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Ceiling for the reconnect backoff.</summary>
    public TimeSpan MaxReconnectDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Raised after the welcome frame arrives, with the assigned client id.</summary>
    public event Action<string>? Connected;

    /// <summary>Raised when the socket drops, with the reason.</summary>
    public event Action<string>? Disconnected;

    /// <summary>Raised before each reconnect attempt, with the attempt number.</summary>
    public event Action<int>? Reconnecting;

    // =========================================================================================
    // Registration
    // =========================================================================================

    /// <summary>Registers a method the server can call, taking raw <see cref="JsonElement"/> arguments.</summary>
    public void On(string method, Func<JsonElement[], Task<object?>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new DynamicHandler((_, args) => handler(args)));
    }

    /// <summary>Registers a method that takes no arguments.</summary>
    public void On<TResult>(string method, Func<ValueTask<TResult>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new TypedHandler<TResult>(_ => handler()));
    }

    /// <summary>Registers a method whose argument is deserialised straight into <typeparamref name="T1"/>.</summary>
    public void On<T1, TResult>(string method, Func<T1?, ValueTask<TResult>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new TypedHandler<T1, TResult>((_, a1) => handler(a1)));
    }

    /// <inheritdoc cref="On{T1, TResult}"/>
    public void On<T1, T2, TResult>(string method, Func<T1?, T2?, ValueTask<TResult>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new TypedHandler<T1, T2, TResult>((_, a1, a2) => handler(a1, a2)));
    }

    /// <inheritdoc cref="On{T1, TResult}"/>
    public void On<T1, T2, T3, TResult>(string method, Func<T1?, T2?, T3?, ValueTask<TResult>> handler)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        ArgumentNullException.ThrowIfNull(handler);
        _handlers.Set(method, new TypedHandler<T1, T2, T3, TResult>((_, a1, a2, a3) => handler(a1, a2, a3)));
    }

    /// <summary>Removes a registration.</summary>
    public bool Off(string method) => _handlers.Remove(method);

    // =========================================================================================
    // Connect
    // =========================================================================================

    /// <summary>Dials the server and waits for its welcome frame.</summary>
    public async Task ConnectAsync(Uri serverUri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(serverUri);
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        _serverUri = serverUri;
        _lifetime ??= CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await OpenAsync(_lifetime.Token).ConfigureAwait(false);
    }

    private async Task OpenAsync(CancellationToken token)
    {
        await _connectLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            var socket = new ClientWebSocket();
            socket.Options.KeepAliveInterval = TimeSpan.Zero; // protocol-level ping instead
            await socket.ConnectAsync(_serverUri!, token).ConfigureAwait(false);

            var connection = new SignalConnection(socket, _options, _handlers, sender: null);
            var welcomed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

            connection.Welcomed += id => welcomed.TrySetResult(id);
            connection.Closed += reason =>
            {
                welcomed.TrySetException(new SignalConnectionClosedException(reason));
                OnClosed(reason);
            };

            _connection = connection;
            _pump = connection.RunAsync(token);

            // The welcome is the handshake: until it lands there is no client id to report.
            ClientId = await welcomed.Task.WaitAsync(_options.CallTimeout == Timeout.InfiniteTimeSpan
                ? TimeSpan.FromSeconds(30)
                : _options.CallTimeout, token).ConfigureAwait(false);

            _ = KeepAliveLoopAsync(connection, token);
            Connected?.Invoke(ClientId);
        }
        finally
        {
            _connectLock.Release();
        }
    }

    private void OnClosed(string reason)
    {
        Disconnected?.Invoke(reason);

        if (!AutoReconnect || _disposed != 0 || _lifetime is null || _lifetime.IsCancellationRequested)
            return;

        _ = ReconnectAsync(_lifetime.Token);
    }

    private async Task ReconnectAsync(CancellationToken token)
    {
        TimeSpan delay = ReconnectDelay;
        for (int attempt = 1; !token.IsCancellationRequested && _disposed == 0; attempt++)
        {
            Reconnecting?.Invoke(attempt);
            try
            {
                await Task.Delay(delay, token).ConfigureAwait(false);
                await OpenAsync(token).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch
            {
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxReconnectDelay.Ticks));
            }
        }
    }

    /// <summary>Pings while the connection is idle so a half-open socket is noticed rather than trusted.</summary>
    private async Task KeepAliveLoopAsync(SignalConnection connection, CancellationToken token)
    {
        if (_options.KeepAliveInterval == Timeout.InfiniteTimeSpan)
            return;

        using var timer = new PeriodicTimer(_options.KeepAliveInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                if (!connection.IsOpen) return;
                if (DateTime.UtcNow - connection.LastActivityUtc < _options.KeepAliveInterval) continue;
                await connection.SendPingAsync(token).ConfigureAwait(false);
            }
        }
        catch
        {
            // Cancelled, or the socket died. Either way the pump reports it.
        }
    }

    // =========================================================================================
    // Calls
    // =========================================================================================

    /// <summary>Calls a server method and waits for its return value.</summary>
    public ValueTask<TResult?> CallAsync<TResult>(string method, params object?[] args) =>
        Require().CallAsync<TResult>(method, args);

    /// <summary>
    /// Calls a server method with exactly one argument, deserialising the reply into
    /// <typeparamref name="TResult"/>. This overload never builds an <c>object[]</c> and never
    /// boxes a value-type argument - it is the fast path for hot calls.
    /// </summary>
    public ValueTask<TResult?> CallAsync<TArg, TResult>(string method, TArg arg) =>
        Require().CallAsync<TArg, TResult>(method, arg);

    /// <summary>Calls a server method and returns its raw result. Kept for v1 compatibility.</summary>
    public ValueTask<JsonElement?> CallAsync(string method, params object?[] args) =>
        Require().CallAsync<JsonElement?>(method, args);

    /// <summary>Calls a server method without waiting for a reply.</summary>
    public ValueTask SendAsync(string method, params object?[] args) =>
        Require().NotifyAsync(method, args);

    /// <summary>Fire-and-forget with a single typed argument - no <c>object[]</c>, no boxing.</summary>
    public ValueTask SendAsync<TArg>(string method, TArg arg) =>
        Require().NotifyAsync(method, arg);

    private SignalConnection Require()
    {
        SignalConnection? connection = _connection;
        if (connection is null || !connection.IsOpen)
            throw new SignalConnectionClosedException("the client is not connected");
        return connection;
    }

    // =========================================================================================
    // Shutdown
    // =========================================================================================

    /// <summary>Closes the connection without tearing the client down - it can connect again.</summary>
    public async Task DisconnectAsync()
    {
        AutoReconnect = false;
        if (_connection is not null)
            await _connection.CloseAsync("closed by client").ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        AutoReconnect = false;
        if (_lifetime is not null)
            await _lifetime.CancelAsync().ConfigureAwait(false);

        if (_connection is not null)
            await _connection.DisposeAsync().ConfigureAwait(false);

        if (_pump is not null)
        {
            try { await _pump.ConfigureAwait(false); }
            catch { /* cancelled */ }
        }

        _lifetime?.Dispose();
        _connectLock.Dispose();
    }
}
