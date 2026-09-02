// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.Net.Sockets;

namespace BlackHole.Transport;

/// <summary>
/// Accepts connections on a Unix domain socket.
/// </summary>
/// <remarks>
/// The socket is a file, which brings one wrinkle a TCP port does not have: bind fails if the path
/// already exists, and a crashed process leaves it behind. This host deletes a stale path on
/// <see cref="Start"/> and removes its own on dispose, so a restart does not need manual cleanup.
/// </remarks>
public sealed class UnixSocketListenerHost : IListenerHost
{
    private readonly Socket _listener;
    private readonly TransportOptions _options;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ConcurrentDictionary<string, ITransport> _connections = new();
    private readonly string _path;
    private Task? _acceptLoop;

    /// <param name="path">Filesystem path for the socket.</param>
    /// <param name="options">Transport settings applied to every accepted connection.</param>
    public UnixSocketListenerHost(string path, TransportOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);

        _path = Path.GetFullPath(path);
        _options = options ?? new TransportOptions();
        _listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
    }

    /// <inheritdoc />
    public string Endpoint => $"unix:{_path}";

    /// <summary>The socket path this host is bound to.</summary>
    public string SocketPath => _path;

    /// <inheritdoc />
    public int ConnectionCount => _connections.Count;

    /// <inheritdoc />
    public int MaxConnections { get; set; } = 10_000;

    /// <inheritdoc />
    public event Action<ITransport>? TransportConnected;

    /// <inheritdoc />
    public event Action<ITransport, Exception?>? TransportDisconnected;

    /// <inheritdoc />
    public void Start(int backlog = 512)
    {
        // A previous run that did not shut down cleanly leaves the socket file behind, and bind
        // refuses to overwrite it.
        RemoveStalePath();

        string? directory = Path.GetDirectoryName(_path);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        _listener.Bind(new UnixDomainSocketEndPoint(_path));
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
                socket.Dispose();
                continue;
            }

            // Created unstarted so the handler below can install a dispatcher before the first
            // frame is delivered.
            StreamTransport transport = UnixSocketTransport.ForConnectedSocket(
                socket, _path, _options, startReceiving: false);

            _connections[transport.Id] = transport;
            transport.Closed += OnTransportClosed;

            try
            {
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
        if (_connections.TryRemove(transport.Id, out ITransport? removed))
            TransportDisconnected?.Invoke(removed, failure);
    }

    private void RemoveStalePath()
    {
        try
        {
            if (File.Exists(_path))
                File.Delete(_path);
        }
        catch (Exception ex)
        {
            _options.ErrorHandler?.Invoke(ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try { await _shutdown.CancelAsync().ConfigureAwait(false); } catch (ObjectDisposedException) { }
        _listener.Dispose();

        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); }
            catch (Exception) { }
        }

        foreach (ITransport transport in _connections.Values)
            await transport.DisposeAsync().ConfigureAwait(false);
        _connections.Clear();

        RemoveStalePath();
        _shutdown.Dispose();
    }
}
