// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.IO.Pipes;

namespace BlackHole.Transport;

/// <summary>
/// Accepts connections on a named pipe.
/// </summary>
/// <remarks>
/// A named pipe server instance serves exactly one client, so "listening" means keeping a fresh
/// unconnected instance waiting at all times: when one is claimed, another is created behind it.
/// That is the shape of the Windows API, and <see cref="MaxServerInstances"/> is the hard ceiling
/// the OS enforces on how many can exist at once.
/// </remarks>
public sealed class NamedPipeListenerHost : IListenerHost
{
    private readonly string _pipeName;
    private readonly TransportOptions _options;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ConcurrentDictionary<string, ITransport> _connections = new();
    private Task? _acceptLoop;

    /// <param name="pipeName">Pipe name, without the <c>\\.\pipe\</c> prefix.</param>
    /// <param name="options">Transport settings applied to every accepted connection.</param>
    public NamedPipeListenerHost(string pipeName, TransportOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipeName);
        _pipeName = pipeName;
        _options = options ?? new TransportOptions();
    }

    /// <inheritdoc />
    public string Endpoint => $"pipe:{_pipeName}";

    /// <summary>The pipe name this host serves.</summary>
    public string PipeName => _pipeName;

    /// <inheritdoc />
    public int ConnectionCount => _connections.Count;

    /// <inheritdoc />
    public int MaxConnections { get; set; } = 254;

    /// <summary>
    /// Server instances the OS will allow. Defaults to
    /// <see cref="NamedPipeServerStream.MaxAllowedServerInstances"/>, which is 255 on Windows.
    /// </summary>
    public int MaxServerInstances { get; set; } = NamedPipeServerStream.MaxAllowedServerInstances;

    /// <inheritdoc />
    public event Action<ITransport>? TransportConnected;

    /// <inheritdoc />
    public event Action<ITransport, Exception?>? TransportDisconnected;

    /// <inheritdoc />
    /// <remarks>
    /// The first pipe instance is created here, synchronously, before this returns. Creating it
    /// inside the accept loop instead left a window where the pipe did not yet exist and a client
    /// connecting immediately got ENOENT - the same class of race as delivering messages before a
    /// dispatcher is installed. A TCP listener binds in its constructor and never has this problem.
    /// </remarks>
    public void Start(int backlog = 512)
    {
        NamedPipeServerStream first = CreateInstance();
        _acceptLoop = Task.Run(() => AcceptLoopAsync(first, _shutdown.Token));
    }

    /// <summary>One unconnected server instance, ready for the next client.</summary>
    private NamedPipeServerStream CreateInstance() =>
        new(
            _pipeName,
            PipeDirection.InOut,
            MaxServerInstances,
            // Byte mode: BlackHole frames its own messages, and message mode would impose a
            // second, redundant framing on top.
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous | PipeOptions.WriteThrough,
            inBufferSize: _options.ReceiveBufferSize,
            outBufferSize: _options.SendBufferSize);

    private async Task AcceptLoopAsync(NamedPipeServerStream pending, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            NamedPipeServerStream pipe = pending;

            try
            {
                await pipe.WaitForConnectionAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                await pipe.DisposeAsync().ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                _options.ErrorHandler?.Invoke(ex);
                await pipe.DisposeAsync().ConfigureAwait(false);
                try
                {
                    pending = CreateInstance();
                    continue;
                }
                catch (Exception createEx)
                {
                    _options.ErrorHandler?.Invoke(createEx);
                    return;
                }
            }

            // Put the next instance in place before handling this one, so there is never a moment
            // when the pipe name has no listener waiting on it.
            try
            {
                pending = CreateInstance();
            }
            catch (Exception ex)
            {
                _options.ErrorHandler?.Invoke(ex);
                pending = null!;
            }

            if (_connections.Count >= MaxConnections)
            {
                await pipe.DisposeAsync().ConfigureAwait(false);
                if (pending is null) return;
                continue;
            }

            // Created unstarted so the handler below can install a dispatcher before the first
            // frame is delivered.
            StreamTransport transport = NamedPipeTransport.ForConnectedPipe(
                pipe, _pipeName, _options, startReceiving: false);

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

            if (pending is null)
                return;
        }
    }

    private void OnTransportClosed(ITransport transport, Exception? failure)
    {
        if (_connections.TryRemove(transport.Id, out ITransport? removed))
            TransportDisconnected?.Invoke(removed, failure);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try { await _shutdown.CancelAsync().ConfigureAwait(false); } catch (ObjectDisposedException) { }

        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); }
            catch (Exception) { }
        }

        foreach (ITransport transport in _connections.Values)
            await transport.DisposeAsync().ConfigureAwait(false);
        _connections.Clear();

        _shutdown.Dispose();
    }
}
