// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using BlackHole.Diagnostics;
using BlackHole.Protocol;

namespace BlackHole.Transport;

/// <summary>
/// The TCP transport, used unchanged by both the dialling side and the accepting side.
/// </summary>
/// <remarks>
/// <para>
/// v2 had two near-identical classes with their own copies of serialise, deserialise and the read
/// loop; a change to one silently desynchronised the other. There is now one implementation, and
/// framing lives in <see cref="FrameCodec"/>.
/// </para>
/// <para>
/// The read side is <see cref="System.IO.Pipelines"/>: the pipe owns the buffers, hands out
/// <see cref="ReadOnlySequence{T}"/> views, and handles partial frames without a per-message
/// <c>byte[]</c> allocation. Steady-state receive of a fully buffered frame allocates nothing at
/// all - no message object, no payload copy, and no header string once the
/// <see cref="HeaderCache"/> is warm.
/// </para>
/// </remarks>
public sealed class TcpTransport : ITransport
{
    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private readonly PipeReader _reader;
    private readonly PipeWriter _writer;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly HeaderCache _headerCache;
    private readonly TransportOptions _options;
    private readonly string _remoteEndPoint;

    private Task? _receiveLoop;
    private Task? _keepAliveLoop;
    private long _pingSentTimestamp;
    private int _closed;
    private int _started;

    private TcpTransport(Socket socket, TransportOptions options)
    {
        _socket = socket;
        _options = options;
        _socket.NoDelay = options.NoDelay;
        _remoteEndPoint = SafeEndPoint(socket);
        _stream = new NetworkStream(socket, ownsSocket: false);
        _headerCache = options.CreateHeaderCache();

        _reader = PipeReader.Create(_stream, new StreamPipeReaderOptions(
            pool: MemoryPool<byte>.Shared,
            bufferSize: options.ReceiveBufferSize,
            minimumReadSize: Math.Min(1024, options.ReceiveBufferSize),
            leaveOpen: true));

        _writer = PipeWriter.Create(_stream, new StreamPipeWriterOptions(
            pool: MemoryPool<byte>.Shared,
            minimumBufferSize: options.SendBufferSize,
            leaveOpen: true));
    }

    /// <inheritdoc />
    public string Id { get; } = Guid.NewGuid().ToString("N")[..8];

    /// <inheritdoc />
    public bool IsConnected => Volatile.Read(ref _closed) == 0 && _socket.Connected;

    /// <inheritdoc />
    public string RemoteEndPoint => _remoteEndPoint;

    /// <inheritdoc />
    public MessageDispatch? Dispatcher { get; set; }

    /// <inheritdoc />
    public TransportStatistics Statistics { get; } = new();

    /// <summary>Header cache backing this connection; exposed for diagnostics.</summary>
    public HeaderCache HeaderCache => _headerCache;

    /// <inheritdoc />
    public event Action<ITransport, Exception?>? Closed;

    // ---------------------------------------------------------------- creation

    /// <summary>Dials <paramref name="host"/> and returns a connected transport.</summary>
    /// <param name="host">Host name or address to dial.</param>
    /// <param name="port">Port to dial.</param>
    /// <param name="options">Transport settings; defaults are used when omitted.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <param name="startReceiving">
    /// Leave true for a transport you will use directly. Pass false when you need to install
    /// <see cref="Dispatcher"/> first, then call <see cref="Start"/> - otherwise a message that
    /// arrives before the dispatcher is set has nowhere to go and is dropped.
    /// </param>
    public static async Task<TcpTransport> ConnectAsync(
        string host,
        int port,
        TransportOptions? options = null,
        CancellationToken cancellationToken = default,
        bool startReceiving = true)
    {
        options ??= new TransportOptions();
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        try
        {
            await socket.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        var transport = new TcpTransport(socket, options);
        if (startReceiving)
            transport.Start();
        return transport;
    }

    /// <summary>
    /// Dials with exponential backoff. Useful for a client that starts before its server does.
    /// </summary>
    public static async Task<TcpTransport> ConnectWithRetryAsync(
        string host,
        int port,
        int attempts = 5,
        TimeSpan? initialDelay = null,
        TransportOptions? options = null,
        CancellationToken cancellationToken = default,
        bool startReceiving = true)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(attempts);
        TimeSpan delay = initialDelay ?? TimeSpan.FromMilliseconds(100);
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await ConnectAsync(host, port, options, cancellationToken, startReceiving).ConfigureAwait(false);
            }
            catch (Exception) when (attempt < attempts && !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 5_000));
            }
        }
    }

    /// <summary>Wraps a socket that is already connected - what the listener does on accept.</summary>
    /// <param name="socket">A connected socket.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="startReceiving">
    /// False to defer the receive loop until <see cref="Start"/>, so the caller can install
    /// <see cref="Dispatcher"/> without racing the first inbound message.
    /// </param>
    public static TcpTransport ForAcceptedSocket(
        Socket socket, TransportOptions? options = null, bool startReceiving = true)
    {
        var transport = new TcpTransport(socket, options ?? new TransportOptions());
        if (startReceiving)
            transport.Start();
        return transport;
    }

    /// <summary>
    /// Begins receiving. Idempotent, so calling it on an already-started transport does nothing.
    /// </summary>
    /// <remarks>
    /// Deliveries begin the moment this returns, so install <see cref="Dispatcher"/> first. A
    /// transport created with <c>startReceiving: false</c> holds the peer's first frames in the
    /// socket buffer until this is called, which is what makes wiring race-free.
    /// </remarks>
    public void Start()
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
            return;

        _receiveLoop = Task.Run(() => ReceiveLoopAsync(_shutdown.Token));
        if (_options.KeepAliveInterval is { } interval && interval > TimeSpan.Zero)
            _keepAliveLoop = Task.Run(() => KeepAliveLoopAsync(interval, _shutdown.Token));
    }

    // ------------------------------------------------------------------ send

    /// <inheritdoc />
    public ValueTask SendAsync(BlackHoleMessage message, CancellationToken cancellationToken = default) =>
        SendCoreAsync(message, flush: true, cancellationToken);

    /// <inheritdoc />
    public ValueTask WriteAsync(BlackHoleMessage message, CancellationToken cancellationToken = default) =>
        SendCoreAsync(message, flush: false, cancellationToken);

    /// <inheritdoc />
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            FlushResult result = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            if (result.IsCompleted)
                Close(null);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async ValueTask SendCoreAsync(BlackHoleMessage message, bool flush, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _closed) != 0, this);

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // FrameCodec copies the payload into the pipe synchronously, so the caller's buffer is
            // free the moment this returns - no defensive copy needed anywhere on the send path.
            int written = FrameCodec.Write(_writer, message);
            Statistics.OnSent(written);

            if (flush)
            {
                FlushResult result = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                if (result.IsCompleted)
                    Close(null);
            }
        }
        catch (Exception ex) when (ex is SocketException or IOException or ObjectDisposedException)
        {
            Close(ex);
            throw;
        }
        finally
        {
            _writeLock.Release();
        }
    }

    // --------------------------------------------------------------- receive

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        Exception? failure = null;
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ReadResult read = await _reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = read.Buffer;
                SequencePosition consumed = buffer.Start;
                SequencePosition examined = buffer.End;

                long remainingBefore = buffer.Length;
                while (FrameCodec.TryRead(ref buffer, _headerCache, _options.MaxFrameLength,
                           out BlackHoleMessage message, out byte[]? rented))
                {
                    // Everything up to here is parsed; the payload stays valid until AdvanceTo.
                    consumed = buffer.Start;
                    int frameSize = (int)(remainingBefore - buffer.Length);
                    remainingBefore = buffer.Length;
                    try
                    {
                        Statistics.OnReceived(frameSize);
                        await HandleAsync(message, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        if (rented is not null)
                            ArrayPool<byte>.Shared.Return(rented);
                    }
                }

                _reader.AdvanceTo(consumed, examined);

                if (read.IsCompleted)
                {
                    if (!buffer.IsEmpty)
                        failure = new BlackHoleProtocolException("Connection ended mid-frame.");
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            failure = ex;
            if (ex is not (SocketException or IOException or ObjectDisposedException))
                _options.ErrorHandler?.Invoke(ex);
        }
        finally
        {
            await _reader.CompleteAsync(failure).ConfigureAwait(false);
            Close(failure);
        }
    }

    private ValueTask HandleAsync(in BlackHoleMessage message, CancellationToken cancellationToken)
    {
        switch (message.Type)
        {
            case MessageType.Ping:
                // Answered here so keepalive never reaches application code.
                return SendCoreAsync(
                    new BlackHoleMessage(MessageType.Pong, correlationId: message.CorrelationId),
                    flush: true, cancellationToken);

            case MessageType.Pong:
                long sent = Interlocked.Exchange(ref _pingSentTimestamp, 0);
                if (sent != 0)
                    Statistics.OnRoundTrip(Stopwatch.GetElapsedTime(sent));
                return ValueTask.CompletedTask;

            default:
                MessageDispatch? dispatcher = Dispatcher;
                return dispatcher is null
                    ? ValueTask.CompletedTask
                    : dispatcher(this, message, cancellationToken);
        }
    }

    // -------------------------------------------------------------- keepalive

    private async Task KeepAliveLoopAsync(TimeSpan interval, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                if (Volatile.Read(ref _closed) != 0)
                    return;

                Interlocked.Exchange(ref _pingSentTimestamp, Stopwatch.GetTimestamp());
                await SendCoreAsync(new BlackHoleMessage(MessageType.Ping), flush: true, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception)
        {
            // A failed keepalive means the connection is gone; the receive loop reports it.
        }
    }

    // ---------------------------------------------------------------- closing

    private void Close(Exception? failure)
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
            return;

        try { _shutdown.Cancel(); } catch (ObjectDisposedException) { }
        try { _socket.Shutdown(SocketShutdown.Both); } catch (Exception) { /* already gone */ }
        Closed?.Invoke(this, failure);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        Close(null);

        try { await _writer.CompleteAsync().ConfigureAwait(false); } catch (Exception) { }

        if (_receiveLoop is not null)
        {
            try { await _receiveLoop.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); }
            catch (Exception) { }
        }

        if (_keepAliveLoop is not null)
        {
            try { await _keepAliveLoop.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); }
            catch (Exception) { }
        }

        _stream.Dispose();
        _socket.Dispose();
        _shutdown.Dispose();
        _writeLock.Dispose();
    }

    private static string SafeEndPoint(Socket socket)
    {
        try
        {
            return socket.RemoteEndPoint?.ToString() ?? "(unknown)";
        }
        catch (SocketException)
        {
            return "(disconnected)";
        }
    }

    /// <summary>Local endpoint, or null once the socket is gone.</summary>
    public EndPoint? LocalEndPoint
    {
        get
        {
            try { return _socket.LocalEndPoint; }
            catch (Exception) { return null; }
        }
    }
}
