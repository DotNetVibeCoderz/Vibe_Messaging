// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using BlackHole.Diagnostics;
using BlackHole.Protocol;

namespace BlackHole.Transport;

/// <summary>
/// The frame loop, over any duplex <see cref="Stream"/>.
/// </summary>
/// <remarks>
/// <para>
/// Every transport BlackHole ships is a stream underneath - a socket, a Unix domain socket, a named
/// pipe, or a shared-memory ring dressed as a stream - so they all share this one implementation.
/// That is deliberate: v2 kept two copies of its framing for the client and server halves of TCP
/// alone, and they drifted. Four transports with four copies would be four times the trap.
/// </para>
/// <para>
/// The read side is <see cref="System.IO.Pipelines"/>: the pipe owns the buffers, hands out
/// <see cref="ReadOnlySequence{T}"/> views, and handles partial frames without a per-message
/// allocation. A fully buffered frame parses with no allocation at all.
/// </para>
/// </remarks>
public sealed class StreamTransport : ITransport
{
    private readonly Stream _stream;
    private readonly PipeReader _reader;
    private readonly PipeWriter _writer;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly HeaderCache _headerCache;
    private readonly TransportOptions _options;
    private readonly Func<bool>? _isAlive;
    private readonly Action? _onDispose;
    private readonly bool _dedicatedReceiveThread;

    private Task? _receiveLoop;
    private Task? _keepAliveLoop;
    private long _pingSentTimestamp;
    private int _closed;
    private int _started;

    /// <summary>Wraps a connected duplex stream.</summary>
    /// <param name="stream">A stream that can be read and written concurrently.</param>
    /// <param name="options">Transport settings.</param>
    /// <param name="remoteEndPoint">How this peer is described in logs.</param>
    /// <param name="kind">Short transport name, e.g. "tcp" or "uds". Shown in diagnostics.</param>
    /// <param name="isAlive">
    /// Optional liveness probe consulted by <see cref="IsConnected"/>. Without one, the transport is
    /// considered connected until it is closed.
    /// </param>
    /// <param name="onDispose">Runs after the stream is disposed, to release whatever owns it.</param>
    /// <param name="dedicatedReceiveThread">
    /// True for a stream whose read waits by spinning rather than parking - shared memory. Such a
    /// loop would otherwise hold a thread-pool thread hostage, and with both ends of a connection
    /// doing it the pool starves: continuations then wait on the pool's slow thread injection, and
    /// a round trip that should take microseconds takes tens of milliseconds. Sockets park
    /// properly and need no dedicated thread.
    /// </param>
    /// <param name="startReceiving">
    /// False to defer the receive loop until <see cref="Start"/>, so the caller can install
    /// <see cref="Dispatcher"/> without racing the first inbound message.
    /// </param>
    public StreamTransport(
        Stream stream,
        TransportOptions? options = null,
        string remoteEndPoint = "(unknown)",
        string kind = "stream",
        Func<bool>? isAlive = null,
        Action? onDispose = null,
        bool dedicatedReceiveThread = false,
        bool startReceiving = true)
    {
        ArgumentNullException.ThrowIfNull(stream);

        _stream = stream;
        _options = options ?? new TransportOptions();
        _isAlive = isAlive;
        _onDispose = onDispose;
        _dedicatedReceiveThread = dedicatedReceiveThread;
        _headerCache = _options.CreateHeaderCache();

        RemoteEndPoint = remoteEndPoint;
        Kind = kind;

        _reader = PipeReader.Create(_stream, new StreamPipeReaderOptions(
            pool: MemoryPool<byte>.Shared,
            bufferSize: _options.ReceiveBufferSize,
            minimumReadSize: Math.Min(1024, _options.ReceiveBufferSize),
            leaveOpen: true));

        _writer = PipeWriter.Create(_stream, new StreamPipeWriterOptions(
            pool: MemoryPool<byte>.Shared,
            minimumBufferSize: _options.SendBufferSize,
            leaveOpen: true));

        if (startReceiving)
            Start();
    }

    /// <inheritdoc />
    public string Id { get; } = Guid.NewGuid().ToString("N")[..8];

    /// <inheritdoc />
    public bool IsConnected => Volatile.Read(ref _closed) == 0 && (_isAlive?.Invoke() ?? true);

    /// <inheritdoc />
    public string RemoteEndPoint { get; }

    /// <summary>Short name of the underlying transport: "tcp", "uds", "pipe", "shm".</summary>
    public string Kind { get; }

    /// <inheritdoc />
    public MessageDispatch? Dispatcher { get; set; }

    /// <inheritdoc />
    public TransportStatistics Statistics { get; } = new();

    /// <summary>Header cache backing this connection; exposed for diagnostics.</summary>
    public HeaderCache HeaderCache => _headerCache;

    /// <inheritdoc />
    public event Action<ITransport, Exception?>? Closed;

    /// <summary>
    /// Begins receiving. Idempotent, so calling it on an already-started transport does nothing.
    /// </summary>
    /// <remarks>
    /// Deliveries begin the moment this returns, so install <see cref="Dispatcher"/> first. A
    /// transport created with <c>startReceiving: false</c> leaves the peer's first frames in the
    /// underlying buffer until this is called, which is what makes wiring race-free.
    /// </remarks>
    public void Start()
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
            return;

        _receiveLoop = _dedicatedReceiveThread
            ? Task.Factory.StartNew(
                () => ReceiveLoopAsync(_shutdown.Token),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap()
            : Task.Run(() => ReceiveLoopAsync(_shutdown.Token));

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
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or System.Net.Sockets.SocketException)
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
            if (ex is not (IOException or ObjectDisposedException or System.Net.Sockets.SocketException))
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

        try { await _stream.DisposeAsync().ConfigureAwait(false); } catch (Exception) { }

        _onDispose?.Invoke();
        _shutdown.Dispose();
        _writeLock.Dispose();
    }
}
