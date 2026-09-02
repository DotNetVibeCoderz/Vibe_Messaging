// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;
using BlackHole.Buffers;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Patterns;

/// <summary>
/// Packs many small messages into one frame, one socket write, one wakeup on the far side.
/// </summary>
/// <remarks>
/// <para>
/// The envelope payload is simply a run of complete BlackHole frames, so
/// <see cref="BatchReceiver"/> unpacks it with the same <see cref="FrameCodec"/> the transport uses -
/// there is no second wire format to keep in sync, which is exactly the trap v2 fell into.
/// </para>
/// <para>
/// <see cref="AddAsync"/> buffers into a pooled writer that is reused for the life of the sender, so
/// a steady stream of telemetry allocates nothing per message. Batches leave on whichever trigger
/// fires first: <see cref="MaxCount"/>, <see cref="MaxBytes"/>, or <see cref="MaxDelay"/>.
/// </para>
/// </remarks>
public sealed class BatchSender : IAsyncDisposable
{
    private readonly ITransport _transport;
    private readonly PooledBufferWriter _buffer = new(16 * 1024);
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly CancellationTokenSource _shutdown = new();
    private Task? _timerLoop;
    private int _count;
    private long _batchesSent;
    private long _messagesSent;

    public BatchSender(ITransport transport)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
    }

    /// <summary>Flush once this many messages are buffered. Default 64.</summary>
    public int MaxCount { get; set; } = 64;

    /// <summary>Flush once the envelope reaches this size. Default 64 KiB.</summary>
    public int MaxBytes { get; set; } = 64 * 1024;

    /// <summary>Flush a partial batch after this long. Null disables the timer. Default 20 ms.</summary>
    public TimeSpan? MaxDelay { get; set; } = TimeSpan.FromMilliseconds(20);

    /// <summary>Messages waiting to go out.</summary>
    public int PendingCount => Volatile.Read(ref _count);

    /// <summary>Envelopes sent so far.</summary>
    public long BatchesSent => Interlocked.Read(ref _batchesSent);

    /// <summary>Messages sent inside those envelopes.</summary>
    public long MessagesSent => Interlocked.Read(ref _messagesSent);

    /// <summary>Starts the delay timer. Call it when <see cref="MaxDelay"/> should apply.</summary>
    public BatchSender Start()
    {
        if (_timerLoop is null && MaxDelay is { } delay && delay > TimeSpan.Zero)
            _timerLoop = Task.Run(() => TimerLoopAsync(delay, _shutdown.Token));
        return this;
    }

    /// <summary>Buffers a message, flushing automatically when a threshold is crossed.</summary>
    public async ValueTask AddAsync(BlackHoleMessage message, CancellationToken cancellationToken = default)
    {
        bool flush;
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            FrameCodec.Write(_buffer, message);
            _count++;
            flush = _count >= MaxCount || _buffer.WrittenCount >= MaxBytes;
            if (flush)
                await FlushLockedAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Buffers several messages, flushing as thresholds are crossed.</summary>
    public async ValueTask AddRangeAsync(IEnumerable<BlackHoleMessage> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        foreach (BlackHoleMessage message in messages)
            await AddAsync(message, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Sends whatever is buffered right now.</summary>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await FlushLockedAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Sends a fixed set of messages as one envelope, bypassing the buffer.</summary>
    public async ValueTask SendBatchAsync(IReadOnlyCollection<BlackHoleMessage> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
            return;

        using var scratch = new PooledBufferWriter(16 * 1024);
        foreach (BlackHoleMessage message in messages)
            FrameCodec.Write(scratch, message);

        await _transport.SendAsync(
            new BlackHoleMessage(MessageType.Batch, string.Empty, scratch.WrittenMemory, messages.Count),
            cancellationToken).ConfigureAwait(false);

        Interlocked.Increment(ref _batchesSent);
        Interlocked.Add(ref _messagesSent, messages.Count);
    }

    private async ValueTask FlushLockedAsync(CancellationToken cancellationToken)
    {
        if (_count == 0)
            return;

        int count = _count;
        await _transport.SendAsync(
            new BlackHoleMessage(MessageType.Batch, string.Empty, _buffer.WrittenMemory, count),
            cancellationToken).ConfigureAwait(false);

        _buffer.Reset();
        _count = 0;
        Interlocked.Increment(ref _batchesSent);
        Interlocked.Add(ref _messagesSent, count);
    }

    private async Task TimerLoopAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(delay);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                if (PendingCount > 0)
                    await FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception)
        {
            // The connection is gone; the transport reports it.
        }
    }

    public async ValueTask DisposeAsync()
    {
        try { _shutdown.Cancel(); } catch (ObjectDisposedException) { }

        if (_timerLoop is not null)
        {
            try { await _timerLoop.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false); }
            catch (Exception) { }
        }

        try
        {
            if (_transport.IsConnected)
                await FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception) { }

        _buffer.Dispose();
        _gate.Dispose();
        _shutdown.Dispose();
    }
}

/// <summary>
/// Unpacks batch envelopes and pushes each inner message back through the router, so a batched
/// publish behaves exactly like one that arrived on its own.
/// </summary>
public sealed class BatchReceiver
{
    private readonly HeaderCache _headerCache;
    private long _batchesReceived;
    private long _messagesReceived;

    /// <param name="inner">Where unpacked messages go. Pass the router to make batching transparent.</param>
    /// <param name="headerCache">Optional shared cache; one is created if omitted.</param>
    public BatchReceiver(MessageDispatch inner, HeaderCache? headerCache = null)
    {
        Inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _headerCache = headerCache ?? new HeaderCache(128);
    }

    /// <summary>Where unpacked messages are delivered.</summary>
    public MessageDispatch Inner { get; }

    /// <summary>Envelopes seen.</summary>
    public long BatchesReceived => Interlocked.Read(ref _batchesReceived);

    /// <summary>Messages unpacked from them.</summary>
    public long MessagesReceived => Interlocked.Read(ref _messagesReceived);

    /// <summary>Raised for each unpacked message, before it reaches <see cref="Inner"/>.</summary>
    public event Action<BlackHoleMessage>? MessageUnpacked;

    /// <summary>Wires this receiver into a router.</summary>
    public BatchReceiver AttachTo(MessageRouter router)
    {
        ArgumentNullException.ThrowIfNull(router);
        router.On(MessageType.Batch, HandleAsync);
        return this;
    }

    /// <summary>Unpacks one envelope. Assign to a router or a dispatcher.</summary>
    public async ValueTask HandleAsync(ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        if (message.Type != MessageType.Batch || message.Payload.IsEmpty)
            return;

        Interlocked.Increment(ref _batchesReceived);
        var sequence = new ReadOnlySequence<byte>(message.Payload);

        while (FrameCodec.TryRead(ref sequence, _headerCache, int.MaxValue,
                   out BlackHoleMessage inner, out byte[]? rented))
        {
            try
            {
                if (inner.Type == MessageType.Batch)
                    continue; // One level only: nested envelopes are a loop waiting to happen.

                Interlocked.Increment(ref _messagesReceived);
                MessageUnpacked?.Invoke(inner);
                await Inner(transport, inner, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (rented is not null)
                    ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }
}
