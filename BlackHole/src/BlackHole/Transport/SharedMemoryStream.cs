// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Diagnostics;
namespace BlackHole.Transport;

/// <summary>
/// How aggressively a shared-memory endpoint waits for its peer.
/// </summary>
/// <remarks>
/// <para>
/// Shared memory has no kernel to park a thread for you, so waiting is a choice between latency and
/// CPU. Waiting happens in three phases, and the defaults are chosen so an active link never
/// reaches the third:
/// </para>
/// <list type="number">
///   <item><description><b>Spin</b> for <see cref="SpinCount"/> iterations - sub-microsecond, for a peer that is already writing.</description></item>
///   <item><description><b>Yield</b> for <see cref="YieldDuration"/> - sub-microsecond per attempt, giving other work the core without arming a timer.</description></item>
///   <item><description><b>Sleep</b> for <see cref="PollInterval"/> - only once the link has been quiet, so an idle endpoint costs nothing.</description></item>
/// </list>
/// <para>
/// The yield phase is not an optimisation, it is the difference between working and not. A timed
/// sleep cannot resolve finer than the OS timer tick - about 15 ms on Windows - so a reader that
/// falls asleep between messages adds a full tick to the next one. Measured on loopback, the ring
/// itself delivers a message in about 3 microseconds; without a long enough yield phase the same
/// message took 15 milliseconds whenever the reader had dozed off.
/// </para>
/// </remarks>
public sealed class SharedMemoryOptions
{
    /// <summary>Bytes per direction. Rounded up to a power of two, minimum 4 KiB. Default 1 MiB.</summary>
    public int RingCapacity { get; set; } = 1024 * 1024;

    /// <summary>
    /// Tight spins before yielding. Default 50 - enough to catch a peer already mid-write, short
    /// enough not to burn a core.
    /// </summary>
    public int SpinCount { get; set; } = 50;

    /// <summary>
    /// How long to keep yielding before falling back to a timed sleep. Default 2 ms.
    /// </summary>
    /// <remarks>
    /// This is the setting that decides whether shared memory is fast or useless, so it is worth
    /// understanding rather than tuning blindly. A yield costs about half a microsecond and does not
    /// arm a timer; a timed sleep cannot resolve finer than the OS timer tick, which on Windows is
    /// about 15 ms. So any message that arrives while the reader is yielding is picked up in
    /// microseconds, and any message that arrives while it is sleeping waits for the tick.
    /// <para>
    /// The budget therefore needs to be longer than a realistic gap between messages. At 2 ms an
    /// active request/response link never reaches the sleep phase at all, at a cost of roughly 2 ms
    /// of one core each time a link goes quiet. Lower it to save CPU on bursty links; raise it if
    /// latency spikes reappear under load. Zero disables yielding, which is almost never what you
    /// want.
    /// </para>
    /// </remarks>
    public TimeSpan YieldDuration { get; set; } = TimeSpan.FromMilliseconds(2);

    /// <summary>
    /// How long to sleep once a link has gone properly quiet. Default 1 ms, though the OS will
    /// round that up - which is fine here, because only an idle endpoint ever gets this far.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(1);

    /// <summary>A copy, so one template can seed several endpoints.</summary>
    public SharedMemoryOptions Clone() => (SharedMemoryOptions)MemberwiseClone();
}

/// <summary>
/// Walks the spin, yield, sleep phases for one wait.
/// </summary>
/// <remarks>
/// A struct reset per operation, so a wait that ends quickly leaves no state behind and the next one
/// starts from the cheapest phase again.
/// </remarks>
internal struct WaitStrategy
{
    private readonly SharedMemoryOptions _options;
    private readonly long _yieldTicks;
    private SpinWait _spin;
    private int _spins;
    private long _yieldStartedAt;

    internal WaitStrategy(SharedMemoryOptions options)
    {
        _options = options;
        _yieldTicks = (long)(options.YieldDuration.TotalSeconds * Stopwatch.Frequency);
        _spin = default;
        _spins = 0;
        _yieldStartedAt = 0;
    }

    /// <summary>
    /// True once spinning and yielding have both been exhausted, meaning the link has genuinely gone
    /// quiet and the caller should fall back to a timed sleep.
    /// </summary>
    internal bool NextIsSleep
    {
        get
        {
            if (_spins < _options.SpinCount)
                return false;
            if (_yieldTicks <= 0)
                return true;
            if (_yieldStartedAt == 0)
                return false;   // the yield phase has not started measuring yet
            return Stopwatch.GetTimestamp() - _yieldStartedAt >= _yieldTicks;
        }
    }

    /// <summary>Advances one step without arming a timer.</summary>
    internal void SpinOrYield()
    {
        if (_spins < _options.SpinCount)
        {
            _spins++;
            // sleep1Threshold: -1 disables SpinWait's own escalation to Thread.Sleep(1). Left on,
            // it starts doing that after roughly 20 iterations, and Thread.Sleep(1) resolves to a
            // full OS timer tick - measured here at 15.6 ms. That single default turned a 3
            // microsecond ring into a 30 millisecond one, and no amount of tuning the phases after
            // it could help, because the stall happened before they were ever reached.
            _spin.SpinOnce(sleep1Threshold: -1);
            return;
        }

        // Start the clock the first time the yield phase is entered, so the budget measures elapsed
        // time rather than a number of attempts - the two are not related closely enough to be
        // interchangeable.
        if (_yieldStartedAt == 0)
            _yieldStartedAt = Stopwatch.GetTimestamp();

        // Thread.Yield reschedules without arming a timer, so it costs well under a microsecond
        // where a timed sleep would cost a full timer tick.
        Thread.Yield();
    }

    /// <summary>The sleep phase, reached only after a link has been quiet for the whole budget.</summary>
    internal void Sleep()
    {
        if (_options.PollInterval <= TimeSpan.Zero)
            Thread.Yield();
        else
            Thread.Sleep(_options.PollInterval);
    }

    /// <summary>Back to the cheapest phase, after progress was made.</summary>
    internal void Reset()
    {
        _spin = default;
        _spins = 0;
        _yieldStartedAt = 0;
    }
}

/// <summary>
/// Presents a pair of shared-memory rings as an ordinary duplex <see cref="Stream"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is what lets shared memory reuse every line of <see cref="StreamTransport"/> - the same
/// framing, the same pipelines, the same patterns - instead of being a fourth parallel
/// implementation of the protocol. The bytes crossing it are exactly the bytes that would cross a
/// socket.
/// </para>
/// <para>
/// The honest trade: a stream API means the data is copied out of the ring into the pipe's buffer
/// before it is parsed, which gives up some of what shared memory could theoretically offer. What
/// it keeps is the part that actually matters - no syscall, no kernel copy, no network stack - and
/// what it buys is one protocol implementation instead of two.
/// </para>
/// </remarks>
internal sealed class SharedMemoryStream : Stream
{
    private readonly SharedMemorySegment _segment;
    private readonly SharedMemoryOptions _options;
    private volatile bool _closed;

    internal SharedMemoryStream(SharedMemorySegment segment, SharedMemoryOptions options)
    {
        _segment = segment;
        _options = options;
    }

    public override bool CanRead => !_closed;
    public override bool CanWrite => !_closed;
    public override bool CanSeek => false;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <summary>True while the peer has not marked itself gone.</summary>
    internal bool IsPeerAlive => !_closed && _segment.IsPeerAlive;

    // ------------------------------------------------------------------ read

    public override ValueTask<int> ReadAsync(
        Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        // Deliberately synchronous, and deliberately not an async method.
        //
        // Awaiting here would post the continuation back to the thread pool on every empty poll,
        // and with both ends of a connection doing that the pool starves - a round trip that should
        // take microseconds ends up waiting on the pool's thread-injection rate instead. The read
        // loop owns a dedicated thread (see StreamTransport's dedicatedReceiveThread), so blocking
        // it here costs nothing that anything else wanted.
        try
        {
            return ValueTask.FromResult(ReadCore(buffer.Span, cancellationToken));
        }
        catch (OperationCanceledException)
        {
            return ValueTask.FromCanceled<int>(cancellationToken);
        }
    }

    public override int Read(Span<byte> buffer) => ReadCore(buffer, CancellationToken.None);

    /// <summary>Blocks until bytes arrive, the peer leaves, or the token fires.</summary>
    private int ReadCore(Span<byte> buffer, CancellationToken cancellationToken)
    {
        if (buffer.IsEmpty)
            return 0;

        var wait = new WaitStrategy(_options);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int read = _segment.Inbound.TryRead(buffer);
            if (read > 0)
                return read;
            if (_closed)
                return 0;

            // A socket read returns 0 when the peer closes, which is how the frame loop learns a
            // connection ended. Shared memory has no such signal, so the liveness flag is checked
            // on every empty poll - otherwise an idle endpoint whose peer vanished would wait
            // forever and its subscriptions would never be cleaned up.
            if (!_segment.IsPeerAlive)
                return _segment.Inbound.TryRead(buffer);   // drain, then end of stream

            if (wait.NextIsSleep)
                wait.Sleep();
            else
                wait.SpinOrYield();
        }
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    // ----------------------------------------------------------------- write

    public override ValueTask WriteAsync(
        ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        // Synchronous for the same reason as ReadAsync. A write only ever waits when the ring is
        // full, which is genuine backpressure - blocking the caller is the correct response, and is
        // what a blocking socket write would do too.
        try
        {
            WriteCore(buffer.Span, cancellationToken);
            return ValueTask.CompletedTask;
        }
        catch (OperationCanceledException)
        {
            return ValueTask.FromCanceled(cancellationToken);
        }
    }

    public override void Write(ReadOnlySpan<byte> buffer) => WriteCore(buffer, CancellationToken.None);

    /// <summary>Blocks until every byte is in the ring, or the peer leaves.</summary>
    private void WriteCore(ReadOnlySpan<byte> buffer, CancellationToken cancellationToken)
    {
        var wait = new WaitStrategy(_options);

        while (!buffer.IsEmpty)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ObjectDisposedException.ThrowIf(_closed, this);

            int written = _segment.Outbound.TryWrite(buffer);
            if (written > 0)
            {
                buffer = buffer[written..];
                wait.Reset();
                continue;
            }

            // The ring is full: the peer is not reading fast enough. If it has gone away entirely
            // there is nobody who ever will.
            if (!_segment.IsPeerAlive)
                throw new IOException("The shared-memory peer disconnected while writing.");

            if (wait.NextIsSleep)
                wait.Sleep();
            else
                wait.SpinOrYield();
        }
    }

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    // A ring write is already visible to the peer the moment its cursor moves; there is nothing
    // held back to push.
    public override void Flush() { }
    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (!_closed)
        {
            _closed = true;
            if (disposing)
                _segment.Dispose();
        }
        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}
