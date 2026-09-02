// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
namespace BlackHole.Diagnostics;

/// <summary>
/// Per-connection counters. Writes are interlocked, reads are snapshots, and nothing here is on the
/// allocation path - the whole type is four longs plus a start timestamp.
/// </summary>
public sealed class TransportStatistics
{
    private long _messagesSent;
    private long _messagesReceived;
    private long _bytesSent;
    private long _bytesReceived;
    private long _lastRoundTripTicks = -1;

    private readonly long _startedTicks = Environment.TickCount64;

    public long MessagesSent => Interlocked.Read(ref _messagesSent);
    public long MessagesReceived => Interlocked.Read(ref _messagesReceived);
    public long BytesSent => Interlocked.Read(ref _bytesSent);
    public long BytesReceived => Interlocked.Read(ref _bytesReceived);

    /// <summary>How long this connection has been up.</summary>
    public TimeSpan Uptime => TimeSpan.FromMilliseconds(Environment.TickCount64 - _startedTicks);

    /// <summary>Round trip of the most recent keepalive, or null if none has completed.</summary>
    public TimeSpan? LastRoundTrip
    {
        get
        {
            long ticks = Interlocked.Read(ref _lastRoundTripTicks);
            return ticks < 0 ? null : TimeSpan.FromTicks(ticks);
        }
    }

    internal void OnSent(int bytes)
    {
        Interlocked.Increment(ref _messagesSent);
        Interlocked.Add(ref _bytesSent, bytes);
    }

    internal void OnReceived(int bytes)
    {
        Interlocked.Increment(ref _messagesReceived);
        Interlocked.Add(ref _bytesReceived, bytes);
    }

    internal void OnRoundTrip(TimeSpan elapsed) =>
        Interlocked.Exchange(ref _lastRoundTripTicks, elapsed.Ticks);

    /// <summary>Immutable copy, safe to hand to a UI thread.</summary>
    public StatisticsSnapshot Snapshot() => new(
        MessagesSent, MessagesReceived, BytesSent, BytesReceived, Uptime, LastRoundTrip);

    public override string ToString() => Snapshot().ToString();
}

/// <summary>A point-in-time copy of <see cref="TransportStatistics"/>.</summary>
public readonly record struct StatisticsSnapshot(
    long MessagesSent,
    long MessagesReceived,
    long BytesSent,
    long BytesReceived,
    TimeSpan Uptime,
    TimeSpan? LastRoundTrip)
{
    /// <summary>Messages received per second averaged over the connection lifetime.</summary>
    public double ReceiveRate => Uptime.TotalSeconds <= 0 ? 0 : MessagesReceived / Uptime.TotalSeconds;

    /// <summary>Messages sent per second averaged over the connection lifetime.</summary>
    public double SendRate => Uptime.TotalSeconds <= 0 ? 0 : MessagesSent / Uptime.TotalSeconds;

    public override string ToString() =>
        $"sent {MessagesSent:N0} msg / {BytesSent:N0} B, received {MessagesReceived:N0} msg / {BytesReceived:N0} B, up {Uptime:hh\\:mm\\:ss}";
}
