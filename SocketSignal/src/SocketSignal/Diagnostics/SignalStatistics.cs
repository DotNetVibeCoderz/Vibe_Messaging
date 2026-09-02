// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
namespace SocketSignal.Diagnostics;

/// <summary>
/// Live counters for one connection, or for a whole server. Updated with interlocked adds on the
/// hot path, so reading them costs nothing and never blocks the pump.
/// </summary>
public sealed class SignalStatistics
{
    private long _framesSent;
    private long _framesReceived;
    private long _bytesSent;
    private long _bytesReceived;
    private long _callsCompleted;
    private long _callsFailed;

    public long FramesSent => Interlocked.Read(ref _framesSent);
    public long FramesReceived => Interlocked.Read(ref _framesReceived);
    public long BytesSent => Interlocked.Read(ref _bytesSent);
    public long BytesReceived => Interlocked.Read(ref _bytesReceived);

    /// <summary>Calls this peer issued that came back with a result.</summary>
    public long CallsCompleted => Interlocked.Read(ref _callsCompleted);

    /// <summary>Calls this peer issued that came back with an error, timed out, or lost the socket.</summary>
    public long CallsFailed => Interlocked.Read(ref _callsFailed);

    internal void OnSent(int bytes)
    {
        Interlocked.Increment(ref _framesSent);
        Interlocked.Add(ref _bytesSent, bytes);
    }

    internal void OnReceived(int bytes)
    {
        Interlocked.Increment(ref _framesReceived);
        Interlocked.Add(ref _bytesReceived, bytes);
    }

    internal void OnCallCompleted() => Interlocked.Increment(ref _callsCompleted);
    internal void OnCallFailed() => Interlocked.Increment(ref _callsFailed);

    /// <summary>Folds another set of counters into this one. Used to roll connections up to the server.</summary>
    internal void Absorb(SignalStatistics other)
    {
        Interlocked.Add(ref _framesSent, other.FramesSent);
        Interlocked.Add(ref _framesReceived, other.FramesReceived);
        Interlocked.Add(ref _bytesSent, other.BytesSent);
        Interlocked.Add(ref _bytesReceived, other.BytesReceived);
        Interlocked.Add(ref _callsCompleted, other.CallsCompleted);
        Interlocked.Add(ref _callsFailed, other.CallsFailed);
    }

    public override string ToString() =>
        $"sent {FramesSent} frames / {BytesSent} B, received {FramesReceived} frames / {BytesReceived} B, " +
        $"calls ok {CallsCompleted} failed {CallsFailed}";
}
