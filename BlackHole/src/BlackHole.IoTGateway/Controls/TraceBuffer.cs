// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
namespace BlackHole.IoTGateway.Controls;

/// <summary>
/// A fixed-size ring of samples for one pen on the chart.
/// </summary>
/// <remarks>
/// The chart repaints at 30 Hz while devices publish at up to 500 Hz each, so this has to absorb
/// writes from the receive loop and reads from the UI thread without allocating or locking on the
/// write side. A ring of doubles with an interlocked cursor does both: writers never block, and a
/// reader can miss a sample under contention, which for a scrolling trace is invisible.
/// </remarks>
public sealed class TraceBuffer
{
    private readonly double[] _samples;
    private readonly int _mask;
    private long _cursor;

    /// <param name="capacity">Samples retained, rounded up to a power of two.</param>
    public TraceBuffer(int capacity = 1024)
    {
        int size = 1;
        while (size < Math.Clamp(capacity, 16, 1 << 16)) size <<= 1;
        _samples = new double[size];
        _mask = size - 1;
        Array.Fill(_samples, double.NaN);
    }

    /// <summary>How many samples the ring holds.</summary>
    public int Capacity => _samples.Length;

    /// <summary>Samples written since construction.</summary>
    public long Written => Interlocked.Read(ref _cursor);

    /// <summary>Appends one sample. Safe to call from any thread.</summary>
    public void Add(double value)
    {
        long index = Interlocked.Increment(ref _cursor) - 1;
        Volatile.Write(ref _samples[index & _mask], value);
    }

    /// <summary>
    /// Copies the most recent <c>destination.Length</c> samples oldest-first into
    /// <paramref name="destination"/>, padding the front with NaN when the ring is not full yet.
    /// Returns how many are real.
    /// </summary>
    public int CopyLatest(Span<double> destination)
    {
        long written = Interlocked.Read(ref _cursor);
        int want = destination.Length;
        int available = (int)Math.Min(written, _samples.Length);
        int take = Math.Min(want, available);
        int pad = want - take;

        destination[..pad].Fill(double.NaN);
        long start = written - take;
        for (int i = 0; i < take; i++)
            destination[pad + i] = Volatile.Read(ref _samples[(start + i) & _mask]);

        return take;
    }

    /// <summary>Most recent sample, or NaN when nothing has been written.</summary>
    public double Latest
    {
        get
        {
            long written = Interlocked.Read(ref _cursor);
            return written == 0 ? double.NaN : Volatile.Read(ref _samples[(written - 1) & _mask]);
        }
    }

    /// <summary>Drops every sample.</summary>
    public void Clear()
    {
        Interlocked.Exchange(ref _cursor, 0);
        Array.Fill(_samples, double.NaN);
    }
}
