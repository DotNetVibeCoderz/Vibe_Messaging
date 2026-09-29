using System.IO.Hashing;
using System.Numerics;
using System.Text;

namespace BigPipe.Analytics;

/// <summary>Streaming mean / variance / min / max (Welford), O(1) memory.</summary>
public sealed class RunningStats
{
    private double _mean;
    private double _m2;

    public long Count { get; private set; }
    public double Mean => Count > 0 ? _mean : double.NaN;
    public double Variance => Count > 1 ? _m2 / (Count - 1) : 0;
    public double StdDev => Math.Sqrt(Variance);
    public double Min { get; private set; } = double.PositiveInfinity;
    public double Max { get; private set; } = double.NegativeInfinity;
    public double Sum { get; private set; }

    public void Add(double x)
    {
        if (double.IsNaN(x)) return;
        Count++;
        Sum += x;
        var d = x - _mean;
        _mean += d / Count;
        _m2 += d * (x - _mean);
        if (x < Min) Min = x;
        if (x > Max) Max = x;
    }

    /// <summary>Standard score of <paramref name="x"/> against what has been seen so far.</summary>
    public double ZScore(double x) => StdDev > 0 ? (x - Mean) / StdDev : 0;

    public void Merge(RunningStats other)
    {
        if (other.Count == 0) return;
        if (Count == 0)
        {
            (Count, _mean, _m2, Min, Max, Sum) = (other.Count, other._mean, other._m2, other.Min, other.Max, other.Sum);
            return;
        }
        var n = Count + other.Count;
        var delta = other._mean - _mean;
        _m2 += other._m2 + delta * delta * Count * other.Count / n;
        _mean += delta * other.Count / n;
        Count = n;
        Sum += other.Sum;
        Min = Math.Min(Min, other.Min);
        Max = Math.Max(Max, other.Max);
    }

    public override string ToString() => $"n={Count} mean={Mean:0.###} sd={StdDev:0.###} min={Min:0.###} max={Max:0.###}";
}

/// <summary>Exponentially weighted moving average and variance.</summary>
public sealed class Ewma(double alpha = 0.1)
{
    public double Alpha { get; } = alpha is > 0 and <= 1 ? alpha : throw new ArgumentOutOfRangeException(nameof(alpha));
    public double Value { get; private set; } = double.NaN;
    public double Variance { get; private set; }
    public double StdDev => Math.Sqrt(Variance);
    public long Count { get; private set; }

    public double Add(double x)
    {
        if (double.IsNaN(x)) return Value;
        Count++;
        if (Count == 1)
        {
            Value = x;
            return Value;
        }
        var diff = x - Value;
        var incr = Alpha * diff;
        Value += incr;
        Variance = (1 - Alpha) * (Variance + diff * incr);
        return Value;
    }
}

/// <summary>
/// Streaming quantiles with bounded memory: a uniform reservoir sample (Vitter's algorithm R).
/// Accurate to ~1/sqrt(capacity) in rank.
/// </summary>
public sealed class QuantileSketch(int capacity = 4096, int seed = 7)
{
    private readonly double[] _sample = new double[capacity];
    private readonly Random _rng = new(seed);
    private int _filled;
    private bool _sorted;

    public long Count { get; private set; }

    public void Add(double x)
    {
        if (double.IsNaN(x)) return;
        Count++;
        _sorted = false;
        if (_filled < _sample.Length)
        {
            _sample[_filled++] = x;
            return;
        }
        var j = _rng.NextInt64(Count);
        if (j < _sample.Length) _sample[j] = x;
    }

    public double Quantile(double q)
    {
        if (_filled == 0) return double.NaN;
        if (!_sorted)
        {
            Array.Sort(_sample, 0, _filled);
            _sorted = true;
        }
        var pos = Math.Clamp(q, 0, 1) * (_filled - 1);
        var lo = (int)Math.Floor(pos);
        var hi = Math.Min(lo + 1, _filled - 1);
        return _sample[lo] + (_sample[hi] - _sample[lo]) * (pos - lo);
    }

    public double Median => Quantile(0.5);
    public double P95 => Quantile(0.95);
    public double P99 => Quantile(0.99);
}

/// <summary>Distinct-count estimation (HyperLogLog, ~1.04/sqrt(2^precision) error).</summary>
public sealed class HyperLogLog
{
    private readonly byte[] _registers;
    private readonly int _p;

    public HyperLogLog(int precision = 14)
    {
        if (precision is < 4 or > 18) throw new ArgumentOutOfRangeException(nameof(precision));
        _p = precision;
        _registers = new byte[1 << precision];
    }

    public void Add(string value) => AddHash(XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(value)));

    public void Add(ReadOnlySpan<byte> value) => AddHash(XxHash64.HashToUInt64(value));

    private void AddHash(ulong h)
    {
        var idx = (int)(h >> (64 - _p));
        var rest = (h << _p) | (1UL << (_p - 1));
        var rank = (byte)(BitOperations.LeadingZeroCount(rest) + 1);
        if (rank > _registers[idx]) _registers[idx] = rank;
    }

    public long Estimate()
    {
        var m = _registers.Length;
        double sum = 0;
        var zeros = 0;
        foreach (var r in _registers)
        {
            sum += Math.Pow(2, -r);
            if (r == 0) zeros++;
        }
        var alpha = 0.7213 / (1 + 1.079 / m);
        var e = alpha * m * m / sum;
        if (e <= 2.5 * m && zeros > 0) e = m * Math.Log((double)m / zeros);
        return (long)Math.Round(e);
    }

    public void Merge(HyperLogLog other)
    {
        if (other._p != _p) throw new ArgumentException("precision mismatch");
        for (var i = 0; i < _registers.Length; i++) _registers[i] = Math.Max(_registers[i], other._registers[i]);
    }
}

/// <summary>Approximate frequency counts (Count-Min sketch).</summary>
public sealed class CountMinSketch
{
    private readonly long[,] _table;
    private readonly int _width;
    private readonly int _depth;

    public CountMinSketch(int width = 2048, int depth = 5)
    {
        _width = width;
        _depth = depth;
        _table = new long[depth, width];
    }

    public long Total { get; private set; }

    private int Bucket(string item, int row) =>
        (int)(XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(item), row * 0x9E3779B1L) % (ulong)_width);

    public void Add(string item, long count = 1)
    {
        Total += count;
        for (var r = 0; r < _depth; r++) _table[r, Bucket(item, r)] += count;
    }

    public long Estimate(string item)
    {
        var min = long.MaxValue;
        for (var r = 0; r < _depth; r++) min = Math.Min(min, _table[r, Bucket(item, r)]);
        return min;
    }
}

/// <summary>Heavy hitters with bounded memory (Space-Saving algorithm).</summary>
public sealed class TopK(int capacity = 100)
{
    private readonly Dictionary<string, (long Count, long Error)> _counters = new();

    public void Add(string item, long count = 1)
    {
        if (_counters.TryGetValue(item, out var c))
        {
            _counters[item] = (c.Count + count, c.Error);
            return;
        }
        if (_counters.Count < capacity)
        {
            _counters[item] = (count, 0);
            return;
        }
        var min = _counters.MinBy(kv => kv.Value.Count);
        _counters.Remove(min.Key);
        _counters[item] = (min.Value.Count + count, min.Value.Count);
    }

    public IReadOnlyList<(string Item, long Count)> Top(int k = 10) =>
        _counters.OrderByDescending(kv => kv.Value.Count).Take(k).Select(kv => (kv.Key, kv.Value.Count)).ToList();
}

/// <summary>Events per second over a sliding time horizon.</summary>
public sealed class RateMeter(TimeSpan horizon)
{
    private readonly Queue<DateTimeOffset> _events = new();

    public RateMeter() : this(TimeSpan.FromMinutes(1))
    {
    }

    public void Mark(DateTimeOffset at)
    {
        _events.Enqueue(at);
        while (_events.Count > 0 && at - _events.Peek() > horizon) _events.Dequeue();
    }

    public double PerSecond => _events.Count == 0 ? 0 : _events.Count / horizon.TotalSeconds;
}
