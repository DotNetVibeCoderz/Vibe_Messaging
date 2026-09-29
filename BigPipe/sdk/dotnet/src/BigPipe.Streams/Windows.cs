namespace BigPipe.Streams;

/// <summary>A key scoped to a time window. Serialized as JSON <c>{"key":…,"start":…,"end":…}</c>.</summary>
public readonly record struct Windowed<TKey>(TKey Key, long Start, long End) : IWindowedKey
{
    long IWindowedKey.WindowEnd => End;

    public DateTimeOffset StartTime => DateTimeOffset.FromUnixTimeMilliseconds(Start);
    public DateTimeOffset EndTime => DateTimeOffset.FromUnixTimeMilliseconds(End);
    public override string ToString() => $"{Key}@[{StartTime:HH:mm:ss}..{EndTime:HH:mm:ss})";
}

/// <summary>Time-based window definition.</summary>
public abstract class TimeWindows
{
    public TimeSpan GracePeriod { get; protected set; } = TimeSpan.Zero;

    /// <summary>How long closed windows stay queryable.</summary>
    public TimeSpan Retention { get; protected set; } = TimeSpan.FromHours(1);

    /// <summary>Window [start, end) intervals containing <paramref name="timestamp"/>.</summary>
    internal abstract IEnumerable<(long Start, long End)> WindowsFor(long timestamp);
}

/// <summary>Fixed-size, non-overlapping windows.</summary>
public sealed class TumblingWindow : TimeWindows
{
    private readonly long _size;

    private TumblingWindow(TimeSpan size) => _size = (long)size.TotalMilliseconds;

    public static TumblingWindow Of(TimeSpan size) => new(size);

    /// <summary>Accept late records for this long after a window ends.</summary>
    public TumblingWindow Grace(TimeSpan grace)
    {
        GracePeriod = grace;
        return this;
    }

    public TumblingWindow Retain(TimeSpan retention)
    {
        Retention = retention;
        return this;
    }

    internal override IEnumerable<(long, long)> WindowsFor(long ts)
    {
        var start = ts - (ts % _size + _size) % _size;
        yield return (start, start + _size);
    }
}

/// <summary>Fixed-size windows that advance by a smaller step (overlapping).</summary>
public sealed class HoppingWindow : TimeWindows
{
    private readonly long _size;
    private readonly long _advance;

    private HoppingWindow(TimeSpan size, TimeSpan advance)
    {
        _size = (long)size.TotalMilliseconds;
        _advance = (long)advance.TotalMilliseconds;
        if (_advance <= 0 || _advance > _size) throw new ArgumentException("advance must be in (0, size]");
    }

    public static HoppingWindow Of(TimeSpan size, TimeSpan advance) => new(size, advance);

    public HoppingWindow Grace(TimeSpan grace)
    {
        GracePeriod = grace;
        return this;
    }

    internal override IEnumerable<(long, long)> WindowsFor(long ts)
    {
        var lastStart = ts - (ts % _advance + _advance) % _advance;
        for (var start = lastStart; start > ts - _size; start -= _advance)
            yield return (start, start + _size);
    }
}

/// <summary>
/// Sliding window of fixed size ending at each record: aggregates every record whose timestamp
/// is within <c>size</c> before it (one window per distinct record time).
/// </summary>
public sealed class SlidingWindow : TimeWindows
{
    private readonly long _size;

    private SlidingWindow(TimeSpan size) => _size = (long)size.TotalMilliseconds;

    public static SlidingWindow Of(TimeSpan size) => new(size);

    public SlidingWindow Grace(TimeSpan grace)
    {
        GracePeriod = grace;
        return this;
    }

    internal long Size => _size;

    internal override IEnumerable<(long, long)> WindowsFor(long ts)
    {
        yield return (ts - _size, ts + 1);
    }
}

/// <summary>Activity sessions separated by gaps of inactivity.</summary>
public sealed class SessionWindow
{
    public TimeSpan InactivityGap { get; }
    public TimeSpan GracePeriod { get; private set; } = TimeSpan.Zero;

    private SessionWindow(TimeSpan gap) => InactivityGap = gap;

    public static SessionWindow WithInactivityGap(TimeSpan gap) => new(gap);

    public SessionWindow Grace(TimeSpan grace)
    {
        GracePeriod = grace;
        return this;
    }
}
