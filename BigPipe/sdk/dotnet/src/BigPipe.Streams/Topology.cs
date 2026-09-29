namespace BigPipe.Streams;

/// <summary>Per-record context available to processors.</summary>
public sealed class RecordContext
{
    public string Topic { get; internal set; } = "";
    public int Partition { get; internal set; }
    public long Offset { get; internal set; }
    public long Timestamp { get; internal set; }
}

internal sealed class ProcessorContext
{
    public required StreamsRuntime Runtime { get; init; }
    public RecordContext Record { get; } = new();
    public long StreamTime { get; set; } = long.MinValue;
}

internal abstract class Node
{
    public List<Node> Children { get; } = [];

    public abstract void Process(object? key, object? value, long timestamp, ProcessorContext ctx);

    /// <summary>Called when stream time advances (window closing).</summary>
    public virtual void Punctuate(ProcessorContext ctx)
    {
    }

    protected void Forward(object? key, object? value, long timestamp, ProcessorContext ctx)
    {
        foreach (var c in Children) c.Process(key, value, timestamp, ctx);
    }

    public T Add<T>(T child) where T : Node
    {
        Children.Add(child);
        return child;
    }
}

internal sealed class SourceNode(string topic, IUntypedSerde keySerde, IUntypedSerde valueSerde) : Node
{
    public string Topic { get; set; } = topic;
    public IUntypedSerde KeySerde { get; } = keySerde;
    public IUntypedSerde ValueSerde { get; } = valueSerde;

    public override void Process(object? key, object? value, long timestamp, ProcessorContext ctx) => Forward(key, value, timestamp, ctx);
}

internal sealed class LambdaNode(Action<object?, object?, long, ProcessorContext, Action<object?, object?, long>> fn) : Node
{
    public override void Process(object? key, object? value, long timestamp, ProcessorContext ctx) =>
        fn(key, value, timestamp, ctx, (k, v, ts) => Forward(k, v, ts, ctx));
}

internal sealed class SinkNode(string topic, IUntypedSerde keySerde, IUntypedSerde valueSerde) : Node
{
    public string Topic { get; set; } = topic;

    public override void Process(object? key, object? value, long timestamp, ProcessorContext ctx) =>
        ctx.Runtime.Emit(Topic, keySerde.Serialize(key, Topic, true), valueSerde.Serialize(value, Topic, false), timestamp);
}

internal sealed class AggregateNode(KeyValueStore store, Func<object?> init, Func<object?, object?, object?, object?> agg) : Node
{
    public override void Process(object? key, object? value, long timestamp, ProcessorContext ctx)
    {
        if (key is null) return; // records without a key cannot be grouped
        var acc = store.TryGet(key, out var cur) ? cur : init();
        acc = agg(key, value, acc);
        store.Put(key, acc);
        Forward(key, acc, timestamp, ctx);
    }
}

/// <summary>Tumbling/hopping window aggregation with grace, optional emit-on-close.</summary>
internal sealed class WindowAggregateNode(
    KeyValueStore store, TimeWindows windows, Func<object?> init, Func<object?, object?, object?, object?> agg,
    Func<object, long, long, object> makeWindowed, bool emitOnClose) : Node
{
    private readonly SortedDictionary<long, List<object>> _openByEnd = new();

    public override void Process(object? key, object? value, long timestamp, ProcessorContext ctx)
    {
        if (key is null) return;
        var grace = (long)windows.GracePeriod.TotalMilliseconds;
        foreach (var (start, end) in windows.WindowsFor(timestamp))
        {
            if (ctx.StreamTime != long.MinValue && end + grace <= ctx.StreamTime) continue; // late: window closed
            var wkey = makeWindowed(key, start, end);
            var fresh = !store.TryGet(wkey, out var cur);
            var acc = agg(key, value, fresh ? init() : cur);
            store.Put(wkey, acc);
            if (emitOnClose)
            {
                if (fresh)
                {
                    if (!_openByEnd.TryGetValue(end, out var list)) _openByEnd[end] = list = [];
                    list.Add(wkey);
                }
            }
            else
            {
                Forward(wkey, acc, timestamp, ctx);
            }
        }
    }

    public override void Punctuate(ProcessorContext ctx)
    {
        var grace = (long)windows.GracePeriod.TotalMilliseconds;
        var retention = (long)windows.Retention.TotalMilliseconds;
        while (_openByEnd.Count > 0)
        {
            var first = _openByEnd.First();
            if (first.Key + grace > ctx.StreamTime) break;
            _openByEnd.Remove(first.Key);
            foreach (var wkey in first.Value)
                if (store.TryGet(wkey, out var acc)) Forward(wkey, acc, first.Key - 1, ctx);
        }
        // Expire windows past retention.
        if (retention > 0 && store.Count > 0 && ctx.StreamTime != long.MinValue)
        {
            foreach (var kv in store.All().ToList())
            {
                if (kv.Key is IWindowedKey w && w.WindowEnd + grace + retention < ctx.StreamTime)
                    store.Delete(kv.Key);
            }
        }
    }
}

/// <summary>Sliding windows: one window per record covering the preceding <c>size</c>.</summary>
internal sealed class SlidingAggregateNode(
    KeyValueStore store, SlidingWindow window, Func<object?> init, Func<object?, object?, object?, object?> agg,
    Func<object, long, long, object> makeWindowed) : Node
{
    private readonly Dictionary<object, LinkedList<(long Ts, object? Value)>> _buffers = new();

    public override void Process(object? key, object? value, long timestamp, ProcessorContext ctx)
    {
        if (key is null) return;
        if (!_buffers.TryGetValue(key, out var buf)) _buffers[key] = buf = new LinkedList<(long, object?)>();
        buf.AddLast((timestamp, value));
        var from = timestamp - window.Size;
        while (buf.First is { } f && f.Value.Ts < from) buf.RemoveFirst();
        var acc = init();
        foreach (var (ts, v) in buf)
            if (ts <= timestamp) acc = agg(key, v, acc);
        var wkey = makeWindowed(key, from, timestamp + 1);
        store.Put(wkey, acc);
        Forward(wkey, acc, timestamp, ctx);
    }
}

/// <summary>Session windows merging activity closer than the inactivity gap.</summary>
internal sealed class SessionAggregateNode(
    KeyValueStore store, SessionWindow window, Func<object?> init, Func<object?, object?, object?, object?> agg,
    Func<object?, object?, object?, object?> merger, Func<object, long, long, object> makeWindowed) : Node
{
    private readonly Dictionary<object, List<(long Start, long End, object? Acc)>> _sessions = new();

    public override void Process(object? key, object? value, long timestamp, ProcessorContext ctx)
    {
        if (key is null) return;
        var gap = (long)window.InactivityGap.TotalMilliseconds;
        if (!_sessions.TryGetValue(key, out var list)) _sessions[key] = list = [];
        var overlapping = list.Where(s => timestamp >= s.Start - gap && timestamp <= s.End + gap).ToList();
        var start = timestamp;
        var end = timestamp;
        var acc = agg(key, value, init());
        foreach (var s in overlapping)
        {
            list.Remove(s);
            store.Delete(makeWindowed(key, s.Start, s.End));
            // Kafka semantics: tell downstream the merged-away session is gone.
            Forward(makeWindowed(key, s.Start, s.End), null, timestamp, ctx);
            start = Math.Min(start, s.Start);
            end = Math.Max(end, s.End);
            acc = merger(key, s.Acc, acc);
        }
        list.Add((start, end, acc));
        var wkey = makeWindowed(key, start, end);
        store.Put(wkey, acc);
        Forward(wkey, acc, timestamp, ctx);
    }
}

internal sealed class TableSourceNode(KeyValueStore store) : Node
{
    public KeyValueStore Store { get; } = store;

    public override void Process(object? key, object? value, long timestamp, ProcessorContext ctx)
    {
        if (key is null) return;
        Store.Put(key, value);
        Forward(key, value, timestamp, ctx);
    }
}

internal sealed class TableJoinNode(KeyValueStore table, Func<object?, object?, object?, object?> joiner, bool left) : Node
{
    public override void Process(object? key, object? value, long timestamp, ProcessorContext ctx)
    {
        object? other = null;
        var found = key is not null && table.TryGet(key, out other);
        if (!found && !left) return;
        Forward(key, joiner(key, value, found ? other : null), timestamp, ctx);
    }
}

internal interface IWindowedKey
{
    long WindowEnd { get; }
}

