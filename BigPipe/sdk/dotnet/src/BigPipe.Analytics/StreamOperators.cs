using System.Runtime.CompilerServices;
using System.Text.Json;
using BigPipe.Client;
using BigPipe.Client.Admin;

namespace BigPipe.Analytics;

/// <summary>A closed time window of items.</summary>
public sealed record Window<T>(DateTimeOffset Start, DateTimeOffset End, IReadOnlyList<T> Items)
{
    public int Count => Items.Count;
    public TimeSpan Length => End - Start;
}

/// <summary>Per-key aggregate produced by <see cref="StreamOperators.GroupWindow{T,TKey}"/>.</summary>
public sealed record KeyedWindow<TKey, T>(TKey Key, DateTimeOffset Start, DateTimeOffset End, IReadOnlyList<T> Items)
{
    public int Count => Items.Count;
}

/// <summary>Where an analytics stream comes from.</summary>
public static class BigPipeSource
{
    /// <summary>
    /// Consumes a topic over the Kafka protocol as <see cref="AnalyticsRecord"/>s.
    /// With a group, progress is committed automatically; without, reads from <paramref name="from"/>.
    /// </summary>
    public static async IAsyncEnumerable<AnalyticsRecord> Kafka(string bootstrap, string topic, string? group = null,
        OffsetReset from = OffsetReset.Latest, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var consumer = new ConsumerBuilder<string?, string?>(new ConsumerConfig
        {
            Bootstrap = bootstrap,
            GroupId = group ?? $"analytics-{Guid.NewGuid():N}",
            AutoOffsetReset = from,
            EnableAutoCommit = group is not null,
        }).Build();
        consumer.Subscribe(topic);
        await foreach (var r in consumer.ConsumeAsync(ct).ConfigureAwait(false))
            yield return AnalyticsRecord.From(r);
    }

    /// <summary>Streams a topic over HTTP (SSE) with an optional server-side bpql filter.</summary>
    public static async IAsyncEnumerable<AnalyticsRecord> Http(string httpUrl, string topic, string from = "latest", string? filter = null,
        string? group = null, [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var http = new BigPipeHttpClient(httpUrl);
        await foreach (var r in http.StreamAsync(topic, from, filter, group, ct).ConfigureAwait(false))
            yield return AnalyticsRecord.From(r);
    }

    /// <summary>An in-memory sequence as a stream (tests, notebooks, replays).</summary>
    public static async IAsyncEnumerable<AnalyticsRecord> FromEnumerable(IEnumerable<AnalyticsRecord> records, TimeSpan? delay = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var r in records)
        {
            ct.ThrowIfCancellationRequested();
            if (delay is { } d && d > TimeSpan.Zero) await Task.Delay(d, ct).ConfigureAwait(false);
            yield return r;
        }
    }
}

/// <summary>LINQ-style operators for realtime analytics over <see cref="IAsyncEnumerable{T}"/>.</summary>
public static class StreamOperators
{
    public static async IAsyncEnumerable<T> WhereAsync<T>(this IAsyncEnumerable<T> source, Func<T, bool> predicate,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var x in source.WithCancellation(ct).ConfigureAwait(false))
            if (predicate(x)) yield return x;
    }

    public static async IAsyncEnumerable<TOut> SelectAsync<T, TOut>(this IAsyncEnumerable<T> source, Func<T, TOut> selector,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var x in source.WithCancellation(ct).ConfigureAwait(false))
            yield return selector(x);
    }

    public static async IAsyncEnumerable<T> TakeAsync<T>(this IAsyncEnumerable<T> source, int count,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (count <= 0) yield break;
        var n = 0;
        await foreach (var x in source.WithCancellation(ct).ConfigureAwait(false))
        {
            yield return x;
            if (++n >= count) yield break;
        }
    }

    /// <summary>Collects the stream into a list (optionally stopping after <paramref name="max"/> items).</summary>
    public static async Task<List<T>> CollectAsync<T>(this IAsyncEnumerable<T> source, int max = int.MaxValue, CancellationToken ct = default)
    {
        var list = new List<T>();
        if (max <= 0) return list;
        await foreach (var x in source.WithCancellation(ct).ConfigureAwait(false))
        {
            list.Add(x);
            if (list.Count >= max) break;
        }
        return list;
    }

    /// <summary>
    /// Tumbling windows by event time (<paramref name="time"/>, default: record timestamp).
    /// A window is emitted when an item past its end arrives, and at the end of the stream.
    /// </summary>
    public static async IAsyncEnumerable<Window<T>> TumblingWindow<T>(this IAsyncEnumerable<T> source, TimeSpan size,
        Func<T, DateTimeOffset> time, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var sizeTicks = size.Ticks;
        long? current = null;
        var items = new List<T>();
        await foreach (var x in source.WithCancellation(ct).ConfigureAwait(false))
        {
            var t = time(x).UtcTicks;
            var start = t - t % sizeTicks;
            if (current is { } c && start > c)
            {
                yield return new Window<T>(new DateTimeOffset(c, TimeSpan.Zero), new DateTimeOffset(c + sizeTicks, TimeSpan.Zero), items);
                items = [];
            }
            if (current is null || start > current) current = start;
            items.Add(x);
        }
        if (current is { } last && items.Count > 0)
            yield return new Window<T>(new DateTimeOffset(last, TimeSpan.Zero), new DateTimeOffset(last + sizeTicks, TimeSpan.Zero), items);
    }

    public static IAsyncEnumerable<Window<AnalyticsRecord>> TumblingWindow(this IAsyncEnumerable<AnalyticsRecord> source, TimeSpan size, CancellationToken ct = default) =>
        source.TumblingWindow(size, r => r.Timestamp, ct);

    /// <summary>Count-based windows: every <paramref name="size"/> items, sliding by <paramref name="slide"/>.</summary>
    public static async IAsyncEnumerable<IReadOnlyList<T>> CountWindow<T>(this IAsyncEnumerable<T> source, int size, int? slide = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var step = slide ?? size;
        var buf = new List<T>(size);
        var sinceEmit = step; // emit as soon as the first window is full
        await foreach (var x in source.WithCancellation(ct).ConfigureAwait(false))
        {
            buf.Add(x);
            if (buf.Count > size) buf.RemoveAt(0);
            if (buf.Count < size) continue;
            if (sinceEmit >= step)
            {
                sinceEmit = 0;
                yield return buf.ToArray();
            }
            sinceEmit++;
        }
    }

    /// <summary>Sliding time window: after every item, the items within <paramref name="size"/> before it.</summary>
    public static async IAsyncEnumerable<Window<T>> SlidingWindow<T>(this IAsyncEnumerable<T> source, TimeSpan size,
        Func<T, DateTimeOffset> time, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var buf = new Queue<T>();
        await foreach (var x in source.WithCancellation(ct).ConfigureAwait(false))
        {
            var now = time(x);
            buf.Enqueue(x);
            while (buf.Count > 0 && now - time(buf.Peek()) > size) buf.Dequeue();
            yield return new Window<T>(now - size, now, buf.ToArray());
        }
    }

    /// <summary>Tumbling windows split by key.</summary>
    public static async IAsyncEnumerable<KeyedWindow<TKey, T>> GroupWindow<T, TKey>(this IAsyncEnumerable<Window<T>> windows, Func<T, TKey> key,
        [EnumeratorCancellation] CancellationToken ct = default) where TKey : notnull
    {
        await foreach (var w in windows.WithCancellation(ct).ConfigureAwait(false))
            foreach (var g in w.Items.GroupBy(key))
                yield return new KeyedWindow<TKey, T>(g.Key, w.Start, w.End, g.ToList());
    }

    /// <summary>Scores a numeric field with a detector and yields every observation with its score.</summary>
    public static async IAsyncEnumerable<(T Item, AnomalyScore Score)> Detect<T>(this IAsyncEnumerable<T> source, Func<T, double> value,
        IAnomalyDetector detector, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var x in source.WithCancellation(ct).ConfigureAwait(false))
            yield return (x, detector.Observe(value(x)));
    }

    /// <summary>Only the anomalies.</summary>
    public static IAsyncEnumerable<(T Item, AnomalyScore Score)> Anomalies<T>(this IAsyncEnumerable<T> source, Func<T, double> value,
        IAnomalyDetector detector, CancellationToken ct = default) =>
        source.Detect(value, detector, ct).WhereAsync(x => x.Score.IsAnomaly, ct);

    /// <summary>Writes each item as JSON to a topic (fire-and-forget batching through the producer).</summary>
    public static async Task SinkToBigPipeAsync<T>(this IAsyncEnumerable<T> source, Producer<string?, string> producer, string topic,
        Func<T, string?>? key = null, CancellationToken ct = default)
    {
        var pending = new List<Task>();
        await foreach (var x in source.WithCancellation(ct).ConfigureAwait(false))
        {
            pending.Add(producer.SendAsync(topic, key?.Invoke(x), x as string ?? JsonSerializer.Serialize(x, BigPipeJson.Options), ct: ct));
            if (pending.Count >= 1000)
            {
                await Task.WhenAll(pending).ConfigureAwait(false);
                pending.Clear();
            }
        }
        await Task.WhenAll(pending).ConfigureAwait(false);
    }
}

/// <summary>Summary statistics of a numeric field over a window.</summary>
public sealed record WindowSummary(DateTimeOffset Start, DateTimeOffset End, long Count, double Sum, double Mean, double StdDev, double Min, double Max, double P50, double P95)
{
    public static WindowSummary Of(Window<AnalyticsRecord> w, string field)
    {
        var stats = new RunningStats();
        var q = new QuantileSketch(Math.Max(16, w.Count));
        foreach (var r in w.Items)
        {
            var v = r.Num(field);
            stats.Add(v);
            q.Add(v);
        }
        return new WindowSummary(w.Start, w.End, stats.Count, stats.Sum, stats.Mean, stats.StdDev,
            stats.Count > 0 ? stats.Min : double.NaN, stats.Count > 0 ? stats.Max : double.NaN, q.Median, q.P95);
    }
}
