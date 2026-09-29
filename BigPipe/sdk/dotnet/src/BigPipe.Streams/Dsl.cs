namespace BigPipe.Streams;

/// <summary>Builds a processing topology. Mirrors the Kafka Streams DSL.</summary>
public sealed class StreamsBuilder
{
    internal List<SourceNode> Sources { get; } = [];
    internal Dictionary<string, KeyValueStore> Stores { get; } = new();
    internal List<(SinkNode Sink, SourceNode Source, string Suffix)> Repartitions { get; } = [];
    private int _counter;

    internal string NextName(string kind) => $"{kind}-{++_counter:D4}";

    /// <summary>Reads a topic as a record stream.</summary>
    public KStream<TKey, TValue> Stream<TKey, TValue>(string topic, Serde<TKey>? keySerde = null, Serde<TValue>? valueSerde = null)
    {
        var src = new SourceNode(topic, new UntypedSerde<TKey>(keySerde ?? Serde<TKey>.Default()), new UntypedSerde<TValue>(valueSerde ?? Serde<TValue>.Default()));
        Sources.Add(src);
        return new KStream<TKey, TValue>(this, src);
    }

    /// <summary>Reads a topic as a changelog table (latest value per key), materialized in a store.</summary>
    public KTable<TKey, TValue> Table<TKey, TValue>(string topic, StoreSpec? store = null, Serde<TKey>? keySerde = null, Serde<TValue>? valueSerde = null)
    {
        var src = new SourceNode(topic, new UntypedSerde<TKey>(keySerde ?? Serde<TKey>.Default()), new UntypedSerde<TValue>(valueSerde ?? Serde<TValue>.Default()));
        Sources.Add(src);
        // The source topic is itself the changelog, so no extra changelog is needed.
        var kv = AddStore(store ?? global::BigPipe.Streams.Stores.InMemory(NextName("table")), typeof(TKey), typeof(TValue), forceNoChangelog: true);
        var node = src.Add(new TableSourceNode(kv));
        return new KTable<TKey, TValue>(this, node, kv);
    }

    internal KeyValueStore AddStore(StoreSpec spec, Type key, Type value, bool forceNoChangelog = false)
    {
        if (Stores.ContainsKey(spec.Name)) throw new InvalidOperationException($"store '{spec.Name}' is defined twice");
        var s = new KeyValueStore(forceNoChangelog && spec.Changelog ? global::BigPipe.Streams.Stores.InMemory(spec.Name) : spec, key, value);
        Stores[spec.Name] = s;
        return s;
    }

    /// <summary>Routes records through an internal topic keyed by the new key (so grouping scales out).</summary>
    internal SourceNode Repartition<TKey, TValue>(Node parent, Serde<TKey> keySerde, Serde<TValue> valueSerde)
    {
        var suffix = NextName("repartition");
        var sink = parent.Add(new SinkNode(suffix, new UntypedSerde<TKey>(keySerde), new UntypedSerde<TValue>(valueSerde)));
        var src = new SourceNode(suffix, new UntypedSerde<TKey>(keySerde), new UntypedSerde<TValue>(valueSerde));
        Sources.Add(src);
        Repartitions.Add((sink, src, suffix));
        return src;
    }
}

/// <summary>An unbounded stream of key/value records.</summary>
public sealed class KStream<TKey, TValue>
{
    private readonly StreamsBuilder _b;
    internal Node Node { get; }

    internal KStream(StreamsBuilder b, Node node)
    {
        _b = b;
        Node = node;
    }

    private KStream<K2, V2> Then<K2, V2>(Action<object?, object?, long, ProcessorContext, Action<object?, object?, long>> fn) =>
        new(_b, Node.Add(new LambdaNode(fn)));

    public KStream<TKey, TValue> Filter(Func<TKey, TValue, bool> predicate) =>
        Then<TKey, TValue>((k, v, ts, _, fwd) =>
        {
            if (predicate((TKey)k!, (TValue)v!)) fwd(k, v, ts);
        });

    public KStream<TKey, TValue> FilterNot(Func<TKey, TValue, bool> predicate) => Filter((k, v) => !predicate(k, v));

    public KStream<K2, V2> Map<K2, V2>(Func<TKey, TValue, (K2 Key, V2 Value)> mapper) =>
        Then<K2, V2>((k, v, ts, _, fwd) =>
        {
            var (nk, nv) = mapper((TKey)k!, (TValue)v!);
            fwd(nk, nv, ts);
        });

    public KStream<TKey, V2> MapValues<V2>(Func<TValue, V2> mapper) =>
        Then<TKey, V2>((k, v, ts, _, fwd) => fwd(k, mapper((TValue)v!), ts));

    public KStream<TKey, V2> MapValues<V2>(Func<TKey, TValue, V2> mapper) =>
        Then<TKey, V2>((k, v, ts, _, fwd) => fwd(k, mapper((TKey)k!, (TValue)v!), ts));

    public KStream<K2, TValue> SelectKey<K2>(Func<TKey, TValue, K2> selector) =>
        Then<K2, TValue>((k, v, ts, _, fwd) => fwd(selector((TKey)k!, (TValue)v!), v, ts));

    public KStream<K2, V2> FlatMap<K2, V2>(Func<TKey, TValue, IEnumerable<(K2 Key, V2 Value)>> mapper) =>
        Then<K2, V2>((k, v, ts, _, fwd) =>
        {
            foreach (var (nk, nv) in mapper((TKey)k!, (TValue)v!)) fwd(nk, nv, ts);
        });

    public KStream<TKey, V2> FlatMapValues<V2>(Func<TValue, IEnumerable<V2>> mapper) =>
        Then<TKey, V2>((k, v, ts, _, fwd) =>
        {
            foreach (var nv in mapper((TValue)v!)) fwd(k, nv, ts);
        });

    /// <summary>Runs a side effect and passes records through unchanged.</summary>
    public KStream<TKey, TValue> Peek(Action<TKey, TValue> action) =>
        Then<TKey, TValue>((k, v, ts, _, fwd) =>
        {
            action((TKey)k!, (TValue)v!);
            fwd(k, v, ts);
        });

    /// <summary>Terminal side effect.</summary>
    public void ForEach(Action<TKey, TValue> action) => Node.Add(new LambdaNode((k, v, _, _, _) => action((TKey)k!, (TValue)v!)));

    /// <summary>Splits the stream: each record goes to the first branch whose predicate matches.</summary>
    public KStream<TKey, TValue>[] Branch(params Func<TKey, TValue, bool>[] predicates)
    {
        var branches = predicates.Select(_ => new LambdaNode((k, v, ts, ctx, fwd) => fwd(k, v, ts))).ToArray();
        Node.Add(new LambdaNode((k, v, ts, ctx, _) =>
        {
            for (var i = 0; i < predicates.Length; i++)
            {
                if (!predicates[i]((TKey)k!, (TValue)v!)) continue;
                branches[i].Process(k, v, ts, ctx);
                return;
            }
        }));
        return branches.Select(n => new KStream<TKey, TValue>(_b, n)).ToArray();
    }

    /// <summary>Combines two streams of the same types.</summary>
    public KStream<TKey, TValue> Merge(KStream<TKey, TValue> other)
    {
        var merged = new LambdaNode((k, v, ts, _, fwd) => fwd(k, v, ts));
        Node.Add(merged);
        other.Node.Add(merged);
        return new KStream<TKey, TValue>(_b, merged);
    }

    /// <summary>Writes the stream to a topic.</summary>
    public void To(string topic, Serde<TKey>? keySerde = null, Serde<TValue>? valueSerde = null) =>
        Node.Add(new SinkNode(topic, new UntypedSerde<TKey>(keySerde ?? Serde<TKey>.Default()), new UntypedSerde<TValue>(valueSerde ?? Serde<TValue>.Default())));

    /// <summary>Groups by the existing key (no repartition).</summary>
    public KGroupedStream<TKey, TValue> GroupByKey() => new(_b, Node);

    /// <summary>Groups by a new key; records are repartitioned through an internal topic.</summary>
    public KGroupedStream<K2, TValue> GroupBy<K2>(Func<TKey, TValue, K2> selector, Serde<K2>? keySerde = null, Serde<TValue>? valueSerde = null)
    {
        var rekeyed = SelectKey(selector);
        var src = _b.Repartition(rekeyed.Node, keySerde ?? Serde<K2>.Default(), valueSerde ?? Serde<TValue>.Default());
        return new KGroupedStream<K2, TValue>(_b, src);
    }

    /// <summary>Enriches each record with the table's current value for its key (inner join).</summary>
    public KStream<TKey, VR> Join<VT, VR>(KTable<TKey, VT> table, Func<TValue, VT, VR> joiner) =>
        new(_b, Node.Add(new TableJoinNode(table.Store, (_, v, t) => joiner((TValue)v!, (VT)t!), left: false)));

    /// <summary>Like <see cref="Join{VT,VR}"/> but keeps records without a table match (table value is default).</summary>
    public KStream<TKey, VR> LeftJoin<VT, VR>(KTable<TKey, VT> table, Func<TValue, VT?, VR> joiner) =>
        new(_b, Node.Add(new TableJoinNode(table.Store, (_, v, t) => joiner((TValue)v!, t is null ? default : (VT)t), left: true)));
}

/// <summary>A stream grouped by key, ready for aggregation.</summary>
public sealed class KGroupedStream<TKey, TValue>
{
    private readonly StreamsBuilder _b;
    private readonly Node _node;

    internal KGroupedStream(StreamsBuilder b, Node node)
    {
        _b = b;
        _node = node;
    }

    public KTable<TKey, TAgg> Aggregate<TAgg>(Func<TAgg> initializer, Func<TKey, TValue, TAgg, TAgg> aggregator, StoreSpec? store = null)
    {
        var kv = _b.AddStore(store ?? Stores.Persistent(_b.NextName("aggregate")), typeof(TKey), typeof(TAgg));
        var n = _node.Add(new AggregateNode(kv, () => initializer(), (k, v, a) => aggregator((TKey)k!, (TValue)v!, (TAgg)a!)));
        return new KTable<TKey, TAgg>(_b, n, kv);
    }

    public KTable<TKey, long> Count(StoreSpec? store = null) => Aggregate(() => 0L, (_, _, c) => c + 1, store);

    public KTable<TKey, TValue> Reduce(Func<TValue, TValue, TValue> reducer, StoreSpec? store = null)
    {
        var kv = _b.AddStore(store ?? Stores.Persistent(_b.NextName("reduce")), typeof(TKey), typeof(TValue));
        var n = _node.Add(new AggregateNode(kv, () => null, (_, v, a) => a is null ? v : reducer((TValue)a, (TValue)v!)));
        return new KTable<TKey, TValue>(_b, n, kv);
    }

    public TimeWindowedKStream<TKey, TValue> WindowedBy(TimeWindows windows) => new(_b, _node, windows);

    public SessionWindowedKStream<TKey, TValue> WindowedBy(SessionWindow window) => new(_b, _node, window);
}

/// <summary>A grouped stream with time windows.</summary>
public sealed class TimeWindowedKStream<TKey, TValue>
{
    private readonly StreamsBuilder _b;
    private readonly Node _node;
    private readonly TimeWindows _windows;
    private bool _emitOnClose;

    internal TimeWindowedKStream(StreamsBuilder b, Node node, TimeWindows windows)
    {
        _b = b;
        _node = node;
        _windows = windows;
    }

    /// <summary>Emit one final result per window after it closes (end + grace) instead of every update.</summary>
    public TimeWindowedKStream<TKey, TValue> EmitOnWindowClose()
    {
        _emitOnClose = true;
        return this;
    }

    public KTable<Windowed<TKey>, TAgg> Aggregate<TAgg>(Func<TAgg> initializer, Func<TKey, TValue, TAgg, TAgg> aggregator, StoreSpec? store = null)
    {
        var kv = _b.AddStore(store ?? Stores.Persistent(_b.NextName("window")), typeof(Windowed<TKey>), typeof(TAgg));
        Func<object?> init = () => initializer();
        Func<object?, object?, object?, object?> agg = (k, v, a) => aggregator((TKey)k!, (TValue)v!, (TAgg)a!);
        Func<object, long, long, object> mk = (k, s, e) => new Windowed<TKey>((TKey)k, s, e);
        Node n = _windows is SlidingWindow sw
            ? new SlidingAggregateNode(kv, sw, init, agg, mk)
            : new WindowAggregateNode(kv, _windows, init, agg, mk, _emitOnClose);
        _node.Add(n);
        return new KTable<Windowed<TKey>, TAgg>(_b, n, kv);
    }

    public KTable<Windowed<TKey>, long> Count(StoreSpec? store = null) => Aggregate(() => 0L, (_, _, c) => c + 1, store);
}

/// <summary>A grouped stream with session windows.</summary>
public sealed class SessionWindowedKStream<TKey, TValue>
{
    private readonly StreamsBuilder _b;
    private readonly Node _node;
    private readonly SessionWindow _window;

    internal SessionWindowedKStream(StreamsBuilder b, Node node, SessionWindow window)
    {
        _b = b;
        _node = node;
        _window = window;
    }

    /// <summary>Aggregates per session; <paramref name="merger"/> combines two sessions that grow together.</summary>
    public KTable<Windowed<TKey>, TAgg> Aggregate<TAgg>(Func<TAgg> initializer, Func<TKey, TValue, TAgg, TAgg> aggregator,
        Func<TKey, TAgg, TAgg, TAgg> merger, StoreSpec? store = null)
    {
        var kv = _b.AddStore(store ?? Stores.Persistent(_b.NextName("session")), typeof(Windowed<TKey>), typeof(TAgg));
        var n = _node.Add(new SessionAggregateNode(kv, _window, () => initializer(),
            (k, v, a) => aggregator((TKey)k!, (TValue)v!, (TAgg)a!),
            (k, a, b) => merger((TKey)k!, (TAgg)a!, (TAgg)b!),
            (k, s, e) => new Windowed<TKey>((TKey)k, s, e)));
        return new KTable<Windowed<TKey>, TAgg>(_b, n, kv);
    }

    public KTable<Windowed<TKey>, long> Count(StoreSpec? store = null) =>
        Aggregate(() => 0L, (_, _, c) => c + 1, (_, a, b) => a + b, store);
}

/// <summary>A changelog view: the latest value per key, backed by a state store.</summary>
public sealed class KTable<TKey, TValue>
{
    private readonly StreamsBuilder _b;
    private readonly Node _node;

    internal KTable(StreamsBuilder b, Node node, KeyValueStore store)
    {
        _b = b;
        _node = node;
        Store = store;
    }

    internal KeyValueStore Store { get; }

    /// <summary>Name for interactive queries: <c>app.Store&lt;K,V&gt;(name)</c>.</summary>
    public string StoreName => Store.Name;

    /// <summary>Every update of the table as a stream (null values are deletions).</summary>
    public KStream<TKey, TValue> ToStream() => new(_b, _node);

    public KTable<TKey, TValue> Filter(Func<TKey, TValue, bool> predicate)
    {
        var kv = _b.AddStore(Stores.InMemory(_b.NextName("table-filter")), typeof(TKey), typeof(TValue));
        var n = _node.Add(new LambdaNode((k, v, ts, _, fwd) =>
        {
            if (k is null) return;
            if (v is not null && predicate((TKey)k, (TValue)v))
            {
                kv.Put(k, v);
                fwd(k, v, ts);
            }
            else if (kv.TryGet(k, out var existing) && existing is not null)
            {
                kv.Delete(k);
                fwd(k, null, ts);
            }
        }));
        return new KTable<TKey, TValue>(_b, n, kv);
    }

    public KTable<TKey, V2> MapValues<V2>(Func<TValue, V2> mapper)
    {
        var kv = _b.AddStore(Stores.InMemory(_b.NextName("table-map")), typeof(TKey), typeof(V2));
        var n = _node.Add(new LambdaNode((k, v, ts, _, fwd) =>
        {
            if (k is null) return;
            var mapped = v is null ? (object?)null : mapper((TValue)v);
            kv.Put(k, mapped);
            fwd(k, mapped, ts);
        }));
        return new KTable<TKey, V2>(_b, n, kv);
    }
}
