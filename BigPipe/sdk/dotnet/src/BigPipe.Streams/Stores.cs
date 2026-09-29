using System.Collections.Concurrent;

namespace BigPipe.Streams;

/// <summary>Describes a named state store.</summary>
public sealed class StoreSpec
{
    internal StoreSpec(string name, bool changelog)
    {
        Name = name;
        Changelog = changelog;
    }

    public string Name { get; }

    /// <summary>Changes are written to <c>&lt;application-id&gt;-&lt;store&gt;-changelog</c> and replayed on startup/rebalance.</summary>
    public bool Changelog { get; }
}

/// <summary>State store factories.</summary>
public static class Stores
{
    /// <summary>Memory-resident store made durable through a changelog topic.</summary>
    public static StoreSpec Persistent(string name) => new(name, changelog: true);

    /// <summary>Memory-resident store without a changelog (state is lost on restart).</summary>
    public static StoreSpec InMemory(string name) => new(name, changelog: false);

    /// <summary>
    /// Name kept for source compatibility with the design sample; BigPipe.Streams stores are
    /// memory-resident with changelog durability (same as <see cref="Persistent"/>).
    /// </summary>
    public static StoreSpec RocksDb(string name) => Persistent(name);
}

/// <summary>Read-only view of a store for interactive queries.</summary>
public interface IReadOnlyStore<TKey, TValue>
{
    string Name { get; }
    bool TryGet(TKey key, out TValue value);
    IEnumerable<KeyValuePair<TKey, TValue>> All();
    int Count { get; }
}

internal sealed class KeyValueStore(StoreSpec spec, Type keyType, Type valueType)
{
    private readonly ConcurrentDictionary<object, object?> _data = new();
    private readonly ConcurrentDictionary<object, byte> _dirty = new();

    public StoreSpec Spec { get; } = spec;
    public Type KeyType { get; } = keyType;
    public Type ValueType { get; } = valueType;
    public string Name => Spec.Name;

    public bool TryGet(object key, out object? value) => _data.TryGetValue(key, out value);

    public void Put(object key, object? value)
    {
        if (value is null) _data.TryRemove(key, out _);
        else _data[key] = value;
        if (Spec.Changelog) _dirty[key] = 0;
    }

    public void Delete(object key) => Put(key, null);

    public IEnumerable<KeyValuePair<object, object?>> All() => _data;

    public int Count => _data.Count;

    /// <summary>Keys changed since the last call (for the changelog).</summary>
    public List<(object Key, object? Value)> DrainDirty()
    {
        var list = new List<(object, object?)>();
        foreach (var k in _dirty.Keys)
        {
            _dirty.TryRemove(k, out _);
            list.Add((k, _data.TryGetValue(k, out var v) ? v : null));
        }
        return list;
    }

    public void RestorePut(object key, object? value)
    {
        if (value is null) _data.TryRemove(key, out _);
        else _data[key] = value;
    }

    public void Clear() => _data.Clear();
}

internal sealed class TypedStoreView<TKey, TValue>(KeyValueStore store) : IReadOnlyStore<TKey, TValue>
{
    public string Name => store.Name;

    public bool TryGet(TKey key, out TValue value)
    {
        if (store.TryGet(key!, out var v) && v is TValue t)
        {
            value = t;
            return true;
        }
        value = default!;
        return false;
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> All() =>
        store.All().Where(kv => kv.Value is TValue).Select(kv => new KeyValuePair<TKey, TValue>((TKey)kv.Key, (TValue)kv.Value!));

    public int Count => store.Count;
}
