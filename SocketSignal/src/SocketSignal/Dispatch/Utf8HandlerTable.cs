// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Text;

namespace SocketSignal.Dispatch;

/// <summary>
/// Maps a UTF-8 method name to its handler without ever materialising a <see cref="string"/>.
/// </summary>
/// <remarks>
/// A <see cref="Dictionary{TKey,TValue}"/> keyed by <c>string</c> would force one UTF-16 allocation
/// per received frame just to look the handler up. This is a small open-addressed table probed with
/// the raw bytes off the receive buffer instead. Registrations are expected at start-up and are
/// copy-on-write under a lock, so the read path is lock free and never sees a torn table.
/// </remarks>
internal sealed class Utf8HandlerTable
{
    private readonly Dictionary<string, HandlerEntry> _source = new(StringComparer.Ordinal);
    private volatile Bucket[] _buckets = new Bucket[8];

    private struct Bucket
    {
        public byte[]? Key;
        public uint Hash;
        public HandlerEntry? Value;
    }

    public int Count
    {
        get { lock (_source) return _source.Count; }
    }

    public IReadOnlyCollection<string> Methods
    {
        get { lock (_source) return _source.Keys.ToArray(); }
    }

    public void Set(string method, HandlerEntry entry)
    {
        lock (_source)
        {
            _source[method] = entry;
            Rebuild();
        }
    }

    public bool Remove(string method)
    {
        lock (_source)
        {
            if (!_source.Remove(method)) return false;
            Rebuild();
            return true;
        }
    }

    /// <summary>Looks a handler up by its raw UTF-8 name. Returns null when nothing is registered.</summary>
    public HandlerEntry? Find(ReadOnlySpan<byte> method)
    {
        Bucket[] buckets = _buckets;
        uint mask = (uint)buckets.Length - 1;
        uint hash = Hash(method);
        uint slot = hash & mask;

        for (uint probe = 0; probe <= mask; probe++)
        {
            ref Bucket bucket = ref buckets[slot];
            if (bucket.Key is null) return null;
            if (bucket.Hash == hash && method.SequenceEqual(bucket.Key))
                return bucket.Value;
            slot = (slot + 1) & mask;
        }
        return null;
    }

    private void Rebuild()
    {
        int capacity = 8;
        while (capacity < _source.Count * 2) capacity <<= 1;

        var buckets = new Bucket[capacity];
        uint mask = (uint)capacity - 1;

        foreach ((string method, HandlerEntry entry) in _source)
        {
            byte[] key = Encoding.UTF8.GetBytes(method);
            uint hash = Hash(key);
            uint slot = hash & mask;
            while (buckets[slot].Key is not null)
                slot = (slot + 1) & mask;
            buckets[slot] = new Bucket { Key = key, Hash = hash, Value = entry };
        }

        _buckets = buckets;
    }

    /// <summary>FNV-1a. Method names are short, so a byte-at-a-time hash beats anything with set-up cost.</summary>
    private static uint Hash(ReadOnlySpan<byte> data)
    {
        uint hash = 2166136261u;
        for (int i = 0; i < data.Length; i++)
        {
            hash ^= data[i];
            hash *= 16777619u;
        }
        return hash;
    }
}
