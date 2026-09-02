// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Text;

namespace BlackHole.Protocol;

/// <summary>
/// Turns UTF-8 header bytes back into strings without allocating a new string per message.
/// </summary>
/// <remarks>
/// Real traffic reuses a tiny set of headers - a handful of RPC method names, a few dozen topics,
/// one id per stream - so a direct-mapped cache keyed on the raw bytes hits nearly always. On a
/// miss it decodes and replaces the slot: no eviction policy to tune, no lock, and losing a race
/// costs one extra decode. Entries are immutable once published.
/// </remarks>
public sealed class HeaderCache
{
    private sealed class Entry
    {
        public required byte[] Key { get; init; }
        public required string Value { get; init; }
    }

    private readonly Entry?[] _slots;
    private readonly int _mask;
    private long _hits;
    private long _misses;

    /// <param name="capacity">Slot count, rounded up to a power of two.</param>
    public HeaderCache(int capacity = 512)
    {
        int size = 1;
        int target = Math.Clamp(capacity, 8, 1 << 16);
        while (size < target) size <<= 1;
        _slots = new Entry?[size];
        _mask = size - 1;
    }

    /// <summary>Cache hits since construction.</summary>
    public long Hits => Interlocked.Read(ref _hits);

    /// <summary>Decodes that fell through to <see cref="Encoding.UTF8"/>.</summary>
    public long Misses => Interlocked.Read(ref _misses);

    /// <summary>Returns the string for <paramref name="utf8"/>, decoding only on a miss.</summary>
    public string GetString(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
            return string.Empty;

        int slot = (int)(Hash(utf8) & (uint)_mask);
        Entry? entry = Volatile.Read(ref _slots[slot]);
        if (entry is not null && utf8.SequenceEqual(entry.Key))
        {
            Interlocked.Increment(ref _hits);
            return entry.Value;
        }

        Interlocked.Increment(ref _misses);
        string value = Encoding.UTF8.GetString(utf8);
        Volatile.Write(ref _slots[slot], new Entry { Key = utf8.ToArray(), Value = value });
        return value;
    }

    /// <summary>
    /// Pre-seeds a header the application knows it will see, so even the first message on a
    /// connection is a hit.
    /// </summary>
    public void Prime(string header)
    {
        ArgumentException.ThrowIfNullOrEmpty(header);
        int byteCount = Encoding.UTF8.GetByteCount(header);
        Span<byte> scratch = byteCount <= 256 ? stackalloc byte[256] : new byte[byteCount];
        int written = Encoding.UTF8.GetBytes(header, scratch);
        GetString(scratch[..written]);
    }

    // FNV-1a: cheap, spreads short ASCII keys well, allocation free.
    private static uint Hash(ReadOnlySpan<byte> data)
    {
        uint hash = 2166136261u;
        foreach (byte b in data)
        {
            hash ^= b;
            hash *= 16777619u;
        }
        return hash;
    }
}
