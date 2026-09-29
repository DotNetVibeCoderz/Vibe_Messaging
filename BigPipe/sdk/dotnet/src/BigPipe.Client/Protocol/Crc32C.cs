using System.Buffers.Binary;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;

namespace BigPipe.Client.Protocol;

/// <summary>CRC-32C (Castagnoli) used by Kafka record batches, hardware accelerated when available.</summary>
internal static class Crc32C
{
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var t = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var c = i;
            for (var k = 0; k < 8; k++)
                c = (c & 1) != 0 ? 0x82F63B78u ^ (c >> 1) : c >> 1;
            t[i] = c;
        }
        return t;
    }

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        if (Sse42.X64.IsSupported)
        {
            ulong c = crc;
            while (data.Length >= 8)
            {
                c = Sse42.X64.Crc32(c, BinaryPrimitives.ReadUInt64LittleEndian(data));
                data = data[8..];
            }
            crc = (uint)c;
            foreach (var b in data) crc = Sse42.Crc32(crc, b);
        }
        else if (Crc32.Arm64.IsSupported)
        {
            while (data.Length >= 8)
            {
                crc = Crc32.Arm64.ComputeCrc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(data));
                data = data[8..];
            }
            foreach (var b in data) crc = Crc32.ComputeCrc32C(crc, b);
        }
        else
        {
            foreach (var b in data) crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        }
        return crc ^ 0xFFFFFFFFu;
    }
}

/// <summary>Kafka's default partitioner hash, so keyed records land where Java clients put them.</summary>
public static class Murmur2
{
    public static int Hash(ReadOnlySpan<byte> data)
    {
        const uint seed = 0x9747b28c;
        const uint m = 0x5bd1e995;
        const int r = 24;
        var length = data.Length;
        var h = seed ^ (uint)length;
        var i = 0;
        for (; i + 4 <= length; i += 4)
        {
            var k = BinaryPrimitives.ReadUInt32LittleEndian(data[i..]);
            k *= m;
            k ^= k >> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        switch (length - i)
        {
            case 3:
                h ^= (uint)data[i + 2] << 16;
                goto case 2;
            case 2:
                h ^= (uint)data[i + 1] << 8;
                goto case 1;
            case 1:
                h ^= data[i];
                h *= m;
                break;
        }
        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;
        return (int)h;
    }

    /// <summary>Partition for a key, identical to Kafka's DefaultPartitioner.</summary>
    public static int Partition(ReadOnlySpan<byte> key, int partitions) => (Hash(key) & 0x7fffffff) % Math.Max(1, partitions);
}
