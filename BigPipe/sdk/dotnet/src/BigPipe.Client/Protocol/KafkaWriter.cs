using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace BigPipe.Client.Protocol;

/// <summary>
/// Big-endian Kafka primitive writer over a pooled, growable buffer.
/// </summary>
internal sealed class KafkaWriter : IDisposable
{
    private byte[] _buffer;
    private int _length;

    public KafkaWriter(int initialCapacity = 256)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
    }

    public int Length => _length;

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _length);

    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _length);

    private Span<byte> Reserve(int count)
    {
        if (_length + count > _buffer.Length)
        {
            var bigger = ArrayPool<byte>.Shared.Rent(Math.Max(_buffer.Length * 2, _length + count));
            _buffer.AsSpan(0, _length).CopyTo(bigger);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = bigger;
        }
        var span = _buffer.AsSpan(_length, count);
        _length += count;
        return span;
    }

    public void Int8(sbyte v) => Reserve(1)[0] = (byte)v;
    public void Bool(bool v) => Reserve(1)[0] = v ? (byte)1 : (byte)0;
    public void Int16(short v) => BinaryPrimitives.WriteInt16BigEndian(Reserve(2), v);
    public void Int32(int v) => BinaryPrimitives.WriteInt32BigEndian(Reserve(4), v);
    public void Int64(long v) => BinaryPrimitives.WriteInt64BigEndian(Reserve(8), v);

    public void String(string? s)
    {
        if (s is null)
        {
            Int16(-1);
            return;
        }
        var n = Encoding.UTF8.GetByteCount(s);
        Int16((short)n);
        Encoding.UTF8.GetBytes(s, Reserve(n));
    }

    public void Bytes(ReadOnlySpan<byte> b)
    {
        Int32(b.Length);
        b.CopyTo(Reserve(b.Length));
    }

    public void NullableBytes(ReadOnlyMemory<byte>? b)
    {
        if (b is null)
        {
            Int32(-1);
            return;
        }
        Bytes(b.Value.Span);
    }

    public void Raw(ReadOnlySpan<byte> b) => b.CopyTo(Reserve(b.Length));

    public void ArrayLength(int n) => Int32(n);

    /// <summary>Overwrites 4 bytes at <paramref name="position"/> (frame size prefix).</summary>
    public void PatchInt32(int position, int value) => BinaryPrimitives.WriteInt32BigEndian(_buffer.AsSpan(position, 4), value);

    public void Dispose()
    {
        if (_buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = [];
        }
    }
}

/// <summary>
/// Big-endian Kafka primitive reader over a response frame.
/// </summary>
internal struct KafkaReader
{
    private readonly ReadOnlyMemory<byte> _data;
    private int _pos;

    public KafkaReader(ReadOnlyMemory<byte> data)
    {
        _data = data;
        _pos = 0;
    }

    public readonly int Remaining => _data.Length - _pos;

    private ReadOnlySpan<byte> Take(int n)
    {
        if (_pos + n > _data.Length)
            throw new BigPipeException(ErrorCode.CorruptMessage, "response frame truncated");
        var s = _data.Span.Slice(_pos, n);
        _pos += n;
        return s;
    }

    public sbyte Int8() => (sbyte)Take(1)[0];
    public bool Bool() => Take(1)[0] != 0;
    public short Int16() => BinaryPrimitives.ReadInt16BigEndian(Take(2));
    public int Int32() => BinaryPrimitives.ReadInt32BigEndian(Take(4));
    public long Int64() => BinaryPrimitives.ReadInt64BigEndian(Take(8));

    public string String() => NullableString() ?? throw new BigPipeException(ErrorCode.CorruptMessage, "unexpected null string");

    public string? NullableString()
    {
        var n = Int16();
        return n < 0 ? null : Encoding.UTF8.GetString(Take(n));
    }

    public ReadOnlyMemory<byte>? NullableBytes()
    {
        var n = Int32();
        if (n < 0) return null;
        if (_pos + n > _data.Length)
            throw new BigPipeException(ErrorCode.CorruptMessage, "response frame truncated");
        var m = _data.Slice(_pos, n);
        _pos += n;
        return m;
    }

    public ReadOnlyMemory<byte> Bytes() => NullableBytes() ?? ReadOnlyMemory<byte>.Empty;

    public int ArrayLength() => Int32();
}
