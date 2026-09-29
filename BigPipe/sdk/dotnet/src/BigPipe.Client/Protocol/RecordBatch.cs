using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;

namespace BigPipe.Client.Protocol;

internal static class Varint
{
    public static void WriteVarlong(IBufferWriter<byte> w, long v)
    {
        var raw = (ulong)((v << 1) ^ (v >> 63));
        var span = w.GetSpan(10);
        var i = 0;
        while (raw >= 0x80)
        {
            span[i++] = (byte)(raw | 0x80);
            raw >>= 7;
        }
        span[i++] = (byte)raw;
        w.Advance(i);
    }

    public static int SizeOf(long v)
    {
        var raw = (ulong)((v << 1) ^ (v >> 63));
        var n = 1;
        while (raw >= 0x80)
        {
            raw >>= 7;
            n++;
        }
        return n;
    }

    public static long ReadVarlong(ReadOnlySpan<byte> s, ref int pos)
    {
        ulong raw = 0;
        var shift = 0;
        while (true)
        {
            if (pos >= s.Length) throw new BigPipeException(ErrorCode.CorruptMessage, "varint truncated");
            var b = s[pos++];
            raw |= (ulong)(b & 0x7f) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) throw new BigPipeException(ErrorCode.CorruptMessage, "varint too long");
        }
        return (long)(raw >> 1) ^ -(long)(raw & 1);
    }
}

/// <summary>Encodes one Kafka magic-v2 record batch.</summary>
internal sealed class RecordBatchBuilder : IDisposable
{
    private readonly ArrayBufferWriter<byte> _body = new(4096);
    private long _baseTimestamp = -1;
    private long _maxTimestamp = -1;
    private readonly CompressionType _compression;

    public RecordBatchBuilder(CompressionType compression) => _compression = compression;

    public int Count { get; private set; }
    public int EstimatedSize => 61 + _body.WrittenCount;
    public long ProducerId { get; set; } = -1;
    public short ProducerEpoch { get; set; } = -1;
    public int BaseSequence { get; set; } = -1;

    public void Add(ReadOnlySpan<byte> key, bool keyNull, ReadOnlySpan<byte> value, bool valueNull, IReadOnlyList<Header>? headers, long timestamp)
    {
        if (Count == 0) _baseTimestamp = timestamp;
        _maxTimestamp = Math.Max(_maxTimestamp, timestamp);
        var tsDelta = timestamp - _baseTimestamp;
        long offDelta = Count;
        var len = 1 + Varint.SizeOf(tsDelta) + Varint.SizeOf(offDelta);
        len += keyNull ? Varint.SizeOf(-1) : Varint.SizeOf(key.Length) + key.Length;
        len += valueNull ? Varint.SizeOf(-1) : Varint.SizeOf(value.Length) + value.Length;
        var hc = headers?.Count ?? 0;
        len += Varint.SizeOf(hc);
        if (headers is not null)
        {
            foreach (var h in headers)
            {
                var kl = Encoding.UTF8.GetByteCount(h.Key);
                len += Varint.SizeOf(kl) + kl;
                len += h.Value is { } hv ? Varint.SizeOf(hv.Length) + hv.Length : Varint.SizeOf(-1);
            }
        }

        Varint.WriteVarlong(_body, len);
        _body.GetSpan(1)[0] = 0;
        _body.Advance(1);
        Varint.WriteVarlong(_body, tsDelta);
        Varint.WriteVarlong(_body, offDelta);
        WriteField(key, keyNull);
        WriteField(value, valueNull);
        Varint.WriteVarlong(_body, hc);
        if (headers is not null)
        {
            foreach (var h in headers)
            {
                var kb = Encoding.UTF8.GetBytes(h.Key);
                WriteField(kb, false);
                if (h.Value is { } hv) WriteField(hv.Span, false);
                else WriteField(default, true);
            }
        }
        Count++;
    }

    private void WriteField(ReadOnlySpan<byte> b, bool isNull)
    {
        if (isNull)
        {
            Varint.WriteVarlong(_body, -1);
            return;
        }
        Varint.WriteVarlong(_body, b.Length);
        b.CopyTo(_body.GetSpan(b.Length));
        _body.Advance(b.Length);
    }

    /// <summary>Returns the encoded batch (base offset 0; the broker assigns offsets).</summary>
    public byte[] Build()
    {
        var payload = Compression.Compress(_compression, _body.WrittenSpan);
        var total = 61 + payload.Length;
        var b = new byte[total];
        var s = b.AsSpan();
        BinaryPrimitives.WriteInt64BigEndian(s, 0);
        BinaryPrimitives.WriteInt32BigEndian(s[8..], total - 12);
        BinaryPrimitives.WriteInt32BigEndian(s[12..], 0);
        s[16] = 2;
        BinaryPrimitives.WriteInt16BigEndian(s[21..], (short)_compression);
        BinaryPrimitives.WriteInt32BigEndian(s[23..], Math.Max(0, Count - 1));
        BinaryPrimitives.WriteInt64BigEndian(s[27..], _baseTimestamp);
        BinaryPrimitives.WriteInt64BigEndian(s[35..], _maxTimestamp);
        BinaryPrimitives.WriteInt64BigEndian(s[43..], ProducerId);
        BinaryPrimitives.WriteInt16BigEndian(s[51..], ProducerEpoch);
        BinaryPrimitives.WriteInt32BigEndian(s[53..], BaseSequence);
        BinaryPrimitives.WriteInt32BigEndian(s[57..], Count);
        payload.CopyTo(s[61..]);
        BinaryPrimitives.WriteUInt32BigEndian(s[17..], Crc32C.Compute(s[21..]));
        return b;
    }

    public void Dispose() => _body.Clear();
}

/// <summary>Decodes stored record batches (fetch responses).</summary>
internal static class RecordBatchReader
{
    public const int HeaderSize = 61;

    /// <summary>Decodes every complete batch in <paramref name="data"/>, skipping offsets below <paramref name="minOffset"/>.</summary>
    public static void Decode(ReadOnlyMemory<byte> data, long minOffset, List<RawRecord> output, out long nextOffset)
    {
        nextOffset = minOffset;
        var pos = 0;
        var span = data.Span;
        while (pos + HeaderSize <= span.Length)
        {
            var s = span[pos..];
            var baseOffset = BinaryPrimitives.ReadInt64BigEndian(s);
            var batchLength = BinaryPrimitives.ReadInt32BigEndian(s[8..]);
            var total = batchLength + 12;
            if (batchLength < HeaderSize - 12 || pos + total > span.Length) break; // partial trailing batch
            var magic = s[16];
            if (magic != 2)
                throw new BigPipeException(ErrorCode.CorruptMessage, $"unsupported record batch magic {magic}");
            var attributes = BinaryPrimitives.ReadInt16BigEndian(s[21..]);
            var lastOffsetDelta = BinaryPrimitives.ReadInt32BigEndian(s[23..]);
            var baseTimestamp = BinaryPrimitives.ReadInt64BigEndian(s[27..]);
            var maxTimestamp = BinaryPrimitives.ReadInt64BigEndian(s[35..]);
            var count = BinaryPrimitives.ReadInt32BigEndian(s[57..]);
            var lastOffset = baseOffset + lastOffsetDelta;
            var isControl = (attributes & 0x20) != 0;
            if (lastOffset >= minOffset && !isControl)
            {
                var codec = (CompressionType)(attributes & 0x7);
                ReadOnlyMemory<byte> body = data.Slice(pos + HeaderSize, total - HeaderSize);
                if (codec != CompressionType.None) body = Compression.Decompress(codec, body.Span);
                var logAppendTime = (attributes & 0x08) != 0;
                DecodeRecords(body, count, baseOffset, baseTimestamp, logAppendTime ? maxTimestamp : -1, minOffset, output);
            }
            nextOffset = Math.Max(nextOffset, lastOffset + 1);
            pos += total;
        }
    }

    private static void DecodeRecords(ReadOnlyMemory<byte> body, int count, long baseOffset, long baseTs, long overrideTs, long minOffset, List<RawRecord> output)
    {
        var s = body.Span;
        var pos = 0;
        for (var i = 0; i < count; i++)
        {
            var len = (int)Varint.ReadVarlong(s, ref pos);
            var start = pos;
            pos++; // attributes
            var tsDelta = Varint.ReadVarlong(s, ref pos);
            var offDelta = Varint.ReadVarlong(s, ref pos);
            var key = ReadField(body, s, ref pos);
            var value = ReadField(body, s, ref pos);
            var hc = (int)Varint.ReadVarlong(s, ref pos);
            var headers = hc == 0 ? (IReadOnlyList<Header>)[] : new Header[hc];
            for (var h = 0; h < hc; h++)
            {
                var k = ReadField(body, s, ref pos);
                var v = ReadField(body, s, ref pos);
                ((Header[])headers)[h] = new Header(k is { } kk ? Encoding.UTF8.GetString(kk.Span) : "", v);
            }
            pos = start + len;
            var offset = baseOffset + offDelta;
            if (offset < minOffset) continue;
            output.Add(new RawRecord
            {
                Offset = offset,
                Timestamp = overrideTs >= 0 ? overrideTs : baseTs + tsDelta,
                Key = key,
                Value = value,
                Headers = headers,
            });
        }
    }

    private static ReadOnlyMemory<byte>? ReadField(ReadOnlyMemory<byte> body, ReadOnlySpan<byte> s, ref int pos)
    {
        var len = (int)Varint.ReadVarlong(s, ref pos);
        if (len < 0) return null;
        var m = body.Slice(pos, len);
        pos += len;
        return m;
    }
}

internal static class Compression
{
    private static readonly byte[] XerialMagic = [0x82, (byte)'S', (byte)'N', (byte)'A', (byte)'P', (byte)'P', (byte)'Y', 0];

    public static byte[] Compress(CompressionType codec, ReadOnlySpan<byte> data)
    {
        switch (codec)
        {
            case CompressionType.None:
                return data.ToArray();
            case CompressionType.Gzip:
            {
                using var ms = new MemoryStream(data.Length / 2 + 64);
                using (var gz = new GZipStream(ms, CompressionLevel.Fastest, leaveOpen: true))
                    gz.Write(data);
                return ms.ToArray();
            }
            case CompressionType.Snappy:
            {
                var block = Snappier.Snappy.CompressToArray(data);
                var out_ = new byte[16 + 4 + block.Length];
                XerialMagic.CopyTo(out_, 0);
                BinaryPrimitives.WriteInt32BigEndian(out_.AsSpan(8), 1);
                BinaryPrimitives.WriteInt32BigEndian(out_.AsSpan(12), 1);
                BinaryPrimitives.WriteInt32BigEndian(out_.AsSpan(16), block.Length);
                block.CopyTo(out_, 20);
                return out_;
            }
            case CompressionType.Lz4:
            {
                using var ms = new MemoryStream(data.Length / 2 + 64);
                using (var lz = K4os.Compression.LZ4.Streams.LZ4Stream.Encode(ms, leaveOpen: true))
                    lz.Write(data);
                return ms.ToArray();
            }
            case CompressionType.Zstd:
            {
                using var c = new ZstdSharp.Compressor(3);
                return c.Wrap(data).ToArray();
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(codec));
        }
    }

    public static byte[] Decompress(CompressionType codec, ReadOnlySpan<byte> data)
    {
        switch (codec)
        {
            case CompressionType.Gzip:
            {
                using var src = new MemoryStream(data.ToArray());
                using var gz = new GZipStream(src, CompressionMode.Decompress);
                using var dst = new MemoryStream(data.Length * 4);
                gz.CopyTo(dst);
                return dst.ToArray();
            }
            case CompressionType.Snappy:
            {
                if (data.Length >= 16 && data[..8].SequenceEqual(XerialMagic))
                {
                    using var dst = new MemoryStream(data.Length * 4);
                    var pos = 16;
                    while (pos + 4 <= data.Length)
                    {
                        var len = BinaryPrimitives.ReadInt32BigEndian(data[pos..]);
                        pos += 4;
                        dst.Write(Snappier.Snappy.DecompressToArray(data.Slice(pos, len)));
                        pos += len;
                    }
                    return dst.ToArray();
                }
                return Snappier.Snappy.DecompressToArray(data);
            }
            case CompressionType.Lz4:
            {
                using var src = new MemoryStream(data.ToArray());
                using var lz = K4os.Compression.LZ4.Streams.LZ4Stream.Decode(src);
                using var dst = new MemoryStream(data.Length * 4);
                lz.CopyTo(dst);
                return dst.ToArray();
            }
            case CompressionType.Zstd:
            {
                using var d = new ZstdSharp.Decompressor();
                return d.Unwrap(data).ToArray();
            }
            default:
                return data.ToArray();
        }
    }
}
