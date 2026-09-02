// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace BlackHole.Protocol;

/// <summary>
/// The one and only place the BlackHole wire format is written and parsed. Both ends of every
/// connection go through this type, so the format can never drift between client and server.
/// </summary>
/// <remarks>
/// <code>
/// +----------------+------+-------+--------------+---------------+--------+---------+
/// | FrameLength(4) | Type | Flags | HeaderLen(2) | CorrelationId | Header | Payload |
/// |    int32 LE    |  u8  |  u8   |  uint16 LE   |    int64 LE   | UTF-8  |  bytes  |
/// +----------------+------+-------+--------------+---------------+--------+---------+
///  counts every byte after itself
/// </code>
/// The 16-byte prefix keeps the payload 8-byte aligned inside the frame and costs 8 bytes less per
/// message than the v2 GUID-based header.
/// </remarks>
public static class FrameCodec
{
    /// <summary>Size of the length prefix itself.</summary>
    public const int LengthPrefixSize = 4;

    /// <summary>Bytes between the length prefix and the header text.</summary>
    public const int FixedHeaderSize = 12;

    /// <summary>Total bytes before the header text.</summary>
    public const int PrefixSize = LengthPrefixSize + FixedHeaderSize;

    /// <summary>Largest UTF-8 header the two-byte length field can describe.</summary>
    public const int MaxHeaderLength = ushort.MaxValue;

    /// <summary>Default cap on a single frame (16 MiB), enforced while parsing.</summary>
    public const int DefaultMaxFrameLength = 16 * 1024 * 1024;

    /// <summary>
    /// Serialises <paramref name="message"/> into <paramref name="writer"/> and returns the total
    /// bytes written. The payload is copied straight through the writer, so a large body never
    /// forces one oversized contiguous buffer.
    /// </summary>
    public static int Write(IBufferWriter<byte> writer, in BlackHoleMessage message)
    {
        ArgumentNullException.ThrowIfNull(writer);

        string header = message.Header ?? string.Empty;
        int headerLength = header.Length == 0 ? 0 : Encoding.UTF8.GetByteCount(header);
        if (headerLength > MaxHeaderLength)
            throw new BlackHoleProtocolException($"Header is {headerLength} bytes; the limit is {MaxHeaderLength}.");

        int payloadLength = message.Payload.Length;
        long frameLength = (long)FixedHeaderSize + headerLength + payloadLength;
        if (frameLength > int.MaxValue)
            throw new BlackHoleProtocolException("Frame exceeds int32 addressing.");

        Span<byte> span = writer.GetSpan(PrefixSize + headerLength);
        BinaryPrimitives.WriteInt32LittleEndian(span, (int)frameLength);
        span[4] = (byte)message.Type;
        span[5] = (byte)message.Flags;
        BinaryPrimitives.WriteUInt16LittleEndian(span[6..], (ushort)headerLength);
        BinaryPrimitives.WriteInt64LittleEndian(span[8..], message.CorrelationId);
        if (headerLength > 0)
            Encoding.UTF8.GetBytes(header, span.Slice(PrefixSize, headerLength));
        writer.Advance(PrefixSize + headerLength);

        if (payloadLength > 0)
            writer.Write(message.Payload.Span);

        return LengthPrefixSize + (int)frameLength;
    }

    /// <summary>
    /// Parses one frame off the front of <paramref name="buffer"/>, advancing it past what it
    /// consumed. Returns false when the buffer does not hold a whole frame yet.
    /// </summary>
    /// <param name="buffer">Unparsed bytes; sliced past the frame that was read.</param>
    /// <param name="headerCache">Cache used to turn header bytes back into a string.</param>
    /// <param name="maxFrameLength">Largest frame accepted before the connection is failed.</param>
    /// <param name="message">The parsed frame when this returns true.</param>
    /// <param name="rentedPayload">
    /// Non-null when the payload straddled segment boundaries and had to be copied into a pooled
    /// array. The caller returns it to <see cref="ArrayPool{T}.Shared"/> once dispatch is done.
    /// </param>
    /// <exception cref="BlackHoleProtocolException">The frame header is not self-consistent.</exception>
    public static bool TryRead(
        ref ReadOnlySequence<byte> buffer,
        HeaderCache headerCache,
        int maxFrameLength,
        out BlackHoleMessage message,
        out byte[]? rentedPayload)
    {
        message = default;
        rentedPayload = null;

        if (buffer.Length < LengthPrefixSize)
            return false;

        int frameLength = ReadInt32(buffer);
        if (frameLength < FixedHeaderSize)
            throw new BlackHoleProtocolException(
                $"Frame length {frameLength} is below the {FixedHeaderSize}-byte minimum; the stream is out of sync.");
        if (frameLength > maxFrameLength)
            throw new BlackHoleProtocolException(
                $"Frame length {frameLength} exceeds the {maxFrameLength}-byte limit.");

        if (buffer.Length < LengthPrefixSize + frameLength)
            return false;

        ReadOnlySequence<byte> frame = buffer.Slice(LengthPrefixSize, frameLength);

        Span<byte> fixedScratch = stackalloc byte[FixedHeaderSize];
        ReadOnlySequence<byte> fixedSeq = frame.Slice(0, FixedHeaderSize);
        scoped ReadOnlySpan<byte> fixedSpan;
        if (fixedSeq.IsSingleSegment)
        {
            fixedSpan = fixedSeq.FirstSpan;
        }
        else
        {
            fixedSeq.CopyTo(fixedScratch);
            fixedSpan = fixedScratch;
        }

        var type = (MessageType)fixedSpan[0];
        var flags = (MessageFlags)fixedSpan[1];
        int headerLength = BinaryPrimitives.ReadUInt16LittleEndian(fixedSpan[2..]);
        long correlationId = BinaryPrimitives.ReadInt64LittleEndian(fixedSpan[4..]);

        if (FixedHeaderSize + headerLength > frameLength)
            throw new BlackHoleProtocolException(
                $"Header length {headerLength} does not fit in a {frameLength}-byte frame.");

        string header = headerLength == 0
            ? string.Empty
            : ReadHeader(frame.Slice(FixedHeaderSize, headerLength), headerCache);

        int payloadLength = frameLength - FixedHeaderSize - headerLength;
        ReadOnlyMemory<byte> payload = default;
        if (payloadLength > 0)
        {
            ReadOnlySequence<byte> payloadSeq = frame.Slice(FixedHeaderSize + headerLength);
            if (payloadSeq.IsSingleSegment)
            {
                // Zero-copy: this memory stays valid until the pipe reader is advanced.
                payload = payloadSeq.First;
            }
            else
            {
                rentedPayload = ArrayPool<byte>.Shared.Rent(payloadLength);
                payloadSeq.CopyTo(rentedPayload);
                payload = rentedPayload.AsMemory(0, payloadLength);
            }
        }

        message = new BlackHoleMessage(type, header, payload, correlationId, flags);
        buffer = buffer.Slice(LengthPrefixSize + frameLength);
        return true;
    }

    private static int ReadInt32(in ReadOnlySequence<byte> buffer)
    {
        ReadOnlySequence<byte> slice = buffer.Slice(0, LengthPrefixSize);
        if (slice.IsSingleSegment)
            return BinaryPrimitives.ReadInt32LittleEndian(slice.FirstSpan);

        Span<byte> scratch = stackalloc byte[LengthPrefixSize];
        slice.CopyTo(scratch);
        return BinaryPrimitives.ReadInt32LittleEndian(scratch);
    }

    private static string ReadHeader(in ReadOnlySequence<byte> headerSeq, HeaderCache cache)
    {
        if (headerSeq.IsSingleSegment)
            return cache.GetString(headerSeq.FirstSpan);

        int length = (int)headerSeq.Length;
        byte[] rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            headerSeq.CopyTo(rented);
            return cache.GetString(rented.AsSpan(0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
