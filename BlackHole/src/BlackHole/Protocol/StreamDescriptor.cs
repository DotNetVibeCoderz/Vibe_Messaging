// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers.Binary;
using System.Text;

namespace BlackHole.Protocol;

/// <summary>
/// Metadata carried by <see cref="MessageType.StreamStart"/> so the receiver knows what is coming
/// before the first chunk lands: how big it is, what to call it, and what it holds.
/// </summary>
/// <remarks>
/// v2 sent a bare stream id, so a receiver could not size a buffer, show progress, or route the
/// content anywhere sensible. The encoding is fixed-order binary rather than JSON to keep
/// StreamStart small and parsing allocation-light.
/// </remarks>
public readonly record struct StreamDescriptor(string Name, long TotalLength, string ContentType)
{
    /// <summary>Placeholder for a stream whose length is not known up front.</summary>
    public const long UnknownLength = -1;

    /// <summary>A descriptor with no length known in advance.</summary>
    public static StreamDescriptor Unknown(string name, string contentType = "application/octet-stream") =>
        new(name, UnknownLength, contentType);

    /// <summary>True when the sender declared a size.</summary>
    public bool HasLength => TotalLength >= 0;

    /// <summary>Serialises to the StreamStart payload layout.</summary>
    public byte[] Encode()
    {
        byte[] name = Encoding.UTF8.GetBytes(Name ?? string.Empty);
        byte[] contentType = Encoding.UTF8.GetBytes(ContentType ?? string.Empty);
        var buffer = new byte[8 + 2 + name.Length + 2 + contentType.Length];

        Span<byte> span = buffer;
        BinaryPrimitives.WriteInt64LittleEndian(span, TotalLength);
        BinaryPrimitives.WriteUInt16LittleEndian(span[8..], (ushort)name.Length);
        name.CopyTo(span[10..]);
        BinaryPrimitives.WriteUInt16LittleEndian(span[(10 + name.Length)..], (ushort)contentType.Length);
        contentType.CopyTo(span[(12 + name.Length)..]);
        return buffer;
    }

    /// <summary>Parses a StreamStart payload. An empty payload yields an unnamed descriptor.</summary>
    public static StreamDescriptor Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < 10)
            return Unknown(string.Empty);

        long total = BinaryPrimitives.ReadInt64LittleEndian(payload);
        int nameLength = BinaryPrimitives.ReadUInt16LittleEndian(payload[8..]);
        if (payload.Length < 10 + nameLength + 2)
            return new StreamDescriptor(string.Empty, total, string.Empty);

        string name = Encoding.UTF8.GetString(payload.Slice(10, nameLength));
        int contentTypeLength = BinaryPrimitives.ReadUInt16LittleEndian(payload[(10 + nameLength)..]);
        string contentType = payload.Length < 12 + nameLength + contentTypeLength
            ? string.Empty
            : Encoding.UTF8.GetString(payload.Slice(12 + nameLength, contentTypeLength));

        return new StreamDescriptor(name, total, contentType);
    }
}
