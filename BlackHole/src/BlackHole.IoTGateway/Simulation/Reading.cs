// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers.Binary;

namespace BlackHole.IoTGateway.Simulation;

/// <summary>
/// One sensor reading as it travels over BlackHole: 20 bytes, fixed layout, no JSON.
/// </summary>
/// <remarks>
/// A real gateway pushing tens of thousands of readings a second cannot afford to serialise objects
/// per sample, and this is the shape the library is built for - a small fixed struct written
/// straight into the frame. The timestamp is Unix milliseconds so a device and a gateway on
/// different machines agree without a shared type.
/// </remarks>
public readonly record struct Reading(long TimestampMs, double Value, int Sequence)
{
    /// <summary>Bytes on the wire.</summary>
    public const int Size = 20;

    /// <summary>When the device took the reading.</summary>
    public DateTimeOffset Timestamp => DateTimeOffset.FromUnixTimeMilliseconds(TimestampMs);

    /// <summary>A reading taken now.</summary>
    public static Reading Now(double value, int sequence) =>
        new(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), value, sequence);

    /// <summary>Writes this reading into <paramref name="destination"/>, which must hold <see cref="Size"/> bytes.</summary>
    public void WriteTo(Span<byte> destination)
    {
        BinaryPrimitives.WriteInt64LittleEndian(destination, TimestampMs);
        BinaryPrimitives.WriteDoubleLittleEndian(destination[8..], Value);
        BinaryPrimitives.WriteInt32LittleEndian(destination[16..], Sequence);
    }

    /// <summary>Parses a reading. Returns false when the payload is the wrong size.</summary>
    public static bool TryParse(ReadOnlySpan<byte> source, out Reading reading)
    {
        if (source.Length < Size)
        {
            reading = default;
            return false;
        }

        reading = new Reading(
            BinaryPrimitives.ReadInt64LittleEndian(source),
            BinaryPrimitives.ReadDoubleLittleEndian(source[8..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[16..]));
        return true;
    }
}
