using System.Text;

namespace BigPipe.Client;

/// <summary>Record batch compression codecs (Kafka attribute bits 0-2).</summary>
public enum CompressionType : short
{
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

/// <summary>A record header. Values are raw bytes; use <see cref="Header.GetString"/> for text.</summary>
public readonly record struct Header(string Key, ReadOnlyMemory<byte>? Value)
{
    public string? GetString() => Value is { } v ? Encoding.UTF8.GetString(v.Span) : null;

    public static Header Of(string key, string? value) =>
        new(key, value is null ? null : Encoding.UTF8.GetBytes(value));
}

/// <summary>A decoded record as stored in the log.</summary>
public sealed class RawRecord
{
    public long Offset { get; init; }
    public long Timestamp { get; init; }
    public ReadOnlyMemory<byte>? Key { get; init; }
    public ReadOnlyMemory<byte>? Value { get; init; }
    public IReadOnlyList<Header> Headers { get; init; } = [];
}

