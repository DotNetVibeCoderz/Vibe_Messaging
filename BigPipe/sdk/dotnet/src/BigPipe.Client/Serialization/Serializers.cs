using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace BigPipe.Client;

/// <summary>Converts a value to bytes. Return <c>null</c> for a null key/value.</summary>
public interface ISerializer<in T>
{
    byte[]? Serialize(T value, SerializationContext context);
}

/// <summary>Converts bytes back to a value; the data is null for null keys/values.</summary>
public interface IDeserializer<out T>
{
    T Deserialize(ReadOnlyMemory<byte>? data, SerializationContext context);
}

public readonly record struct SerializationContext(string Topic, bool IsKey);

/// <summary>Built-in serializers. <see cref="For{T}"/> picks a sensible default per type.</summary>
public static class Serializers
{
    public static readonly ISerializer<string?> Utf8 = new Utf8Serializer();
    public static readonly ISerializer<byte[]?> ByteArray = new BytesSerializer();
    public static readonly ISerializer<int> Int32 = new Int32Serializer();
    public static readonly ISerializer<long> Int64 = new Int64Serializer();
    public static readonly ISerializer<double> Double = new DoubleSerializer();

    public static ISerializer<T> Json<T>(JsonSerializerOptions? options = null) => new JsonSerializer<T>(options);

    /// <summary>Default serializer: strings as UTF-8, byte[] as-is, numbers big-endian, everything else JSON.</summary>
    public static ISerializer<T> For<T>()
    {
        object s = typeof(T) switch
        {
            var t when t == typeof(string) => Utf8,
            var t when t == typeof(byte[]) => ByteArray,
            var t when t == typeof(int) => Int32,
            var t when t == typeof(long) => Int64,
            var t when t == typeof(double) => Double,
            var t when t == typeof(Null) => new NullSerializer(),
            _ => new JsonSerializer<T>(null),
        };
        return (ISerializer<T>)s;
    }

    private sealed class Utf8Serializer : ISerializer<string?>
    {
        public byte[]? Serialize(string? value, SerializationContext context) => value is null ? null : Encoding.UTF8.GetBytes(value);
    }

    private sealed class BytesSerializer : ISerializer<byte[]?>
    {
        public byte[]? Serialize(byte[]? value, SerializationContext context) => value;
    }

    private sealed class Int32Serializer : ISerializer<int>
    {
        public byte[] Serialize(int value, SerializationContext context)
        {
            var b = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(b, value);
            return b;
        }
    }

    private sealed class Int64Serializer : ISerializer<long>
    {
        public byte[] Serialize(long value, SerializationContext context)
        {
            var b = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(b, value);
            return b;
        }
    }

    private sealed class DoubleSerializer : ISerializer<double>
    {
        public byte[] Serialize(double value, SerializationContext context)
        {
            var b = new byte[8];
            BinaryPrimitives.WriteDoubleBigEndian(b, value);
            return b;
        }
    }

    private sealed class NullSerializer : ISerializer<Null>
    {
        public byte[]? Serialize(Null value, SerializationContext context) => null;
    }

    private sealed class JsonSerializer<T>(JsonSerializerOptions? options) : ISerializer<T>
    {
        private readonly JsonSerializerOptions _options = options ?? BigPipeJson.Options;

        public byte[]? Serialize(T value, SerializationContext context) =>
            value is null ? null : JsonSerializer.SerializeToUtf8Bytes(value, _options);
    }
}

/// <summary>Built-in deserializers mirroring <see cref="Serializers"/>.</summary>
public static class Deserializers
{
    public static readonly IDeserializer<string?> Utf8 = new Utf8Deserializer();
    public static readonly IDeserializer<byte[]?> ByteArray = new BytesDeserializer();
    public static readonly IDeserializer<int> Int32 = new Int32Deserializer();
    public static readonly IDeserializer<long> Int64 = new Int64Deserializer();
    public static readonly IDeserializer<double> Double = new DoubleDeserializer();

    public static IDeserializer<T> Json<T>(JsonSerializerOptions? options = null) => new JsonDeserializer<T>(options);

    public static IDeserializer<T> For<T>()
    {
        object d = typeof(T) switch
        {
            var t when t == typeof(string) => Utf8,
            var t when t == typeof(byte[]) => ByteArray,
            var t when t == typeof(int) => Int32,
            var t when t == typeof(long) => Int64,
            var t when t == typeof(double) => Double,
            var t when t == typeof(Null) => new NullDeserializer(),
            _ => new JsonDeserializer<T>(null),
        };
        return (IDeserializer<T>)d;
    }

    private sealed class Utf8Deserializer : IDeserializer<string?>
    {
        public string? Deserialize(ReadOnlyMemory<byte>? data, SerializationContext context) =>
            data is { } d ? Encoding.UTF8.GetString(d.Span) : null;
    }

    private sealed class BytesDeserializer : IDeserializer<byte[]?>
    {
        public byte[]? Deserialize(ReadOnlyMemory<byte>? data, SerializationContext context) => data?.ToArray();
    }

    private sealed class Int32Deserializer : IDeserializer<int>
    {
        public int Deserialize(ReadOnlyMemory<byte>? data, SerializationContext context) =>
            data is { Length: 4 } d ? BinaryPrimitives.ReadInt32BigEndian(d.Span) : 0;
    }

    private sealed class Int64Deserializer : IDeserializer<long>
    {
        public long Deserialize(ReadOnlyMemory<byte>? data, SerializationContext context) =>
            data is { Length: 8 } d ? BinaryPrimitives.ReadInt64BigEndian(d.Span) : 0;
    }

    private sealed class DoubleDeserializer : IDeserializer<double>
    {
        public double Deserialize(ReadOnlyMemory<byte>? data, SerializationContext context) =>
            data is { Length: 8 } d ? BinaryPrimitives.ReadDoubleBigEndian(d.Span) : 0;
    }

    private sealed class NullDeserializer : IDeserializer<Null>
    {
        public Null Deserialize(ReadOnlyMemory<byte>? data, SerializationContext context) => default;
    }

    private sealed class JsonDeserializer<T>(JsonSerializerOptions? options) : IDeserializer<T>
    {
        private readonly JsonSerializerOptions _options = options ?? BigPipeJson.Options;

        public T Deserialize(ReadOnlyMemory<byte>? data, SerializationContext context) =>
            data is { Length: > 0 } d ? JsonSerializer.Deserialize<T>(d.Span, _options)! : default!;
    }
}

/// <summary>Marker type for records without a key (or value).</summary>
public readonly struct Null;

/// <summary>JSON options used by BigPipe serializers: camelCase, case-insensitive reads.</summary>
public static class BigPipeJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        TypeInfoResolver = new System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver(),
    };
}
