using System.Text.Json;
using BigPipe.Client;

namespace BigPipe.Streams;

/// <summary>Serializer + deserializer pair for one type.</summary>
public sealed class Serde<T>(ISerializer<T> serializer, IDeserializer<T> deserializer)
{
    public ISerializer<T> Serializer { get; } = serializer;
    public IDeserializer<T> Deserializer { get; } = deserializer;

    /// <summary>Strings as UTF-8, byte[] raw, numbers big-endian, everything else JSON.</summary>
    public static Serde<T> Default() => new(Serializers.For<T>(), Deserializers.For<T>());

    public static Serde<T> Json(JsonSerializerOptions? options = null) => new(Serializers.Json<T>(options), Deserializers.Json<T>(options));
}

/// <summary>Untyped view used by the runtime.</summary>
internal interface IUntypedSerde
{
    byte[]? Serialize(object? value, string topic, bool isKey);
    object? Deserialize(ReadOnlyMemory<byte>? data, string topic, bool isKey);
}

internal sealed class UntypedSerde<T>(Serde<T> serde) : IUntypedSerde
{
    public byte[]? Serialize(object? value, string topic, bool isKey) => serde.Serializer.Serialize((T)value!, new SerializationContext(topic, isKey));

    public object? Deserialize(ReadOnlyMemory<byte>? data, string topic, bool isKey) => serde.Deserializer.Deserialize(data, new SerializationContext(topic, isKey));
}
