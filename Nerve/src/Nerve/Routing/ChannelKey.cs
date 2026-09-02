// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve.Routing;

/// <summary>
/// Identifies one route: a concrete topic plus the message type carried on it.
/// </summary>
/// <remarks>
/// The hash is computed once at construction so the publish-path dictionary lookup never rehashes
/// the topic string. Type identity is a reference comparison, which short-circuits most misses
/// before the string comparison runs.
/// </remarks>
internal readonly struct ChannelKey : IEquatable<ChannelKey>
{
    public readonly string Topic;
    public readonly Type MessageType;
    private readonly int _hash;

    public ChannelKey(string topic, Type messageType)
    {
        Topic = topic;
        MessageType = messageType;
        _hash = HashCode.Combine(
            string.GetHashCode(topic, StringComparison.Ordinal),
            RuntimeHelpers.GetHashCode(messageType));
    }

    public bool Equals(ChannelKey other) =>
        _hash == other._hash
        && ReferenceEquals(MessageType, other.MessageType)
        && string.Equals(Topic, other.Topic, StringComparison.Ordinal);

    public override bool Equals(object? obj) => obj is ChannelKey other && Equals(other);

    public override int GetHashCode() => _hash;
}
