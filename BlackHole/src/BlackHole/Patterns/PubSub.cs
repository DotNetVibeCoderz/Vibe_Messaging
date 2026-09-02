// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.Text;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Patterns;

/// <summary>
/// Matches published topics against subscription patterns, MQTT style.
/// </summary>
/// <remarks>
/// <c>+</c> matches exactly one segment, <c>#</c> matches the rest of the topic and may only appear
/// last. <c>sensor/+/temperature</c> matches <c>sensor/tank-3/temperature</c>;
/// <c>sensor/#</c> matches everything below <c>sensor</c>. Matching walks the two strings with
/// spans and allocates nothing.
/// </remarks>
public static class TopicFilter
{
    /// <summary>True when <paramref name="filter"/> contains a wildcard segment.</summary>
    public static bool HasWildcard(string filter) => filter.Contains('+') || filter.Contains('#');

    /// <summary>True when <paramref name="topic"/> matches <paramref name="filter"/>.</summary>
    public static bool Matches(ReadOnlySpan<char> filter, ReadOnlySpan<char> topic)
    {
        while (true)
        {
            if (filter.IsEmpty)
                return topic.IsEmpty;

            int filterSlash = filter.IndexOf('/');
            ReadOnlySpan<char> filterSegment = filterSlash < 0 ? filter : filter[..filterSlash];

            if (filterSegment.Length == 1 && filterSegment[0] == '#')
                return true; // '#' swallows the remainder, including nothing at all.

            if (topic.IsEmpty)
                return false;

            int topicSlash = topic.IndexOf('/');
            ReadOnlySpan<char> topicSegment = topicSlash < 0 ? topic : topic[..topicSlash];

            bool single = filterSegment.Length == 1 && filterSegment[0] == '+';
            if (!single && !filterSegment.SequenceEqual(topicSegment))
                return false;

            if (filterSlash < 0 || topicSlash < 0)
                return filterSlash < 0 && topicSlash < 0;

            filter = filter[(filterSlash + 1)..];
            topic = topic[(topicSlash + 1)..];
        }
    }
}

/// <summary>
/// Server-side topic broker: keeps who wants what and fans published messages out.
/// </summary>
/// <remarks>
/// Exact-match subscriptions - the overwhelming majority - resolve through a dictionary; wildcard
/// filters are kept in a small separate list and scanned. Each subscriber set is an immutable array
/// swapped under a lock, so the fan-out path reads it without locking and a slow subscriber cannot
/// block a publisher from reaching the others.
/// </remarks>
public sealed class PubSubBroker
{
    private readonly ConcurrentDictionary<string, ITransport[]> _exact = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ITransport[]> _wildcard = new(StringComparer.Ordinal);
    private readonly Lock _mutation = new();

    /// <summary>Send published messages back to the publisher when it also subscribes. Default true, matching v2.</summary>
    public bool EchoToPublisher { get; set; } = true;

    /// <summary>Distinct topics and filters with at least one subscriber.</summary>
    public int TopicCount => _exact.Count + _wildcard.Count;

    /// <summary>Raised after a subscribe or unsubscribe, for dashboards.</summary>
    public event Action<string, int>? SubscriptionsChanged;

    /// <summary>Wires this broker into a router.</summary>
    public PubSubBroker AttachTo(MessageRouter router)
    {
        ArgumentNullException.ThrowIfNull(router);
        router.On([MessageType.Subscribe, MessageType.Unsubscribe, MessageType.Publish], HandleAsync);
        return this;
    }

    /// <summary>Handles subscribe, unsubscribe and publish. Assign to a router or a dispatcher.</summary>
    public ValueTask HandleAsync(ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        switch (message.Type)
        {
            case MessageType.Subscribe:
                Subscribe(message.Header, transport);
                return ValueTask.CompletedTask;

            case MessageType.Unsubscribe:
                Unsubscribe(message.Header, transport);
                return ValueTask.CompletedTask;

            case MessageType.Publish:
                return PublishAsync(message.Header, message.Payload, transport, cancellationToken);

            default:
                return ValueTask.CompletedTask;
        }
    }

    /// <summary>Adds a subscriber to a topic or wildcard filter.</summary>
    public void Subscribe(string filter, ITransport subscriber)
    {
        ArgumentException.ThrowIfNullOrEmpty(filter);
        ConcurrentDictionary<string, ITransport[]> table = TableFor(filter);

        lock (_mutation)
        {
            ITransport[] current = table.TryGetValue(filter, out ITransport[]? existing) ? existing : [];
            if (Array.IndexOf(current, subscriber) >= 0)
                return;
            table[filter] = [.. current, subscriber];
            SubscriptionsChanged?.Invoke(filter, current.Length + 1);
        }
    }

    /// <summary>Removes a subscriber from one filter.</summary>
    public void Unsubscribe(string filter, ITransport subscriber)
    {
        if (string.IsNullOrEmpty(filter)) return;
        ConcurrentDictionary<string, ITransport[]> table = TableFor(filter);

        lock (_mutation)
        {
            if (!table.TryGetValue(filter, out ITransport[]? current))
                return;
            ITransport[] next = current.Where(t => !ReferenceEquals(t, subscriber)).ToArray();
            if (next.Length == 0)
                table.TryRemove(filter, out _);
            else
                table[filter] = next;
            SubscriptionsChanged?.Invoke(filter, next.Length);
        }
    }

    /// <summary>
    /// Drops a connection from every filter. Hook this to <see cref="ITransport.Closed"/> or the
    /// subscriber set leaks for the lifetime of the process.
    /// </summary>
    public void RemoveSubscriber(ITransport subscriber)
    {
        lock (_mutation)
        {
            RemoveFrom(_exact, subscriber);
            RemoveFrom(_wildcard, subscriber);
        }

        static void RemoveFrom(ConcurrentDictionary<string, ITransport[]> table, ITransport subscriber)
        {
            foreach ((string filter, ITransport[] subscribers) in table)
            {
                if (Array.IndexOf(subscribers, subscriber) < 0)
                    continue;
                ITransport[] next = subscribers.Where(t => !ReferenceEquals(t, subscriber)).ToArray();
                if (next.Length == 0)
                    table.TryRemove(filter, out _);
                else
                    table[filter] = next;
            }
        }
    }

    /// <summary>Delivers a payload to everyone subscribed to <paramref name="topic"/>.</summary>
    public async ValueTask PublishAsync(
        string topic,
        ReadOnlyMemory<byte> payload,
        ITransport? publisher = null,
        CancellationToken cancellationToken = default)
    {
        var message = new BlackHoleMessage(MessageType.Publish, topic, payload);

        if (_exact.TryGetValue(topic, out ITransport[]? subscribers))
            await FanOutAsync(subscribers, message, publisher, cancellationToken).ConfigureAwait(false);

        if (!_wildcard.IsEmpty)
        {
            foreach ((string filter, ITransport[] matched) in _wildcard)
            {
                if (TopicFilter.Matches(filter, topic))
                    await FanOutAsync(matched, message, publisher, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask FanOutAsync(
        ITransport[] subscribers, BlackHoleMessage message, ITransport? publisher, CancellationToken cancellationToken)
    {
        foreach (ITransport subscriber in subscribers)
        {
            if (!EchoToPublisher && ReferenceEquals(subscriber, publisher))
                continue;
            if (!subscriber.IsConnected)
                continue;

            try
            {
                await subscriber.SendAsync(message, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // A dead subscriber must not stop delivery to the rest; its Closed event cleans up.
            }
        }
    }

    /// <summary>Subscriber count for one exact topic or filter.</summary>
    public int SubscriberCount(string filter) =>
        TableFor(filter).TryGetValue(filter, out ITransport[]? subs) ? subs.Length : 0;

    private ConcurrentDictionary<string, ITransport[]> TableFor(string filter) =>
        TopicFilter.HasWildcard(filter) ? _wildcard : _exact;
}

/// <summary>
/// Client side of Pub/Sub: subscribes, publishes, and raises received topics.
/// </summary>
public sealed class PubSubClient
{
    private readonly ITransport _transport;
    private readonly ConcurrentDictionary<string, byte> _subscriptions = new(StringComparer.Ordinal);

    public PubSubClient(ITransport transport) =>
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));

    /// <summary>
    /// Raised for each delivered message, on the receive loop. The payload dies when the handler
    /// returns - copy it if you keep it.
    /// </summary>
    public event Action<string, ReadOnlyMemory<byte>>? Received;

    /// <summary>Filters this client has subscribed to.</summary>
    public IReadOnlyCollection<string> Subscriptions => _subscriptions.Keys.ToArray();

    /// <summary>Wires this client into a router.</summary>
    public PubSubClient AttachTo(MessageRouter router)
    {
        ArgumentNullException.ThrowIfNull(router);
        router.On(MessageType.Publish, HandleAsync);
        return this;
    }

    /// <summary>Raises <see cref="Received"/>. Assign to a router or a dispatcher.</summary>
    public ValueTask HandleAsync(ITransport transport, BlackHoleMessage message, CancellationToken cancellationToken)
    {
        if (message.Type == MessageType.Publish)
            Received?.Invoke(message.Header, message.Payload);
        return ValueTask.CompletedTask;
    }

    /// <summary>Asks the broker for a topic or wildcard filter.</summary>
    public async ValueTask SubscribeAsync(string filter, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(filter);
        _subscriptions[filter] = 0;
        await _transport.SendAsync(new BlackHoleMessage(MessageType.Subscribe, filter), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Stops receiving a filter.</summary>
    public async ValueTask UnsubscribeAsync(string filter, CancellationToken cancellationToken = default)
    {
        _subscriptions.TryRemove(filter, out _);
        await _transport.SendAsync(new BlackHoleMessage(MessageType.Unsubscribe, filter), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Publishes bytes to a topic.</summary>
    public ValueTask PublishAsync(string topic, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default) =>
        _transport.SendAsync(new BlackHoleMessage(MessageType.Publish, topic, payload), cancellationToken);

    /// <summary>Publishes UTF-8 text to a topic.</summary>
    public ValueTask PublishAsync(string topic, string payload, CancellationToken cancellationToken = default) =>
        PublishAsync(topic, Encoding.UTF8.GetBytes(payload), cancellationToken);
}
