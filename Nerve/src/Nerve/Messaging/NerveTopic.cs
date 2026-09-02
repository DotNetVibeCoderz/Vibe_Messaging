// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Nerve.Routing;

namespace Nerve;

/// <summary>
/// A topic resolved once and held onto. Publishing through it skips the hub's dictionary lookup,
/// which is the only per-message cost left on the hot path.
/// </summary>
/// <typeparam name="T">The message type carried on the topic.</typeparam>
/// <remarks>
/// This is a struct wrapping two references, so holding one in a field costs nothing at runtime.
/// Get one from <see cref="NerveHub.Topic{T}(string)"/> when a component publishes to the same
/// topic in a loop; for anything less frequent, publishing by name is already fast enough.
/// </remarks>
/// <example>
/// <code>
/// private readonly NerveTopic&lt;Reading&gt; _readings = hub.Topic&lt;Reading&gt;("sensor/tank-3");
/// // ...
/// _readings.Publish(reading);
/// </code>
/// </example>
public readonly struct NerveTopic<T>
{
    private readonly NerveHub _hub;
    private readonly Route<T> _route;

    internal NerveTopic(NerveHub hub, Route<T> route)
    {
        _hub = hub;
        _route = route;
    }

    /// <summary>The topic this handle publishes to.</summary>
    public string Name => _route.Topic;

    /// <summary>True when at least one subscriber would receive a message published here.</summary>
    public bool HasSubscribers => _route.HasSubscribers;

    /// <summary>How many subscribers this topic currently resolves to, wildcards included.</summary>
    public int SubscriberCount => _route.SubscriberCount;

    /// <summary>Publishes without waiting for the subscribers.</summary>
    /// <param name="message">The message.</param>
    public void Publish(T message)
    {
        ValueTask pending = _route.PublishAsync(message, CancellationToken.None);
        if (pending.IsCompletedSuccessfully) return;
        _hub.ObserveDetached(pending, _route.Topic);
    }

    /// <summary>Publishes and completes when every subscriber has finished.</summary>
    /// <param name="message">The message.</param>
    /// <param name="cancellationToken">Passed to handlers that asked for one.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask PublishAsync(T message, CancellationToken cancellationToken = default)
        => _route.PublishAsync(message, cancellationToken);

    /// <summary>Publishes and keeps the message as this topic's retained value.</summary>
    /// <param name="message">The message to deliver and retain.</param>
    /// <param name="cancellationToken">Passed to handlers that asked for one.</param>
    public ValueTask PublishRetainedAsync(T message, CancellationToken cancellationToken = default)
    {
        _route.SetRetained(message);
        return _route.PublishAsync(message, cancellationToken);
    }

    /// <summary>Subscribes to this exact topic.</summary>
    /// <param name="handler">Runs on the publishing thread.</param>
    /// <returns>Dispose it to unsubscribe.</returns>
    public IDisposable Subscribe(Action<T> handler) => _hub.Subscribe(_route.Topic, handler);

    /// <summary>Subscribes to this exact topic with an asynchronous handler.</summary>
    /// <param name="handler">Awaited by <see cref="PublishAsync"/>.</param>
    /// <returns>Dispose it to unsubscribe.</returns>
    public IDisposable Subscribe(Func<T, ValueTask> handler) => _hub.Subscribe(_route.Topic, handler);
}
