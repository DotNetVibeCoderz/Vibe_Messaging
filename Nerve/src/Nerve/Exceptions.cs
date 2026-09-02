// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve;

/// <summary>
/// Wraps a subscriber failure when the hub is configured with
/// <see cref="HandlerErrorBehavior.Propagate"/>.
/// </summary>
public sealed class NerveHandlerException : Exception
{
    /// <summary>The concrete topic being published to.</summary>
    public string Topic { get; }

    /// <summary>The type of the message being delivered.</summary>
    public Type MessageType { get; }

    /// <summary>The filter the failing subscription was registered under.</summary>
    public string SubscriptionFilter { get; }

    /// <summary>Creates the wrapper.</summary>
    /// <param name="topic">The concrete topic being published to.</param>
    /// <param name="messageType">The type of the message being delivered.</param>
    /// <param name="subscriptionFilter">The filter the failing subscription was registered under.</param>
    /// <param name="inner">What the handler threw.</param>
    public NerveHandlerException(string topic, Type messageType, string subscriptionFilter, Exception inner)
        : base($"A subscriber on '{subscriptionFilter}' failed while handling '{topic}': {inner.Message}", inner)
    {
        Topic = topic;
        MessageType = messageType;
        SubscriptionFilter = subscriptionFilter;
    }
}

/// <summary>
/// Thrown by <c>RequestAsync</c> when nothing is registered to answer on the requested topic.
/// </summary>
/// <remarks>
/// This is deliberately not a timeout. Waiting thirty seconds to discover that a responder was
/// never wired up is the single most common way to lose an afternoon with a message bus.
/// </remarks>
public sealed class NerveNoResponderException : Exception
{
    /// <summary>The topic that had no responder.</summary>
    public string Topic { get; }

    /// <summary>Creates the exception.</summary>
    /// <param name="topic">The topic that had no responder.</param>
    /// <param name="requestType">The request payload type.</param>
    /// <param name="responseType">The expected response type.</param>
    public NerveNoResponderException(string topic, Type requestType, Type responseType)
        : base($"No responder is registered on '{topic}' for {requestType.Name} -> {responseType.Name}.")
        => Topic = topic;
}
