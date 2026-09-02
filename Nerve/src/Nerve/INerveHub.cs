// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve;

/// <summary>
/// The hub as seen by the components that use it. Take this in a constructor rather than
/// <see cref="NerveHub"/>, so a test can hand over a hub of its own.
/// </summary>
public interface INerveHub
{
    /// <summary>Publishes without waiting for the subscribers.</summary>
    /// <typeparam name="T">The message type. Part of the route.</typeparam>
    /// <param name="topic">A concrete topic. Wildcards are not allowed here.</param>
    /// <param name="message">The message.</param>
    void Publish<T>(string topic, T message);

    /// <summary>Publishes and completes when every subscriber has finished.</summary>
    /// <typeparam name="T">The message type. Part of the route.</typeparam>
    /// <param name="topic">A concrete topic. Wildcards are not allowed here.</param>
    /// <param name="message">The message.</param>
    /// <param name="cancellationToken">Passed to handlers that asked for one.</param>
    ValueTask PublishAsync<T>(string topic, T message, CancellationToken cancellationToken = default);

    /// <summary>Publishes and keeps the message as the topic's retained value.</summary>
    /// <typeparam name="T">The message type. Part of the route.</typeparam>
    /// <param name="topic">A concrete topic. Wildcards are not allowed here.</param>
    /// <param name="message">The message to deliver and retain.</param>
    /// <param name="cancellationToken">Passed to handlers that asked for one.</param>
    ValueTask PublishRetainedAsync<T>(string topic, T message, CancellationToken cancellationToken = default);

    /// <summary>Subscribes with a synchronous handler.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="handler">Runs on the publishing thread.</param>
    /// <returns>Dispose it to unsubscribe.</returns>
    IDisposable Subscribe<T>(string topicFilter, Action<T> handler);

    /// <summary>Subscribes with an asynchronous handler.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="handler">Awaited by <see cref="PublishAsync{T}"/>.</param>
    /// <returns>Dispose it to unsubscribe.</returns>
    IDisposable Subscribe<T>(string topicFilter, Func<T, ValueTask> handler);

    /// <summary>Subscribes for exactly one message, then unsubscribes itself.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="handler">Runs once.</param>
    /// <returns>Dispose it to cancel before the message arrives.</returns>
    IDisposable SubscribeOnce<T>(string topicFilter, Action<T> handler);

    /// <summary>Registers a responder.</summary>
    /// <typeparam name="TRequest">The request payload type.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="responder">Produces the answer.</param>
    /// <returns>Dispose it to stop responding.</returns>
    IDisposable Respond<TRequest, TResponse>(
        string topicFilter, Func<TRequest, CancellationToken, ValueTask<TResponse>> responder);

    /// <summary>Registers a synchronous responder.</summary>
    /// <typeparam name="TRequest">The request payload type.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="responder">Produces the answer.</param>
    /// <returns>Dispose it to stop responding.</returns>
    IDisposable Respond<TRequest, TResponse>(string topicFilter, Func<TRequest, TResponse> responder);

    /// <summary>Sends a request and waits for the first answer.</summary>
    /// <typeparam name="TRequest">The request payload type.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="topic">A concrete topic. Wildcards are not allowed here.</param>
    /// <param name="request">The payload.</param>
    /// <param name="timeout">How long to wait, or <see langword="null"/> for the hub default.</param>
    /// <param name="cancellationToken">Abandons the wait, and is handed to the responder.</param>
    /// <returns>The response.</returns>
    Task<TResponse> RequestAsync<TRequest, TResponse>(
        string topic, TRequest request, TimeSpan? timeout = null, CancellationToken cancellationToken = default);

    /// <summary>Consumes a topic as an asynchronous sequence, buffered and drained by the consumer.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="capacity">Messages to buffer before the oldest is dropped.</param>
    /// <param name="cancellationToken">Ends the sequence.</param>
    /// <returns>A sequence that ends when the token is cancelled.</returns>
    IAsyncEnumerable<T> StreamAsync<T>(string topicFilter, int capacity = 0, CancellationToken cancellationToken = default);

    /// <summary>Waits for the next message on a topic.</summary>
    /// <typeparam name="T">The message type to wait for.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="match">Optional test; the wait continues until a message satisfies it.</param>
    /// <param name="timeout">How long to wait. <see langword="null"/> waits indefinitely.</param>
    /// <param name="cancellationToken">Abandons the wait.</param>
    /// <returns>The first matching message.</returns>
    Task<T> WaitForAsync<T>(
        string topicFilter, Predicate<T>? match = null, TimeSpan? timeout = null, CancellationToken cancellationToken = default);

    /// <summary>A pre-resolved handle to one topic, for publishing in a loop.</summary>
    /// <typeparam name="T">The message type carried on the topic.</typeparam>
    /// <param name="topic">A concrete topic.</param>
    NerveTopic<T> Topic<T>(string topic);

    /// <summary>True when at least one subscriber would receive a message published here.</summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="topic">A concrete topic.</param>
    bool HasSubscribers<T>(string topic);

    /// <summary>Sums the per-route counters into one snapshot.</summary>
    NerveStatistics GetStatistics();
}
