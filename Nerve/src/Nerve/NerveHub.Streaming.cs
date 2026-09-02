// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using System.Threading.Channels;

namespace Nerve;

/// <summary>
/// Consuming a topic as a sequence rather than a callback.
/// </summary>
/// <remarks>
/// Every other way of subscribing runs the handler on the publisher's thread. A stream is the
/// deliberate exception: it buffers, and the consumer drains it on its own. That inversion is what
/// makes it the right tool for a UI, a file writer, or anything else too slow to sit in the
/// publishing path - and the reason the buffer drops rather than blocks. A publisher is never
/// punished for a consumer that cannot keep up; the drops are counted instead.
/// </remarks>
public sealed partial class NerveHub
{
    /// <summary>Consumes a topic as an asynchronous sequence.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="capacity">Messages to buffer before the oldest is dropped. Defaults to
    /// <see cref="NerveOptions.DefaultStreamCapacity"/>.</param>
    /// <param name="cancellationToken">Ends the sequence.</param>
    /// <returns>A sequence that ends when the token is cancelled.</returns>
    /// <remarks>
    /// The subscription lives exactly as long as the enumeration: it is registered on the first
    /// <c>MoveNextAsync</c> and disposed when the loop ends, breaks, or throws.
    /// </remarks>
    public async IAsyncEnumerable<T> StreamAsync<T>(
        string topicFilter,
        int capacity = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var options = new BoundedChannelOptions(capacity > 0 ? capacity : DefaultStreamCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        };

        Channel<T> buffer = Channel.CreateBounded<T>(options);

        // TryWrite never blocks on a DropOldest channel, so the publishing thread is not held up by
        // a slow consumer. It also never reports the drop, so a full buffer is counted here on the
        // way in - approximate under concurrency, but honest about a consumer falling behind.
        using IDisposable subscription = Subscribe<T>(topicFilter, message =>
        {
            if (buffer.Reader.Count >= options.Capacity) CountStreamDrop();
            buffer.Writer.TryWrite(message);
        });

        try
        {
            await foreach (T message in buffer.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return message;
        }
        finally
        {
            buffer.Writer.TryComplete();
        }
    }

    /// <summary>Waits for the next message on a topic.</summary>
    /// <typeparam name="T">The message type to wait for.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="match">Optional test; the wait continues until a message satisfies it.</param>
    /// <param name="timeout">How long to wait. <see langword="null"/> waits indefinitely.</param>
    /// <param name="cancellationToken">Abandons the wait.</param>
    /// <returns>The first matching message.</returns>
    /// <exception cref="TimeoutException">Nothing matched in time.</exception>
    /// <remarks>
    /// Useful in tests and in start-up sequences - "carry on once the roster has been published" -
    /// without hand-rolling a <see cref="TaskCompletionSource"/> and remembering to unsubscribe.
    /// </remarks>
    public async Task<T> WaitForAsync<T>(
        string topicFilter,
        Predicate<T>? match = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        using IDisposable subscription = match is null
            ? Subscribe<T>(topicFilter, message => completion.TrySetResult(message))
            : Subscribe(topicFilter, match, message => completion.TrySetResult(message));

        if (timeout is null)
            return await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            return await completion.Task.WaitAsync(timeout.Value, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException($"No {typeof(T).Name} matched '{topicFilter}' within {timeout.Value}.");
        }
    }
}
