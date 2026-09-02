// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve;

/// <summary>What a hub does when a subscriber throws.</summary>
public enum HandlerErrorBehavior
{
    /// <summary>
    /// Report the failure and carry on with the remaining subscribers. One broken handler cannot
    /// stop the others from seeing the message. This is the default.
    /// </summary>
    Isolate,

    /// <summary>
    /// Abandon the remaining subscribers and surface the failure to whoever awaited the publish,
    /// wrapped in a <see cref="NerveHandlerException"/>. A fire-and-forget
    /// <see cref="NerveHub.Publish{T}(string, T)"/> has nobody to surface to, so it still reports
    /// through <see cref="NerveHub.HandlerError"/>.
    /// </summary>
    Propagate,
}

/// <summary>Construction-time settings for a <see cref="NerveHub"/>.</summary>
public sealed class NerveOptions
{
    /// <summary>What happens when a subscriber throws. Defaults to
    /// <see cref="HandlerErrorBehavior.Isolate"/>.</summary>
    public HandlerErrorBehavior ErrorBehavior { get; set; } = HandlerErrorBehavior.Isolate;

    /// <summary>
    /// Whether per-route counters are kept. Defaults to <see langword="true"/>; turning it off
    /// removes four interlocked increments per publish and makes
    /// <see cref="NerveHub.GetStatistics"/> return zeroes.
    /// </summary>
    public bool CollectStatistics { get; set; } = true;

    /// <summary>
    /// Called for every handler failure, in addition to the <see cref="NerveHub.HandlerError"/>
    /// event. Useful for wiring a logger at construction time.
    /// </summary>
    public Action<NerveError>? OnError { get; set; }

    /// <summary>How long <c>RequestAsync</c> waits when no timeout is given. Defaults to 30s.</summary>
    public TimeSpan DefaultRequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How many messages a stream buffers before the oldest is dropped. Defaults to 1024. A
    /// stream never blocks the publisher; a consumer that cannot keep up loses the oldest
    /// messages, and <see cref="NerveStatistics.StreamDrops"/> counts them.
    /// </summary>
    public int DefaultStreamCapacity { get; set; } = 1024;
}

/// <summary>A subscriber failure, reported rather than thrown.</summary>
/// <param name="Topic">The concrete topic the message was published to.</param>
/// <param name="MessageType">The type of the message being delivered.</param>
/// <param name="SubscriptionFilter">The filter the failing subscription was registered under.</param>
/// <param name="Exception">What the handler threw.</param>
public readonly record struct NerveError(
    string Topic,
    Type MessageType,
    string SubscriptionFilter,
    Exception Exception);
