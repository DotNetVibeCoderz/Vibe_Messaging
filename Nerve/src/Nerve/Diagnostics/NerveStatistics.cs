// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve;

/// <summary>
/// A snapshot of what a hub has carried, summed across every route at the moment
/// <see cref="NerveHub.GetStatistics"/> was called.
/// </summary>
/// <remarks>
/// The counters are read without a global lock, so a snapshot taken while messages are in flight
/// is internally consistent to within a few messages. It is a gauge, not a ledger.
/// </remarks>
public readonly record struct NerveStatistics
{
    /// <summary>Messages handed to <c>Publish</c> or <c>PublishAsync</c>.</summary>
    public long Published { get; init; }

    /// <summary>Handler invocations that completed. One message to eight subscribers counts eight.</summary>
    public long Delivered { get; init; }

    /// <summary>Messages published to a topic nothing was listening to.</summary>
    public long Unrouted { get; init; }

    /// <summary>Handler invocations that threw.</summary>
    public long Errors { get; init; }

    /// <summary>Messages dropped because a stream consumer fell behind its buffer.</summary>
    public long StreamDrops { get; init; }

    /// <summary>Distinct topic and message-type pairs the hub has resolved.</summary>
    public int Routes { get; init; }

    /// <summary>Live subscriptions across every route.</summary>
    public int Subscriptions { get; init; }

    /// <summary>Topics currently holding a retained message.</summary>
    public int Retained { get; init; }

    /// <summary>A one-line summary, handy in a log or a status bar.</summary>
    public override string ToString() =>
        $"published={Published:N0} delivered={Delivered:N0} unrouted={Unrouted:N0} " +
        $"errors={Errors:N0} drops={StreamDrops:N0} routes={Routes} subs={Subscriptions}";
}
