// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve.Routing;

/// <summary>
/// The resolved dispatch target for one concrete topic carrying one message type. This is the hot
/// path: every publish ends here.
/// </summary>
/// <remarks>
/// <para>
/// A route holds its subscribers as an immutable array that is swapped wholesale on change, so
/// dispatch reads it with a single volatile read and takes no lock. Publishing to a topic with no
/// subscribers, one subscriber, or many subscribers that all complete synchronously allocates
/// nothing at all - the loop only turns into a state machine at the first handler that genuinely
/// suspends.
/// </para>
/// <para>
/// Counters live per route rather than per hub. Two threads publishing to different topics never
/// touch the same cache line, and the hub sums them when someone asks for statistics.
/// </para>
/// </remarks>
internal sealed class Route<T> : IRouteCounters
{
    internal readonly string Topic;

    private readonly NerveHub _hub;
    private readonly Registry<T> _registry;
    private readonly Lock _gate = new();

    private Subscription<T>[] _exact = [];
    private Subscription<T>[] _merged = [];
    private int _exactStamp;
    private int _mergedExactStamp = -1;
    private int _mergedWildcardVersion = -1;

    private T _retained = default!;
    private bool _hasRetained;

    private long _published;
    private long _delivered;
    private long _unrouted;
    private long _errors;

    internal Route(NerveHub hub, Registry<T> registry, string topic)
    {
        _hub = hub;
        _registry = registry;
        Topic = topic;
    }

    public long Published => Volatile.Read(ref _published);
    public long Delivered => Volatile.Read(ref _delivered);
    public long Unrouted => Volatile.Read(ref _unrouted);
    public long Errors => Volatile.Read(ref _errors);

    public bool HasRetained
    {
        get { lock (_gate) return _hasRetained; }
    }
    internal int SubscriberCount => Handlers.Length;
    internal bool HasSubscribers => Handlers.Length != 0;

    // ============================== Dispatch ==============================

    /// <summary>
    /// The subscribers to run, exact ones first. Rebuilt only when the exact list on this route or
    /// the message type's wildcard set has changed since it was last assembled.
    /// </summary>
    private Subscription<T>[] Handlers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            int wildcardVersion = _registry.WildcardVersion;

            // No wildcard has ever been registered for this message type, so there is nothing to
            // merge and the exact list can be handed out directly.
            if (wildcardVersion == 0) return Volatile.Read(ref _exact);

            if (Volatile.Read(ref _mergedWildcardVersion) == wildcardVersion
                && Volatile.Read(ref _mergedExactStamp) == Volatile.Read(ref _exactStamp))
                return Volatile.Read(ref _merged);

            return Rebuild();
        }
    }

    internal ValueTask PublishAsync(T message, CancellationToken cancellationToken)
    {
        Subscription<T>[] handlers = Handlers;
        bool stats = _hub.CollectStatistics;

        if (stats) Interlocked.Increment(ref _published);

        if (handlers.Length == 0)
        {
            if (stats) Interlocked.Increment(ref _unrouted);
            return default;
        }

        for (int i = 0; i < handlers.Length; i++)
        {
            Subscription<T> subscription = handlers[i];
            if (!subscription.Active) continue;

            ValueTask pending;
            try
            {
                pending = subscription.InvokeAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                if (_hub.PropagateHandlerErrors) return ValueTask.FromException(Fail(ex, subscription));
                Report(ex, subscription);
                continue;
            }

            // The first handler that actually suspends is where this stops being allocation-free.
            if (!pending.IsCompletedSuccessfully)
                return DispatchRestAsync(handlers, i, pending, message, cancellationToken);

            if (stats) Interlocked.Increment(ref _delivered);
        }

        return default;
    }

    private async ValueTask DispatchRestAsync(
        Subscription<T>[] handlers, int index, ValueTask pending, T message, CancellationToken cancellationToken)
    {
        bool stats = _hub.CollectStatistics;

        try
        {
            await pending.ConfigureAwait(false);
            if (stats) Interlocked.Increment(ref _delivered);
        }
        catch (Exception ex)
        {
            if (_hub.PropagateHandlerErrors) throw Fail(ex, handlers[index]);
            Report(ex, handlers[index]);
        }

        for (int i = index + 1; i < handlers.Length; i++)
        {
            Subscription<T> subscription = handlers[i];
            if (!subscription.Active) continue;

            try
            {
                await subscription.InvokeAsync(message, cancellationToken).ConfigureAwait(false);
                if (stats) Interlocked.Increment(ref _delivered);
            }
            catch (Exception ex)
            {
                if (_hub.PropagateHandlerErrors) throw Fail(ex, subscription);
                Report(ex, subscription);
            }
        }
    }

    private void Report(Exception exception, Subscription<T> subscription)
    {
        Interlocked.Increment(ref _errors);
        _hub.ReportHandlerError(new NerveError(Topic, typeof(T), subscription.Filter, exception));
    }

    private Exception Fail(Exception exception, Subscription<T> subscription)
    {
        Interlocked.Increment(ref _errors);
        return new NerveHandlerException(Topic, typeof(T), subscription.Filter, exception);
    }

    // ============================ Registration ============================

    internal void AddExact(Subscription<T> subscription)
    {
        lock (_gate)
        {
            _exact = [.. _exact, subscription];
            _exactStamp++;
        }
    }

    internal void RemoveExact(Subscription<T> subscription)
    {
        lock (_gate)
        {
            int index = Array.IndexOf(_exact, subscription);
            if (index < 0) return;

            var next = new Subscription<T>[_exact.Length - 1];
            Array.Copy(_exact, next, index);
            Array.Copy(_exact, index + 1, next, index, next.Length - index);
            _exact = next;
            _exactStamp++;
        }
    }

    private Subscription<T>[] Rebuild()
    {
        lock (_gate)
        {
            // Read both stamps before the data they describe. A change landing in between makes
            // this rebuild look stale and costs one extra rebuild later - never a missed handler.
            int wildcardVersion = _registry.WildcardVersion;
            int exactStamp = _exactStamp;
            Subscription<T>[] wildcards = _registry.Wildcards;
            Subscription<T>[] exact = _exact;

            int matches = 0;
            for (int i = 0; i < wildcards.Length; i++)
                if (TopicFilter.Matches(wildcards[i].Filter, Topic)) matches++;

            Subscription<T>[] merged;
            if (matches == 0)
            {
                merged = exact;
            }
            else
            {
                merged = new Subscription<T>[exact.Length + matches];
                exact.CopyTo(merged, 0);
                int next = exact.Length;
                for (int i = 0; i < wildcards.Length; i++)
                    if (TopicFilter.Matches(wildcards[i].Filter, Topic)) merged[next++] = wildcards[i];
            }

            Volatile.Write(ref _merged, merged);
            Volatile.Write(ref _mergedExactStamp, exactStamp);
            Volatile.Write(ref _mergedWildcardVersion, wildcardVersion);
            return merged;
        }
    }

    // ============================== Retained ==============================

    internal void SetRetained(T message)
    {
        lock (_gate)
        {
            _retained = message;
            _hasRetained = true;
        }
    }

    internal void ClearRetained()
    {
        lock (_gate)
        {
            _retained = default!;
            _hasRetained = false;
        }
    }

    internal bool TryGetRetained(out T message)
    {
        lock (_gate)
        {
            message = _retained;
            return _hasRetained;
        }
    }
}
