// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve.Routing;

/// <summary>
/// One live subscription: the filter it was registered under and the delegate to run.
/// </summary>
/// <remarks>
/// The three delegate shapes are kept in separate fields rather than normalised into one canonical
/// signature at registration time. Normalising would mean allocating a wrapper closure per
/// subscription and paying two delegate invocations per message; keeping them apart costs two
/// null reference slots per subscription - paid once - and lets a plain <see cref="Action{T}"/>
/// handler run with a single call and no <see cref="ValueTask"/> machinery at all.
/// </remarks>
internal sealed class Subscription<T>
{
    internal readonly string Filter;
    internal readonly bool HasWildcard;
    internal readonly bool Once;

    private readonly Action<T>? _sync;
    private readonly Func<T, ValueTask>? _valueTask;
    private readonly Func<T, CancellationToken, ValueTask>? _cancellable;
    private readonly Predicate<T>? _predicate;

    private readonly Action<Subscription<T>> _release;
    private int _fired;
    private volatile bool _active = true;

    /// <summary>
    /// False once disposed. Checked on every dispatch so a disposed handler stops firing
    /// immediately, rather than lingering until routes notice the registration changed.
    /// </summary>
    internal bool Active => _active;

    internal Subscription(
        string filter,
        Action<T>? sync,
        Func<T, ValueTask>? valueTask,
        Func<T, CancellationToken, ValueTask>? cancellable,
        Predicate<T>? predicate,
        bool once,
        Action<Subscription<T>> release)
    {
        Filter = filter;
        HasWildcard = TopicFilter.IsWildcard(filter);
        Once = once;
        _sync = sync;
        _valueTask = valueTask;
        _cancellable = cancellable;
        _predicate = predicate;
        _release = release;
    }

    /// <summary>
    /// Runs the handler. Returns a completed <see cref="ValueTask"/> - allocating nothing - when
    /// the handler is synchronous, filtered out, or already spent.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask InvokeAsync(T message, CancellationToken cancellationToken)
    {
        if (_predicate is not null && !_predicate(message)) return default;

        if (Once)
        {
            if (Interlocked.Exchange(ref _fired, 1) != 0) return default;
            Dispose();
        }

        if (_sync is not null)
        {
            _sync(message);
            return default;
        }

        if (_valueTask is not null) return _valueTask(message);
        return _cancellable!(message, cancellationToken);
    }

    internal void Dispose()
    {
        if (!_active) return;
        _active = false;
        _release(this);
    }
}

/// <summary>The handle every <c>Subscribe</c> overload hands back.</summary>
internal sealed class SubscriptionToken<T> : IDisposable
{
    private Subscription<T>? _subscription;

    internal SubscriptionToken(Subscription<T> subscription) => _subscription = subscription;

    /// <summary>Unsubscribes. Safe to call more than once, and from any thread.</summary>
    public void Dispose() => Interlocked.Exchange(ref _subscription, null)?.Dispose();
}
