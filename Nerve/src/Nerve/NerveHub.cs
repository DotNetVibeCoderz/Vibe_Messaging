// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Nerve.Routing;

namespace Nerve;

/// <summary>
/// An in-process publish/subscribe hub: MQTT-shaped topics and wildcards, but every message stays
/// inside your application's memory and is handed straight to the subscriber.
/// </summary>
/// <remarks>
/// <para>
/// One hub per application is normal. It is safe to publish and subscribe from any thread at any
/// time, and nothing here starts a thread of its own: a message is delivered on the thread that
/// published it, so a synchronous handler has finished before <see cref="Publish{T}(string, T)"/>
/// returns. Handlers that want their own thread should say so - use
/// <see cref="StreamAsync{T}(string, int, CancellationToken)"/>, which gives the consumer its own
/// loop and a buffer, and never lets it block the publisher.
/// </para>
/// <para>
/// Messages are routed by topic <em>and</em> by type. Publishing an <c>int</c> to a topic somebody
/// subscribed to as <c>string</c> reaches nobody - the two are different routes that happen to
/// share a name. This is what lets the whole dispatch path stay free of boxing and casts.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// var nerve = new NerveHub();
/// using var _ = nerve.Subscribe&lt;double&gt;("sensor/+/temperature", t => Console.WriteLine(t));
/// await nerve.PublishAsync("sensor/tank-3/temperature", 28.4);
/// </code>
/// </example>
public sealed partial class NerveHub : INerveHub, IDisposable
{
    private readonly ConcurrentDictionary<ChannelKey, object> _routes = new();
    private readonly ConcurrentDictionary<Type, object> _registries = new();
    private readonly Action<NerveError>? _onError;

    private int _subscriptions;
    private long _streamDrops;
    private volatile bool _disposed;

    /// <summary>Creates a hub with default options.</summary>
    public NerveHub() : this(null) { }

    /// <summary>Creates a hub.</summary>
    /// <param name="options">Settings, or <see langword="null"/> for the defaults.</param>
    public NerveHub(NerveOptions? options)
    {
        NerveOptions effective = options ?? new NerveOptions();
        CollectStatistics = effective.CollectStatistics;
        PropagateHandlerErrors = effective.ErrorBehavior == HandlerErrorBehavior.Propagate;
        DefaultRequestTimeout = effective.DefaultRequestTimeout;
        DefaultStreamCapacity = effective.DefaultStreamCapacity;
        _onError = effective.OnError;
    }

    /// <summary>
    /// A process-wide hub, for applications that want one bus and no plumbing to pass it around.
    /// Anything that can be unit tested should take an <see cref="INerveHub"/> instead.
    /// </summary>
    public static NerveHub Shared { get; } = new();

    /// <summary>Raised for every subscriber failure. Never raised on the publishing path twice for
    /// the same failure, and never allowed to throw back into dispatch.</summary>
    public event Action<NerveError>? HandlerError;

    internal bool CollectStatistics { get; }
    internal bool PropagateHandlerErrors { get; }
    internal TimeSpan DefaultRequestTimeout { get; }
    internal int DefaultStreamCapacity { get; }

    // ============================== Publishing ==============================

    /// <summary>
    /// Publishes without waiting for the subscribers. Synchronous handlers still run inline and
    /// have finished by the time this returns; genuinely asynchronous ones continue on their own,
    /// and anything they throw is reported through <see cref="HandlerError"/>.
    /// </summary>
    /// <typeparam name="T">The message type. Part of the route.</typeparam>
    /// <param name="topic">A concrete topic. Wildcards are not allowed here.</param>
    /// <param name="message">The message.</param>
    public void Publish<T>(string topic, T message)
    {
        ValueTask pending = PublishAsync(topic, message);
        if (pending.IsCompletedSuccessfully) return;
        ObserveDetached(pending, topic);
    }

    /// <summary>
    /// Publishes and completes when every subscriber has finished. Allocates nothing when the
    /// subscribers are synchronous.
    /// </summary>
    /// <typeparam name="T">The message type. Part of the route.</typeparam>
    /// <param name="topic">A concrete topic. Wildcards are not allowed here.</param>
    /// <param name="message">The message.</param>
    /// <param name="cancellationToken">Passed to handlers that asked for one.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask PublishAsync<T>(string topic, T message, CancellationToken cancellationToken = default)
        => GetRoute<T>(topic).PublishAsync(message, cancellationToken);

    /// <summary>
    /// Publishes, and keeps the message as the topic's retained value: whoever subscribes next
    /// receives it immediately, before any newly published message.
    /// </summary>
    /// <typeparam name="T">The message type. Part of the route.</typeparam>
    /// <param name="topic">A concrete topic. Wildcards are not allowed here.</param>
    /// <param name="message">The message to deliver and retain.</param>
    /// <param name="cancellationToken">Passed to handlers that asked for one.</param>
    /// <remarks>
    /// One value per topic, replaced on each call. This is how a late joiner learns the current
    /// state of something - a roster, a configuration, the last reading - without asking for it.
    /// </remarks>
    public ValueTask PublishRetainedAsync<T>(string topic, T message, CancellationToken cancellationToken = default)
    {
        Route<T> route = GetRoute<T>(topic);
        route.SetRetained(message);
        return route.PublishAsync(message, cancellationToken);
    }

    /// <summary>Fire-and-forget form of <see cref="PublishRetainedAsync{T}"/>.</summary>
    /// <typeparam name="T">The message type. Part of the route.</typeparam>
    /// <param name="topic">A concrete topic. Wildcards are not allowed here.</param>
    /// <param name="message">The message to deliver and retain.</param>
    public void PublishRetained<T>(string topic, T message)
    {
        ValueTask pending = PublishRetainedAsync(topic, message);
        if (pending.IsCompletedSuccessfully) return;
        ObserveDetached(pending, topic);
    }

    /// <summary>Forgets a topic's retained message. New subscribers get nothing until the next
    /// retained publish.</summary>
    /// <typeparam name="T">The message type. Part of the route.</typeparam>
    /// <param name="topic">The topic to clear.</param>
    public void ClearRetained<T>(string topic) => GetRoute<T>(topic).ClearRetained();

    /// <summary>Reads a topic's retained message without subscribing.</summary>
    /// <typeparam name="T">The message type. Part of the route.</typeparam>
    /// <param name="topic">The topic to read.</param>
    /// <param name="message">The retained message, when there is one.</param>
    /// <returns><see langword="true"/> when the topic holds a retained message.</returns>
    public bool TryGetRetained<T>(string topic, out T message) => GetRoute<T>(topic).TryGetRetained(out message);

    // ============================= Subscribing =============================

    /// <summary>Subscribes with a synchronous handler - the cheapest shape there is.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="handler">Runs on the publishing thread.</param>
    /// <returns>Dispose it to unsubscribe.</returns>
    public IDisposable Subscribe<T>(string topicFilter, Action<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return SubscribeCore(topicFilter, handler, null, null, null, once: false);
    }

    /// <summary>Subscribes with an asynchronous handler.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="handler">Awaited by <see cref="PublishAsync{T}"/>.</param>
    /// <returns>Dispose it to unsubscribe.</returns>
    public IDisposable Subscribe<T>(string topicFilter, Func<T, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return SubscribeCore(topicFilter, null, handler, null, null, once: false);
    }

    /// <summary>Subscribes with an asynchronous handler that receives the publisher's token.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="handler">Awaited by <see cref="PublishAsync{T}"/>.</param>
    /// <returns>Dispose it to unsubscribe.</returns>
    public IDisposable Subscribe<T>(string topicFilter, Func<T, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return SubscribeCore(topicFilter, null, null, handler, null, once: false);
    }

    /// <summary>Subscribes to the messages on a topic that satisfy a predicate.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="predicate">Tested first; a message it rejects never reaches the handler.</param>
    /// <param name="handler">Runs on the publishing thread.</param>
    /// <returns>Dispose it to unsubscribe.</returns>
    /// <remarks>
    /// The predicate runs inside dispatch, so keep it cheap and free of side effects. It exists so
    /// a subscriber can decline a message without the cost of an <c>if</c> in every handler and,
    /// more usefully, so <see cref="WaitForAsync{T}"/> can wait for a particular message.
    /// </remarks>
    public IDisposable Subscribe<T>(string topicFilter, Predicate<T> predicate, Action<T> handler)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(handler);
        return SubscribeCore(topicFilter, handler, null, null, predicate, once: false);
    }

    /// <summary>Subscribes for exactly one message, then unsubscribes itself.</summary>
    /// <typeparam name="T">The message type to receive.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="handler">Runs once.</param>
    /// <returns>Dispose it to cancel before the message arrives.</returns>
    public IDisposable SubscribeOnce<T>(string topicFilter, Action<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return SubscribeCore(topicFilter, handler, null, null, null, once: true);
    }

    private IDisposable SubscribeCore<T>(
        string topicFilter,
        Action<T>? sync,
        Func<T, ValueTask>? valueTask,
        Func<T, CancellationToken, ValueTask>? cancellable,
        Predicate<T>? predicate,
        bool once)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        TopicFilter.ValidateFilter(topicFilter);

        Registry<T> registry = GetRegistry<T>();
        bool wildcard = TopicFilter.IsWildcard(topicFilter);
        Route<T>? route = wildcard ? null : GetRoute<T>(topicFilter);

        Action<Subscription<T>> release = wildcard
            ? s => { registry.RemoveWildcard(s); Interlocked.Decrement(ref _subscriptions); }
            : s => { route!.RemoveExact(s); Interlocked.Decrement(ref _subscriptions); };

        var subscription = new Subscription<T>(
            topicFilter, sync, valueTask, cancellable, predicate, once, release);

        if (wildcard) registry.AddWildcard(subscription);
        else route!.AddExact(subscription);

        Interlocked.Increment(ref _subscriptions);

        DeliverRetained(subscription, route, registry);
        return new SubscriptionToken<T>(subscription);
    }

    /// <summary>
    /// Hands a brand-new subscriber whatever the topics it covers are currently holding, the way
    /// an MQTT broker replays retained messages on subscribe.
    /// </summary>
    private void DeliverRetained<T>(Subscription<T> subscription, Route<T>? route, Registry<T> registry)
    {
        if (route is not null)
        {
            if (route.TryGetRetained(out T message)) Deliver(subscription, route.Topic, message);
            return;
        }

        foreach (Route<T> candidate in registry.Routes)
        {
            if (!TopicFilter.Matches(subscription.Filter, candidate.Topic)) continue;
            if (candidate.TryGetRetained(out T message)) Deliver(subscription, candidate.Topic, message);
        }

        void Deliver(Subscription<T> target, string topic, T message)
        {
            try
            {
                ValueTask pending = target.InvokeAsync(message, CancellationToken.None);
                if (!pending.IsCompletedSuccessfully) ObserveDetached(pending, topic);
            }
            catch (Exception ex)
            {
                ReportHandlerError(new NerveError(topic, typeof(T), target.Filter, ex));
            }
        }
    }

    // ============================== Inspection ==============================

    /// <summary>True when at least one subscriber would receive a message published here.</summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="topic">A concrete topic.</param>
    public bool HasSubscribers<T>(string topic) => GetRoute<T>(topic).HasSubscribers;

    /// <summary>How many subscribers a topic currently resolves to, wildcards included.</summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="topic">A concrete topic.</param>
    public int SubscriberCount<T>(string topic) => GetRoute<T>(topic).SubscriberCount;

    /// <summary>
    /// A pre-resolved handle to one topic. Publishing through it skips the dictionary lookup, which
    /// is the only remaining per-message cost on the hot path.
    /// </summary>
    /// <typeparam name="T">The message type carried on the topic.</typeparam>
    /// <param name="topic">A concrete topic.</param>
    /// <remarks>Hold one of these in a field when a component publishes to the same topic in a loop.</remarks>
    public NerveTopic<T> Topic<T>(string topic) => new(this, GetRoute<T>(topic));

    /// <summary>Sums the per-route counters into one snapshot.</summary>
    /// <remarks>
    /// This walks every route, so it is a diagnostic call rather than something to put in a tight
    /// loop. A status bar refreshing a few times a second is exactly the right use.
    /// </remarks>
    public NerveStatistics GetStatistics()
    {
        long published = 0, delivered = 0, unrouted = 0, errors = 0;
        int retained = 0;

        foreach (object route in _routes.Values)
        {
            var counters = (IRouteCounters)route;
            published += counters.Published;
            delivered += counters.Delivered;
            unrouted += counters.Unrouted;
            errors += counters.Errors;
            if (counters.HasRetained) retained++;
        }

        return new NerveStatistics
        {
            Published = published,
            Delivered = delivered,
            Unrouted = unrouted,
            Errors = errors,
            StreamDrops = Volatile.Read(ref _streamDrops),
            Routes = _routes.Count,
            Subscriptions = Volatile.Read(ref _subscriptions),
            Retained = retained,
        };
    }

    // ============================== Internals ==============================

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Route<T> GetRoute<T>(string topic)
    {
        // Unsafe.As rather than a cast: the dictionary is keyed on typeof(T), so the value can only
        // ever be a Route<T>, and this is the one lookup every published message pays for.
        var key = new ChannelKey(topic, typeof(T));
        return _routes.TryGetValue(key, out object? existing)
            ? Unsafe.As<Route<T>>(existing)
            : CreateRoute<T>(key, topic);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private Route<T> CreateRoute<T>(ChannelKey key, string topic)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        TopicFilter.ValidateTopic(topic);

        Registry<T> registry = GetRegistry<T>();
        var created = new Route<T>(this, registry, topic);
        object stored = _routes.GetOrAdd(key, created);

        // Only the thread whose route actually went in publishes it to the registry, so the
        // retained-message scan never sees a duplicate.
        if (ReferenceEquals(stored, created)) registry.AddRoute(created);
        return Unsafe.As<Route<T>>(stored);
    }

    private Registry<T> GetRegistry<T>() =>
        Unsafe.As<Registry<T>>(_registries.GetOrAdd(typeof(T), static _ => new Registry<T>()));

    internal void ReportHandlerError(NerveError error)
    {
        try
        {
            _onError?.Invoke(error);
            HandlerError?.Invoke(error);
        }
        catch
        {
            // An error reporter that throws must not take down the publisher that tripped it.
        }
    }

    internal void CountStreamDrop() => Interlocked.Increment(ref _streamDrops);

    /// <summary>
    /// Keeps a fire-and-forget publish from becoming an unobserved task exception. The continuation
    /// is only ever allocated for a publish that actually suspended.
    /// </summary>
    internal void ObserveDetached(ValueTask pending, string topic)
    {
        _ = Awaited(this, pending, topic);

        static async Task Awaited(NerveHub hub, ValueTask pending, string topic)
        {
            try
            {
                await pending.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                hub.ReportHandlerError(new NerveError(topic, typeof(void), "(fire-and-forget)", ex));
            }
        }
    }

    /// <summary>
    /// Drops every route and subscription. Handlers already running are not interrupted; nothing
    /// new is dispatched afterwards.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _routes.Clear();
        _registries.Clear();
        Volatile.Write(ref _subscriptions, 0);
    }
}

/// <summary>
/// Lets the hub read counters off a <c>Route&lt;T&gt;</c> without knowing what <c>T</c> is.
/// </summary>
internal interface IRouteCounters
{
    long Published { get; }
    long Delivered { get; }
    long Unrouted { get; }
    long Errors { get; }
    bool HasRetained { get; }
}
