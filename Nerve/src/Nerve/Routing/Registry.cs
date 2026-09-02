// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve.Routing;

/// <summary>
/// Everything the hub knows about one message type: the wildcard subscriptions, and the routes
/// that have been resolved so far.
/// </summary>
/// <remarks>
/// Exact subscriptions are deliberately not held here - they live on the route they name, so
/// subscribing to <c>agents/task/writer</c> invalidates that one route and nothing else. Only
/// wildcard subscriptions are global to the type, and only they bump <see cref="WildcardVersion"/>.
/// A hub that never uses a wildcard leaves the version at zero, which routes read as "the exact
/// list is the whole answer" and skip merging entirely.
/// </remarks>
internal sealed class Registry<T>
{
    private readonly Lock _gate = new();
    private Subscription<T>[] _wildcards = [];
    private Route<T>[] _routes = [];
    private int _wildcardVersion;

    internal int WildcardVersion => Volatile.Read(ref _wildcardVersion);

    internal Subscription<T>[] Wildcards => Volatile.Read(ref _wildcards);

    internal Route<T>[] Routes => Volatile.Read(ref _routes);

    internal void AddWildcard(Subscription<T> subscription)
    {
        lock (_gate)
        {
            _wildcards = [.. _wildcards, subscription];
            Interlocked.Increment(ref _wildcardVersion);
        }
    }

    internal void RemoveWildcard(Subscription<T> subscription)
    {
        lock (_gate)
        {
            int index = Array.IndexOf(_wildcards, subscription);
            if (index < 0) return;

            var next = new Subscription<T>[_wildcards.Length - 1];
            Array.Copy(_wildcards, next, index);
            Array.Copy(_wildcards, index + 1, next, index, next.Length - index);
            _wildcards = next;
            Interlocked.Increment(ref _wildcardVersion);
        }
    }

    internal void AddRoute(Route<T> route)
    {
        lock (_gate) _routes = [.. _routes, route];
    }
}
