// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;

namespace Nerve.Benchmarks.Baseline;

/// <summary>
/// Nerve v1, kept verbatim so the v2 numbers are measured against something real rather than
/// against an estimate. Do not tidy this up - its costs are the point.
/// </summary>
/// <remarks>
/// Three of them show up in every measurement: the handler list is copied to an array under a lock
/// on every publish, each message is boxed into <see cref="object"/> and type-tested with
/// <c>is T</c> inside the handler, and every dispatch runs through an async wrapper that allocates
/// a state machine whether or not the handler is synchronous.
/// </remarks>
public class LegacyHub
{
    private readonly ConcurrentDictionary<string, List<HandlerWrapper>> _subscriptions = new();

    private sealed class HandlerWrapper
    {
        public Guid Id { get; } = Guid.NewGuid();
        public Func<object, Task> Action { get; set; } = _ => Task.CompletedTask;
    }

    public IDisposable Subscribe<T>(string topic, Func<T, Task> handler)
    {
        var wrapper = new HandlerWrapper
        {
            Action = async obj =>
            {
                if (obj is T typed) await handler(typed);
            },
        };

        _subscriptions.AddOrUpdate(
            topic,
            _ => [wrapper],
            (_, existing) =>
            {
                lock (existing) existing.Add(wrapper);
                return existing;
            });

        return new SubscriptionToken(() => Unsubscribe(topic, wrapper));
    }

    public IDisposable Subscribe<T>(string topic, Action<T> handler) =>
        Subscribe<T>(topic, message =>
        {
            handler(message);
            return Task.CompletedTask;
        });

    public async Task PublishAsync<T>(string topic, T message)
    {
        if (!_subscriptions.TryGetValue(topic, out List<HandlerWrapper>? handlers)) return;

        HandlerWrapper[] snapshot;
        lock (handlers) snapshot = handlers.ToArray();

        foreach (HandlerWrapper handler in snapshot)
        {
            try
            {
                await handler.Action(message!);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Nerve Error] Error on handling topic {topic}: {ex.Message}");
            }
        }
    }

    public void Publish<T>(string topic, T message) => _ = PublishAsync(topic, message);

    private void Unsubscribe(string topic, HandlerWrapper wrapper)
    {
        if (!_subscriptions.TryGetValue(topic, out List<HandlerWrapper>? handlers)) return;
        lock (handlers) handlers.RemoveAll(x => x.Id == wrapper.Id);
    }

    private sealed class SubscriptionToken(Action unsubscribe) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            unsubscribe();
            _disposed = true;
        }
    }
}
