# API reference

Built by Gravicode Studios, led by Kang Fadhil.

Everything public, in one place. Namespace `Nerve` unless noted.

## NerveHub

`sealed class NerveHub : INerveHub, IDisposable`

### Construction

| | |
|---|---|
| `NerveHub()` | Default options. |
| `NerveHub(NerveOptions? options)` | `null` means the defaults. |
| `static NerveHub Shared { get; }` | A process-wide hub, for applications that would rather not pass one around. |

### Publishing

| | |
|---|---|
| `void Publish<T>(string topic, T message)` | Delivers without waiting. Synchronous handlers have still finished when this returns. |
| `ValueTask PublishAsync<T>(string topic, T message, CancellationToken = default)` | Completes when every subscriber has finished. Allocates nothing when they are all synchronous. |
| `ValueTask PublishRetainedAsync<T>(string topic, T message, CancellationToken = default)` | Publishes, and keeps the message as the topic's retained value. |
| `void PublishRetained<T>(string topic, T message)` | Fire-and-forget form of the above. |
| `void ClearRetained<T>(string topic)` | Forgets a topic's retained message. |
| `bool TryGetRetained<T>(string topic, out T message)` | Reads it without subscribing. |

`topic` must be a concrete topic. Publishing to one containing `+` or `#` throws
`ArgumentException`.

### Subscribing

Every overload returns an `IDisposable`. Dispose it to unsubscribe; disposing twice is harmless.

| | |
|---|---|
| `IDisposable Subscribe<T>(string topicFilter, Action<T> handler)` | Synchronous. The cheapest shape. |
| `IDisposable Subscribe<T>(string topicFilter, Func<T, ValueTask> handler)` | Asynchronous. |
| `IDisposable Subscribe<T>(string topicFilter, Func<T, CancellationToken, ValueTask> handler)` | Asynchronous, given the publisher's token. |
| `IDisposable Subscribe<T>(string topicFilter, Predicate<T> predicate, Action<T> handler)` | The predicate runs inside dispatch; a message it rejects never reaches the handler. |
| `IDisposable SubscribeOnce<T>(string topicFilter, Action<T> handler)` | Fires once, then unsubscribes itself. |

`topicFilter` may use `+` and `#`. A malformed filter throws `ArgumentException` at subscribe time.

### Request and reply

| | |
|---|---|
| `IDisposable Respond<TRequest, TResponse>(string topicFilter, Func<TRequest, CancellationToken, ValueTask<TResponse>> responder)` | Asynchronous responder. |
| `IDisposable Respond<TRequest, TResponse>(string topicFilter, Func<TRequest, TResponse> responder)` | Synchronous responder. |
| `Task<TResponse> RequestAsync<TRequest, TResponse>(string topic, TRequest request, TimeSpan? timeout = null, CancellationToken = default)` | Sends and waits for the first answer. |

`RequestAsync` throws `NerveNoResponderException` immediately when nothing is registered,
`TimeoutException` when nothing answers in time, and whatever the responder threw when it failed.
`timeout` defaults to `NerveOptions.DefaultRequestTimeout`; pass `Timeout.InfiniteTimeSpan` to wait
indefinitely.

### Streams and waiting

| | |
|---|---|
| `IAsyncEnumerable<T> StreamAsync<T>(string topicFilter, int capacity = 0, CancellationToken = default)` | Buffered, drop-oldest. `capacity` of 0 means `NerveOptions.DefaultStreamCapacity`. |
| `Task<T> WaitForAsync<T>(string topicFilter, Predicate<T>? match = null, TimeSpan? timeout = null, CancellationToken = default)` | The next matching message. `TimeoutException` if none arrives. |

The stream's subscription is registered on the first `MoveNextAsync` and disposed when the
enumeration ends, breaks or throws.

### Inspection

| | |
|---|---|
| `bool HasSubscribers<T>(string topic)` | Whether a message published here would reach anyone. |
| `int SubscriberCount<T>(string topic)` | How many, wildcards included. |
| `NerveTopic<T> Topic<T>(string topic)` | A pre-resolved handle, for publishing in a loop. |
| `NerveStatistics GetStatistics()` | Sums the per-route counters. Walks every route, so it is a diagnostic call. |
| `event Action<NerveError>? HandlerError` | Raised for every subscriber failure. Never allowed to throw back into dispatch. |

### Lifetime

`Dispose()` drops every route and subscription. Handlers already running are not interrupted;
nothing new is dispatched afterwards. Using a disposed hub throws `ObjectDisposedException`.

## NerveTopic&lt;T&gt;

`readonly struct NerveTopic<T>` — a topic resolved once, from `NerveHub.Topic<T>(topic)`.

| | |
|---|---|
| `string Name { get; }` | The topic. |
| `bool HasSubscribers { get; }` | Whether anyone is listening. |
| `int SubscriberCount { get; }` | How many. |
| `void Publish(T message)` | Without waiting. |
| `ValueTask PublishAsync(T message, CancellationToken = default)` | Waiting. |
| `ValueTask PublishRetainedAsync(T message, CancellationToken = default)` | And retain. |
| `IDisposable Subscribe(Action<T> handler)` | To this exact topic. |
| `IDisposable Subscribe(Func<T, ValueTask> handler)` | To this exact topic. |

## NerveOptions

`sealed class NerveOptions`

| | Default | |
|---|---|---|
| `HandlerErrorBehavior ErrorBehavior` | `Isolate` | What happens when a subscriber throws. |
| `bool CollectStatistics` | `true` | Whether per-route counters are kept. Off removes four interlocked increments per publish. |
| `Action<NerveError>? OnError` | `null` | Called for every handler failure, alongside the event. |
| `TimeSpan DefaultRequestTimeout` | 30 s | Used when `RequestAsync` is given no timeout. |
| `int DefaultStreamCapacity` | 1024 | Messages a stream buffers before the oldest is dropped. |

### HandlerErrorBehavior

| | |
|---|---|
| `Isolate` | Report the failure and carry on with the remaining subscribers. |
| `Propagate` | Abandon the rest and surface it to whoever awaited the publish, as `NerveHandlerException`. `Publish` still reports through the event, having nowhere to throw. |

## NerveStatistics

`readonly record struct NerveStatistics`

| | |
|---|---|
| `long Published` | Messages handed to `Publish` or `PublishAsync`. |
| `long Delivered` | Handler invocations that completed. One message to eight subscribers counts eight. |
| `long Unrouted` | Messages published to a topic nothing was listening to. |
| `long Errors` | Handler invocations that threw. |
| `long StreamDrops` | Messages dropped because a stream consumer fell behind. |
| `int Routes` | Distinct topic and message-type pairs resolved so far. |
| `int Subscriptions` | Live subscriptions across every route. |
| `int Retained` | Topics currently holding a retained message. |

`ToString()` gives a one-line summary suitable for a log or a status bar.

## NerveError

`readonly record struct NerveError(string Topic, Type MessageType, string SubscriptionFilter, Exception Exception)`

`Topic` is the concrete topic being published to; `SubscriptionFilter` is what the failing
subscription was registered under, which for a wildcard subscriber is not the same string.

## NerveRequest&lt;TRequest, TResponse&gt;

`sealed class NerveRequest<TRequest, TResponse>` — the envelope a request travels in. You only see
one if you subscribe to a request topic directly rather than using `Respond`.

| | |
|---|---|
| `TRequest Payload { get; }` | What the caller sent. |
| `CancellationToken CancellationToken { get; }` | The caller's token. |
| `bool IsAnswered { get; }` | Whether someone has already replied or failed it. |
| `bool Reply(TResponse response)` | Answers. `true` if this reply was the one that reached the caller. |
| `bool Fail(Exception exception)` | Fails the request. `true` if this failure was the one that reached the caller. |

## Exceptions

| | |
|---|---|
| `NerveHandlerException` | A subscriber failure, under `HandlerErrorBehavior.Propagate`. Carries `Topic`, `MessageType`, `SubscriptionFilter`, and the original as `InnerException`. |
| `NerveNoResponderException` | `RequestAsync` found nothing registered to answer. Carries `Topic`. Deliberately not a timeout. |

## TopicFilter

`static class TopicFilter`, namespace `Nerve.Routing` — the matcher, public so you can reuse it.

| | |
|---|---|
| `bool Matches(string filter, string topic)` | Whether the filter covers the topic. |
| `bool Matches(ReadOnlySpan<char> filter, ReadOnlySpan<char> topic)` | The same, allocation-free. |
| `bool IsWildcard(string filter)` | Whether it contains `+` or `#`. |
| `void ValidateFilter(string filter, string paramName = "topicFilter")` | Throws for a malformed filter. |
| `void ValidateTopic(string topic, string paramName = "topic")` | Throws for a topic containing a wildcard. |
| `const char Separator = '/'`, `SingleLevel = '+'`, `MultiLevel = '#'` | |

## INerveHub

The subset worth depending on from a component: `Publish`, `PublishAsync`,
`PublishRetainedAsync`, both `Subscribe` overloads, `SubscribeOnce`, both `Respond` overloads,
`RequestAsync`, `StreamAsync`, `WaitForAsync`, `Topic`, `HasSubscribers` and `GetStatistics`.
