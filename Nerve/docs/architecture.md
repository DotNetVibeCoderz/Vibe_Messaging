# Architecture

Built by Gravicode Studios, led by Kang Fadhil.

Read this before changing anything under `src/Nerve/Routing/`.

## The shape of it

```
NerveHub
  _routes      ConcurrentDictionary<ChannelKey, object>   topic + type  ->  Route<T>
  _registries  ConcurrentDictionary<Type, object>         type          ->  Registry<T>

Route<T>       one concrete topic carrying one message type
  _exact       Subscription<T>[]   subscribers naming this topic exactly
  _merged      Subscription<T>[]   those, plus every wildcard that covers it
  _retained    T                   the topic's retained value, if any
  counters     long x4             published, delivered, unrouted, errors

Registry<T>    everything type-wide
  _wildcards   Subscription<T>[]   wildcard subscriptions for this message type
  _routes      Route<T>[]          every route of this type, for the retained scan
```

`ChannelKey` is a struct holding the topic string, the message `Type`, and a hash computed once at
construction. Equality compares the hash, then the type by reference, then the string ordinally — so
most misses are settled before the string comparison runs.

## What a publish does

```csharp
public ValueTask PublishAsync<T>(string topic, T message, CancellationToken ct = default)
    => GetRoute<T>(topic).PublishAsync(message, ct);
```

One dictionary lookup, then `Route<T>.PublishAsync`:

1. Read the handler array (one volatile read; see below).
2. Increment `_published`, if statistics are on.
3. If the array is empty, increment `_unrouted` and return `default`.
4. Walk it. For each live subscription, call the handler. If the returned `ValueTask` is already
   completed, carry on to the next one.
5. At the **first handler that actually suspends**, hand the rest to an `async` continuation.

Step 5 is the whole trick. A `ValueTask`-returning method that never awaits allocates nothing, so a
publish to any number of synchronous subscribers stays allocation-free. Only a genuinely
asynchronous handler brings a state machine into existence, and only from that handler onwards.

### Why there is no boxing

`_routes` is keyed on `typeof(T)` as part of `ChannelKey`, so the value stored against a key can
only ever be a `Route<T>` for that same `T`. The lookup uses `Unsafe.As<Route<T>>` rather than a
cast, and the handler array is `Subscription<T>[]` — a `struct` message travels from
`PublishAsync<T>` to the handler without ever becoming an `object`.

This is also why publishing an `int` to a topic subscribed as `long` reaches nobody: they are
different keys. That is the cost of the design, and it is a deliberate trade.

### Why there is no lock

Handler arrays are immutable. Subscribing builds a new array and swaps it in under a lock;
publishing reads it with `Volatile.Read` and takes no lock at all. A subscription that arrives
mid-publish is picked up by the next publish, and one that is disposed mid-publish stops
immediately, because dispatch checks `Subscription<T>.Active` on every handler.

Counters live on the route rather than the hub, so two threads publishing to different topics never
touch the same cache line. `GetStatistics()` sums them when asked.

## Wildcard resolution

The naive design keeps every subscription in one list and matches filters on every publish. Nerve
does not: matching happens when a route is first asked for its handlers, and the answer is cached.

- **Exact subscriptions live on the route they name.** Subscribing to `agents/task/writer`
  invalidates that one route and nothing else.
- **Wildcard subscriptions live on the type's `Registry<T>`** and bump a `WildcardVersion`.
- A route rebuilds its merged array only when its own exact list has changed or the type's
  `WildcardVersion` has moved.
- **When `WildcardVersion` is still zero** — no wildcard has ever been registered for this message
  type — the route skips merging entirely and hands out its exact array directly.

Rebuild reads both version stamps *before* the data they describe. A change landing in between makes
the rebuild look stale and costs one extra rebuild later; it can never lose a handler.

The consequence: a wildcard subscriber is free per message, and the cost of having one shows up
where you would expect it — on the first publish to each new topic, which is 265 ns including the
topic string's own allocation.

## Retained messages

`Route<T>` holds a typed `_retained` field, so a retained value is not boxed either. It is read and
written under the route's lock, because assigning a struct larger than a word is not atomic and
retained operations are rare enough that it does not matter.

On subscribe:

- An **exact** subscription is offered its own route's retained value.
- A **wildcard** subscription walks `Registry<T>.Routes` and is offered every matching topic's
  retained value.

That walk is why the registry keeps a route list at all — the hot-path dictionary is keyed for
lookup, not enumeration.

## Request/reply and streams

Neither is a special case in the dispatch path.

**Request/reply** publishes a `NerveRequest<TRequest, TResponse>` — a class holding the payload and a
`TaskCompletionSource`. `Respond` is a subscription to that envelope type; `RequestAsync` publishes
one and awaits the completion source. Wildcards, statistics and ordinary observers all work on
request topics for free, because it is all just pub/sub.

**Streams** are a subscription that writes into a bounded `System.Threading.Channels.Channel<T>` with
`BoundedChannelFullMode.DropOldest`. `TryWrite` on such a channel never blocks and never fails for
fullness, so the publisher is never held up; the consumer drains the reader. The drop count is
sampled on the way in, which is approximate under concurrency but honest about a consumer falling
behind.

## Error handling

`Route<T>` catches around every handler invocation. Under the default `Isolate`, the failure is
counted, reported through `NerveHub.ReportHandlerError`, and the walk continues. Under `Propagate`
the walk stops and the failure is returned as a faulted `ValueTask` wrapping
`NerveHandlerException` — returned rather than thrown, so a synchronous failure behaves the same way
as an asynchronous one at the call site.

The reporter itself is wrapped in a bare `catch`. An error handler that throws must not take down
the publisher that tripped it.

## What this costs

| | |
|---|---|
| Per publish | one dictionary lookup, one volatile read, one delegate call per subscriber |
| Per publish, allocation | none, until a handler suspends |
| Per subscribe | one array copy on the affected route, or one on the registry for a wildcard |
| Per new topic | one route object, one registry entry, and the wildcard match for each filter |

The design is tuned for hubs where subscriptions are set up once and messages flow afterwards. A
workload that subscribes and unsubscribes as often as it publishes would spend most of its time
copying arrays — that is the trade a copy-on-write structure makes, and it is the right one here.
