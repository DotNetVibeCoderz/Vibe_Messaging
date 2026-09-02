# Getting started

Built by Gravicode Studios, led by Kang Fadhil.

## Install

```bash
dotnet add package Nerve
```

Nerve targets .NET 10 and has no dependencies.

## One hub

```csharp
using Nerve;

var nerve = new NerveHub();
```

One hub per application is normal. Register it as a singleton and take `INerveHub` in constructors
so a test can hand over a hub of its own:

```csharp
services.AddSingleton<NerveHub>();
services.AddSingleton<INerveHub>(sp => sp.GetRequiredService<NerveHub>());
```

There is also `NerveHub.Shared`, a process-wide instance, for applications small enough that passing
a hub around is more ceremony than it is worth.

## Publish and subscribe

```csharp
using IDisposable reader = nerve.Subscribe<double>("sensor/tank-3/temperature",
    celsius => Console.WriteLine($"{celsius:N1} C"));

await nerve.PublishAsync("sensor/tank-3/temperature", 28.4);
```

`Subscribe` returns the subscription. **Dispose it** — a subscription that is never disposed keeps
its handler, and everything the handler closes over, alive for as long as the hub is.

Four handler shapes are accepted:

```csharp
nerve.Subscribe<T>(topic, value => { });                        // synchronous
nerve.Subscribe<T>(topic, async value => await DoWork(value));   // ValueTask
nerve.Subscribe<T>(topic, (value, token) => Work(value, token)); // with the publisher's token
nerve.Subscribe<T>(topic, v => v.Priority > 3, value => { });    // filtered by a predicate
```

The synchronous one is the cheapest by a wide margin: it runs with a single delegate call and no
`ValueTask` machinery at all.

## Publishing, awaited or not

```csharp
await nerve.PublishAsync(topic, message);   // completes when every subscriber has finished
nerve.Publish(topic, message);              // returns without waiting
```

Both deliver the message the same way. The difference is only whether you wait: `Publish` is
`PublishAsync` with the result dropped, and anything an asynchronous handler throws afterwards is
reported through `HandlerError` rather than becoming an unobserved task exception.

## The two mistakes worth avoiding

### 1. Handlers run on the publisher's thread

Nerve starts no threads. A synchronous handler has finished before `Publish` returns, and an
`await Task.Delay(500)` inside a handler is five hundred milliseconds the publisher spends waiting.

That is the right default — it is what makes a publish cost 21 ns — but it means slow work does not
belong in a handler. Use a stream instead, which gives the consumer its own loop and a buffer:

```csharp
await foreach (Reading reading in nerve.StreamAsync<Reading>("sensor/#", cancellationToken: token))
{
    await WriteToDatabase(reading);   // takes as long as it likes; the publisher never waits
}
```

### 2. Topic and type both have to match

A route is a topic *and* a message type. These two do not talk to each other:

```csharp
nerve.Subscribe<int>("counter", v => { });
nerve.Publish("counter", 42L);   // long, not int - nobody receives this
```

Nothing throws, because there is no way to tell a deliberate multi-type topic from a typo. If a
message seems to vanish, check the type first — `nerve.GetStatistics().Unrouted` counts every
message published to a topic nothing was listening to.

## Errors

By default a subscriber that throws is reported and skipped, and the remaining subscribers still get
the message:

```csharp
nerve.HandlerError += error =>
    logger.LogError(error.Exception, "handler on {Filter} failed for {Topic}",
        error.SubscriptionFilter, error.Topic);
```

If you would rather the failure reached whoever published:

```csharp
var nerve = new NerveHub(new NerveOptions { ErrorBehavior = HandlerErrorBehavior.Propagate });
```

Then `await PublishAsync(...)` throws `NerveHandlerException` and the remaining subscribers are
skipped. `Publish` still reports through the event, because it has nowhere to throw.

## Checking what is going on

```csharp
NerveStatistics stats = nerve.GetStatistics();
Console.WriteLine(stats);
// published=14 delivered=16 unrouted=1 errors=1 drops=0 routes=10 subs=0
```

`Unrouted` is the useful one: it counts messages nobody was listening for, which is almost always a
topic typo or a type mismatch.

## Next

- [patterns.md](patterns.md) — wildcards, retained messages, request/reply, streams
- [architecture.md](architecture.md) — what actually happens on a publish
