# Migrating from v1

Built by Gravicode Studios, led by Kang Fadhil.

v1 was a single file: a `ConcurrentDictionary<string, List<HandlerWrapper>>`, handlers boxed behind
`Func<object, Task>`, and a `is T` test inside each one. v2 keeps the shape of the API and replaces
everything underneath it. The v1 README is kept at [legacy-readme.md](legacy-readme.md).

## What still compiles

```csharp
var nerve = new NerveHub();
using var sub = nerve.Subscribe<double>("sensor/suhu", suhu => Console.WriteLine(suhu));
await nerve.PublishAsync("sensor/suhu", 25.5);
nerve.Publish("chat/general", "Halo dunia!");
```

All of that is unchanged. `Subscribe` still returns an `IDisposable`, `Publish` is still
fire-and-forget, and handlers still run on the publishing thread.

## What changed

### `PublishAsync` returns `ValueTask`, not `Task`

```csharp
await nerve.PublishAsync("t", 1);        // unchanged
Task t = nerve.PublishAsync("t", 1);     // no longer compiles
```

Awaiting is unaffected. If you genuinely need a `Task`, call `.AsTask()`.

This is what lets a publish to synchronous subscribers allocate nothing at all.

### `Func<T, Task>` handlers are now `Func<T, ValueTask>`

```csharp
// v1
nerve.Subscribe<double>("t", async v => { await Work(v); });
```

That lambda still compiles — an `async` lambda infers to whichever the parameter wants. What no
longer works is passing a **method group** that returns `Task`:

```csharp
nerve.Subscribe<double>("t", HandleAsync);            // if HandleAsync returns Task: no
nerve.Subscribe<double>("t", async v => await HandleAsync(v));   // yes
```

Better still, change the handler to return `ValueTask`.

The `Task`-returning overload was removed rather than kept alongside the `ValueTask` one because
having both made **every async lambda ambiguous** — `CS0121` on the most common way to write a
handler. One overload is worth more than the compatibility.

### Handler exceptions no longer print to the console

v1 wrote `[Nerve Error] ...` to `Console`. v2 reports them:

```csharp
nerve.HandlerError += error =>
    logger.LogError(error.Exception, "handler on {Filter} failed for {Topic}",
        error.SubscriptionFilter, error.Topic);

// or at construction:
var nerve = new NerveHub(new NerveOptions { OnError = e => logger.LogError(e.Exception, "...") });
```

The behaviour is otherwise the same: the failure is isolated and the remaining subscribers still get
the message. If you would rather it reached the publisher, pass
`ErrorBehavior = HandlerErrorBehavior.Propagate`.

### Topics are validated

Publishing to a topic containing `+` or `#` now throws `ArgumentException`, and so does subscribing
to a malformed filter such as `a/#/b`. In v1 both were ordinary strings that silently matched
nothing.

Validation happens when a route is first created, not on every publish, so it costs nothing per
message.

### Empty topics are no longer leaked

v1 never removed a topic's entry after the last unsubscribe, so a process that used many short-lived
topics grew forever. v2 keeps a route per topic too — but with no per-topic `List` allocation and no
lock, and the route is the thing that caches wildcard resolution. If you create unbounded distinct
topics, create a new hub for each batch rather than one hub for the process lifetime.

## What is new

| | |
|---|---|
| **Wildcards** | `+` and `#`, matched once per topic rather than once per message. |
| **Retained messages** | `PublishRetainedAsync`, delivered to whoever subscribes next. |
| **Request/reply** | `Respond` and `RequestAsync`, with deadlines and a missing-responder error. |
| **Streams** | `StreamAsync` for consumers that need their own thread. |
| **Waiting** | `WaitForAsync` and `SubscribeOnce`. |
| **Statistics** | `GetStatistics()`. |
| **Predicates** | `Subscribe<T>(topic, predicate, handler)`. |
| **Topic handles** | `Topic<T>(name)`, to skip the lookup when publishing in a loop. |
| **`INerveHub`** | For constructor injection. |

## What it bought

Measured on the machine in [performance.md](performance.md), same workload through both:

| | v1 | v2 |
|---|---|---|
| Publish by topic name | 70.8 ns | 32.4 ns |
| Allocated over 5,000,000 messages | 267 MB | 376 B |
| Gen0 collections | 66 | 0 |

The allocation is the interesting number. v1 boxed every value-type message, allocated a state
machine per handler invocation, and copied the handler list to a new array on every publish. v2 does
none of those.

## Upgrading

1. `dotnet add package Nerve` — v2 targets .NET 10.
2. Fix any `Task t = PublishAsync(...)` to `await` or `.AsTask()`.
3. Fix any method group handler returning `Task`.
4. Wire `HandlerError` to your logger; you were relying on `Console` before.
5. Check for topics containing `+` or `#` — they now throw instead of matching nothing.
