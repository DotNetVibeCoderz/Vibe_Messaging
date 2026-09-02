# Patterns

Built by Gravicode Studios, led by Kang Fadhil.

Everything here is built on the one dispatch path described in [architecture.md](architecture.md).
None of it is a special case inside the hub.

## Wildcards

Nerve uses MQTT's filter syntax. Levels are separated by `/`, `+` stands for exactly one level, and
`#` stands for the remaining levels and is only legal last.

```csharp
nerve.Subscribe<double>("sensor/+/temperature", c => { });   // any tank's temperature
nerve.Subscribe<double>("sensor/#", c => { });               // anything under sensor, at any depth
nerve.Subscribe<double>("#", c => { });                      // every double, everywhere
```

| Filter | Covers | Does not cover |
|---|---|---|
| `sensor/tank-3/temp` | that topic only | anything else |
| `sensor/+/temp` | `sensor/tank-3/temp` | `sensor/temp`, `sensor/a/b/temp` |
| `sensor/#` | `sensor`, `sensor/a`, `sensor/a/b` | `other/a` |
| `#` | everything | — |

Two things follow from how this is implemented:

- **A wildcard costs nothing per message.** Matching happens when a topic is first published to, and
  the result is cached on that topic's route. Measured: an exact subscriber is 21.7 ns, a wildcard
  subscriber on the same topic is 23.3 ns.
- **Exact subscribers run before wildcard ones**, and each subscription is delivered to exactly
  once, even when several of its filters would match.

Publishing to a wildcard throws — `+` and `#` mean something on the subscribing side only:

```csharp
nerve.Publish("sensor/+/temp", 1.0);   // ArgumentException
```

## Retained messages

A retained message is the topic's current value, kept and handed to whoever subscribes next.

```csharp
await nerve.PublishRetainedAsync("config/mode", "maintenance");

// Any time later, however much later:
using var late = nerve.Subscribe<string>("config/mode",
    mode => Console.WriteLine(mode));       // prints "maintenance" immediately
```

One value per topic, replaced on each retained publish. `ClearRetained<T>(topic)` forgets it, and
`TryGetRetained<T>(topic, out var value)` reads it without subscribing.

A wildcard subscriber is given every matching topic's retained value on subscribe, which is what
makes it a roster:

```csharp
// Six specialists each retain their own status on agents/roster/{name}.
using var roster = nerve.Subscribe<AgentStatus>("agents/roster/+", status => Show(status));
// All six arrive at once, before this line runs.
```

That is how the simulator's panel can be opened at any point and immediately show six populated
terminals instead of an empty board that fills in as work happens.

## Request and reply

```csharp
using var responder = nerve.Respond<string, int>("text/length", text => text.Length);

int length = await nerve.RequestAsync<string, int>("text/length", "gravicode");   // 9
```

Asynchronous responders get the caller's cancellation token:

```csharp
using var responder = nerve.Respond<int, string>("agents/+/ping", async (id, token) =>
{
    await Task.Delay(20, token);
    return $"agent {id} is awake";
});

string answer = await nerve.RequestAsync<int, string>("agents/writer/ping", 4);
```

Note the wildcard: one responder can answer for a family of topics.

Three behaviours worth knowing:

- **A missing responder is reported immediately**, as `NerveNoResponderException`, rather than after
  the timeout. Waiting thirty seconds to discover nothing was ever registered is the most expensive
  way to find a wiring mistake.
- **A responder's exception surfaces at the call site**, not in `HandlerError`. `RequestAsync`
  throws whatever the responder threw.
- **The first reply wins.** If two responders are listening, the second is ignored rather than
  throwing — a race between responders is a wiring mistake the caller cannot do anything about.

Timeouts default to `NerveOptions.DefaultRequestTimeout` (30 seconds) and can be given per call:

```csharp
await nerve.RequestAsync<int, int>("slow", 1, TimeSpan.FromSeconds(2));
await nerve.RequestAsync<int, int>("slow", 1, Timeout.InfiniteTimeSpan);   // wait forever
```

A request is an ordinary message carrying a `NerveRequest<TRequest, TResponse>` envelope, so it
shows up in the statistics and an ordinary subscriber can watch the traffic.

## Streams

Every other way of subscribing runs the handler on the publisher's thread. A stream is the
deliberate exception: it buffers, and the consumer drains it on its own.

```csharp
await foreach (Reading reading in nerve.StreamAsync<Reading>("sensor/#", cancellationToken: token))
{
    await WriteToDatabase(reading);
}
```

- The subscription lives exactly as long as the enumeration — registered on the first
  `MoveNextAsync`, disposed when the loop ends, breaks or throws.
- The buffer is **drop-oldest**, 1024 by default. A publisher is never blocked by a slow consumer;
  the drops are counted in `NerveStatistics.StreamDrops` instead.
- Pass `capacity:` to size it for the consumer you have.

```csharp
nerve.StreamAsync<Reading>("sensor/#", capacity: 64, cancellationToken: token)
```

This is the right tool whenever a subscriber does real work — a file write, a database call, a UI
update — and it is what the simulator's specialists use so all six run at once.

## Waiting for one message

```csharp
string ready = await nerve.WaitForAsync<string>("startup/ready", timeout: TimeSpan.FromSeconds(5));

// Or wait for a particular message:
int big = await nerve.WaitForAsync<int>("readings", v => v > 100, TimeSpan.FromSeconds(5));
```

It subscribes, waits, and unsubscribes, so start-up sequencing and tests do not have to hand-roll a
`TaskCompletionSource` and remember to clean it up. Nothing matching in time throws
`TimeoutException`.

## Subscribing once

```csharp
nerve.SubscribeOnce<Ready>("startup/ready", _ => Begin());
```

Fires for the first matching message, then unsubscribes itself. Disposing the handle before that
cancels it.

## Pre-resolved topic handles

When a component publishes to the same topic in a loop, resolve it once:

```csharp
private readonly NerveTopic<Reading> _readings = hub.Topic<Reading>("sensor/tank-3");

// ...
_readings.Publish(reading);
```

That removes the dictionary lookup, which is the only per-message cost left: 32.4 ns by name becomes
21.0 ns through a handle. `NerveTopic<T>` is a struct wrapping two references, so holding one in a
field costs nothing.

For anything less frequent than a tight loop, publishing by name is already fast enough — the handle
is an optimisation, not the normal way to use the library.
