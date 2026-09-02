# Performance

Built by Gravicode Studios, led by Kang Fadhil.

Every number here was measured. Raw output: [benchmark-run.txt](benchmark-run.txt) for the
sustained-load harness, [benchmark-micro.txt](benchmark-micro.txt) for BenchmarkDotNet.

## The machine

```
Intel Core i7-8650U, 1.90 GHz (Kaby Lake R), 4 physical / 8 logical cores
Windows 11 25H2, .NET 10.0.11, X64 RyuJIT x86-64-v3
```

A four-core laptop, not a server. Treat the ratios as the finding and the absolute numbers as a
lower bound.

## Headline

Sustained load, 5,000,000 messages, one topic, one subscriber:

| | v1 | v2 |
|---|---|---|
| Publish by topic name | 70.8 ns · 14.1M msg/s | **32.4 ns · 30.8M msg/s** |
| Publish through a resolved handle | — | **21.0 ns · 47.5M msg/s** |
| Allocated over the run | 267.0 MB | **376 B** |
| Gen0 collections | 66 | **0** |

376 bytes is the harness itself. The publish path allocates nothing.

## Where the time goes

From the BenchmarkDotNet run, publishing a 16-byte struct:

| Subscribers | v1 | v2 by name | v2 by name, no statistics | v2 by handle |
|---|---|---|---|---|
| 0 | 19.9 ns | 40.9 ns | 31.4 ns | **16.3 ns** |
| 1 | 68.4 ns | 43.2 ns | 41.8 ns | **22.3 ns** |
| 8 | 236.2 ns | 107.8 ns | **78.7 ns** | 85.2 ns |

Three things fall out of that table, and one of them is not flattering.

### Publishing by name costs a string hash

`ByName` minus `ByHandle` is about 22 ns at every subscriber count. That is the dictionary lookup,
and most of it is hashing the topic string — `ChannelKey` caches its hash, but a fresh key is built
on every publish, so the string is hashed every time.

That is exactly what `Topic<T>(name)` is for. Resolve once, hold the handle, and the cost is gone:

```csharp
private readonly NerveTopic<Reading> _readings = hub.Topic<Reading>("sensor/tank-3");
```

### Statistics cost about 3 ns per counter

`ByName` minus `ByNameNoStatistics` is 9.6 ns at zero subscribers, 1.4 ns at one, and 29.1 ns at
eight. The counters are one increment per publish plus one per delivery, so eight subscribers means
nine interlocked operations.

At eight subscribers that is enough for `ByNameNoStatistics` (78.7 ns) to beat `ByHandle`
(85.2 ns) — turning statistics off saves more than skipping the lookup does. If you fan out widely
and do not need the counters:

```csharp
new NerveHub(new NerveOptions { CollectStatistics = false })
```

### v2 is slower than v1 at publishing into the void

With **no subscribers**, v1 is 19.9 ns and v2 by name is 40.9 ns — v2 is twice as slow.

v1 misses its dictionary and returns immediately. v2 resolves a route, creates one if this topic is
new, and counts the message as published and unrouted. It is optimised for messages that get
delivered, and publishing to nobody is the one case where doing bookkeeping properly costs more than
not doing it.

Through a handle it is 16.3 ns, faster than v1 even here. If a hot path publishes to a topic that is
usually empty, either hold a handle or check `HasSubscribers<T>` first.

## Fan-out

One publisher, one topic, N subscribers, through a handle:

| Subscribers | | |
|---|---|---|
| 1 | 21.2 ns | 47.2M msg/s |
| 2 | 33.0 ns | 30.3M msg/s |
| 8 | 83.9 ns | 11.9M msg/s |
| 32 | 300.2 ns | 3.3M msg/s |

Roughly 9 ns per additional subscriber, and no allocation at any width. Fan-out costs what fan-out
costs: the work is calling the handlers.

## Wildcards

The fair comparison is one exact subscriber against one wildcard subscriber on the same topic, which
the sustained-load harness measures:

| | | |
|---|---|---|
| exact subscriber | 21.7 ns | 46.2M msg/s |
| one wildcard subscriber | 23.3 ns | 42.9M msg/s |

Within noise of each other, and both allocation-free. Matching happened when the route was resolved;
by the time a message is published there is only an array to walk.

> The BenchmarkDotNet `WildcardBenchmarks` table looks worse — 40.9 ns against 49.6 ns — but that
> case has **two** matching wildcard subscribers against one exact subscriber, so the difference is
> the second delivery, not wildcard overhead. The harness numbers above are the like-for-like ones.

The cost of a wildcard shows up where you would expect it, on the first publish to each new topic:

| | | |
|---|---|---|
| 100,000 distinct topics, cold | 265.0 ns each | 5.3 MB total |

That includes allocating each topic string, the route object, and running every wildcard filter
against it. After the first message, that topic is 21 ns like any other.

`TopicFilter.Matches` on its own is 37.2 ns for a three-level filter, allocation-free.

## Concurrency

Eight publishers, one topic each, on eight logical cores:

| | | |
|---|---|---|
| 8 publishers | 40.7 ns | 24.6M msg/s aggregate |

Lower aggregate throughput than a single publisher's 47M, on a four-physical-core laptop with
hyperthreading — the cores are shared. What matters is that nothing serialises: there is no lock on
the publish path, and counters live per route so two topics never share a cache line.

## The patterns

| | | |
|---|---|---|
| Request/reply round trip | 120.0 ns | 208 B |
| Subscribe, publish, unsubscribe | 211.2 ns | 256 B |

Request/reply allocates because it has to: an envelope and a `TaskCompletionSource` per call. 120 ns
for a full round trip through the pub/sub machinery is the price of building it on topics rather
than beside them, and it buys wildcards, statistics and observability for free.

The subscribe/unsubscribe figure is the copy-on-write cost — two array copies. That is the trade
this design makes: dispatch takes no lock, and registration pays for it. Fine for hubs where
subscriptions are set up once; wrong for a workload that churns subscriptions as fast as it
publishes.

## Reproducing

```bash
dotnet run --project src/Nerve.Benchmarks -c Release -- --quick            # ~1 min
dotnet run --project src/Nerve.Benchmarks -c Release -- --quick legacy     # one stage
dotnet run --project src/Nerve.Benchmarks -c Release -- --micro            # ~10 min
dotnet run --project src/Nerve.Benchmarks -c Release -- --micro --job short
dotnet run --project src/Nerve.Benchmarks -c Release -- --filter "*Wildcard*"
```

The v1 hub is kept verbatim in `src/Nerve.Benchmarks/Baseline/LegacyHub.cs` so the comparison is
against real code rather than an estimate. Do not tidy it up — its costs are the point.

If you change something on the publish path, re-run both and update this page, or say plainly that
the figures predate the change. Do not estimate a benchmark number.
