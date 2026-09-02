# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Nerve is a .NET 10 in-process publish/subscribe hub: MQTT-shaped topics with `+` and `#` wildcards,
retained messages, request/reply, `IAsyncEnumerable` streams and per-route statistics, with no
boxing, no locks and no allocation on the publish path. It ships to nuget.org as **`Nerve`**.

Built by Gravicode Studios, led by Kang Fadhil. Keep that attribution in new source file headers and
new docs — every existing file carries it.

The git root is the parent directory (`Vibe_Messaging/`), which also holds unrelated projects
(`BlackHole/`, `SocketSignal/`). Keep Nerve changes inside `Nerve/`, except for the CI workflows:
GitHub only runs workflows at the repository root, so they live in
`../.github/workflows/nerve-{ci,release}.yml` and are path-filtered to `Nerve/**`.
`Directory.Build.props` sets the target framework and package identity for every project here.

## Layout

```
src/Nerve/              the library (the only packable project)
src/Nerve.Demo/         console app exercising every feature
src/Nerve.Benchmarks/   BenchmarkDotNet + a sustained-load harness + the v1 hub as a baseline
src/Nerve.AgentSim/     Avalonia panel: an orchestrator and six specialists, talking only via topics
tests/Nerve.Tests/      xunit, 79 tests
docs/                   English; docs/id/ is Bahasa Indonesia
```

## Commands

```bash
dotnet build Nerve.slnx -c Release
dotnet test tests/Nerve.Tests                                          # ~1s
dotnet test tests/Nerve.Tests --filter "FullyQualifiedName~Wildcard"   # one class

dotnet run --project src/Nerve.Demo                     # every feature end to end
dotnet run --project src/Nerve.AgentSim -- --demo 8     # the panel, self-starting

# Benchmarks
dotnet run --project src/Nerve.Benchmarks -c Release -- --quick           # ~1 min
dotnet run --project src/Nerve.Benchmarks -c Release -- --quick legacy    # one stage
dotnet run --project src/Nerve.Benchmarks -c Release -- --micro           # ~10 min
dotnet run --project src/Nerve.Benchmarks -c Release -- --filter "*Wildcard*"

dotnet pack src/Nerve/Nerve.csproj -c Release -o artifacts
```

The build is warning-free — keep it that way. `GenerateDocumentationFile` is on, so a public member
missing a `<param>` for each parameter warns.

## Architecture

Read `docs/architecture.md` before changing anything under `src/Nerve/Routing/`.

A **route** is one concrete topic carrying one message type. `NerveHub._routes` is keyed by
`ChannelKey` (topic + `Type`, hash cached at construction), so a publish is **one dictionary lookup**
and the value can only ever be a `Route<T>` — hence `Unsafe.As`, not a cast.

### Invariants that will bite you

**The dispatch loop must stay allocation-free until a handler suspends.** `Route<T>.PublishAsync`
walks the handler array inline and only hands off to an `async` continuation at the first
`ValueTask` that is not already completed. Making that method `async` would allocate a state machine
on every publish and undo the whole design.

**Handler arrays are immutable and swapped wholesale.** Registration copies; dispatch does a single
`Volatile.Read` and takes no lock. Anything that mutates a handler list in place breaks the
lock-free read.

**A disposed subscription must stop firing immediately.** Dispatch walks a snapshot, so it checks
`Subscription<T>.Active` on every handler. There is a regression test for a handler that disposes a
later one mid-dispatch.

**Exact subscriptions live on their route; wildcard ones live on `Registry<T>`.** That is why
subscribing to an exact topic invalidates one route rather than every route of that type. Only
wildcards bump `WildcardVersion`, and a version of zero is the fast path that skips merging
entirely. `Route<T>.Rebuild` reads both version stamps *before* the data they describe — reversing
that would lose handlers.

**Counters live per route, not per hub.** Two threads publishing to different topics must not share
a cache line. `GetStatistics()` sums them on demand.

**Request/reply and streams are not special cases.** A request is a normal message carrying a
`NerveRequest<,>` envelope; a stream is a normal subscription writing into a drop-oldest
`Channel<T>`. Keep it that way — the moment either grows its own path in the hub, wildcards and
statistics stop working for it.

**There is deliberately no `Func<T, Task>` overload.** Having it alongside `Func<T, ValueTask>` made
every `async` lambda ambiguous (CS0121). Do not add it back.

## Testing

`tests/Nerve.Tests` covers the matcher against MQTT's own examples, plus concurrency: a
subscribe-during-publish test and an 80,000-message parallel publish. Both exist because the
copy-on-write registration is the part most likely to break.

## Avalonia panel

Target `net10.0`, Avalonia 11.3.x. Two gotchas already paid for:

- **Never hand-write `InitializeComponent`.** The generated overload takes optional parameters, so a
  parameterless hand-written one silently wins and named controls stay null.
- **XML comments cannot contain `--`.** Use `====` for separator rules in `.axaml`.

The panel's five hub subscriptions run on agent threads and **must not touch a bound collection**.
They enqueue into a `ConcurrentQueue`, and a 33 ms `DispatcherTimer` drains it and applies one
coalesced batch per frame. `Arbor` owns its own 16 ms frame clock and only ever mutates `ArborField`
from the UI thread.

Specialists take work through `StreamAsync`, not `Subscribe`. A subscription handler runs on the
publisher's thread, so an agent that slept in one would sleep on the orchestrator's dispatch loop
and the six would take turns instead of working at once.

Screenshots are captured from the live window, not mocked:

```bash
dotnet run --project src/Nerve.AgentSim -- --screenshot docs/images/agent-sim.png --demo 8 --wait 5600
```

## Docs and benchmarks

Docs are bilingual: `docs/` English, `docs/id/` Bahasa Indonesia. A user-facing change to behaviour
should update both, and the README's two language sections as well.

**Every number in `docs/performance.md` was measured**; `docs/benchmark-run.txt` and
`docs/benchmark-micro.txt` hold the raw output. That page also records where v2 is *slower* than v1
(publishing to a topic with no subscribers) — keep that kind of finding in rather than trimming to
the flattering numbers. If you change the publish path, re-run both harnesses and update the page,
or say plainly that the figures predate the change. Do not estimate a benchmark number.

## Releasing

`Directory.Build.props` carries `VersionPrefix`. Tag `nerve-v<version>` to trigger
`../.github/workflows/nerve-release.yml`, which takes the version from the tag and needs a
`NUGET_API_KEY` repository secret.
