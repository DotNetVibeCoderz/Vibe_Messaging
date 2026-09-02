# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

BlackHole is a .NET 10 messaging library: a custom length-prefixed binary protocol over TCP with
RPC, Pub/Sub, Streaming and Batching, built on `System.IO.Pipelines`. It ships to nuget.org as
**`BlackHole.Messaging`** (the id `BlackHole` was taken); the assembly and all namespaces stay
`BlackHole.*`.

Built by Gravicode Studios, led by Kang Fadhil. Keep that attribution in new source file headers and
new docs — every existing file carries it.

The git root is the parent directory (`Vibe_Messaging/`), which also holds unrelated projects
(`Nerve/`, `SocketSignal/`). There is no solution file; build each project from its own directory,
and keep BlackHole changes inside `BlackHole/`. `Directory.Build.props` sets the target framework
and package identity for every project here.

## Layout

```
src/BlackHole/            the library (the only packable project)
src/BlackHole.Demo/       console app exercising every pattern
src/BlackHole.Benchmarks/ BenchmarkDotNet + a sustained-load harness
src/BlackHole.IoTGateway/ Avalonia panel: a real gateway with simulated devices
tests/BlackHole.Tests/    xunit, 40 tests
docs/                     English; docs/id/ is Bahasa Indonesia
```

## Commands

```bash
dotnet build src/BlackHole/BlackHole.csproj
dotnet test tests/BlackHole.Tests                      # ~1s, real loopback sockets
dotnet test tests/BlackHole.Tests --filter "FullyQualifiedName~RpcEchoes"   # one test

dotnet run --project src/BlackHole.Demo                # every pattern end to end
dotnet run --project src/BlackHole.IoTGateway -- --demo 12   # panel, self-starting

# Benchmarks
dotnet run --project src/BlackHole.Benchmarks -c Release -- --quick            # ~3 min
dotnet run --project src/BlackHole.Benchmarks -c Release -- --quick latency    # one stage
dotnet run --project src/BlackHole.Benchmarks -c Release -- --filter "*Codec*" --job short

dotnet pack src/BlackHole/BlackHole.csproj -c Release -o ../artifacts/packages
```

The build is warning-free — keep it that way. `GenerateDocumentationFile` is on, so a public member
without a `<param>` for each parameter warns.

## Architecture

Four layers, each knowing only the one below. Read `docs/architecture.md` before a structural change.

**`Protocol/`** — `BlackHoleMessage` is a 40-byte **readonly struct** (a class would allocate per
message). `FrameCodec` is the *only* place the wire format is written or parsed. `HeaderCache`
returns the same `string` instance for repeated headers.

**`Transport/`** — one `TcpTransport` for both dialling and accepting sides. Read side is
`System.IO.Pipelines`; a fully buffered frame parses with **zero allocations**.

**`Hosting/MessageRouter`** — routes by type byte via array index, copy-on-write registration.

**`Patterns/`** — each takes an `ITransport`, exposes `HandleAsync` matching `MessageDispatch`, and
offers `AttachTo(router)`.

**`Hosting/BlackHoleServer|Client`** — everything wired with correct lifetimes.

### Invariants that will bite you

**A received payload is valid only until dispatch returns.** It points into the transport's buffer.
Handlers that keep bytes must call `ToOwned()`. This is what makes the receive path zero-copy.

**Wire the dispatcher before starting the transport.** `TcpTransport` is created with
`startReceiving: false`, the caller sets `Dispatcher`, then calls `Start()`. Starting first drops
messages that arrive in the gap — this was a real bug (a `Subscribe` sent immediately on connect
vanished under load). Two regression tests in `EndToEndTests.cs` pin it in both directions.

**Batch envelopes hold complete BlackHole frames**, parsed by the same `FrameCodec`. Never invent a
second inner format — v2 did, and the two parsers drifted.

**Per-connection vs server-wide lifetimes** (see `BlackHoleServer`): `RpcServer` and `PubSubBroker`
are server-wide; `MessageRouter`, `StreamReceiver` and `BatchReceiver` are **per connection**. Two
clients uploading the same stream id would corrupt each other through a shared `StreamReceiver`.

**Unsubscribe on disconnect.** `PubSubBroker.RemoveSubscriber` must be called or the subscriber list
leaks for the process lifetime.

**Telemetry up, commands down — never both on one path.** Subscribing every gateway connection to a
wildcard covering its own devices makes the broker fan readings back to all of them; each then
blocks writing to peers that are blocked writing, and the whole thing deadlocks. This actually
happened in the IoT gateway.

## Testing

`tests/BlackHole.Tests/EndToEndTests.cs` uses **real loopback sockets**, not fakes — that is the
point, since the v2 bugs were all in the seams. Bind port 0 and read `server.EndPoint.Port`. Pass
`KeepAliveInterval = null` in tests so stray pings don't add noise.

## Avalonia panel

Target `net10.0`, Avalonia 11.3.x. Two gotchas already paid for:

- **Never hand-write `InitializeComponent`.** The generated overload takes optional parameters, so a
  parameterless hand-written one silently wins and named controls stay null.
- **XML comments cannot contain `--`.** Use `====` for separator rules in `.axaml`.

The panel drives high-rate data the way any UI should: the receive loop writes into a lock-free
`TraceBuffer`, and a 33 ms `DispatcherTimer` publishes one coalesced update per frame. Never bind a
UI property straight to the receive loop.

The gateway binds `IPAddress.Loopback`, not `Any` — everything runs in-process, and binding Any
triggers a Windows firewall prompt.

## Docs and benchmarks

Docs are bilingual: `docs/` English, `docs/id/` Bahasa Indonesia. A user-facing change to behaviour
should update both.

**Every number in `docs/benchmarks.md` was measured**, and `docs/benchmark-run.txt` holds the raw
output. If you change something on the hot path, re-run `--quick` and update both, or say plainly
that the figures predate the change. Do not estimate a benchmark number.
