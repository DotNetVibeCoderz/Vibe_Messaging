# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

SocketSignal is a bidirectional realtime RPC library over raw WebSockets for .NET 10, published to
NuGet as `SocketSignal`. Clients call server methods (with or without a return value); the server
pushes invocations to one client, a group, or all of them.

The git root is the parent directory (`Vibe_Messaging/`), which also holds unrelated sibling
projects (`BlackHole/`, `Nerve/`). Keep commits touching SocketSignal inside this folder.

`README.md` is bilingual (English + Bahasa Indonesia) and `docs/` is mirrored in `docs/id/` — a
change to one language needs the same change in the other.

## Commands

```bash
dotnet build SocketSignal.slnx
dotnet test tests/SocketSignal.Tests/SocketSignal.Tests.csproj

# One test, or one class
dotnet test tests/SocketSignal.Tests/SocketSignal.Tests.csproj --filter "FullyQualifiedName~Server_broadcasts"
dotnet test tests/SocketSignal.Tests/SocketSignal.Tests.csproj --filter "FullyQualifiedName~ProtocolTests"

dotnet run --project src/SocketSignal.Demo             # server + two clients + a round-trip measurement
dotnet run --project src/SocketSignal.Demo -- serve    # server only, for the client SDK examples
dotnet run --project src/SocketSignal.SonarDemo        # the Avalonia sonar console

dotnet run -c Release --project src/SocketSignal.Benchmarks -- throughput  # end-to-end v1 vs v2 + allocations
dotnet run -c Release --project src/SocketSignal.Benchmarks -- micro       # BenchmarkDotNet (~13 min)
dotnet run -c Release --project src/SocketSignal.Benchmarks -- alloc       # allocations only (fast)
```

There is no linter config. The build is warning-clean — keep it that way.

The end-to-end tests bind a real `HttpListener` on a free loopback port, so they are sensitive to
firewall prompts on a fresh machine but need no special setup.

## Architecture

**The protocol is symmetric, and so is the implementation.** An `invoke` looks the same whichever
end sends it, so both ends need the same four things: a receive loop, a handler table, a
pending-call table, and a serialised send path. `SignalConnection` (`Hosting/`) is all four — it is
what `SocketSignalClient` wraps *and* what sits behind every server-side `ClientConnection`. v1 had
this loop written out twice and the copies had drifted; if you find yourself adding behaviour to
one side only, that is the bug reappearing.

**The wire format is one JSON object per WebSocket message.** `type` is one of `welcome`, `invoke`,
`result`, `ping`, `pong`; `id` correlates a `result` with its `invoke`; `expectReturn` decides
whether a reply is owed. Full spec in `docs/protocol.md`. **A protocol change must land in five
places**: `Protocol/SignalFrame.cs` (decode), `Protocol/SignalWriter.cs` (encode), the three SDKs
in `clients/`, the browser example in `README.md`, and `docs/protocol.md` + `docs/id/protocol.md`.

**Public types live in namespace `SocketSignal`** even though the files sit in `Hosting/` —
`SocketSignalServer`, `SocketSignalClient`, `ClientConnection`, `SocketSignalOptions`, the
exceptions. Internals keep their folder namespaces (`SocketSignal.Protocol`, `.Dispatch`,
`.Buffers`, `.Hosting`, `.Diagnostics`). This preserves v1 source compatibility; don't "fix" it.

### The allocation contract

The codec paths are allocation-free in steady state, and that is a property to preserve, not an
accident. It rests on four things:

- **One pooled send buffer + one reused `Utf8JsonWriter` per connection**, both only touched while
  the send lock is held. Encoding inside the lock is what makes reuse safe *and* what stops two
  concurrent sends interleaving bytes on the socket. Do not move encoding outside it.
- **`SignalFrame` is a `ref struct`** whose fields are slices of the receive buffer. It cannot
  cross an `await` or be stored — the compiler enforces the rule. Anything outliving the dispatch
  call must be copied (that is what the pooled `Invocation` is for).
- **`Utf8HandlerTable`** looks handlers up by raw UTF-8 bytes, so no string is built per frame.
  Registration rebuilds and swaps the table under a lock; reads are lock-free.
- **Correlation ids are a `long` counter** formatted straight into the frame, not GUIDs.

`docs/performance.md` has the measured numbers and an honest account of what still allocates
(~3.3 KB per end-to-end call: async state machines, the pending-call TCS, the timeout registration,
boxed args). Benchmarks compare against the real v1 code, recovered from git into
`src/SocketSignal.Benchmarks/Baseline/` — keep it building, it is the baseline.

### Watch out for

- **Overload resolution on the writer.** `SignalWriter.WriteInvoke` (takes `object?[]`) and
  `WriteInvokeSingle<T>` (one typed argument) are named apart on purpose: as overloads, the generic
  one wins against an `object?[]` and silently nests the whole argument list inside one argument.
  Tests caught this once; don't merge them back.
- **Handlers run off the pump**, up to `MaxConcurrentInvocations` per connection, so they may run
  concurrently and out of order. The gate is the backpressure valve — when it is full the pump
  stops reading and flow control falls to TCP. There is no unbounded queue anywhere; keep it so.
- **Keepalive is protocol-level `ping`/`pong`, not WebSocket ping frames**, because browsers answer
  those transparently and JavaScript never sees them. Both ends set the socket's own
  `KeepAliveInterval` to zero.
- **Groups are server-side only.** There is no join frame; a client asks via an application method
  that calls `client.JoinGroup`. Membership is an authorisation decision.
- Every failure path funnels through `CloseCoreAsync`, which fails pending calls with
  `SignalConnectionClosedException`. Nothing may be left hanging — that was v1's worst bug.

## Client SDKs

`clients/{python,go,nodejs}` are ~300 lines each and speak the same protocol. CI runs all three
against a real .NET server (`SocketSignal.Demo -- serve`). Node and Python have been verified
locally; Go has not been compiled here (no Go toolchain on this machine) — CI covers it.

## Publishing

`src/SocketSignal/SocketSignal.csproj` carries the package metadata.

**The workflows live at the repository root**, not in this folder — GitHub only reads
`.github/workflows/` from the root, so a workflow under `SocketSignal/.github/` silently never
runs. They are `socketsignal-ci.yml` and `socketsignal-release.yml`, matching the sibling projects'
`<project>-<job>.yml` convention, and every path filter and working directory is scoped to
`SocketSignal/` so they never build a sibling.

The release workflow publishes on a `socketsignal-v*` tag, taking the version from the tag and
using the `NUGET_API_KEY` repository secret. Never put the key in a file.
