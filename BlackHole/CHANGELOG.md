# Changelog

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

Published to nuget.org as [**BlackHole.Messaging**](https://www.nuget.org/packages/BlackHole.Messaging)
(the id `BlackHole` was already taken; the assembly and all namespaces remain `BlackHole.*`).

---

## 3.1.0

Three same-machine transports alongside TCP. No breaking changes — every 3.0.0 API still compiles
and behaves the same.

### Added

**Unix domain sockets, named pipes and shared memory.** All four transports carry the same wire
format through the same patterns, so switching is one line and nothing above the transport changes.

```csharp
var listener = new SharedMemoryListenerHost("blackhole-ipc", slots: 8);   // 3.2 us round trip
var listener = new UnixSocketListenerHost("/tmp/blackhole.sock");         // 29 us, off the network
var listener = new NamedPipeListenerHost("blackhole-gateway");            // 37 us, ACL security

await using var server = new BlackHoleServer(listener);
```

- `StreamTransport` — the frame loop over any duplex `Stream`, which all four transports share.
- `IListenerHost` — the seam that lets `BlackHoleServer` serve any of them.
- `BlackHoleClient.ConnectUnixAsync`, `ConnectPipeAsync`, `ConnectSharedMemoryAsync`, and
  `Over(transport)` for a transport you built yourself.
- `BlackHoleServer(IListenerHost, …)` and a `Endpoint` string alongside the TCP-only `EndPoint`.

**`RpcServer.RegisterDetached`** — for a handler that awaits a call back on its own connection. A
normal handler is awaited by the receive loop, so one that calls back would wait for a reply only
that blocked loop could deliver. Detached handlers run off the loop, copying their payload first.

**A `configure` callback on `BlackHoleClient.ConnectAsync`** — runs before the receive loop starts,
so a handler cannot miss a message a server pushes the instant it accepts.

### Measured

Loopback, .NET 10, 8 cores. Full tables in [docs/transports.md](docs/transports.md).

| Transport | RPC p50 | calls/sec |
|---|---:|---:|
| TCP loopback | 59.5 µs | 15,448 |
| Unix socket | 29.0 µs | 27,985 |
| Named pipe | 37.1 µs | 25,479 |
| **Shared memory** | **3.2 µs** | **271,848** |

Shared memory is the one transport where batching does not help — it is already faster unbatched,
because there is no syscall to amortise.

### Fixed

- **`SharedMemorySegment.Dispose` never cleared its liveness flag**, because it set `_disposed`
  before calling `MarkClosed`, whose guard checked `_disposed`. A departed peer was therefore never
  noticed and its subscriptions never cleaned up. Caught by running one contract suite across all
  four transports.

### Client SDKs

Python, Go and Node.js clients, each verified against the real .NET server rather than a mock. The
wire format is unchanged, so they work against 3.0.0 and 3.1.0 alike. See [clients/](clients/).

### Two traps worth knowing about

Both cost real debugging time, and both are the kind that are hard to diagnose in production.

- **`SpinWait.SpinOnce()` sleeps.** It escalates to `Thread.Sleep(1)` after roughly 20 iterations,
  which on Windows is a full 15.6 ms timer tick — 50 iterations measured **446 ms**. That single
  default made shared-memory RPC 32 ms per round trip, 500× *slower* than the loopback TCP it was
  meant to beat. Pass `sleep1Threshold: -1` in any spin loop of your own.
- **A spinning read loop must not run on a thread-pool thread.** It holds the thread, and with both
  ends of a connection doing it the pool starves. Shared-memory transports run their receive loop on
  a dedicated thread and keep their waits synchronous.

---

## 3.0.0

A rewrite on `System.IO.Pipelines`, targeting .NET 10. **The wire format changed, so both ends must
be 3.x** — see [docs/migration-v2.md](docs/migration-v2.md).

### Changed

- **One `FrameCodec`** writes and parses every byte. v2 kept duplicate copies in its client and
  server transports that could silently desynchronise.
- **One `TcpTransport`** for both dialling and accepting sides, on Pipelines. A fully buffered frame
  now decodes with **zero allocations**, measured.
- **`ITransport` takes one awaited `MessageDispatch`** instead of a multicast `void` event, so the
  transport can hold its buffer steady during dispatch. That is what makes the receive path
  zero-copy; `MessageRouter` does the fan-out.
- **An int64 correlation id** replaces the per-message GUID, saving 8 bytes and a cryptographic RNG
  draw per request.
- **Batch envelopes hold complete frames**, parsed by the same codec. v2 had a second inner format.

### Added

- RPC deadlines and error propagation — a failed or unknown method raises `RpcException` rather than
  hanging the caller forever.
- MQTT-style `+` and `#` wildcards in Pub/Sub, and subscriber cleanup on disconnect.
- Stream descriptors, progress reporting, sinks and size limits.
- Auto-flush batching on count, size or delay.
- Per-connection statistics and keepalive round-trip measurement.

### Fixed

- **Messages arriving between a transport starting and its dispatcher being installed were dropped.**
  Reliably reproducible under load: a `Subscribe` sent immediately on connect would vanish.
  Transports are now created unstarted and `Start()` is called after wiring.

### Measured

41 µs p50 RPC round trip, 200,800 calls/sec across 16 connections, 2.3M messages/sec batched
(22× the one-send-per-message path), 520 MiB/s streaming, and 0 bytes allocated encoding or decoding
a frame. See [docs/benchmarks.md](docs/benchmarks.md).

---

*Built by Gravicode Studios, led by Kang Fadhil.*
