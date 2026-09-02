# Transports

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

Four ways to carry the same protocol. The wire format, the patterns and the API are identical across
all of them — choosing one is a deployment decision, not an application one.

| | Reaches | Latency (p50) | Best at | Costs |
|---|---|---:|---|---|
| **TCP** | anywhere | 60 µs | any network, any host | a full network stack |
| **Unix socket** | one machine | 29 µs | same-machine, not on the network | a filesystem path to manage |
| **Named pipe** | one machine* | 37 µs | same-machine on Windows, ACL security | one server instance per client |
| **Shared memory** | one machine | **3.2 µs** | lowest latency, highest message rate | a dedicated thread and resident memory per connection |

<sub>*Named pipes can cross machines on Windows, but that is not what they are good at.</sub>

## Picking one

**Start with TCP.** It works everywhere, an idle connection costs nothing, and 60 µs is fine for
almost everything.

**Use a Unix socket or a named pipe** when both processes are on one machine and you want the port
off the network entirely. Roughly twice as fast as loopback TCP, and the endpoint is a file or a
pipe name whose permissions are the access control. Prefer a Unix socket on Linux and macOS, a named
pipe on Windows — though .NET implements named pipes over Unix sockets elsewhere, so either name
works.

**Use shared memory** when you have a handful of links that genuinely need microsecond latency —
**18× faster than loopback TCP** and 265,000 RPC calls a second. It is the wrong choice for many
mostly-idle connections: every connection gets a dedicated thread and `2 × RingCapacity` of resident
memory whether it is busy or not.

---

## TCP

```csharp
await using var server = new BlackHoleServer(5000);                       // every interface
await using var server = new BlackHoleServer(                             // loopback only
    new IPEndPoint(IPAddress.Loopback, 5000));
server.Start();

await using var client = await BlackHoleClient.ConnectAsync("127.0.0.1", 5000);
```

Binding to `IPAddress.Any` exposes the port to the network and, on Windows, triggers a firewall
prompt. Bind loopback when you do not need either.

## Unix domain sockets

```csharp
var listener = new UnixSocketListenerHost("/tmp/blackhole.sock");
await using var server = new BlackHoleServer(listener);
server.Start();

await using var client = await BlackHoleClient.ConnectUnixAsync("/tmp/blackhole.sock");
```

The socket is a **file**, which brings one wrinkle a port does not: `bind` fails if the path already
exists, and a crashed process leaves it behind. `UnixSocketListenerHost` deletes a stale path on
`Start` and removes its own on dispose, so a restart needs no manual cleanup.

Supported on Linux, macOS, and Windows 10 build 17063 or later — check
`UnixSocketTransport.IsSupported`. Paths are capped near 100 bytes on Unix, so keep them short;
`UnixSocketTransport.TempPath("name")` gives you one under the temp directory.

## Named pipes

```csharp
var listener = new NamedPipeListenerHost("blackhole-gateway");
await using var server = new BlackHoleServer(listener);
server.Start();

await using var client = await BlackHoleClient.ConnectPipeAsync("blackhole-gateway");
```

A pipe server instance serves exactly one client, so the listener keeps a fresh unconnected instance
waiting at all times: when one is claimed another is created behind it. `MaxServerInstances` is the
OS ceiling — 255 on Windows.

Pipes are opened in **byte mode**, not message mode: BlackHole frames its own messages, and message
mode would impose a second, redundant framing.

## Shared memory

```csharp
var listener = new SharedMemoryListenerHost("blackhole-ipc", slots: 8);
await using var server = new BlackHoleServer(listener);
server.Start();

await using var client = await BlackHoleClient.ConnectSharedMemoryAsync("blackhole-ipc", slots: 8);
```

A named segment holds one lock-free ring per direction. Sending is a copy into the ring and a cursor
advance; receiving is a copy out and a cursor advance. **The kernel is not involved once the segment
is mapped** — that is where the latency goes.

### The pool

One segment carries one connection, so a listener is a pool: it creates `{name}-0` through
`{name}-{slots-1}` up front, and a client claims a free one with an atomic compare-and-exchange on
its liveness flag. Several clients racing for the same pool each end up on a different segment; a
slot is recycled once its connection ends.

Keep the pool small. Every slot costs `2 × RingCapacity` resident — 2 MiB each by default — whether
it is in use or not.

### Tuning

```csharp
var shared = new SharedMemoryOptions
{
    RingCapacity  = 1024 * 1024,               // per direction, power of two
    SpinCount     = 50,                        // tight spins before yielding
    YieldDuration = TimeSpan.FromMilliseconds(2),  // yield window before sleeping
    PollInterval  = TimeSpan.FromMilliseconds(1),  // sleep once genuinely idle
};
```

Waiting happens in three phases, and the defaults are chosen so an **active link never reaches the
third**: spin (sub-microsecond), yield (half a microsecond each, no timer), then sleep. The yield
window is the setting that matters — see below for why.

### The layout

```
+--------------------+   header: magic, version, capacity, liveness flags
| Header (64 bytes)  |
+--------------------+
| Ring A to B        |   write cursor (64) | read cursor (64) | data (capacity)
+--------------------+
| Ring B to A        |   same again
+--------------------+
```

One writer and one reader per ring, coordinating through two monotonically increasing cursors.
Neither side ever moves the other's, so there is no lock and no compare-and-swap on the data path.
The cursors sit on separate 64-byte cache lines: sharing one would put reader and writer in a
permanent cache-coherence fight that costs far more than the padding saves.

### Cleaning up

On Windows a segment lives in the kernel namespace and disappears with its last handle. Elsewhere it
is a file under `/dev/shm` or the temp directory, and an unclean shutdown leaves it behind — call
`SharedMemoryTransport.Cleanup(name)` to remove it. The listener does this for its own slots.

---

## Measured on this machine

.NET 10.0.11, Windows 11, 8 logical cores, both ends in one process. Reproduce with
`dotnet run --project src/BlackHole.Benchmarks -c Release -- --transports`; raw output in
[transport-comparison.txt](transport-comparison.txt).

### RPC latency, 30-byte payload

| Transport | p50 | p90 | p99 | calls/sec |
|---|---:|---:|---:|---:|
| TCP loopback | 59.5 µs | 87.3 µs | 119.3 µs | 15,448 |
| Unix socket | 29.0 µs | 53.2 µs | 75.0 µs | 27,985 |
| Named pipe | 37.1 µs | 49.5 µs | 76.7 µs | 25,479 |
| **Shared memory** | **3.2 µs** | **4.1 µs** | **9.0 µs** | **271,848** |

### Publish throughput, 100,000 small messages

| Transport | one at a time | batched (256) | speed-up |
|---|---:|---:|---:|
| TCP loopback | 100,638/s | 2,022,273/s | 20.1× |
| Unix socket | 475,849/s | 2,357,145/s | 5.0× |
| Named pipe | 74,617/s | 2,058,854/s | 27.6× |
| **Shared memory** | **2,106,376/s** | 1,752,563/s | **0.8×** |

Shared memory is the one transport where **batching does not help** — it is already faster
unbatched, because there is no syscall for batching to amortise. Everywhere else batching is the
single biggest win available.

### Streaming, 32 MiB at a 16 KiB chunk size

| Transport | Throughput |
|---|---:|
| Unix socket | 1,007 MiB/s |
| TCP loopback | 491 MiB/s |
| Named pipe | 351 MiB/s |
| Shared memory | 139 MiB/s *(920 MiB/s measured in isolation — see below)* |

**Read that last row carefully.** Measured on its own, shared-memory streaming reaches ~920 MiB/s.
In the comparison run — after three other transports have each created and torn down connections in
the same process — it measures 139 MiB/s. The difference is contention: every shared-memory
connection holds a dedicated spinning thread, so it is far more sensitive to a busy machine than the
socket transports are. On a box with cores to spare it is fast; on a contended one it degrades first.

### CPU used by one idle connection

All four measure between 1.6% and 3.9% of a core here, which is close enough to the noise floor of
this measurement to treat as "about the same". Shared memory's polling cost shows up under
contention rather than at idle.

---

## Two bugs worth knowing about

Both were found by building these transports, and both are the kind that would be very hard to
diagnose in production.

### `SpinWait.SpinOnce()` sleeps

`SpinWait.SpinOnce()` escalates to `Thread.Sleep(1)` after roughly 20 iterations. On Windows that
resolves to a full timer tick. Measured here:

| | |
|---|---:|
| 20 × `SpinOnce()` | 22 µs |
| 50 × `SpinOnce()` | **446,091 µs** |
| 50 × `SpinOnce(sleep1Threshold: -1)` | 22 µs |

That one default made shared-memory RPC **32 milliseconds** per round trip — 500× *slower* than the
loopback TCP it was supposed to beat. Passing `-1` disables the escalation and took it to 3.2 µs,
a 10,000× difference from a single argument. If you write a spin loop of your own, pass `-1`.

### A spinning loop must not run on a thread-pool thread

A read loop that waits by spinning holds whatever thread it is on. On a pool thread, with both ends
of a connection doing it, the pool starves — continuations then wait on the pool's slow thread
injection, and everything gets multi-millisecond stalls. Shared-memory transports therefore run
their receive loop on a dedicated thread (`TaskCreationOptions.LongRunning`) and keep their waits
synchronous, so no continuation ever hops back to the pool. Sockets park properly and need none of
this.

## Writing your own

`StreamTransport` wraps any duplex `Stream` and gives it the full protocol:

```csharp
var transport = new StreamTransport(
    myStream,
    options,
    remoteEndPoint: "my-transport://endpoint",
    kind: "custom",
    isAlive: () => myStream.CanRead,
    dedicatedReceiveThread: false);   // true if your reads spin rather than park

await using var client = BlackHoleClient.Over(transport);
```

For a listener, implement `IListenerHost` — three members and two events — and hand it to
`new BlackHoleServer(listener)`.

---

*Built by Gravicode Studios, led by Kang Fadhil.*
