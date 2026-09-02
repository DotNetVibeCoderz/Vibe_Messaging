# Performance

*Gravicode Studios, led by Kang Fadhil.*

v2 is a rewrite of the codec and the connection pump. This page says what changed, what it bought,
and — because it matters more than the wins — what still allocates.

Every number here is measured on this repository:

```bash
dotnet run -c Release --project src/SocketSignal.Benchmarks -- throughput   # end-to-end + allocations
dotnet run -c Release --project src/SocketSignal.Benchmarks -- micro        # BenchmarkDotNet
```

The comparison is against the real v1 implementation, recovered from git history and kept in
`src/SocketSignal.Benchmarks/Baseline/`. It is not a strawman written to lose — it is the code
that was here before.

Measured on .NET 10.0.11, Windows 11, 8 logical cores.

## End to end

20,000 sequential RPC round trips over a loopback WebSocket, one call in flight at a time:

| stack | calls/sec | latency | allocated per call |
|---|---:|---:|---:|
| v1 | 6,809 | 146.9 µs | 16,379 B |
| **v2** | **9,989** | **100.1 µs** | **3,311 B** |
| | ×1.47 | −32% | −79.8% |

The allocation figure is the one that decides whether a server holds up at ten thousand
connections. It is not zero, and the section at the end explains why.

## The codec

Time from BenchmarkDotNet; allocation measured over 200,000 operations with
`GC.GetTotalAllocatedBytes`, which is what makes pooled buffers show up honestly — a pool rent
looks like an allocation the first time and like nothing every time after.

| operation | v1 | v2 | speedup | v1 alloc | v2 alloc |
|---|---:|---:|---:|---:|---:|
| encode one invoke frame | 1,386 ns | **327 ns** | ×4.2 | 1,200 B | **0 B** |
| encode, single typed argument | — | **269 ns** | — | — | **0 B** |
| decode one invoke frame | 1,119 ns | **314 ns** | ×3.6 | 1,296 B | **0 B** |
| decode and read both arguments | — | **609 ns** | — | — | **0 B** |
| find the handler for a method | 52.8 ns | **21.8 ns** | ×2.4 | 64 B | **0 B** |
| mint a correlation id | 118 ns | 130 ns | ×0.9 | 88 B | **0 B** |

> The raw BenchmarkDotNet log in [`benchmark-micro.txt`](benchmark-micro.txt) shows a larger
> allocation figure for the v1 decode (21,806 B) than the 1,296 B quoted above. Both are real: the
> BenchmarkDotNet run charges `JsonDocument`'s pooled rentals to the first operation, while the
> `-- alloc` measurement runs 200,000 operations and so sees the amortised cost — which is what a
> long-lived connection actually pays. The table above quotes the amortised number throughout.

## What changed, and why

### Writing: no string, no per-frame array

v1 encoded a frame by serialising a POCO to a `string` and then copying it into a fresh `byte[]`
with `Encoding.UTF8.GetBytes`. Two allocations and two passes over the data, per message, for a
string nothing ever read.

v2 keeps one pooled buffer and one `Utf8JsonWriter` per connection and writes UTF-8 straight into
the buffer that goes to the socket. `Reset` rewinds both without releasing, so a warmed-up
connection encodes frames without allocating.

The writer is only touched while the connection holds its send lock, which is also what makes
concurrent sends safe — see below.

### Reading: parse in place

v1 turned the received bytes into a UTF-16 `string`, bound them to a POCO, and gave every argument
its own `JsonElement` — and therefore its own `JsonDocument`.

v2 reads the envelope with `Utf8JsonReader` directly over the receive buffer. `SignalFrame` is a
`ref struct` whose fields are slices of that buffer, so decoding a frame allocates nothing at all.
Arguments stay as raw JSON until a handler asks for them, and a typed handler deserialises them
straight into its parameter types — an `int` argument costs nothing.

The buffer is rented from `ArrayPool` once per connection, grown in place when a large message
arrives, and kept at that size.

### Dispatch: look up by UTF-8

A `Dictionary<string, Handler>` forces one UTF-16 allocation per received frame just to look the
handler up. `Utf8HandlerTable` is a small open-addressed table probed with the raw bytes off the
receive buffer: 64 B and 52.8 ns become 0 B and 21.8 ns.

Registrations rebuild the table under a lock and swap it in; reads are lock-free and never see a
torn table.

### Correlation ids: a counter, not a GUID

v1 spent `Guid.NewGuid().ToString("N")` — 88 bytes and 32 characters — on every call. v2 uses a
monotonic `long` formatted into a stack buffer as it is written into the frame.

This is the one row where v2's *time* is slightly worse (130 ns against 118 ns), because the
benchmark measures the id together with writing a whole ping frame while v1's measures only the
`Guid`. The allocation goes to zero either way, which is what the change was for.

Ids only need to be unique per connection and direction, so a counter is enough. See
[protocol.md](protocol.md#correlation-ids).

### Sends are serialised

v1 called `WebSocket.SendAsync` from wherever a send happened. Two concurrent sends on one socket
interleave their bytes and corrupt the stream — a bug that shows up as a peer disconnecting under
load and is very hard to find afterwards.

v2 holds a per-connection `SemaphoreSlim` across encode-and-send. Encoding inside the lock is what
lets the buffer and writer be reused, so correctness and the allocation win come from the same
decision.

### Handlers run off the pump, with a limit

Awaiting a handler on the receive loop means one slow handler stalls the socket. v2 dispatches
each invocation as a separate operation, gated by a semaphore of `MaxConcurrentInvocations`
(64 by default). Once the limit is reached the pump stops reading, which pushes flow control down
to TCP — that is the backpressure valve.

The per-invocation state (the correlation id and the raw arguments, copied out of the receive
buffer so it can be reused) comes from a small pool, so a call in flight costs a task and little else.

## What still allocates

Per end-to-end call, v2 spends about 3.3 KB. It is worth being precise about where that goes,
because "zero allocation" claims that stop at the codec are not useful:

- **The async machinery.** A round trip suspends several times on each side — socket send, socket
  receive, the handler, the reply. Each suspension boxes a state machine.
- **The pending call.** A `TaskCompletionSource` and its `Task` per outstanding call.
- **The timeout.** `Task.WaitAsync(timeout, ct)` allocates a timer registration per call. Setting
  `CallTimeout = Timeout.InfiniteTimeSpan` removes it, at the cost of the protection it buys.
- **Boxed arguments.** `CallAsync<int>("sum", 5, 7)` builds an `object[]` and boxes both ints. The
  single-argument overload `CallAsync<TArg, TResult>` avoids both.
- **The return value.** A handler returning a value type boxes it once on the way out.
- **`System.Net.WebSockets` itself**, which has its own per-operation costs.

The codec, the framing, the dispatch and the buffers — everything SocketSignal owns end to end —
are allocation free in steady state. The remainder is the runtime's, and reducing it further means
`IValueTaskSource` plumbing whose complexity is not obviously worth it. If you have a workload
where it is, the pump is one file: `src/SocketSignal/Hosting/SignalConnection.cs`.

## Getting the most out of it

1. **Use the typed overloads.** `Register<int, int, int>` and `CallAsync<TArg, TResult>` skip
   `JsonElement`, the `object[]`, and the boxing.
2. **Prefer one object argument over several.** One record deserialises in a single pass.
3. **Fire and forget when no answer is needed.** `SendAsync` costs no pending call, no timeout
   registration, and no reply frame.
4. **Set `CallTimeout` deliberately.** Long enough for a slow handler, short enough to notice a
   dead peer.
5. **Raise `MaxConcurrentInvocations`** only if handlers are genuinely I/O-bound; the default is a
   backpressure limit, not a throttle to be tuned away.
6. **Watch `Statistics`.** `BytesSent / FramesSent` tells you whether frames are the size you
   think they are, and `CallsFailed` climbing is usually a timeout that wants raising.

## Reproducing

```bash
# End-to-end round trips, v1 against v2, plus per-operation allocations
dotnet run -c Release --project src/SocketSignal.Benchmarks -- throughput

# BenchmarkDotNet over the codec and dispatch paths
dotnet run -c Release --project src/SocketSignal.Benchmarks -- micro

# Allocations alone
dotnet run -c Release --project src/SocketSignal.Benchmarks -- alloc
```

Numbers move with hardware, .NET version, and whether a laptop is on battery. What should hold is
the shape: the codec paths allocate nothing, and an end-to-end call costs a fraction of what v1
cost.
