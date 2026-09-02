# Performance

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

Every claim on this page is backed by a number in [benchmarks.md](benchmarks.md).

## The goal

**Zero allocations in the steady state.** Not "few" — zero. Encoding a frame, decoding a frame,
matching a topic, and packing or unpacking a batch all measure **0 B** under
BenchmarkDotNet's `MemoryDiagnoser`. What remains is per-*operation* overhead an API cannot avoid:
the `TaskCompletionSource` an awaited RPC call needs, and the array a caller keeps.

Allocation matters more than nanoseconds here. A messaging library sits under everything else in the
process; if it produces gen-0 garbage per message, it imposes GC pauses on code that never asked for
them.

## What each decision bought

### Pipelines instead of `NetworkStream` + `byte[]`

v2 allocated a `byte[]` per frame on receive, then a `MemoryStream` and `BinaryReader` per message
to parse it. `System.IO.Pipelines` owns the buffers and hands out `ReadOnlySequence<byte>` views, so
partial frames need no allocation and a whole one is parsed in place.

**Result: decode is 105 ns and 0 B, flat across payload sizes** — because nothing is copied.

### A struct message

`BlackHoleMessage` is a 40-byte readonly struct. As a class it would be one gen-0 allocation per
message, in both directions. At 200,000 messages/sec that is 400,000 objects/sec of pure garbage.

### A zero-copy payload

When a payload sits in one contiguous segment — the common case — `Payload` points **into the
transport's buffer**. Nothing is copied on the way in.

The cost is one rule: **a received payload is valid only until your handler returns.** Keep it and
you must copy. `BlackHoleMessage.ToOwned()` exists for exactly that.

This is also why `ITransport` has one `MessageDispatch` returning a `ValueTask` rather than a
multicast event. The transport awaits dispatch, so it knows when the buffer can be reclaimed. An
event returning `void` could not offer that guarantee — which is why v2 had to copy.

### A header cache

Every received message decodes a UTF-8 header. Real traffic reuses a tiny vocabulary, so a
direct-mapped cache keyed on the raw bytes returns the same `string` instance:

| | Mean | Gen0 |
|---|---:|---:|
| `Encoding.UTF8.GetString` | 31.6 ns | 0.0042 |
| `HeaderCache.GetString` | **25.9 ns** | **—** |

18% faster and, more importantly, no allocation. The demo run: **20,000 hits, 7 misses.**

### An int64 correlation id

v2 sent a 16-byte `Guid` per message and called `Guid.NewGuid()` per request — a cryptographic RNG
draw on the hot path. An `Interlocked.Increment` counter is 8 bytes and free. **8 bytes saved per
message**, in both directions.

### A lazy linked token

`CallAsync` needs a deadline. Linking that to a caller's token costs a second
`CancellationTokenSource` plus a registration — so the link is only created when the caller actually
passes a cancellable token. Most calls do not.

### Pooled buffers

`PooledBufferWriter` wraps `ArrayPool<byte>.Shared` and — the important part — `Reset()` keeps the
rented array and only rewinds the cursor. A long-lived `BatchSender` is therefore allocation-free
after its first envelope.

## Getting the most out of it

### Batch small messages

The single biggest win available to you:

| | Messages/sec | Socket writes |
|---|---:|---:|
| One send per message | 101,214 | 200,000 |
| **Batches of 256** | **2,236,709** | **783** |

22×. Past 256 the curve is flat, so choose the batch size your latency budget allows, not the
largest one.

### Or coalesce a burst you already hold

```csharp
foreach (var message in burst)
    await transport.WriteAsync(message);   // no flush
await transport.FlushAsync();              // one socket write
```

### Copy only when you keep

```csharp
// Reading it here: no copy needed.
router.On(MessageType.Publish, (_, m) => _gauge.Set(BitConverter.ToDouble(m.Payload.Span)));

// Keeping it: copy.
router.On(MessageType.Publish, (_, m) => _queue.Enqueue(m.ToOwned()));
```

### Do not block the receive loop

Handlers run on the connection's receive loop, in order. Blocking one stops that connection's
inbound traffic. For slow work, copy the payload and hand it to a queue.

### Share the header cache across many connections

```csharp
var options = new TransportOptions { SharedHeaderCache = new HeaderCache(2048) };
```

With hundreds of connections publishing from the same small topic vocabulary, one shared cache is
both smaller and warmer than one per connection. The [IoT gateway](iot-gateway.md) does this.

### Prime it if you know your topics

```csharp
cache.Prime("sensor/tank-3/temperature");   // now even the first message is a hit
```

### Tune chunk size for streams

16 KiB measured fastest (520 MiB/s). `FlushThreshold` matters more than chunk size: chunks are
written without flushing until it is crossed, so a small chunk size does not mean a small write.

### Server GC for servers

```xml
<ServerGarbageCollection>true</ServerGarbageCollection>
<ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
<TieredPGO>true</TieredPGO>
```

## Feeding a UI at high rates

A UI thread cannot absorb 100,000 updates a second, and it does not need to — a display refreshes
60 times a second at most. The pattern the IoT gateway uses:

1. The receive loop writes into a **lock-free ring buffer** (`TraceBuffer`) — no lock, no allocation.
2. A **33 ms dispatcher timer** publishes one coalesced update per property.
3. Charts draw from the ring directly, one `InvalidateVisual` per frame.

Render cost is then flat whether it is 4 devices at 2 Hz or 40 at 200 Hz. Binding straight to the
receive loop would peg the dispatcher and freeze the window.

## Measuring your own

```csharp
long before = GC.GetTotalAllocatedBytes(precise: true);
// ... work ...
long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
```

Per connection, the library counts for you:

```csharp
StatisticsSnapshot stats = transport.Statistics.Snapshot();
Console.WriteLine($"{stats.MessagesReceived:N0} received, {stats.ReceiveRate:N0}/sec");
Console.WriteLine($"round trip {stats.LastRoundTrip?.TotalMilliseconds:F2} ms");
```

`Snapshot()` is immutable and safe to hand to a UI thread.

---

*Built by Gravicode Studios, led by Kang Fadhil.*
