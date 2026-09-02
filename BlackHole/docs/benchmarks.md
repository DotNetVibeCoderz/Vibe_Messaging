# Benchmarks

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

Every number here was measured on the machine described below, by the code in
[`src/BlackHole.Benchmarks`](../src/BlackHole.Benchmarks). Nothing is estimated.

## The machine

| | |
|---|---|
| Runtime | .NET 10.0.11 |
| OS | Windows 11 (10.0.26200) |
| Logical cores | 8 |
| GC | Server GC, concurrent |
| Transport | TCP over loopback, **both ends in one process** |
| Date | 2026-09-02 |

**Read loopback numbers as an upper bound on the library, not a prediction of your network.** They
measure framing, dispatch and allocation with the network removed. Over a real link, latency is
dominated by the link; the allocation figures still hold.

## Two harnesses, two questions

| | `--quick` | BenchmarkDotNet |
|---|---|---|
| Answers | What does a running system look like? | What does one operation cost? |
| Reports | Percentiles, aggregate rates, bandwidth | Nanoseconds and bytes per op |
| Run | `dotnet run -c Release -- --quick` | `dotnet run -c Release -- --filter "*"` |

---

## RPC latency

50,000 sequential round trips, 30-byte JSON payload, after 5,000 warm-up calls.

| | |
|---|---|
| Throughput | **21,139 calls/sec** |
| Mean | 47.2 µs |
| **p50** | **41.3 µs** |
| p90 | 70.7 µs |
| p99 | 110.0 µs |
| p99.9 | 218.7 µs |
| Max | 1,822 µs |
| Allocated | 776 B/call *(client **and** server, in one process)* |
| Gen-0 collections | 15 across all 50,000 calls |

Sequential means one call in flight at a time, so this is latency, not capacity. The 776 bytes are
per *round trip* and cover both halves: the `TaskCompletionSource`, the pending-call entry, the
timeout registration, and the result array the caller keeps. The framing itself allocates nothing —
see the codec table below.

## RPC throughput

16 calls in flight per connection.

| Connections | Calls | Duration | Throughput |
|---:|---:|---:|---:|
| 1 | 20,000 | 583 ms | 34,324/sec |
| 4 | 80,000 | 499 ms | 160,293/sec |
| 16 | 320,000 | 1,593 ms | **200,817/sec** |

Scaling is close to linear to 4 connections and then flattens — at 16 connections both ends are on
the same 8 cores, so the process is competing with itself.

## Pub/Sub fan-out

| | |
|---|---|
| Subscribers | 50 |
| Publishes | 2,000 |
| Deliveries | 100,000 |
| Duration | 1,435 ms |
| Rate | **69,711 deliveries/sec** |

One publish becomes 50 socket writes, so the delivery rate is the number that matters.

## Batching

200,000 small publishes (a 4-byte payload on a 25-character topic).

| Mode | Duration | Messages/sec | Socket writes |
|---|---:|---:|---:|
| One send per message | 1,976 ms | 101,214 | 200,000 |
| Batches of 256 | 89 ms | 2,236,709 | 783 |
| **Batches of 1024** | **87 ms** | **2,305,513** | **196** |

**22× faster**, and 1,000 fewer syscalls. Past 256 per batch the curve is flat — the syscall is
already amortised, so pick the batch size that fits your latency budget rather than the largest one.

## Streaming

64 MiB per transfer.

| Chunk size | Duration | Throughput | Chunks |
|---:|---:|---:|---:|
| 4 KiB | 142 ms | 452 MiB/s | 16,384 |
| **16 KiB** | **123 ms** | **520 MiB/s** | 4,096 |
| 64 KiB | 134 ms | 479 MiB/s | 1,024 |

16 KiB is the sweet spot: large enough to amortise per-chunk overhead, small enough to stay in cache.
`StreamSender.FlushThreshold` (64 KiB by default) is why 4 KiB chunks still reach 452 MiB/s — chunks
are written without flushing until the threshold is crossed, so the chunk size does not dictate the
write size.

---

## Codec micro-benchmarks

BenchmarkDotNet, short job, `MemoryDiagnoser`. **This is where the zero-allocation claim is proved.**

### One frame

| Operation | Payload | Mean | **Allocated** |
|---|---:|---:|---:|
| Encode | 16 B | 41.5 ns | **0 B** |
| Decode | 16 B | 110.8 ns | **0 B** |
| Round trip | 16 B | 153.2 ns | **0 B** |
| Encode | 512 B | 47.8 ns | **0 B** |
| Decode | 512 B | 111.7 ns | **0 B** |
| Round trip | 512 B | 152.0 ns | **0 B** |
| Encode | 4 KiB | 108.0 ns | **0 B** |
| Decode | 4 KiB | 104.8 ns | **0 B** |
| Round trip | 4 KiB | 200.9 ns | **0 B** |

Decode is flat across payload sizes because it never copies: the parsed payload points into the
buffer it arrived in. Encode grows with the payload because those bytes are genuinely copied into
the send buffer.

### Header decoding

| Method | Mean | Ratio | Gen0 |
|---|---:|---:|---:|
| `Encoding.UTF8.GetString` | 31.6 ns | 1.00 | 0.0042 |
| **`HeaderCache.GetString`** | **25.9 ns** | **0.82** | **—** |

18% faster, and — the real point — **no gen-0 pressure**. Every received message decodes a header,
so this runs once per message forever. In the demo run the cache took 20,000 hits against 7 misses.

### Batch envelopes

| Operation | Messages | Mean | Per message | Allocated |
|---|---:|---:|---:|---:|
| Pack | 16 | 603 ns | 37.7 ns | **0 B** |
| Unpack | 16 | 1,768 ns | 110.5 ns | **0 B** |
| Pack | 256 | 10,131 ns | 39.6 ns | **0 B** |
| Unpack | 256 | 26,647 ns | 104.1 ns | **0 B** |

Per-message cost is constant with batch size, which is what you want: batching wins on syscalls, not
on codec efficiency.

### Topic matching

| Filter | Mean | Allocated |
|---|---:|---:|
| Exact | 33.7 ns | **0 B** |
| `sensor/+/temperature` | 37.0 ns | **0 B** |
| `sensor/#` | 17.7 ns | **0 B** |
| Non-matching | 31.7 ns | **0 B** |

Matching walks both strings as spans. `#` is fastest because it stops at the first wildcard segment.
The broker runs this once per wildcard filter per publish, so exact topics — resolved through a
dictionary — never pay it at all.

---

## Reproducing

```bash
cd BlackHole/src/BlackHole.Benchmarks

# Sustained load: percentiles, rates, bandwidth (~3 minutes)
dotnet run -c Release -- --quick

# One stage at a time
dotnet run -c Release -- --quick latency
dotnet run -c Release -- --quick throughput fanout batch stream

# Micro-benchmarks (~10 minutes with --job short)
dotnet run -c Release -- --filter "*CodecBenchmarks*" --job short
dotnet run -c Release -- --filter "*" --job short          # everything
```

Reports land in `BenchmarkDotNet.Artifacts/results/` as Markdown, CSV and HTML. The raw output
behind this page is in [benchmark-run.txt](benchmark-run.txt).

CI can run the whole suite on demand — the **BlackHole CI** workflow has a `run_benchmarks` input —
but benchmarks never gate a merge. Shared runners are too noisy for absolute numbers; read the
trend.

## Reading these honestly

- **Loopback removes the network.** Over a real link, latency is the link's. The allocation numbers
  are the portable ones.
- **Both ends share 8 cores.** Split across machines, per-connection numbers improve and aggregate
  numbers change shape.
- **`--quick` runs once.** BenchmarkDotNet's figures carry error bars; the harness's do not. Treat
  the harness as a system smoke test with numbers attached.
- **Your handlers are not free.** Every figure here uses `echo`. A handler that hits a database
  moves the bottleneck there, which is exactly why the library tries to cost nothing.

---

*Built by Gravicode Studios, led by Kang Fadhil.*
