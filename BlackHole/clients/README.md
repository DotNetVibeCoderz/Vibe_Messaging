# Client SDKs

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

Clients for the BlackHole binary protocol in **Python**, **Go** and **Node.js**. Each one implements
the wire format independently and is verified against the real .NET server, not a mock.

| | Package | Requires | Tests |
|---|---|---|---|
| [Python](python/) | `blackhole-messaging` | 3.10+ | 49 |
| [Go](go/) | `.../BlackHole/clients/go` | 1.22+ | 30 |
| [Node.js](nodejs/) | `@gravicode/blackhole-messaging` | 18+ | 34 |

All three cover RPC (both directions), Pub/Sub with `+` and `#` wildcards, Streaming, Batching,
keepalive, and per-connection statistics.

## Transports

| | TCP | Unix socket | Named pipe | Shared memory |
|---|:---:|:---:|:---:|:---:|
| .NET | ✅ | ✅ | ✅ | ✅ |
| Python | ✅ | ✅ *(not on Windows)* | — | — |
| Go | ✅ | ✅ | — | — |
| Node.js | ✅ | ✅ | ✅ | — |

Every SDK speaks TCP plus whatever local IPC its runtime offers natively, and the wire format is
identical across all of them — a Node client on a named pipe talks to the same .NET server as a Go
client on a Unix socket.

The gaps are deliberate rather than unfinished. Python's asyncio has no named-pipe client and no
`AF_UNIX` on Windows; Go would need a third-party package for named pipes. **Shared memory is
.NET-only** — it needs a mapped segment and a dedicated polling thread, which none of these three
can offer without native code. Use it where the latency matters and .NET is on both ends; see
[docs/transports.md](../docs/transports.md).

Each SDK ships a benchmark that measures its transports against the same .NET server:

```bash
cd clients/python  && PYTHONPATH=. python example/benchmark.py
cd clients/go      && go run ./example/benchmark
cd clients/nodejs  && node example/benchmark.js
```

## The same program, three times

```python
# Python
from blackhole import connect

async with await connect("127.0.0.1", 5000) as client:
    print(await client.call_text("upper", "halo blackhole"))
    await client.subscribe("sensor/+/temperature", lambda t, p: print(t, p))
    await client.publish("sensor/tank-3/temperature", "28.4")
```

```go
// Go
client, _ := blackhole.Connect(ctx, "127.0.0.1:5000", nil)
defer client.Close()

shouted, _ := client.CallText(ctx, "upper", "halo blackhole")
client.Subscribe("sensor/+/temperature", func(topic string, payload []byte) { ... })
client.PublishText("sensor/tank-3/temperature", "28.4")
```

```js
// Node.js
const client = await connect({ host: '127.0.0.1', port: 5000 });

console.log(await client.callText('upper', 'halo blackhole'));
await client.subscribe('sensor/+/temperature', (topic, payload) => { ... });
await client.publish('sensor/tank-3/temperature', '28.4');
await client.close();
```

## Interop is tested, not assumed

A second implementation of a wire format is only correct if it agrees with the first. Every SDK's
test suite starts [`tests/BlackHole.InteropServer`](../tests/BlackHole.InteropServer) — the actual
.NET library — and asserts against it over a real socket. If a codec drifts by one byte, a test
fails rather than a deployment.

The server exposes a fixed contract each suite is written against:

| Method | Proves |
|---|---|
| `echo` | Framing round-trips byte for byte |
| `upper` | UTF-8 in both directions |
| `sum` | Numeric payloads survive (returns int32 LE) |
| `boom` | A handler failure surfaces as an error, not a hang |
| `sleep` | The client's own deadline fires |
| `big` | Large payloads cross intact (1 B to 1 MiB) |
| `callback` | The server can call a method **on the client** |

Plus: a completed upload is echoed back on the `stream/done` topic as `<id>:<length>`, so a client
can assert its stream arrived.

Run them:

```bash
# Build the reference peer once
dotnet build tests/BlackHole.InteropServer -c Release

cd clients/python  && python -m pytest tests/ -q
cd clients/go      && go test ./blackhole/ -count=1
cd clients/nodejs  && node --test
```

Each suite starts and stops the server itself. Codec-only tests need no .NET at all:
`pytest tests/test_protocol.py`, `go test -short ./blackhole/`, `node --test test/protocol.test.js`.

## Two things every SDK gets right

**Wire before you read.** All three take a `configure` callback that runs *before* the read loop
starts. A server that pushes the instant it accepts would otherwise beat a handler registered after
connecting — the same race that was a real bug in the .NET transport.

```python
await connect(host, port, configure=lambda c: c.on_publish(handler))          # Python
bh.Connect(ctx, addr, &bh.Options{Configure: func(c *bh.Client) { ... }})     # Go
await connect({ host, port, configure: (c) => c.register('m', handler) })     # Node.js
```

**Topic matching mirrors the broker.** Including where the broker is stricter than MQTT:
`sensor/#` does **not** match the bare parent `sensor`, because `#` must have at least one segment
to swallow. The broker decides delivery, so agreeing with it beats agreeing with the spec.

## Measuring latency

On Windows, `time.Now()` in Go resolves to roughly **500 µs** and Python's `time.monotonic()` to
about **15 ms** — both coarser than a loopback round trip, which will read as zero. The SDKs use the
highest-resolution clock each runtime offers (`perf_counter`, `process.hrtime.bigint`), and Go adds
`PingAverage(ctx, n)` to average over several probes when one is below the clock's granularity.

## Running the examples

Start a server, then run any example against it:

```bash
dotnet run --project tests/BlackHole.InteropServer -- --port 5000

cd clients/python  && PYTHONPATH=. python example/demo.py --port 5000
cd clients/go      && go run ./example -addr 127.0.0.1:5000
cd clients/nodejs  && node example/demo.js --port 5000
```

Each exercises RPC, Pub/Sub, Streaming, Batching, a server callback and keepalive:

```
connected to 127.0.0.1:5000
rpc        : upper("halo blackhole") -> "HALO BLACKHOLE"
rpc error  : Unknown method 'does-not-exist'.
pubsub     : sensor/tank-3/temperature = 28.4
streaming  : 1.1 MiB in 17 ms
batching   : 1000 messages in one write, 0.9 ms
callback   : server asked, client answered "python-example:hello"
keepalive  : 0.193 ms round trip
```

## The protocol

See [docs/protocol.md](../docs/protocol.md) for the byte layout. In short, each frame is:

```
[FrameLength int32][Type u8][Flags u8][HeaderLen uint16][CorrelationId int64][Header][Payload]
```

Little-endian throughout, with `FrameLength` counting everything after itself. A batch envelope's
payload is a run of complete frames, so the same decoder unpacks it.

---

*Built by Gravicode Studios, led by Kang Fadhil.*
