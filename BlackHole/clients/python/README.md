# BlackHole Messaging — Python client

*Gravicode Studios, led by Kang Fadhil.*

Asyncio client for the BlackHole binary protocol: **RPC**, **Pub/Sub**, **Streaming** and
**Batching** over TCP. Speaks the same wire format as the [.NET library](../../README.md), verified
against it by the interop suite.

Requires Python 3.10+. No dependencies.

## Install

```bash
pip install blackhole-messaging
```

Or from this repository:

```bash
cd BlackHole/clients/python && pip install -e .
```

## Transports

TCP everywhere, plus Unix domain sockets on Linux and macOS:

```python
client = await connect("127.0.0.1", 5000)                  # TCP
client = await connect_unix("/tmp/blackhole.sock")         # Unix domain socket
```

Both carry the same wire format; only the connection setup differs. CPython exposes no `AF_UNIX`
to asyncio on Windows, so `connect_unix` raises there — check `BlackHoleClient.unix_supported()`.
Named pipes and shared memory are .NET-only; see [docs/transports.md](../../docs/transports.md).

Compare them yourself:

```bash
PYTHONPATH=. python example/benchmark.py
```

## Thirty seconds

```python
import asyncio
from blackhole import connect

async def main():
    async with await connect("127.0.0.1", 5000) as client:
        # RPC
        print(await client.call_text("upper", "halo blackhole"))   # HALO BLACKHOLE

        # Pub/Sub, with MQTT-style wildcards
        await client.subscribe(
            "sensor/+/temperature",
            lambda topic, payload: print(topic, payload.decode()),
        )
        await client.publish("sensor/tank-3/temperature", "28.4")
        await asyncio.sleep(1)

asyncio.run(main())
```

## RPC

```python
result = await client.call("echo", b"bytes")
text   = await client.call_text("upper", "halo", timeout=5.0)
await client.notify("log", b"fire and forget")
```

Every call has a deadline — `default_timeout` is 30 seconds. Failures raise `RpcError` rather than
hanging:

```python
from blackhole import RpcError

try:
    await client.call("risky", payload, timeout=5.0)
except RpcError as error:
    # Raised when the handler failed, the method is unknown, the deadline passed,
    # or the connection dropped mid-call.
    print(error.method, error)
```

Serve methods the peer may call on you — handlers may be sync or async, and return `bytes` or `str`:

```python
client.register("device/status", lambda request: "ok: 4 sensors online")
client.register("device/read", async_handler)
```

## Pub/Sub

`+` matches one segment, `#` matches the remainder.

```python
await client.subscribe("sensor/+/temperature", on_reading)   # per-filter handler
await client.subscribe("alarm/#", on_alarm)
client.on_publish(lambda topic, payload: ...)                # everything

await client.publish("sensor/tank-3/temperature", "28.4")
await client.unsubscribe("alarm/#")
```

## Streaming

```python
sent = await client.send_stream(
    "firmware-2026",
    open("firmware.bin", "rb"),          # bytes or any binary file object
    descriptor=StreamDescriptor("firmware.bin", size, "application/octet-stream"),
    chunk_size=16 * 1024,
    progress=lambda sent: print(f"{sent / 1024:,.0f} KiB"),
)

client.on_stream(lambda stream_id, descriptor, data: save(stream_id, data))
```

Chunks are written into the socket buffer and drained once per 64 KiB rather than per chunk, so a
small chunk size does not mean a small write.

## Batching

```python
from blackhole import Message, MessageType

await client.send_batch([
    Message(MessageType.PUBLISH, f"log/entry/{i}", f"line {i}".encode())
    for i in range(1000)
])
```

One frame, one socket write. The envelope holds complete BlackHole frames, so the peer unpacks it
with the same decoder and each message routes individually.

## Wire your handlers before the read loop starts

`configure` runs after the client is built but **before** anything is delivered. A server that
pushes the instant it accepts would otherwise beat a handler registered after `connect` returns:

```python
client = await connect(
    "127.0.0.1", 5000,
    configure=lambda c: c.on_publish(handler),
)
```

## The one rule

**A received payload is only guaranteed for the duration of your handler.** Copy it if you keep it:

```python
client.on_publish(lambda topic, payload: queue.append((topic, bytes(payload))))
```

## Connection

```python
await client.ping()                  # round trip in seconds
client.statistics                    # messages and bytes, both directions
client.is_closed
await client.wait_closed()
```

`ping` is timed with `perf_counter`, not `monotonic`: on Windows the latter has roughly 15 ms
resolution, coarser than a loopback round trip, and would report zero.

## Testing

```bash
python -m pytest tests/                     # 49 tests
python -m pytest tests/test_protocol.py     # codec only, no .NET needed
```

The interop suite starts the real .NET server and asserts against it. See
[../README.md](../README.md).

## Example

```bash
dotnet run --project ../../tests/BlackHole.InteropServer -- --port 5000
PYTHONPATH=. python example/demo.py --port 5000
```

---

*Built by Gravicode Studios, led by Kang Fadhil.*
