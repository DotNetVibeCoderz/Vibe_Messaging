# Getting started

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

## Requirements

- **.NET 10 SDK** or later
- Any platform .NET 10 runs on. CI builds and tests on Linux, Windows and macOS.

## Install

```bash
dotnet add package BlackHole.Messaging
```

The id `BlackHole` was already taken on nuget.org, so the package is **BlackHole.Messaging**. The
assembly and every namespace are still `BlackHole.*` — nothing in your code says "Messaging".

## A server

```csharp
using BlackHole.Hosting;

await using var server = new BlackHoleServer(5000);

server.Rpc
    .RegisterText("upper", text => text.ToUpperInvariant())
    .Register("echo", request => request.Payload)
    .Register("lookup", async (request, ct) =>
    {
        Customer customer = await database.FindAsync(request.Text(), ct);
        return JsonSerializer.SerializeToUtf8Bytes(customer);
    });

server.Start();
Console.WriteLine($"Listening on {server.EndPoint}");
await Task.Delay(Timeout.Infinite);
```

Port `0` lets the OS pick; read `server.EndPoint.Port` afterwards to find out which. To keep a
server off the network entirely — a simulator, a test — bind loopback explicitly:

```csharp
var server = new BlackHoleServer(new IPEndPoint(IPAddress.Loopback, 5000));
```

## A client

```csharp
await using var client = await BlackHoleClient.ConnectAsync("127.0.0.1", 5000);

string shouted = await client.Rpc.CallTextAsync("upper", "halo blackhole");
byte[] raw = await client.Rpc.CallAsync("echo", "bytes"u8.ToArray());
```

If the client may start before the server:

```csharp
await using var client = await BlackHoleClient.ConnectWithRetryAsync("127.0.0.1", 5000, attempts: 5);
```

### Failures surface, they do not hang

```csharp
try
{
    var result = await client.Rpc.CallAsync("risky", payload, timeout: TimeSpan.FromSeconds(5));
}
catch (RpcException ex)
{
    // Thrown for: the handler threw, the method does not exist,
    // the deadline passed, or the connection dropped mid-call.
    Console.WriteLine($"{ex.Method}: {ex.Message}");
}
```

Every call has a deadline — `DefaultTimeout` is 30 seconds. In v2 a lost reply blocked the caller
forever.

## Pub/Sub

```csharp
// Subscriber
client.PubSub.Received += (topic, payload) =>
{
    // The payload dies when this handler returns. Copy it if you keep it.
    Console.WriteLine($"{topic}: {Encoding.UTF8.GetString(payload.Span)}");
};

await client.PubSub.SubscribeAsync("sensor/+/temperature");  // one segment
await client.PubSub.SubscribeAsync("alarm/#");               // everything below

// Publisher
await client.PubSub.PublishAsync("sensor/tank-3/temperature", "28.4");

// Or from the server
await server.PublishAsync("alarm/floor-1/pump", "overheating"u8.ToArray());
```

`+` matches exactly one segment; `#` matches the rest and must come last.

## Streaming

```csharp
// Send
await using var file = File.OpenRead("firmware.bin");
long sent = await client.OutgoingStreams.SendAsync(
    "firmware-2026",
    file,
    new StreamDescriptor("firmware.bin", file.Length, "application/octet-stream"),
    chunkSize: 16 * 1024,
    progress: new Progress<long>(b => Console.WriteLine($"{b / 1024:N0} KiB")));

// Receive
server.ClientConnected += connection =>
{
    connection.Streams.Completed += (_, e) =>
        Console.WriteLine($"{e.StreamId}: {e.Length:N0} bytes");
};
```

To keep a large upload out of memory, give the receiver a sink:

```csharp
connection.Streams.SinkFactory = (id, descriptor) =>
    File.Create(Path.Combine("uploads", Path.GetFileName(descriptor.Name)));
```

## Batching

```csharp
// Explicit: this set, one envelope, now
await client.Batch.SendBatchAsync(messages);

// Automatic: buffer and flush on whichever threshold trips first
client.Batch.MaxCount = 256;
client.Batch.MaxBytes = 64 * 1024;
client.Batch.MaxDelay = TimeSpan.FromMilliseconds(20);
client.Batch.Start();

foreach (var reading in readings)
    await client.Batch.AddAsync(new BlackHoleMessage(MessageType.Publish, topic, reading));
```

Worth 22× on small-message throughput — see [benchmarks.md](benchmarks.md).

## Calling the client from the server

Both ends can serve. This is how you command a device that dialled out from behind NAT:

```csharp
// On the client
client.Handlers.RegisterText("device/status", _ => "ok: 4 sensors online");

// On the server
var caller = new RpcClient(connection.Transport);
connection.Router.On(MessageType.RpcResponse, caller.HandleAsync);
string status = await caller.CallTextAsync("device/status", "?");
```

## The one rule to remember

**A received payload is valid only until your handler returns.** It points into the transport's
buffer — that is why receiving allocates nothing. Keep the bytes and you must copy:

```csharp
client.PubSub.Received += (topic, payload) =>
{
    byte[] mine = payload.ToArray();          // copy, then queue
    _queue.Enqueue((topic, mine));
};
```

## Where to next

- [Architecture](architecture.md) — how the layers fit together
- [Patterns](patterns.md) — each pattern in depth
- [Performance](performance.md) — keeping the allocations at zero
- [IoT Gateway](iot-gateway.md) — a full application built on all of it

---

*Built by Gravicode Studios, led by Kang Fadhil.*
