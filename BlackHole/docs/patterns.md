# Patterns

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

Four patterns, all built the same way: a class that takes an `ITransport`, exposes a `HandleAsync`
matching `MessageDispatch`, and offers `AttachTo(router)` for the common case. Use one, use all
four, or write a fifth.

---

## RPC

### Serving

```csharp
var rpc = new RpcServer();

rpc.RegisterText("upper", text => text.ToUpperInvariant());
rpc.Register("echo", request => request.Payload);
rpc.Register("lookup", async (request, ct) =>
{
    var customer = await database.FindAsync(request.Text(), ct);
    return JsonSerializer.SerializeToUtf8Bytes(customer);
});

rpc.AttachTo(router);
```

The synchronous overload may return `request.Payload` directly — the reply is written before the
handler's frame is released, so no copy is needed. That is how `echo` costs nothing.

### Calling

```csharp
var client = new RpcClient(transport) { DefaultTimeout = TimeSpan.FromSeconds(10) };
client.AttachTo(router);

byte[] result = await client.CallAsync("lookup", key, timeout: TimeSpan.FromSeconds(2));
string text = await client.CallTextAsync("upper", "halo");
await client.NotifyAsync("log", entry);   // fire and forget, no reply expected
```

### Failure is a first-class outcome

| What happened | What the caller sees |
|---|---|
| Handler threw | `RpcException` carrying the type and message |
| Method not registered | `RpcException`, immediately |
| Deadline passed | `RpcException` |
| Connection dropped mid-call | `RpcException` on every pending call |

The server **always replies** — an unknown method comes straight back flagged
`MessageFlags.Error`. v2 simply ignored requests it could not route, so the caller waited forever.

### How correlation works

`RpcClient` keeps a `ConcurrentDictionary<long, Pending>` keyed on an interlocked counter. The
server echoes the id back; the client removes the entry and completes the `TaskCompletionSource`.
Hundreds of calls can be in flight on one connection, and a
[test](../tests/BlackHole.Tests/EndToEndTests.cs) fires 200 concurrently and checks every answer
matches its own question.

The result **is** copied out of the transport buffer — the awaiting continuation runs after dispatch
returns, so it has to be.

---

## Pub/Sub

### Wildcards

`+` matches exactly one segment. `#` matches the remainder and must be last.

| Filter | `sensor/tank-3/temperature` | `sensor/tank-3/humidity` | `sensor/a/b/temperature` |
|---|:---:|:---:|:---:|
| `sensor/tank-3/temperature` | ✅ | ❌ | ❌ |
| `sensor/+/temperature` | ✅ | ❌ | ❌ |
| `sensor/#` | ✅ | ✅ | ✅ |
| `+/+/temperature` | ✅ | ❌ | ❌ |

Exact filters resolve through a dictionary; only wildcard filters are scanned, at ~35 ns each with
no allocation.

### The broker

```csharp
var broker = new PubSubBroker { EchoToPublisher = true };
broker.AttachTo(router);

// Required, or the subscriber list grows for the life of the process.
listener.ClientDisconnected += (transport, _) => broker.RemoveSubscriber(transport);
```

`BlackHoleServer` does that cleanup for you. v2's demo did not, and leaked every subscriber that
ever connected.

Each subscriber set is an immutable array swapped under a lock, so fan-out reads it without locking
and one slow subscriber cannot block delivery to the others. A subscriber whose send throws is
skipped, not retried — its `Closed` event will clean it up.

### Not a message queue

There is no persistence, no delivery guarantee, and no offline buffering. A message published while
a subscriber is disconnected is gone. If you need durability, put a broker with a disk behind it.

---

## Streaming

### Sending

```csharp
var sender = new StreamSender(transport) { FlushThreshold = 64 * 1024 };

await using var file = File.OpenRead("firmware.bin");
long sent = await sender.SendAsync(
    streamId: "firmware-2026",
    source: file,
    descriptor: new StreamDescriptor("firmware.bin", file.Length, "application/octet-stream"),
    chunkSize: 16 * 1024,
    progress: new Progress<long>(b => Console.WriteLine($"{b / 1024:N0} KiB")));
```

The chunk buffer is rented once for the whole transfer. Chunks are written **without flushing** until
`FlushThreshold` bytes are pending, which turns "one socket write per 4 KiB" into one per 64 KiB —
the reason 4 KiB chunks still hit 452 MiB/s. v2 flushed every chunk.

### Receiving

```csharp
var receiver = new StreamReceiver
{
    MaxStreamLength = 256L * 1024 * 1024,
    MaxConcurrentStreams = 64,
};

receiver.Started  += (id, d) => Console.WriteLine($"{id}: {d.Name}, {d.TotalLength:N0} B");
receiver.Progress += (id, received, total) => Report(id, received, total);
receiver.Completed += (_, e) => Save(e.StreamId, e.Data);   // e.Data dies when this returns
receiver.Aborted  += (id, why) => Log(id, why);

receiver.AttachTo(router);
```

To keep a large body out of memory:

```csharp
receiver.SinkFactory = (id, descriptor) =>
    File.Create(Path.Combine("uploads", Path.GetFileName(descriptor.Name)));
```

`MaxStreamLength` and `MaxConcurrentStreams` are not optional niceties — without them a peer, hostile
or merely buggy, turns an open stream into unbounded process memory.

Chunks carry their index in `CorrelationId` and the receiver checks the sequence, so a stream that
loses its order is aborted rather than silently reassembled wrong.

### One receiver per connection

Two devices may both upload `firmware.bin`. If they shared a `StreamReceiver`, their chunks would
interleave into corruption. `BlackHoleServer` gives every connection its own — see
[architecture.md](architecture.md#hosting--srcblackholehosting).

---

## Batching

### When it pays

| | Use |
|---|---|
| Many small messages, latency tolerant | **Batching.** 22× on throughput. |
| Request/response | No — a batch cannot be awaited per message. |
| One large body | No — that is streaming. |
| A burst you already have in hand | `WriteAsync` × N then one `FlushAsync`. |

### Explicit

```csharp
await client.Batch.SendBatchAsync(messages);   // this set, one envelope, now
```

### Automatic

```csharp
client.Batch.MaxCount = 256;                          // flush at 256 messages
client.Batch.MaxBytes = 64 * 1024;                    // or 64 KiB
client.Batch.MaxDelay = TimeSpan.FromMilliseconds(20); // or after 20 ms
client.Batch.Start();                                  // starts the delay timer

await client.Batch.AddAsync(message);
```

Whichever threshold trips first sends the envelope. `MaxDelay` is what bounds latency when traffic
is sparse: without it, the last few messages of a burst wait for a batch that may never fill.
`AddAsync` buffers into a pooled writer reused for the life of the sender, so steady telemetry
allocates nothing per message.

### Receiving is transparent

```csharp
var batches = new BatchReceiver(router.Dispatch);
batches.AttachTo(router);
```

Inner messages are pushed back through the router, so a batched publish takes exactly the same path
as one that arrived alone. Your `Publish` handler cannot tell the difference — and does not need to.

An envelope's payload is a run of complete BlackHole frames, unpacked with the same `FrameCodec` the
transport uses. v2 had a separate inner format that its receiver parsed by hand.

---

## Writing your own

```csharp
// 1. A type byte nobody else uses
public const MessageType Heartbeat = (MessageType)0x40;

// 2. A handler with the MessageDispatch shape
ValueTask HandleHeartbeat(ITransport transport, BlackHoleMessage message, CancellationToken ct)
{
    _lastSeen[message.Header] = DateTimeOffset.UtcNow;
    return ValueTask.CompletedTask;
}

// 3. Register it
router.On(Heartbeat, HandleHeartbeat);
```

The transport is type-agnostic; it never needs to know your type exists.

---

*Built by Gravicode Studios, led by Kang Fadhil.*
