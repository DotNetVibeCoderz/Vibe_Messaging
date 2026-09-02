# Migrating from v2

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

v3 is a rewrite. The wire format changed, so **both ends must be v3** — a v2 client cannot talk to a
v3 server.

## Why it was worth breaking

v2 worked, and for a demo it worked well. Under sustained load it had five problems that could not
be fixed without changing the format or the API:

1. **Framing was duplicated.** `TcpClientTransport` and `TcpServerSideTransport` each carried their
   own copy of serialise, deserialise and the read loop. A change to one silently desynchronised the
   other — the file said so in a comment.
2. **A failed RPC hung the caller forever.** No timeouts, and a request for an unknown method was
   dropped rather than answered.
3. **Every message allocated.** A `byte[]` per frame, then a `MemoryStream` and `BinaryReader` per
   parse, plus a `Guid.NewGuid()` per request.
4. **Fan-out could not push back.** `OnMessageReceived` was a multicast `void` event: every pattern
   object saw every message and none could ask the transport to wait.
5. **Batching had a second wire format** — a shorter inner layout with no correlation id, parsed by
   hand in the receiver. Two formats, two parsers, one guaranteed drift.

## The name

The package is **`BlackHole.Messaging`** — the id `BlackHole` was already taken on nuget.org. The
assembly and every namespace are still `BlackHole.*`.

## Namespaces

| v2 | v3 |
|---|---|
| `BlackHole.Common` | `BlackHole.Protocol` |
| `BlackHole.Transports` | `BlackHole.Transport` |
| `BlackHole.Patterns` | `BlackHole.Patterns` (unchanged) |
| — | `BlackHole.Hosting`, `BlackHole.Buffers`, `BlackHole.Diagnostics` |

## The message

```csharp
// v2 — class, 16-byte GUID, mutable
var message = new BlackHoleMessage
{
    CheckId = Guid.NewGuid(),
    Type = MessageType.RpcRequest,
    Header = "echo",
    Payload = Encoding.UTF8.GetBytes("hello"),
};

// v3 — readonly struct, int64 correlation, flags
var message = new BlackHoleMessage(
    MessageType.RpcRequest, "echo", "hello"u8.ToArray(), correlationId: 42);
```

`CheckId` (`Guid`) became `CorrelationId` (`long`), and `Payload` is now `ReadOnlyMemory<byte>`
rather than `byte[]`.

**The rule that comes with it:** a received payload is valid only until your handler returns. Call
`ToOwned()` if you keep it. That constraint is what buys the zero-copy receive path.

## Receiving

```csharp
// v2 — multicast void event
transport.OnMessageReceived += (sender, msg) =>
{
    if (msg.Type == MessageType.RpcRequest)
        rpcServer.HandlePacket((ITransport)sender, msg);
    else if (msg.Type == MessageType.Publish)
        broker.HandlePacket((ITransport)sender, msg);
};

// v3 — one router, awaited
var router = new MessageRouter();
rpcServer.AttachTo(router);
broker.AttachTo(router);
transport.Dispatcher = router.Dispatch;
```

## Whole-application rewrite

Most of v2's setup code disappears into `BlackHoleServer` / `BlackHoleClient`:

```csharp
// v2
var server = new TcpServerHost(5000);
var rpcServer = new RpcServer();
var broker = new PubSubBroker();
rpcServer.RegisterMethod("echo", payload => payload);
server.OnClientConnected += (s, transport) =>
{
    var streams = new StreamReceiver(transport);
    var batches = new BatchReceiver(transport);
    transport.OnMessageReceived += (sender, msg) => { /* hand-wired routing */ };
};
server.Start();

var transport = new TcpClientTransport("127.0.0.1", 5000);
await transport.StartAsync(CancellationToken.None);
var client = new RpcClient(transport);
var response = await client.CallAsync("echo", payload);

// v3
await using var server = new BlackHoleServer(5000);
server.Rpc.Register("echo", request => request.Payload);
server.Start();

await using var client = await BlackHoleClient.ConnectAsync("127.0.0.1", 5000);
byte[] response = await client.Rpc.CallAsync("echo", payload);
```

## API changes worth knowing

| v2 | v3 | Note |
|---|---|---|
| `RegisterMethod(name, Func<byte[], byte[]>)` | `Register(name, Func<RpcRequest, ReadOnlyMemory<byte>>)` | Also an async overload and `RegisterText` |
| `CallAsync(method, payload)` | `CallAsync(method, payload, timeout, ct)` | 30-second default deadline |
| `SubscribeAsync(topic)` | `SubscribeAsync(filter)` | Now accepts `+` and `#` |
| `SendStreamAsync(id, stream, chunkSize)` | `SendAsync(id, stream, descriptor, chunkSize, progress, ct)` | Descriptor and progress are new |
| `OnStreamCompleted` (`Action<string, byte[]>`) | `Completed` (`EventHandler<StreamCompletedEventArgs>`) | `e.Data` is valid only inside the handler |
| `SendBatchAsync(IEnumerable<…>)` | `SendBatchAsync(IReadOnlyCollection<…>)` plus auto-flush `AddAsync` | |
| `IDisposable` | `IAsyncDisposable` | `await using` |

## Behaviour that changed

**Unknown methods now fail fast.** v2 ignored them; v3 replies with an error flag, and the caller
gets an `RpcException` immediately. If you relied on silence, you were relying on a hang.

**Every call has a deadline.** 30 seconds by default. Long-running handlers need an explicit
`timeout:`.

**Subscribers are cleaned up on disconnect.** `BlackHoleServer` calls
`PubSubBroker.RemoveSubscriber` for you. If you build on the raw broker, wire it yourself or leak.

**Stream limits are enforced.** `MaxStreamLength` defaults to 256 MiB and `MaxConcurrentStreams` to
64. Raise them if you legitimately need more.

**Frames are capped.** `MaxFrameLength` defaults to 16 MiB, checked before any buffer is sized. A
frame past it closes the connection.

## What you get for the port

| | v2 | v3 |
|---|---|---|
| Framing code paths | 2 (duplicated) | 1 |
| Per-message header | 25 B + payload | 16 B + payload |
| Steady-state encode/decode | allocates | **0 B** |
| Failed RPC | hangs forever | `RpcException` |
| Batch wire format | separate inner layout | the same frames |
| Subscriber cleanup | leaks | automatic |
| Batched publishes | one send each | **22× faster** |
| Tests | none | 40 |

See [benchmarks.md](benchmarks.md) for the measurements.

---

*Built by Gravicode Studios, led by Kang Fadhil.*
