# Architecture

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

Four layers. Each one knows only about the layer below it, and the seam between any two is a single
type.

```
        your code
  ┌──────────────────────────────────────────────────────────────┐
  │  Hosting     BlackHoleServer   BlackHoleClient               │  wired-up defaults
  ├──────────────────────────────────────────────────────────────┤
  │  Patterns    RpcServer/Client  PubSubBroker/Client           │  what messages mean
  │              StreamSender/Receiver  BatchSender/Receiver     │
  ├──────────────────────────────────────────────────────────────┤
  │  Hosting     MessageRouter                                   │  where a message goes
  ├──────────────────────────────────────────────────────────────┤
  │  Transport   ITransport  TcpTransport  TcpListenerHost       │  bytes in, bytes out
  ├──────────────────────────────────────────────────────────────┤
  │  Protocol    BlackHoleMessage  FrameCodec  HeaderCache       │  the wire format
  └──────────────────────────────────────────────────────────────┘
```

## Protocol — `src/BlackHole/Protocol/`

`BlackHoleMessage` is a **readonly struct**, 40 bytes. The hot path moves millions of these, and a
class would mean one gen-0 allocation per message.

`FrameCodec` is the only place the format is written or read. That is not tidiness — it is the fix
for the specific way v2 broke. See [protocol.md](protocol.md).

`HeaderCache` turns repeated UTF-8 header bytes back into the same `string` instance. Real traffic
reuses a tiny vocabulary — a few method names, a few dozen topics — so a direct-mapped cache keyed
on the raw bytes hits nearly always. In the demo run: **20,000 hits, 7 misses.**

## Transport — `src/BlackHole/Transport/`

`TcpTransport` is used unchanged by the dialling side and the accepting side. v2 had two
near-identical classes, each with its own copy of serialise, deserialise, and the read loop.

The read side is `System.IO.Pipelines`. The pipe owns the buffers and hands out
`ReadOnlySequence<byte>` views, so partial frames need no per-message `byte[]` and a fully buffered
frame is parsed **without a single allocation**.

Writes are serialised behind one `SemaphoreSlim`. `SendAsync` writes and flushes; `WriteAsync`
writes without flushing so a burst can be coalesced into one socket write, then `FlushAsync` once.

### One dispatcher, not an event

```csharp
public interface ITransport : IAsyncDisposable
{
    MessageDispatch? Dispatcher { get; set; }   // exactly one
    ValueTask SendAsync(BlackHoleMessage message, CancellationToken ct = default);
    ValueTask WriteAsync(BlackHoleMessage message, CancellationToken ct = default);
    ValueTask FlushAsync(CancellationToken ct = default);
    event Action<ITransport, Exception?>? Closed;
}
```

v2 exposed a multicast `OnMessageReceived` event. Every pattern object attached to a connection saw
every message and ignored what it did not own; none of them could apply backpressure, because an
event returns `void`.

v3 has exactly one `Dispatcher` returning a `ValueTask` the transport awaits. That single change is
what makes the receive path both zero-copy and correct: the transport can hold its buffer steady
until dispatch completes, so a handler may read the payload in place. Fan-out to several handlers is
the router's job, one layer up.

### Starting is a separate step

A transport can be created without its receive loop running:

```csharp
var transport = await TcpTransport.ConnectAsync(host, port, startReceiving: false);
transport.Dispatcher = router.Dispatch;   // wire first
transport.Start();                        // then let messages in
```

This exists because of a real bug. When the transport started reading inside its constructor, a
client that subscribed the instant it connected could have that subscription **silently dropped** —
the `Subscribe` arrived before the server had installed its dispatcher. It was rare when idle and
reliable under load, which is the worst kind of bug to find in production. `BlackHoleServer` and
`BlackHoleClient` both wire before starting, and
[two regression tests](../tests/BlackHole.Tests/EndToEndTests.cs) pin the behaviour in both
directions.

## Routing — `src/BlackHole/Hosting/MessageRouter.cs`

```csharp
var router = new MessageRouter();
rpcServer.AttachTo(router);
pubSubBroker.AttachTo(router);
transport.Dispatcher = router.Dispatch;
```

Lookup is an array index on the type byte. The single-handler case — which is every case in
practice — forwards without allocating a state machine. Registration is copy-on-write, so handlers
can be added while traffic flows, and a throwing handler surfaces through `HandlerFaulted` instead
of killing the connection.

## Patterns — `src/BlackHole/Patterns/`

Every pattern object follows the same shape: it takes an `ITransport`, exposes a `HandleAsync` with
the `MessageDispatch` signature, and has an `AttachTo(router)` for the common case. They are
independent — use only RPC, or only Pub/Sub, or bring your own.

See [patterns.md](patterns.md) for each one in depth.

## Hosting — `src/BlackHole/Hosting/`

`BlackHoleServer` and `BlackHoleClient` are the batteries-included layer: listener, router, and
every pattern wired together with correct lifetimes.

The lifetimes are the part worth reading:

| Object | Scope | Why |
|---|---|---|
| `RpcServer` | server-wide | A method is the same method for every client. |
| `PubSubBroker` | server-wide | Topics span connections; that is the point. |
| `MessageRouter` | per connection | So a connection can add its own handlers. |
| `StreamReceiver` | **per connection** | Two devices may both upload `firmware.bin`. Sharing one receiver would interleave them into corruption. |
| `BatchReceiver` | per connection | Unpacks into that connection's router. |

`PubSubBroker.RemoveSubscriber` is called from the disconnect handler. Without it the subscriber
list grows for the life of the process — v2's demo had exactly this leak.

## Connections are symmetric

Both ends can serve and call. `BlackHoleClient` exposes `Rpc` (methods it calls) *and* `Handlers`
(methods it serves), so a server can call a device that dialled out from behind NAT, over the socket
the device opened. The [IoT gateway](iot-gateway.md) uses this for every device command.

## Threading

- **One receive loop per connection.** Handlers run on it, in order, one message at a time.
- **Sends are serialised** behind a per-connection lock; any thread may send.
- **Handlers must not block.** Blocking the receive loop stops that connection's inbound traffic.
- **Counters are interlocked** and safe to read from anywhere, including a UI thread.

The IoT gateway shows the pattern for high-rate UI: the receive loop writes into a lock-free ring,
and a 33 ms timer publishes one coalesced update per frame. Binding straight to the receive loop
would peg the dispatcher.

---

*Built by Gravicode Studios, led by Kang Fadhil.*
