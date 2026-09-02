# Architecture

*Gravicode Studios, led by Kang Fadhil.*

The library is about 1,500 lines. This page is the map.

```
src/SocketSignal/
├── Protocol/
│   ├── MessageType.cs        the type discriminator
│   ├── SignalFrame.cs        decode: a ref struct view over the receive buffer
│   └── SignalWriter.cs       encode: straight into a caller-owned buffer
├── Dispatch/
│   ├── Utf8HandlerTable.cs   method name (UTF-8) -> handler, without a string
│   └── HandlerEntry.cs       the typed and untyped handler shapes
├── Buffers/
│   └── PooledBufferWriter.cs a growable IBufferWriter over ArrayPool
├── Hosting/
│   ├── SignalConnection.cs   the pump: one WebSocket, both directions
│   ├── PendingCall.cs        a call this peer issued and is waiting on
│   ├── SocketSignalServer.cs accept loop, groups, fan-out, keepalive sweep
│   ├── ClientConnection.cs   one connected client, as the server sees it
│   └── SocketSignalClient.cs dial, reconnect, call
├── Diagnostics/SignalStatistics.cs
├── SocketSignalOptions.cs
└── Exceptions.cs
```

## The one idea worth knowing

**The protocol is symmetric, so the implementation is too.** An `invoke` looks the same whichever
end sends it, and both ends need the same four things: a receive loop, a handler table, a
pending-call table, and a serialised send path.

`SignalConnection` is all four. It is what a `SocketSignalClient` wraps, and it is also what sits
behind every server-side `ClientConnection`. The client and the server differ only in how they
*obtain* a socket — one dials, the other accepts — and in what handlers receive as their first
parameter.

v1 had this loop written out twice, once in the server and once in the client, which is why the
two copies had drifted: the server could correlate a reply from a client, but the API to use it
was `internal` and unreachable, so server-to-client calls could never actually return a value.
Sharing the loop is what made `CallClientAsync` fall out for free.

## A frame's journey

**Inbound**, `SignalConnection.RunAsync`:

1. `ReceiveFrameAsync` reads one whole WebSocket message into the pooled receive buffer, growing
   it in place if needed and refusing anything over `MaxMessageSize`.
2. `SignalFrame.TryParse` decodes the envelope in place. No allocation: every field is a slice.
3. `DispatchAsync` switches on `Type`:
   - **`invoke`** → `Utf8HandlerTable.Find` with the raw method bytes. Miss with `expectReturn`
     set answers a not-found error. Hit copies the id and raw args into a pooled `Invocation`
     (so the receive buffer can be reused), waits for a slot on the invocation gate, and starts
     the handler off the pump.
   - **`result`** → parse the id back to a `long`, pull the `PendingCall` out of the table, and
     complete it by deserialising the raw result straight into `TResult`.
   - **`ping`** → reply `pong`.
   - **`welcome`** → raise `Welcomed`, which is what `ConnectAsync` is waiting on.
4. Anything else is ignored, having already counted as liveness.

**Outbound**, every send:

1. Take the send lock.
2. `Begin()` rewinds the pooled buffer and the `Utf8JsonWriter`.
3. `SignalWriter` writes the frame as UTF-8 into that buffer.
4. `WebSocket.SendAsync` on the written memory.
5. Release.

Encoding inside the lock is deliberate: it is what allows one buffer and one writer per connection
rather than one per frame, and it is what stops two concurrent sends from interleaving bytes on
the socket.

## Where the threads are

| | |
|---|---|
| **Accept loop** | One task per server, in `StartAsync`. Accepts and hands off; never blocks on a client |
| **Receive pump** | One task per connection. Reads, decodes, dispatches. Never runs a handler to completion |
| **Handlers** | Run off the pump, up to `MaxConcurrentInvocations` at once per connection |
| **Keepalive** | One task per *server*, sweeping all connections; one per client |

The keepalive being one loop for the whole server rather than a timer per connection is what lets
a server hold many idle connections cheaply.

## Backpressure

The invocation gate is the only place the pump blocks. When `MaxConcurrentInvocations` handlers
are already running on a connection, the pump stops reading it; the OS receive buffer fills, the
TCP window closes, and the peer is throttled at the transport. There is no unbounded queue
anywhere, which is the property that matters: a flooding peer slows down instead of growing the
server's heap.

## Lifetime and failure

Every failure path leads to `CloseCoreAsync`, which runs exactly once per connection and:

1. Fails every pending call with `SignalConnectionClosedException` — no caller is left hanging.
2. Sends a close frame if the socket is still open, best-effort.
3. Raises `Closed`, which the server uses to drop the client from its registry and every group,
   and the client uses to fire `Disconnected` and start reconnecting if asked to.

Pooled buffers go back to the pool in `DisposeAsync`.

## Design decisions worth stating

**Groups are server-side only.** There is no join frame. A client asks to be added by calling a
method the application registered, so membership stays an authorisation decision instead of
something a client can grant itself.

**Keepalive is at the protocol level, not the WebSocket's.** Browsers answer WebSocket pings
transparently and JavaScript never sees them, so a browser client could not take part in liveness.
Both ends set the socket's own `KeepAliveInterval` to zero and use `ping`/`pong` frames instead.

**The untyped handler shape stayed.** `Register(string, Func<ClientConnection, JsonElement[], Task<object?>>)`
is v1's signature. It costs a `JsonDocument` per call, which is exactly why the typed overloads
exist — but code written against v1 still compiles.

**`SignalFrame` is a `ref struct` on purpose.** It cannot be stored in a field, captured by a
lambda, or held across an `await`, so the compiler enforces the rule that would otherwise be a
comment: the receive buffer is reused, and anything outliving the dispatch call must be copied.

## Testing

`tests/SocketSignal.Tests` has two halves:

- **`ProtocolTests`** — the codec in isolation: parsing, writing, round-tripping, unknown fields,
  junk input, the handler table past a rebuild, the pooled writer growing.
- **`EndToEndTests`** — a real `HttpListener` on a free loopback port and a real WebSocket against
  it, because the interesting failures in a library like this are timing and lifetime failures.
  Timeouts, dropped sockets mid-call, concurrent handlers, group isolation, and the
  server-to-client call that v1 could not make are each covered.

```bash
dotnet test tests/SocketSignal.Tests/SocketSignal.Tests.csproj
```
