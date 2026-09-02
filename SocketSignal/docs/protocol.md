# The SocketSignal protocol

*Gravicode Studios, led by Kang Fadhil.*

Everything SocketSignal does crosses the wire as **one JSON object per WebSocket text message**.
There is no framing layer, no handshake beyond the WebSocket upgrade, and no binary encoding —
which is the point: a browser can join in with `JSON.stringify`, and you can read a capture in
DevTools without a decoder.

Protocol version: **2**.

## The envelope

Every frame is an object. Only `type` is always present.

| Field | Type | Meaning |
|---|---|---|
| `type` | string | `welcome`, `invoke`, `result`, `ping`, `pong` |
| `id` | string | Correlation id. Echoed verbatim in the reply |
| `method` | string | Method name (`invoke` only) |
| `args` | array | Positional arguments (`invoke` only) |
| `expectReturn` | bool | Whether a `result` is owed (`invoke` only) |
| `result` | any | The return value (`result` only) |
| `error` | string | The failure message (`result` only) |

Two rules keep the protocol able to grow:

- **Unknown fields are skipped.** A peer may add fields; older peers ignore them.
- **Unknown `type` values are ignored, not fatal.** They still count as liveness.

The .NET implementation writes `id` as a string of digits, but accepts a JSON number too, so a
hand-rolled client that sends `"id": 42` still works.

## Frames

### `welcome` — server to client, once

Sent immediately on connect, before anything else. Until it arrives the client has no id.

```json
{ "type": "welcome", "id": "8f1c...", "protocol": 2, "server": "demo-station" }
```

| Field | Meaning |
|---|---|
| `id` | The connection id the server assigned. Used for direct sends and group membership |
| `protocol` | Protocol version |
| `server` | The server's `Name`, for logs and diagnostics |

### `invoke` — either direction

A method call. It travels the same way whichever end sends it.

```json
{ "type": "invoke", "id": "7", "method": "sum", "args": [5, 7], "expectReturn": true }
```

`expectReturn` decides whether the receiver owes a `result`:

- `true` — the receiver **must** answer with a `result` carrying the same `id`, whether the
  handler succeeded, threw, or does not exist.
- `false` or absent — fire and forget. The receiver answers nothing, even on failure.

`args` is positional. Names are not part of the protocol; a handler that wants named arguments
takes a single object argument.

### `result` — the reply to an `invoke`

```json
{ "type": "result", "id": "7", "result": 12 }
{ "type": "result", "id": "7", "error": "reactor offline" }
```

Exactly one of `result` and `error` is meaningful. `error` is a message, never a stack trace —
nothing about the remote process's internals crosses the wire.

A receiver that has no handler for the method answers:

```json
{ "type": "result", "id": "7", "error": "Method 'sum' not found" }
```

The .NET client turns an `error` ending in `not found` into `MethodNotFoundException` and
everything else into `SignalInvocationException`.

### `ping` / `pong` — keepalive, either direction

```json
{ "type": "ping", "id": "12" }
{ "type": "pong", "id": "12" }
```

A `ping` is answered with a `pong` echoing its `id`, and nothing else happens. Both count as
activity, which is what the idle timer watches.

SocketSignal deliberately does **not** use WebSocket's own ping frames: browsers handle those
transparently and JavaScript never sees them, so a browser client could not participate in
liveness. Doing it at the protocol level means every SDK sees the same signal. Both the .NET
server and client set `KeepAliveInterval = TimeSpan.Zero` on the underlying socket for this reason.

## A conversation, start to finish

```
client                                  server
  |                                        |
  |------------- WebSocket upgrade ------->|
  |<-- {"type":"welcome","id":"8f1c..."}---|
  |                                        |
  |-- {"type":"invoke","id":"1",           |
  |    "method":"sum","args":[5,7],        |
  |    "expectReturn":true} -------------->|
  |<- {"type":"result","id":"1",           |
  |    "result":12} -----------------------|
  |                                        |
  |-- {"type":"invoke","id":"2",           |   fire and forget:
  |    "method":"log","args":["hi"]} ----->|   no reply owed
  |                                        |
  |<- {"type":"invoke","id":"a1",          |   server calls the client
  |    "method":"tick","args":[3],         |
  |    "expectReturn":true} ---------------|
  |-- {"type":"result","id":"a1",          |
  |    "result":true} -------------------->|
  |                                        |
  |<- {"type":"ping","id":"9"} ------------|
  |-- {"type":"pong","id":"9"} ----------->|
```

## Correlation ids

Ids only have to be unique **within one connection and one direction**. Each peer keeps its own
pending-call table, so both ends may independently use `"1"`, `"2"`, `"3"` without colliding: a
`result` is matched against the table of the peer that sent the matching `invoke`.

The .NET implementation uses a monotonically increasing counter formatted straight into the frame,
rather than a GUID. That is worth 88 bytes and about 118 ns per call — see
[performance.md](performance.md).

## Groups

Groups are **server-side only**. There is no join frame in the protocol: a client asks to be added
by calling a method the application registered for that purpose, and the handler calls
`client.JoinGroup(...)`. This is deliberate — group membership is an authorisation decision, and
letting a client put itself into any group by sending a frame would be a hole.

```csharp
server.Register<string, bool>("join", (client, group) =>
{
    if (!IsAllowed(client, group)) throw new InvalidOperationException("not permitted");
    client.JoinGroup(group!);
    return ValueTask.FromResult(true);
});
```

Membership is dropped automatically when the connection closes.

## Writing your own client

A minimal client needs to do four things:

1. Open the WebSocket and read the `welcome` frame to learn its id.
2. Keep a map of `id -> pending call`. On a `result`, complete or fail the matching entry.
3. On an `invoke`, look up a local handler; if `expectReturn` is set, always answer — with
   `result`, or with `error` when the handler threw or is missing.
4. Answer `ping` with `pong`.

That is the whole protocol. The three SDKs in [`clients/`](../clients) are each about 300 lines
and implement exactly this. The browser example in the [README](../README.md) is ten.

## Limits and framing

A frame is one whole WebSocket message; the .NET side accepts fragmented messages and reassembles
them. `SocketSignalOptions.MaxMessageSize` (4 MB by default) caps the total — a peer that exceeds
it has its connection closed rather than being allowed to exhaust memory.

Text and binary WebSocket message types are both accepted on receive; SocketSignal always sends
text.
