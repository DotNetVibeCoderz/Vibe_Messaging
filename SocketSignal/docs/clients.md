# Client SDKs

*Gravicode Studios, led by Kang Fadhil.*

SocketSignal is JSON over a plain WebSocket, so anything that speaks WebSocket can join. Three
SDKs ship in [`clients/`](../clients), each about 300 lines and each with the same shape as the
.NET client: `on` to register, `call` to invoke and wait, `send` to fire and forget.

All three are exercised against a real .NET server in CI — the point of three client libraries is
that they all speak the same protocol.

Start a server for any of the examples below:

```bash
dotnet run --project src/SocketSignal.Demo -- serve
```

That registers `sum`, `echo`, `join` and `explode` on `ws://localhost:8080/ws/` and stays up.

---

## Python

Requires Python 3.10+ and the `websockets` package.

```bash
pip install socketsignal

# or run the example from a checkout
cd clients/python
pip install websockets
python example.py
```

```python
import asyncio
from socketsignal import SocketSignalClient, SignalInvocationError

async def main():
    client = SocketSignalClient(call_timeout=10.0, keep_alive=15.0)

    # A method the server may call on us. Sync or async, either is fine.
    @client.on("serverHello")
    async def hello(text):
        print("server said", text)
        return "python heard you"

    await client.connect("ws://localhost:8080/ws/")
    print(client.client_id)

    print(await client.call("sum", 5, 7))       # 12
    await client.send("log", "no reply wanted")

    try:
        await client.call("explode", "now")
    except SignalInvocationError as error:
        print(error.remote_message)

    await client.close()

asyncio.run(main())
```

| | |
|---|---|
| Register | `client.on("name", handler)` or `@client.on("name")` |
| Call | `await client.call("name", *args)` |
| Fire and forget | `await client.send("name", *args)` |
| Errors | `SignalInvocationError`, `SignalTimeoutError`, `SignalClosedError` |
| Reconnect | `SocketSignalClient(auto_reconnect=True)` |
| Context manager | `async with SocketSignalClient() as client:` |

---

## Node.js

No dependencies: Node 22 ships a global `WebSocket`.

```bash
npm install @gravicode/socketsignal

# or run the example from a checkout
cd clients/nodejs
node example.mjs
```

```javascript
import { SocketSignalClient, SignalInvocationError } from "@gravicode/socketsignal";

const client = new SocketSignalClient({ callTimeoutMs: 10_000 });

client.on("serverHello", (text) => {
  console.log("server said", text);
  return "node heard you";
});

client.addEventListener("disconnected", (e) => console.log("lost:", e.detail));

await client.connect("ws://localhost:8080/ws/");

console.log(await client.call("sum", 5, 7));   // 12
client.send("log", "no reply wanted");

try {
  await client.call("explode", "now");
} catch (error) {
  console.log(error.remoteMessage);
}

client.close();
```

| | |
|---|---|
| Register | `client.on("name", handler)` — async handlers are awaited |
| Call | `await client.call("name", ...args)` |
| Fire and forget | `client.send("name", ...args)` |
| Errors | `SignalInvocationError`, `SignalTimeoutError`, `SignalClosedError` |
| Events | `connected`, `disconnected`, `reconnecting` via `addEventListener` |
| Reconnect | `new SocketSignalClient({ autoReconnect: true })` |

---

## Go

Requires Go 1.22+ and `github.com/coder/websocket`.

```bash
go get github.com/DotNetVibeCoderz/Vibe_Messaging/SocketSignal/clients/go/v2/socketsignal

# or run the example from a checkout
cd clients/go
go mod tidy
go run ./example
```

```go
client := socketsignal.New(socketsignal.Options{
    CallTimeout: 10 * time.Second,
    OnDisconnected: func(reason string) { log.Println("lost:", reason) },
})

// Arguments arrive as raw JSON so the handler decodes only what it needs.
client.On("serverHello", func(args []json.RawMessage) (any, error) {
    var text string
    _ = json.Unmarshal(args[0], &text)
    return "go heard " + text, nil
})

if err := client.Connect(ctx, "ws://localhost:8080/ws/"); err != nil {
    log.Fatal(err)
}
defer client.Close()

var total int
if err := client.Call(ctx, &total, "sum", 5, 7); err != nil {
    log.Fatal(err)
}

_ = client.Send(ctx, "log", "no reply wanted")
```

| | |
|---|---|
| Register | `client.On("name", func(args []json.RawMessage) (any, error))` |
| Call | `client.Call(ctx, &result, "name", args...)` — pass `nil` to ignore the result |
| Fire and forget | `client.Send(ctx, "name", args...)` |
| Errors | `*InvocationError`, `*TimeoutError`, `ErrClosed` |

Returning a non-nil `error` from a handler sends that message back to the caller.

---

## The browser

No SDK. The protocol is small enough to write inline:

```html
<script>
const ws = new WebSocket("ws://localhost:8080/ws/");
const pending = new Map();
let nextId = 0;

ws.onmessage = (ev) => {
  const msg = JSON.parse(ev.data);

  if (msg.type === "welcome") {
    console.log("connected as", msg.id);

  } else if (msg.type === "result") {
    const call = pending.get(msg.id);
    if (!call) return;
    pending.delete(msg.id);
    msg.error ? call.reject(new Error(msg.error)) : call.resolve(msg.result);

  } else if (msg.type === "invoke") {
    // A method the server called on us.
    const result = msg.method === "serverHello" ? "hello from the browser" : null;
    if (msg.expectReturn) ws.send(JSON.stringify({ type: "result", id: msg.id, result }));

  } else if (msg.type === "ping") {
    ws.send(JSON.stringify({ type: "pong", id: msg.id }));
  }
};

function call(method, ...args) {
  const id = String(++nextId);
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ type: "invoke", id, method, args, expectReturn: true }));
  });
}

// await call("sum", 5, 7)  ->  12
</script>
```

The one thing to get right is answering `ping` with `pong`: without it the server's idle timeout
will eventually drop a browser that is only listening.

---

## Writing an SDK for another language

Four things, and you have a complete client:

1. Open the WebSocket, read `welcome`, keep the id.
2. Keep `id -> pending call`. On `result`, resolve with `result` or reject with `error`.
3. On `invoke`, find a local handler. **If `expectReturn` is set, always answer** — with `result`,
   or with `error` when the handler threw or does not exist.
4. Answer `ping` with `pong`.

Ids need only be unique within your connection and direction; a counter is enough. The full wire
format is in [protocol.md](protocol.md), and the three SDKs are each a working reference.
