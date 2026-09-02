# SocketSignal — Node.js client

*Gravicode Studios, led by Kang Fadhil.*

Bidirectional RPC over WebSockets. No dependencies: Node 22 ships a global `WebSocket`.

```bash
node example.mjs        # needs a server: dotnet run --project ../../src/SocketSignal.Demo -- serve
```

```javascript
import { SocketSignalClient } from "socketsignal";

const client = new SocketSignalClient({ callTimeoutMs: 10_000, autoReconnect: true });

client.on("serverHello", (text) => "node heard you");

await client.connect("ws://localhost:8080/ws/");
console.log(await client.call("sum", 5, 7));   // 12
client.send("log", "no reply wanted");
client.close();
```

| | |
|---|---|
| Register | `client.on("name", handler)` — async handlers are awaited |
| Call | `await client.call("name", ...args)` |
| Fire and forget | `client.send("name", ...args)` |
| Errors | `SignalInvocationError`, `SignalTimeoutError`, `SignalClosedError` |
| Events | `connected`, `disconnected`, `reconnecting` via `addEventListener` |

Full guide: [docs/clients.md](../../docs/clients.md) · Protocol: [docs/protocol.md](../../docs/protocol.md)
