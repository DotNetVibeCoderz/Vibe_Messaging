# SocketSignal — Python client

*Gravicode Studios, led by Kang Fadhil.*

Bidirectional RPC over WebSockets, on asyncio.

```bash
pip install websockets
python example.py       # needs a server: dotnet run --project ../../src/SocketSignal.Demo -- serve
```

```python
import asyncio
from socketsignal import SocketSignalClient

async def main():
    client = SocketSignalClient(call_timeout=10.0, auto_reconnect=True)

    @client.on("serverHello")
    async def hello(text):
        return "python heard you"

    await client.connect("ws://localhost:8080/ws/")
    print(await client.call("sum", 5, 7))   # 12
    await client.send("log", "no reply wanted")
    await client.close()

asyncio.run(main())
```

| | |
|---|---|
| Register | `client.on("name", handler)` or `@client.on("name")`; sync or async |
| Call | `await client.call("name", *args)` |
| Fire and forget | `await client.send("name", *args)` |
| Errors | `SignalInvocationError`, `SignalTimeoutError`, `SignalClosedError` |

Full guide: [docs/clients.md](../../docs/clients.md) · Protocol: [docs/protocol.md](../../docs/protocol.md)
