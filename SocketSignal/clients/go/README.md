# SocketSignal — Go client

*Gravicode Studios, led by Kang Fadhil.*

Bidirectional RPC over WebSockets.

```bash
go get github.com/DotNetVibeCoderz/Vibe_Messaging/SocketSignal/clients/go/v2/socketsignal

# run the example from a checkout
go mod tidy
go run ./example        # needs a server: dotnet run --project ../../src/SocketSignal.Demo -- serve
```

```go
client := socketsignal.New(socketsignal.Options{CallTimeout: 10 * time.Second})

client.On("serverHello", func(args []json.RawMessage) (any, error) {
    return "go heard you", nil
})

if err := client.Connect(ctx, "ws://localhost:8080/ws/"); err != nil { log.Fatal(err) }
defer client.Close()

var total int
_ = client.Call(ctx, &total, "sum", 5, 7)   // 12
_ = client.Send(ctx, "log", "no reply wanted")
```

| | |
|---|---|
| Register | `client.On("name", func(args []json.RawMessage) (any, error))` |
| Call | `client.Call(ctx, &result, "name", args...)` — `nil` result ignores the reply |
| Fire and forget | `client.Send(ctx, "name", args...)` |
| Errors | `*InvocationError`, `*TimeoutError`, `ErrClosed` |

Requires `github.com/coder/websocket`.

Full guide: [docs/clients.md](../../docs/clients.md) · Protocol: [docs/protocol.md](../../docs/protocol.md)
