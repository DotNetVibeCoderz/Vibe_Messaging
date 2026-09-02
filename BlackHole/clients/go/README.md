# BlackHole Messaging — Go client

*Gravicode Studios, led by Kang Fadhil.*

Go client for the BlackHole binary protocol: **RPC**, **Pub/Sub**, **Streaming** and **Batching**
over TCP. Speaks the same wire format as the [.NET library](../../README.md), verified against it by
the interop suite.

Requires Go 1.22+. No dependencies outside the standard library.

## Install

```bash
go get github.com/DotNetVibeCoderz/Vibe_Messaging/BlackHole/clients/go
```

```go
import bh "github.com/DotNetVibeCoderz/Vibe_Messaging/BlackHole/clients/go/blackhole"
```

## Transports

TCP everywhere, plus Unix domain sockets on Linux, macOS, and Windows 10 build 17063 or later:

```go
client, err := bh.Connect(ctx, "127.0.0.1:5000", nil)              // TCP
client, err := bh.ConnectUnix(ctx, "/tmp/blackhole.sock", nil)     // Unix domain socket
```

Both carry the same wire format; only the connection setup differs. Named pipes would need a
third-party package on Windows, and shared memory needs a mapped segment plus a dedicated polling
thread — both are .NET-only. See [docs/transports.md](../../docs/transports.md).

Compare them yourself:

```bash
go run ./example/benchmark
```

## Thirty seconds

```go
ctx := context.Background()

client, err := bh.Connect(ctx, "127.0.0.1:5000", nil)
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// RPC
shouted, _ := client.CallText(ctx, "upper", "halo blackhole")   // HALO BLACKHOLE

// Pub/Sub, with MQTT-style wildcards
client.Subscribe("sensor/+/temperature", func(topic string, payload []byte) {
    fmt.Println(topic, string(payload))
})
client.PublishText("sensor/tank-3/temperature", "28.4")
```

## RPC

```go
result, err := client.Call(ctx, "echo", []byte("bytes"))
text, err := client.CallText(ctx, "upper", "halo")
err := client.Notify("log", []byte("fire and forget"))
```

Every call has a deadline — `Options.CallTimeout` defaults to 30 seconds, and the context bounds it
too. Failures return an `*RPCError` rather than hanging:

```go
if _, err := client.Call(ctx, "risky", payload); err != nil {
    var rpcErr *bh.RPCError
    if errors.As(err, &rpcErr) {
        // The handler failed, the method is unknown, the deadline passed,
        // or the connection dropped mid-call.
        log.Printf("%s: %s", rpcErr.Method, rpcErr.Reason)
    }
}
```

Serve methods the peer may call on you. Handlers run off the read loop, so they may block or call
back:

```go
client.RegisterText("device/status", func(string) string { return "ok: 4 sensors online" })

client.Register("device/read", func(ctx context.Context, request bh.Message) ([]byte, error) {
    return readSensor(ctx, request.Text())
})
```

## Pub/Sub

`+` matches one segment, `#` matches the remainder.

```go
client.Subscribe("sensor/+/temperature", onReading)   // per-filter handler
client.Subscribe("alarm/#", onAlarm)
client.OnPublish(func(topic string, payload []byte) { ... })   // everything

client.PublishText("sensor/tank-3/temperature", "28.4")
client.Unsubscribe("alarm/#")
```

## Streaming

```go
file, _ := os.Open("firmware.bin")
defer file.Close()
info, _ := file.Stat()

sent, err := client.SendStream(ctx, "firmware-2026", file,
    bh.StreamDescriptor{Name: "firmware.bin", TotalLength: info.Size()},
    16*1024,
    func(sent int64) { log.Printf("%d KiB", sent/1024) },
)

client.OnStream(func(streamID string, d bh.StreamDescriptor, data []byte) { save(streamID, data) })
```

Chunks accumulate and are written once per `Options.FlushThreshold` (64 KiB) rather than once per
chunk, so a small chunk size does not mean a small write.

## Batching

```go
messages := make([]bh.Message, 1000)
for i := range messages {
    messages[i] = bh.Message{
        Type:    bh.TypePublish,
        Header:  fmt.Sprintf("log/entry/%d", i),
        Payload: []byte(fmt.Sprintf("line %d", i)),
    }
}
client.SendBatch(messages)
```

One frame, one socket write. The envelope holds complete BlackHole frames, so the peer unpacks it
with the same decoder and each message routes individually.

## Wire your handlers before the read loop starts

`Options.Configure` runs after the client is built but **before** anything is delivered. A server
that pushes the instant it accepts would otherwise beat a handler registered after `Connect`
returns:

```go
client, err := bh.Connect(ctx, address, &bh.Options{
    Configure: func(c *bh.Client) {
        c.RegisterText("client/identify", func(q string) string { return "tank-3" })
    },
})
```

## The one rule

`DecodeFrame` returns a payload that **aliases the read buffer**, and handlers run on the read
goroutine. Copy anything you keep:

```go
client.OnPublish(func(topic string, payload []byte) {
    queue <- append([]byte(nil), payload...)
})
```

`Call` already copies its result, so the bytes it returns are yours.

## Connection

```go
elapsed, _ := client.Ping(ctx)             // one probe
average, _ := client.PingAverage(ctx, 50)  // averaged over 50
client.Stats                                // messages and bytes, both directions
<-client.Done()                             // closed when the connection ends
client.Err()                                // why it ended, nil on a clean close
```

**Prefer `PingAverage` on Windows.** `time.Now` there resolves to roughly 500 µs, which is coarser
than a loopback round trip, so a single local probe can legitimately return 0. Averaging over many
probes gives a figure the clock can actually represent.

## Performance

Codec only, measured on this machine (Go 1.27, Windows, 8 cores):

| | Time | Allocations |
|---|---:|---:|
| `EncodeFrame` | 33 ns | 1 (48 B) — the frame buffer |
| `DecodeFrame` | 40 ns | 1 (32 B) — the header string |

Not the zero-allocation figure the .NET library reports: that one parses in place out of pooled
pipeline buffers, while this returns an ordinary `[]byte` and a `string`. One small allocation per
frame is the honest cost of an idiomatic Go API. Reproduce with:

```bash
go test ./blackhole/ -short -bench=. -benchmem -run=XXX
```

## Testing

```bash
go test ./blackhole/ -count=1          # 30 tests, starts the .NET server
go test ./blackhole/ -short            # codec only, no .NET needed
go test ./blackhole/ -bench=. -short   # codec benchmarks
```

The interop suite starts the real .NET server and asserts against it. See
[../README.md](../README.md).

## Example

```bash
dotnet run --project ../../tests/BlackHole.InteropServer -- --port 5000
go run ./example -addr 127.0.0.1:5000
```

---

*Built by Gravicode Studios, led by Kang Fadhil.*
