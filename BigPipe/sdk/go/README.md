# BigPipe Go SDK

Pure-Go (no cgo) client for [BigPipe](https://github.com/DotNetVibeCoderz/Vibe_Messaging/tree/main/BigPipe): batching producer, server-assigned consumer groups, share groups with dead-letter topics, SSE streaming with server-side filters, and the admin API.

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*

```bash
go get github.com/DotNetVibeCoderz/Vibe_Messaging/BigPipe/sdk/go
```

```go
import bigpipe "github.com/DotNetVibeCoderz/Vibe_Messaging/BigPipe/sdk/go"

p, _ := bigpipe.NewProducer(bigpipe.ProducerConfig{Bootstrap: []string{"localhost:9092"}, Compression: bigpipe.Zstd})
defer p.Close()
meta, err := p.Send(ctx, &bigpipe.Record{Topic: "orders", Key: []byte("order-1"), Value: []byte(`{"id":1}`)})

c, _ := bigpipe.NewConsumer(bigpipe.ConsumerConfig{Bootstrap: []string{"localhost:9092"}, Group: "shipping", Topics: []string{"orders"}})
defer c.Close()
for rec := range c.Records(ctx) {
    fmt.Println(string(rec.Key), string(rec.Value))
    c.Commit(ctx, rec)
}

records, errs := bigpipe.Stream(ctx, "payments", bigpipe.StreamOptions{Filter: `header("region") == "ID"`})

admin := bigpipe.NewAdmin("http://localhost:9644", "")
admin.CreateTopic(ctx, "clicks", 12, "diskless", nil)
admin.Migrate(ctx, "orders", "tiered")
```

`Send` is safe for concurrent use; records sent within `Linger` share one request.
Tests: `go test ./...` against a running `bigpiped`.

License: Apache-2.0.
