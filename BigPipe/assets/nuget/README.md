# BigPipe for .NET

**BigPipe** is a high performance, Kafka-compatible realtime stream processing platform with per-topic storage: local disk, tiered, or diskless straight to object storage.

| Package | What it gives you |
|---|---|
| `BigPipe.Client` | Managed Kafka-protocol producer/consumer (idempotent, consumer groups), share groups, Schema Registry serializers, admin + HTTP clients, DI integration |
| `BigPipe.Streams` | Stream processing DSL: filter, map, group, tumbling/hopping/sliding/session windows, aggregations, stream-table joins, state stores with changelogs |
| `BigPipe.Analytics` | Realtime analytics: windows, streaming statistics, sketches (HyperLogLog, Count-Min, Top-K), anomaly detectors |
| `BigPipe.Analytics.Scripting` | Filter/query streams with C#, Python and JavaScript scripts |
| `BigPipe.Analytics.ML` | ML.NET: spike/change-point detection, SR-CNN, SSA forecasting, online classifiers |
| `BigPipe.Analytics.Torch` | TorchSharp: online autoencoder anomaly detection, TorchScript scoring |
| `BigPipe.Analytics.Gravicode` | GravicodeScience, HF.Net and MediaPipe.NET for stream analytics |

```csharp
await using var producer = new ProducerBuilder<string, Order>().WithBootstrap("localhost:9092").Build();
await producer.SendAsync("orders", "order-1", new Order(1, 150_000m, "IDR"));
```

Docs: https://github.com/DotNetVibeCoderz/Vibe_Messaging/tree/main/BigPipe/docs

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
