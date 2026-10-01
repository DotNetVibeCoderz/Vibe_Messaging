# SDK

[English](../en/sdks.md) · [Bahasa Indonesia](sdks.md)

| Bahasa | Paket | Transport | Sumber |
|---|---|---|---|
| .NET 10 | `BigPipe.Client` (NuGet) | protokol Kafka native (9092), plus admin/HTTP | `sdk/dotnet/src/BigPipe.Client` |
| Python 3.9+ | `bigpipe` (PyPI) | gateway HTTP (8082), asyncio + httpx | `sdk/python` |
| TypeScript / Node 18+ | `bigpipe-client` (npm) | gateway HTTP, `fetch` bawaan, tanpa dependensi | `sdk/typescript` |
| Go 1.22+ | `github.com/DotNetVibeCoderz/Vibe_Messaging/BigPipe/sdk/go` | gateway HTTP, hanya standard library | `sdk/go` |
| Java 17+ | `io.gravicode:bigpipe-client` (Maven) | gateway HTTP, `java.net.http` + Jackson | `sdk/java` |

Semua klien Kafka standar juga bisa dipakai (klien Java Apache Kafka, librdkafka/confluent-kafka, franz-go, kafka-python, KafkaJS). Arahkan `bootstrap.servers` ke port 9092.

SDK berbasis HTTP menerima `bootstrap: "localhost:9092"` ala Kafka, lalu memakai host yang sama di port gateway (`http://localhost:8082`). Anda juga bisa memberikan `url` secara eksplisit atau mengatur `BIGPIPE_HTTP`. Klien admin secara default memakai `http://localhost:9644` (atau `BIGPIPE_ADMIN`).

## Konsep yang sama di semua bahasa

| Konsep | .NET | Python | TypeScript | Go | Java |
|---|---|---|---|---|---|
| Producer | `ProducerBuilder<K,V>` → `SendAsync` | `Producer.send` | `Producer.send` | `NewProducer` → `Send` | `Producer.builder()` → `send` |
| Consumer group | `ConsumerBuilder<K,V>` → `ConsumeAsync` | `async for msg in Consumer` | `for await (msg of consumer)` | `c.Records(ctx)` | `consumer.poll` |
| Share group | `ShareConsumerBuilder<K,V>` | `ShareConsumer` | `ShareConsumer` | `NewShareConsumer` | `ShareConsumer.builder()` |
| Stream SSE dengan filter | `BigPipeHttpClient.StreamAsync` | `stream()` | `stream()` | `Stream()` | `BigPipeStream.subscribe` |
| Admin | `BigPipeAdminClient` | `Admin` | `Admin` | `NewAdmin` | `Admin` |
| Compact topic | `CompactAsync` | `compact()` | `compact()` | `Compact()` | `compact()` |

## .NET

```bash
dotnet add package BigPipe.Client
```

```csharp
using BigPipe.Client;

await using var producer = new ProducerBuilder<string, Order>()
    .WithBootstrap("localhost:9092")
    .WithCompression(CompressionType.Zstd)
    .WithIdempotence()
    .Build();
var meta = await producer.SendAsync("orders", "order-1", new Order(1, 150_000m, "IDR"));

await using var consumer = new ConsumerBuilder<string, Order>()
    .WithBootstrap("localhost:9092")
    .WithGroup("billing")
    .WithAutoOffsetReset(OffsetReset.Earliest)
    .Build();
consumer.Subscribe("orders");
await foreach (var msg in consumer.ConsumeAsync(ct))
{
    Console.WriteLine($"{msg.Key}: {msg.Value.Amount:N0}");
    await consumer.CommitAsync(msg);
}

public record Order(int Id, decimal Amount, string Currency);
```

Secara default value diserialisasi sebagai JSON UTF-8 (System.Text.Json). `string` dan `byte[]` diteruskan apa adanya. Pasang serializer sendiri dengan `WithValueSerializer`.

**Share group**

```csharp
await using var worker = new ShareConsumerBuilder<string, EmailJob>()
    .WithBootstrap("localhost:9092")
    .WithShareGroup("email-workers")
    .WithMaxDeliveryAttempts(3)
    .WithDeadLetterTopic("email-jobs.dlq")
    .Build();
worker.Subscribe("email-jobs");
await foreach (var job in worker.ConsumeAsync(ct))
{
    try { await Send(job.Value); job.Accept(); }
    catch (TransientException) { job.Release(); }
    catch { job.Reject(); }
}
```

**Admin, HTTP, dan dependency injection**

```csharp
var admin = new BigPipeAdminClient("http://localhost:9644");
await admin.CreateTopicAsync("clicks", 12, StorageMode.Diskless);
await admin.MigrateAsync("orders", StorageMode.Tiered);
var lag = await admin.GetGroupAsync("billing");

builder.Services.AddBigPipe(o => o.Bootstrap = "localhost:9092")
    .AddProducer<string, Order>()
    .AddConsumer<OrderHandler, string, Order>(c => c.Topic("orders").Group("billing"));

public sealed class OrderHandler : IBigPipeHandler<string, Order>
{
    public Task HandleAsync(ConsumeContext<string, Order> ctx, CancellationToken ct) { /* ... */ return Task.CompletedTask; }
}
```

`KafkaAdminClient` membuat topic lewat protokol Kafka hanya dengan alamat bootstrap (misalnya `new NewTopic("users", 6, new Dictionary<string, string> { ["cleanup.policy"] = "compact" })`), terhadap BigPipe maupun Apache Kafka.

**Schema Registry**

```csharp
var serializer = new SchemaRegistrySerializer<Order>("http://localhost:8081");  // format wire Confluent
await using var p = new ProducerBuilder<string, Order>().WithBootstrap("localhost:9092").WithValueSerializer(serializer).Build();
```

## Python

```bash
pip install bigpipe
```

```python
import asyncio
from bigpipe import Producer, Consumer, ShareConsumer, stream

async def main():
    async with Producer(bootstrap="localhost:9092") as p:
        meta = await p.send("orders", {"id": 1, "amount": 150000}, key="order-1", headers={"source": "checkout"})

    async with Consumer(bootstrap="localhost:9092", group="analytics", topics=["orders"],
                        auto_offset_reset="earliest") as c:
        async for msg in c:
            print(msg.key_str, msg.json())
            await c.commit(msg)

    async for r in stream("payments", from_="latest", filter='this.amount > 1000000'):
        print(r.json())

asyncio.run(main())
```

## TypeScript / JavaScript

```bash
npm install bigpipe-client
```

```ts
import { Producer, Consumer, ShareConsumer, stream, Admin } from "bigpipe-client";

const producer = new Producer({ bootstrap: ["localhost:9092"], compression: "zstd" });
await producer.send({ topic: "orders", key: "order-1", value: { id: 1, amount: 150000 } });

const consumer = new Consumer({ bootstrap: ["localhost:9092"], group: "notifier", topics: ["orders"], autoOffsetReset: "earliest" });
for await (const msg of consumer) {
  console.log(msg.keyString, msg.json());
  await consumer.commit(msg);
}
```

## Go

```bash
go get github.com/DotNetVibeCoderz/Vibe_Messaging/BigPipe/sdk/go
```

```go
p, _ := bigpipe.NewProducer(bigpipe.ProducerConfig{Bootstrap: []string{"localhost:9092"}, Compression: bigpipe.Zstd})
defer p.Close()
meta, err := p.Send(ctx, &bigpipe.Record{Topic: "orders", Key: []byte("order-1"), Value: []byte(`{"id":1}`)})

c, _ := bigpipe.NewConsumer(bigpipe.ConsumerConfig{Bootstrap: []string{"localhost:9092"}, Group: "shipping", Topics: []string{"orders"}})
defer c.Close()
for rec := range c.Records(ctx) {
    fmt.Println(string(rec.Key), string(rec.Value))
    c.Commit(ctx, rec)
}
```

## Java

```xml
<dependency>
  <groupId>io.gravicode</groupId>
  <artifactId>bigpipe-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

```java
try (var producer = Producer.builder().bootstrap("localhost:9092").compression("zstd").build()) {
    RecordMetadata m = producer.send("orders", "order-1", "{\"id\":1,\"amount\":150000}").join();
}
try (var consumer = Consumer.builder().bootstrap("localhost:9092").group("billing").topics("orders").earliest().build()) {
    for (BigPipeRecord r : consumer.poll(Duration.ofSeconds(1))) {
        System.out.println(r.keyString() + " " + r.json());
        consumer.commit(r);
    }
}
```

## Menjalankan tes SDK

Setiap suite membutuhkan `bigpiped` yang berjalan di port default.

```bash
cd sdk/python && pip install -e .[test] && pytest
cd sdk/typescript && npm install && npm test
cd sdk/go && go test ./...
cd sdk/java && mvn test
dotnet test sdk/dotnet/tests/BigPipe.Client.Tests   # menjalankan bigpiped sendiri di port 19092/18082/19644/19645
```

README di setiap folder `sdk/<bahasa>` berisi contoh lebih banyak.

---

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*
