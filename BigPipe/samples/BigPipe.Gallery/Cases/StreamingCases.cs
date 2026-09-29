using System.Diagnostics;
using System.Text.Json;
using BigPipe.Client;
using BigPipe.Client.Admin;

namespace BigPipe.Gallery.Cases;

public sealed class HelloCase : GalleryCase
{
    public override string Category => "Getting started";
    public override string Title => "Produce and consume";
    public override string Summary => "Send JSON orders with an idempotent, compressed producer and read them back as a consumer group — plain Kafka protocol on port 9092.";
    public override IReadOnlyList<string> Highlights => ["Keyed records land on the same partition as Java's default partitioner", "Consumer groups commit offsets", "Works with any Kafka client too"];

    public override string Code => """
        await using var producer = new ProducerBuilder<string, Order>()
            .WithBootstrap("localhost:9092")
            .WithCompression(CompressionType.Zstd)
            .WithIdempotence()
            .Build();

        for (var i = 1; i <= 10; i++)
        {
            var r = await producer.SendAsync("orders", $"order-{i}", new Order(i, i * 25_000m, "IDR"));
            Console.WriteLine($"order-{i} -> {r.Partition}@{r.Offset}");
        }

        await using var consumer = new ConsumerBuilder<string, Order>()
            .WithBootstrap("localhost:9092")
            .WithGroup("billing")
            .WithAutoOffsetReset(OffsetReset.Earliest)
            .Build();
        consumer.Subscribe("orders");

        await foreach (var msg in consumer.ConsumeAsync(ct))
        {
            Console.WriteLine($"{msg.Key}: {msg.Value.Amount:N0} {msg.Value.Currency}");
            await consumer.CommitAsync(msg);
        }

        public record Order(int Id, decimal Amount, string Currency);
        """;

    public override IReadOnlyList<Snippet> OtherLanguages =>
    [
        new("Python", """
            async with Producer(bootstrap="localhost:9092") as p:
                meta = await p.send("orders", {"id": 1, "amount": 25000}, key="order-1")
            async with Consumer(bootstrap="localhost:9092", group="billing", topics=["orders"],
                                auto_offset_reset="earliest") as c:
                async for msg in c:
                    print(msg.key_str, msg.json())
            """),
        new("TypeScript", """
            const producer = new Producer({ bootstrap: ["localhost:9092"] });
            await producer.send({ topic: "orders", key: "order-1", value: { id: 1, amount: 25000 } });
            const consumer = new Consumer({ bootstrap: ["localhost:9092"], group: "billing", topics: ["orders"] });
            for await (const msg of consumer) console.log(msg.keyString, msg.json());
            """),
        new("Go", """
            p, _ := bigpipe.NewProducer(bigpipe.ProducerConfig{Bootstrap: []string{"localhost:9092"}})
            meta, _ := p.Send(ctx, &bigpipe.Record{Topic: "orders", Key: []byte("order-1"), Value: []byte(`{"id":1}`)})
            c, _ := bigpipe.NewConsumer(bigpipe.ConsumerConfig{Bootstrap: []string{"localhost:9092"}, Group: "billing", Topics: []string{"orders"}})
            for rec := range c.Records(ctx) { fmt.Println(string(rec.Value)) }
            """),
        new("Java", """
            try (var producer = Producer.builder().bootstrap("localhost:9092").build()) {
                producer.send("orders", "order-1", "{\"id\":1}").join();
            }
            try (var consumer = Consumer.builder().bootstrap("localhost:9092").group("billing").topics("orders").earliest().build()) {
                consumer.poll(Duration.ofSeconds(1)).forEach(r -> System.out.println(r.json()));
            }
            """),
    ];

    public sealed record Order(int Id, decimal Amount, string Currency);

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = ctx.Unique("orders");
        await ctx.Admin.CreateTopicAsync(topic, 3, ct: ct);
        ctx.Log($"created topic {topic} (3 partitions, local)");
        await using (var producer = new ProducerBuilder<string, Order>().WithBootstrap(ctx.Endpoints.Bootstrap)
                         .WithCompression(CompressionType.Zstd).WithIdempotence().Build())
        {
            for (var i = 1; i <= 10; i++)
            {
                var sw = Stopwatch.StartNew();
                var r = await producer.SendAsync(topic, $"order-{i}", new Order(i, i * 25_000m, "IDR"), ct: ct);
                ctx.Log($"order-{i} -> partition {r.Partition} offset {r.Offset}  ({sw.Elapsed.TotalMilliseconds:0.0} ms)");
            }
        }
        await using var consumer = new ConsumerBuilder<string, Order>().WithBootstrap(ctx.Endpoints.Bootstrap)
            .WithGroup(ctx.Unique("billing")).WithAutoOffsetReset(OffsetReset.Earliest).Build();
        consumer.Subscribe(topic);
        var n = 0;
        decimal total = 0;
        await foreach (var msg in consumer.ConsumeAsync(ct))
        {
            ctx.Log($"consumed {msg.Key}: {msg.Value.Amount:N0} {msg.Value.Currency}  [p{msg.Partition}@{msg.Offset}]");
            total += msg.Value.Amount;
            await consumer.CommitAsync(msg, ct);
            if (++n == 10) break;
        }
        ctx.Metric("records", n);
        ctx.Metric("total IDR", (double)total);
        ctx.Log($"done: {n} orders, total {total:N0} IDR, offsets committed");
    }
}

public sealed class StorageModesCase : GalleryCase
{
    public override string Category => "Storage";
    public override string Title => "Local, tiered and diskless";
    public override string Summary => "The same workload on the three storage modes. Local and tiered answer in milliseconds; diskless batches into object storage and trades latency for cost.";
    public override IReadOnlyList<string> Highlights => ["Storage is chosen per topic", "Diskless writes go straight to object storage (S3, GCS, Azure Blob, MinIO or disk)", "Offsets and clients are identical across modes"];

    public override string Code => """
        await admin.CreateTopicAsync("payments", 6, StorageMode.Local);
        await admin.CreateTopicAsync("audit-log", 2, StorageMode.Tiered,
            new Dictionary<string, string> { ["local.retention.ms"] = "3600000" });
        await admin.CreateTopicAsync("clickstream", 12, StorageMode.Diskless);

        // Same producer code for all three:
        var r = await producer.SendAsync("clickstream", userId, click);
        """;

    public override IReadOnlyList<Snippet> OtherLanguages =>
    [
        new("bpctl", """
            bpctl topic create payments    --partitions 6  --mode local
            bpctl topic create audit-log   --partitions 2  --mode tiered -c local.retention.ms=3600000
            bpctl topic create clickstream --partitions 12 --mode diskless
            bpctl topic list
            """),
    ];

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        await using var producer = new ProducerBuilder<string, string>().WithBootstrap(ctx.Endpoints.Bootstrap).WithLinger(TimeSpan.Zero).Build();
        foreach (var mode in new[] { StorageMode.Local, StorageMode.Tiered, StorageMode.Diskless })
        {
            var topic = ctx.Unique(mode.ToString().ToLowerInvariant());
            await ctx.Admin.CreateTopicAsync(topic, 3, mode, ct: ct);
            var lat = new List<double>();
            for (var i = 0; i < 20; i++)
            {
                var sw = Stopwatch.StartNew();
                await producer.SendAsync(topic, $"k{i}", $$"""{"n":{{i}}}""", ct: ct);
                lat.Add(sw.Elapsed.TotalMilliseconds);
            }
            lat.Sort();
            var info = await ctx.Admin.GetTopicAsync(topic, ct);
            ctx.Log($"{mode,-9} p50 {lat[lat.Count / 2],7:0.0} ms   max {lat[^1],7:0.0} ms   local {info.Bytes.Local} B   object {info.Bytes.Remote + info.Bytes.Diskless} B");
            ctx.Metric($"{mode.ToString().ToLowerInvariant()} p50", lat[lat.Count / 2], "ms");
        }
        ctx.Log("diskless latency is the object-storage PUT plus the agent's batching window (diskless_linger_ms).");
    }
}

public sealed class MigrationCase : GalleryCase
{
    public override string Category => "Storage";
    public override string Title => "Online storage migration";
    public override string Summary => "Move a live topic from local disk to diskless and back. New data follows the new mode immediately; every offset stays exactly where it was.";
    public override IReadOnlyList<string> Highlights => ["No downtime, no client changes", "Offsets are contiguous across the switch", "Readers transparently read both locations"];

    public override string Code => """
        var result = await admin.MigrateAsync("orders", StorageMode.Diskless);
        // result.SwitchOffsets: offsets below stay local, new data goes to object storage

        await admin.MigrateAsync("orders", StorageMode.Local);   // and back, any time
        """;

    public override IReadOnlyList<Snippet> OtherLanguages =>
    [
        new("bpctl", "bpctl topic migrate orders --to diskless"),
        new("Python", "await admin.migrate(\"orders\", \"diskless\")"),
    ];

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = ctx.Unique("migrate");
        await ctx.Admin.CreateTopicAsync(topic, 1, ct: ct);
        await using var producer = new ProducerBuilder<Null, string>().WithBootstrap(ctx.Endpoints.Bootstrap).Build();
        async Task Write(string phase)
        {
            await Task.WhenAll(Enumerable.Range(0, 50).Select(i => producer.SendAsync(topic, default, $"{phase}-{i}", ct: ct)));
            var t = await ctx.Admin.GetTopicAsync(topic, ct);
            ctx.Log($"{phase,-9} wrote 50 → records {t.Records,4}  local {t.Bytes.Local,6} B  diskless {t.Bytes.Diskless,6} B");
        }
        await Write("local");
        var m = await ctx.Admin.MigrateAsync(topic, StorageMode.Diskless, ct);
        ctx.Log($"migrated to diskless at offset {m.SwitchOffsets["0"]}");
        await Write("diskless");
        await ctx.Admin.MigrateAsync(topic, StorageMode.Local, ct);
        ctx.Log("migrated back to local");
        await Write("local-2");

        await using var consumer = new ConsumerBuilder<Null, string>().WithBootstrap(ctx.Endpoints.Bootstrap).Build();
        consumer.Assign(new TopicPartitionOffset(topic, 0, 0));
        var expected = 0L;
        await foreach (var r in consumer.ConsumeAsync(ct))
        {
            if (r.Offset != expected) throw new InvalidOperationException($"gap at {expected}");
            expected++;
            if (expected == 150) break;
        }
        ctx.Log("read offsets 0..149 in order across local → diskless → local ✔");
        ctx.Metric("contiguous offsets", expected);
    }
}

public sealed class GroupsCase : GalleryCase
{
    public override string Category => "Consumption";
    public override string Title => "Consumer groups and lag";
    public override string Summary => "Two consumers join a group and split six partitions; the admin API reports committed offsets and lag per partition.";
    public override IReadOnlyList<string> Highlights => ["Classic Kafka group protocol (range / round-robin)", "Lag = end offset − committed offset", "Rebalances when members join or leave"];

    public override string Code => """
        var a = new ConsumerBuilder<string, string>().WithBootstrap(bootstrap).WithGroup("fraud").Build();
        var b = new ConsumerBuilder<string, string>().WithBootstrap(bootstrap).WithGroup("fraud").Build();
        a.PartitionsAssigned += p => Console.WriteLine($"A got {string.Join(",", p)}");
        b.PartitionsAssigned += p => Console.WriteLine($"B got {string.Join(",", p)}");
        a.Subscribe("payments");
        b.Subscribe("payments");

        var group = await admin.GetGroupAsync("fraud");   // members, offsets, lag
        """;

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = ctx.Unique("payments");
        var group = ctx.Unique("fraud");
        await ctx.Admin.CreateTopicAsync(topic, 6, ct: ct);
        await using (var p = new ProducerBuilder<string, string>().WithBootstrap(ctx.Endpoints.Bootstrap).Build())
            await Task.WhenAll(Enumerable.Range(0, 600).Select(i => p.SendAsync(topic, $"tx-{i}", $$"""{"amount":{{i * 1000}}}""", ct: ct)));
        ctx.Log("produced 600 payments across 6 partitions");
        var a = new ConsumerBuilder<string, string>().WithBootstrap(ctx.Endpoints.Bootstrap).WithGroup(group).WithAutoOffsetReset(OffsetReset.Earliest).WithAutoCommit(false).Build();
        var b = new ConsumerBuilder<string, string>().WithBootstrap(ctx.Endpoints.Bootstrap).WithGroup(group).WithAutoOffsetReset(OffsetReset.Earliest).WithAutoCommit(false).Build();
        a.PartitionsAssigned += ps => ctx.Log($"consumer A assigned {string.Join(", ", ps)}");
        b.PartitionsAssigned += ps => ctx.Log($"consumer B assigned {string.Join(", ", ps)}");
        a.Subscribe(topic);
        b.Subscribe(topic);
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (DateTime.UtcNow < deadline && !(a.Assignment.Count > 0 && b.Assignment.Count > 0 && a.Assignment.Count + b.Assignment.Count == 6))
            await Task.Delay(200, ct);
        // A processes half of its records and commits; B stays idle, so lag remains.
        var n = 0;
        while (n < 150 && await a.ConsumeOneAsync(TimeSpan.FromSeconds(2), ct) is { } rec)
        {
            n++;
            if (n % 50 == 0) await a.CommitAsync(ct);
        }
        await a.CommitAsync(ct);
        var info = await ctx.Admin.GetGroupAsync(group, ct);
        foreach (var o in info.Offsets ?? [])
            ctx.Log($"  {o.Topic}-{o.Partition}: committed {o.Committed,4} / end {o.HighWatermark,4}  lag {o.Lag}");
        ctx.Log($"group {group}: state {info.Summary?.State}, total lag {info.Lag}");
        ctx.Metric("consumed by A", n);
        ctx.Metric("group lag", info.Lag);
        await a.DisposeAsync();
        await b.DisposeAsync();
    }
}

public sealed class ShareGroupCase : GalleryCase
{
    public override string Category => "Consumption";
    public override string Title => "Share groups: work queues with a DLQ";
    public override string Summary => "Workers share partitions like a job queue. Each email job is locked to one worker and acknowledged: accept, release for retry, or reject into a dead-letter topic.";
    public override IReadOnlyList<string> Highlights => ["Per-record locks with timeouts", "Delivery count on every record", "Automatic dead-letter topic with diagnostic headers"];

    public override string Code => """
        await using var worker = new ShareConsumerBuilder<string, EmailJob>()
            .WithBootstrap("localhost:9092")
            .WithShareGroup("email-workers")
            .WithMaxDeliveryAttempts(3)
            .WithDeadLetterTopic("email-jobs.dlq")
            .Build();
        worker.Subscribe("email-jobs");

        await foreach (var job in worker.ConsumeAsync(ct))
        {
            try { await emailService.SendAsync(job.Value, ct); job.Accept(); }
            catch (TransientException) { job.Release(); }   // redelivered to any worker
            catch (Exception) { job.Reject(); }             // → email-jobs.dlq
        }
        """;

    public override IReadOnlyList<Snippet> OtherLanguages =>
    [
        new("Python", """
            async with ShareConsumer(group="email-workers", topics=["email-jobs"], max_attempts=3,
                                     dlq_topic="email-jobs.dlq") as worker:
                async for job in worker:
                    job.accept() if send(job.json()) else job.reject()
            """),
    ];

    public sealed record EmailJob(string To, string Subject);

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = ctx.Unique("email-jobs");
        var dlq = topic + ".dlq";
        await ctx.Admin.CreateTopicAsync(topic, 2, ct: ct);
        using var http = new BigPipeHttpClient(ctx.Endpoints.HttpUrl);
        var addresses = new[] { "ana@example.id", "budi@example.id", "bounce@invalid", "citra@example.id", "dewi@example.id", "flaky@example.id" };
        await http.ProduceAsync(topic, addresses.Select(a => new HttpProduceRecord { Value = new EmailJob(a, "Promo September") }), ct: ct);
        ctx.Log($"queued {addresses.Length} email jobs");
        await using var worker = new ShareConsumerBuilder<string, EmailJob>().WithHttpEndpoint(ctx.Endpoints.HttpUrl)
            .WithShareGroup(ctx.Unique("email-workers")).WithMaxDeliveryAttempts(3).WithDeadLetterTopic(dlq).WithLockDuration(TimeSpan.FromSeconds(5)).Build();
        worker.Subscribe(topic);
        var finished = 0;
        await foreach (var job in worker.ConsumeAsync(ct))
        {
            if (job.Value.To.StartsWith("bounce"))
            {
                job.Reject();
                ctx.Log($"✖ {job.Value.To}: hard bounce → rejected");
                finished++;
            }
            else if (job.Value.To.StartsWith("flaky") && job.DeliveryCount < 2)
            {
                job.Release();
                ctx.Log($"↻ {job.Value.To}: SMTP timeout on attempt {job.DeliveryCount} → released");
            }
            else
            {
                job.Accept();
                ctx.Log($"✔ {job.Value.To}: sent (attempt {job.DeliveryCount})");
                finished++;
            }
            if (finished == addresses.Length) break;
        }
        await worker.FlushAcksAsync(ct);
        await Task.Delay(300, ct);
        var dead = await ctx.Admin.BrowseAsync(dlq, ct: ct);
        foreach (var d in dead) ctx.Log($"DLQ: {d.Value}  reason={d.Headers["bigpipe.dlq.reason"]} attempts={d.Headers["bigpipe.dlq.attempts"]}");
        ctx.Metric("sent", addresses.Length - dead.Count);
        ctx.Metric("dead-lettered", dead.Count);
    }
}

public sealed class FilterCase : GalleryCase
{
    public override string Category => "Consumption";
    public override string Title => "Server-side filters over SSE";
    public override string Summary => "Subscribe over HTTP Server-Sent Events and let the broker filter with a bpql expression, so only matching payments cross the network.";
    public override IReadOnlyList<string> Highlights => ["Filter on headers, key and JSON fields", "Works from browsers, curl and every SDK", "Also available on HTTP groups and share groups"];

    public override string Code => """
        using var http = new BigPipeHttpClient("http://localhost:8082");
        var filter = "header(\"city\") == \"Bandung\" && this.amount > 1000000";

        await foreach (var r in http.StreamAsync("payments", from: "earliest", filter: filter, ct: ct))
            Console.WriteLine(r.Value);
        """;

    public override IReadOnlyList<Snippet> OtherLanguages =>
    [
        new("curl", "curl -N 'http://localhost:8082/v1/topics/payments/stream?from=earliest&filter=this.amount%20%3E%201000000'"),
        new("TypeScript", """for await (const r of stream("payments", { filter: 'header("city") == "Bandung"' })) console.log(r.json());"""),
        new("Go", """recs, _ := bigpipe.Stream(ctx, "payments", bigpipe.StreamOptions{Filter: `this.amount > 1000000`})"""),
    ];

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = ctx.Unique("payments");
        await ctx.Admin.CreateTopicAsync(topic, 3, ct: ct);
        using var http = new BigPipeHttpClient(ctx.Endpoints.HttpUrl);
        var cities = new[] { "Jakarta", "Bandung", "Surabaya" };
        var rng = new Random(7);
        var data = Enumerable.Range(0, 60).Select(i => (Tx: $"tx-{i}", Amount: rng.Next(1, 40) * 100_000, City: cities[i % 3])).ToList();
        var records = data.Select(d => new HttpProduceRecord
        {
            Key = d.Tx,
            Value = new { tx = d.Tx, amount = d.Amount },
            Headers = new Dictionary<string, string> { ["city"] = d.City },
        }).ToList();
        await http.ProduceAsync(topic, records, ct: ct);
        const string filter = "header(\"city\") == \"Bandung\" && this.amount > 1000000";
        ctx.Log($"produced 60 payments; streaming with filter: {filter}");
        var expected = data.Count(d => d.City == "Bandung" && d.Amount > 1_000_000);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10));
        var got = 0;
        try
        {
            await foreach (var r in http.StreamAsync(topic, "earliest", filter, ct: cts.Token))
            {
                ctx.Log($"  {r.Headers["city"]} {r.Value}");
                if (++got == expected) break;
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
        }
        ctx.Log($"received {got} of 60 records — the broker dropped the other {60 - got}");
        ctx.Metric("delivered", got);
        ctx.Metric("filtered out", 60 - got);
    }
}

public sealed class FlowCase : GalleryCase
{
    public override string Category => "Processing";
    public override string Title => "BigPipe Flow pipelines";
    public override string Summary => "Deploy a managed pipeline inside the broker: keep high-value payments, strip the card number and route by city — no extra service to run.";
    public override IReadOnlyList<string> Highlights => ["Declarative YAML, same as Kubernetes CRDs", "filter / mapping / route processors in bpql", "Progress tracked like a consumer group"];

    public override string Code => """
        await admin.DeployFlowYamlAsync(\"\"\"
            apiVersion: bigpipe.io/v1
            kind: Flow
            metadata:
              name: high-value
            spec:
              input:  { bigpipe: { topic: payments } }
              pipeline:
                processors:
                  - filter: 'this.amount > 5000000'
                  - mapping: |
                      root = this
                      root.card = deleted()
                      root.flagged = true
                  - route: '"payments.high." + lower(this.city)'
              output: { bigpipe: { topic: payments.high } }
            \"\"\");
        """;

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var src = ctx.Unique("payments");
        await ctx.Admin.CreateTopicAsync(src, 2, ct: ct);
        var flow = ctx.Unique("high-value");
        await ctx.Admin.DeployFlowAsync(new FlowSpec
        {
            Name = flow,
            Input = src,
            Output = src + ".high",
            Processors =
            [
                FlowProcessor.FilterBy("this.amount > 5000000"),
                FlowProcessor.MapWith("root = this\nroot.card = deleted()\nroot.flagged = true"),
                FlowProcessor.RouteBy($"\"{src}.high.\" + lower(this.city)"),
            ],
        }, ct);
        ctx.Log($"deployed flow {flow}");
        using var http = new BigPipeHttpClient(ctx.Endpoints.HttpUrl);
        var rng = new Random(3);
        var cities = new[] { "Jakarta", "Bandung" };
        await http.ProduceAsync(src, Enumerable.Range(0, 40).Select(i => new HttpProduceRecord
        {
            Value = new { tx = i, amount = rng.Next(1, 100) * 100_000, city = cities[i % 2], card = "4111-1111-1111-1111" },
        }), ct: ct);
        ctx.Log("produced 40 payments");
        FlowStatus? status = null;
        for (var i = 0; i < 50; i++)
        {
            status = (await ctx.Admin.ListFlowsAsync(ct)).FirstOrDefault(f => f.Name == flow);
            if (status?.Stats.RecordsIn == 40) break;
            await Task.Delay(200, ct);
        }
        ctx.Log($"flow stats: in {status?.Stats.RecordsIn}, out {status?.Stats.RecordsOut}, filtered {status?.Stats.RecordsFiltered}");
        foreach (var city in cities)
        {
            var out_ = await ctx.Admin.BrowseAsync($"{src}.high.{city.ToLowerInvariant()}", 3, ct: ct);
            foreach (var r in out_) ctx.Log($"  {src}.high.{city.ToLowerInvariant()}: {r.Value}");
        }
        ctx.Metric("in", status?.Stats.RecordsIn ?? 0);
        ctx.Metric("routed out", status?.Stats.RecordsOut ?? 0);
        await ctx.Admin.DeleteFlowAsync(flow, ct);
    }
}

public sealed class SchemaCase : GalleryCase
{
    public override string Category => "Processing";
    public override string Title => "Schema Registry and evolution";
    public override string Summary => "Serialize with a registered JSON Schema (Confluent wire format) and see the registry block an incompatible change.";
    public override bool NeedsRegistry => true;
    public override IReadOnlyList<string> Highlights => ["Confluent-compatible REST API on port 8081", "Avro, JSON Schema and Protobuf", "BACKWARD compatibility by default, stored in __bp_schemas"];

    public override string Code => """
        await using var producer = new ProducerBuilder<string, Order>()
            .WithBootstrap("localhost:9092")
            .WithValueSerializer(new SchemaRegistrySerializer<Order>("http://localhost:8081"))
            .Build();
        await producer.SendAsync("orders", "order-1", new Order(1, 150_000m, "IDR"));

        await using var consumer = new ConsumerBuilder<string, Order>()
            .WithValueDeserializer(new SchemaRegistryDeserializer<Order>("http://localhost:8081"))
            ...
        """;

    public sealed record Order(int Id, decimal Amount, string Currency);

    public override async Task RunAsync(GalleryContext ctx, CancellationToken ct)
    {
        var topic = ctx.Unique("orders");
        await ctx.Admin.CreateTopicAsync(topic, 1, ct: ct);
        var ser = new SchemaRegistrySerializer<Order>(ctx.Endpoints.RegistryUrl);
        ctx.Log("generated JSON Schema:");
        ctx.Log(ser.Schema);
        await using (var p = new ProducerBuilder<string, Order>().WithBootstrap(ctx.Endpoints.Bootstrap).WithValueSerializer(ser).Build())
            await p.SendAsync(topic, "order-1", new Order(1, 150_000m, "IDR"), ct: ct);
        var raw = (await ctx.Admin.BrowseAsync(topic, 1, ct: ct))[0].ValueBytes!;
        ctx.Log($"wire format: magic {raw[0]}, schema id {System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(raw.AsSpan(1))}, {raw.Length - 5} JSON bytes");
        using var registry = new SchemaRegistryClient(ctx.Endpoints.RegistryUrl);
        var breaking = """{"type":"object","properties":{"id":{"type":"string"}},"required":["id","note"]}""";
        var ok = await registry.IsCompatibleAsync($"{topic}-value", breaking, ct: ct);
        ctx.Log($"is 'id: string + required note' compatible? {ok}");
        ctx.Metric("compatible change", ok ? 1 : 0);
    }
}
