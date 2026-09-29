using BigPipe.Client.Protocol;
using System.Collections.Concurrent;
using BigPipe.Client.Admin;

namespace BigPipe.Client.Tests;

public sealed record Order(int Id, decimal Amount, string Currency);

[Collection("bigpipe")]
public class EndToEndTests(BigPipeFixture fx)
{
    [Theory]
    [InlineData(CompressionType.None, StorageMode.Local)]
    [InlineData(CompressionType.Lz4, StorageMode.Local)]
    [InlineData(CompressionType.Zstd, StorageMode.Diskless)]
    public async Task Produce_and_consume_with_group(CompressionType codec, StorageMode mode)
    {
        var topic = BigPipeFixture.Unique("orders");
        await fx.Admin.CreateTopicAsync(topic, 4, mode);
        await using (var producer = new ProducerBuilder<string, Order>().WithBootstrap(fx.Bootstrap).WithCompression(codec).Build())
        {
            var sends = Enumerable.Range(0, 2000).Select(i =>
                producer.SendAsync(topic, $"order-{i}", new Order(i, i * 1000m, "IDR"), new Dictionary<string, string> { ["source"] = "test" }));
            var results = await Task.WhenAll(sends);
            Assert.All(results, r => Assert.True(r.Offset >= 0));
            // Keyed records are placed exactly like Kafka's default partitioner.
            Assert.Equal(Murmur2.Partition("order-7"u8, 4), results[7].Partition);
        }

        await using var consumer = new ConsumerBuilder<string, Order>()
            .WithBootstrap(fx.Bootstrap).WithGroup(BigPipeFixture.Unique("g")).WithAutoOffsetReset(OffsetReset.Earliest).Build();
        consumer.Subscribe(topic);
        var seen = new HashSet<int>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await foreach (var msg in consumer.ConsumeAsync(cts.Token))
        {
            Assert.Equal("test", msg.GetHeader("source"));
            Assert.Equal($"order-{msg.Value.Id}", msg.Key);
            seen.Add(msg.Value.Id);
            if (seen.Count == 2000) break;
        }
        Assert.Equal(2000, seen.Count);
        await consumer.CommitAsync();
    }

    [Fact]
    public async Task Committed_offsets_resume_and_lag_is_reported()
    {
        var topic = BigPipeFixture.Unique("resume");
        var group = BigPipeFixture.Unique("g");
        await fx.Admin.CreateTopicAsync(topic, 2);
        await using (var p = new ProducerBuilder<Null, string>().WithBootstrap(fx.Bootstrap).Build())
            await Task.WhenAll(Enumerable.Range(0, 100).Select(i => p.SendAsync(topic, default, $"v{i}")));

        await using (var c1 = new ConsumerBuilder<Null, string>().WithBootstrap(fx.Bootstrap).WithGroup(group)
                         .WithAutoOffsetReset(OffsetReset.Earliest).WithAutoCommit(false).Build())
        {
            c1.Subscribe(topic);
            var n = 0;
            await foreach (var m in c1.ConsumeAsync(new CancellationTokenSource(30_000).Token))
            {
                if (++n == 60)
                {
                    await c1.CommitAsync();
                    break;
                }
            }
        }
        var info = await fx.Admin.GetGroupAsync(group);
        Assert.Equal(40, info.Lag);

        await using var c2 = new ConsumerBuilder<Null, string>().WithBootstrap(fx.Bootstrap).WithGroup(group).WithAutoOffsetReset(OffsetReset.Earliest).Build();
        c2.Subscribe(topic);
        var rest = 0;
        while (await c2.ConsumeOneAsync(TimeSpan.FromSeconds(5)) is not null) rest++;
        Assert.Equal(40, rest);
    }

    [Fact]
    public async Task Two_consumers_split_partitions()
    {
        var topic = BigPipeFixture.Unique("split");
        var group = BigPipeFixture.Unique("g");
        await fx.Admin.CreateTopicAsync(topic, 6);
        var assigned = new ConcurrentDictionary<int, IReadOnlyList<TopicPartition>>();
        var a = new ConsumerBuilder<string, string>().WithBootstrap(fx.Bootstrap).WithGroup(group).Build();
        var b = new ConsumerBuilder<string, string>().WithBootstrap(fx.Bootstrap).WithGroup(group).Build();
        a.PartitionsAssigned += p => assigned[0] = p;
        b.PartitionsAssigned += p => assigned[1] = p;
        a.Subscribe(topic);
        b.Subscribe(topic);
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline && !(assigned.TryGetValue(0, out var x) && assigned.TryGetValue(1, out var y) && x.Count + y.Count == 6 && x.Count > 0 && y.Count > 0))
            await Task.Delay(200);
        Assert.Equal(6, a.Assignment.Count + b.Assignment.Count);
        Assert.Empty(a.Assignment.Intersect(b.Assignment));
        await a.DisposeAsync();
        await b.DisposeAsync();
    }

    [Fact]
    public async Task Admin_migration_and_flow()
    {
        var src = BigPipeFixture.Unique("src");
        var dst = src + "-big";
        var t = await fx.Admin.CreateTopicAsync(src, 1);
        Assert.Equal(StorageMode.Local, t.Mode);
        var http = new BigPipeHttpClient(fx.HttpUrl);
        await http.ProduceAsync(src, new { amount = 5 });
        var mig = await fx.Admin.MigrateAsync(src, StorageMode.Diskless);
        Assert.Equal(StorageMode.Diskless, mig.To);
        Assert.Equal(1, mig.SwitchOffsets["0"]);

        await fx.Admin.DeployFlowAsync(new FlowSpec
        {
            Name = BigPipeFixture.Unique("flow"),
            Input = src,
            Output = dst,
            Processors = [FlowProcessor.FilterBy("this.amount > 100"), FlowProcessor.MapWith("root = this\nroot.flag = 'big'")],
        });
        await http.ProduceAsync(src, new { amount = 500 });
        List<HttpRecord> got = [];
        for (var i = 0; i < 50 && got.Count == 0; i++)
        {
            await Task.Delay(200);
            try { got = await fx.Admin.BrowseAsync(dst); } catch (BigPipeException) { }
        }
        Assert.Single(got);
        Assert.Contains("\"flag\":\"big\"", got[0].Value);
        var groups = await fx.Admin.ListGroupsAsync();
        Assert.Contains(groups, g => g.GroupId.StartsWith("__flow:"));
    }

    [Fact]
    public async Task Share_consumer_acks_and_dead_letters()
    {
        var topic = BigPipeFixture.Unique("jobs");
        var dlq = topic + ".dlq";
        await fx.Admin.CreateTopicAsync(topic, 2);
        var http = new BigPipeHttpClient(fx.HttpUrl);
        await http.ProduceAsync(topic, Enumerable.Range(0, 6).Select(i => new HttpProduceRecord { Value = $"job-{i}" }));

        await using var worker = new ShareConsumerBuilder<string, string>()
            .WithHttpEndpoint(fx.HttpUrl).WithShareGroup(BigPipeFixture.Unique("workers"))
            .WithMaxDeliveryAttempts(2).WithDeadLetterTopic(dlq).Build();
        worker.Subscribe(topic);
        var done = new HashSet<string>();
        var rejected = false;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await foreach (var job in worker.ConsumeAsync(cts.Token))
        {
            if (job.Value == "job-3")
            {
                job.Reject();
                rejected = true;
            }
            else
            {
                job.Accept();
                done.Add(job.Value);
            }
            if (done.Count == 5 && rejected) break;
        }
        await worker.FlushAcksAsync();
        List<HttpRecord> dead = [];
        for (var i = 0; i < 20 && dead.Count == 0; i++)
        {
            await Task.Delay(100);
            try { dead = await fx.Admin.BrowseAsync(dlq); } catch (BigPipeException) { }
        }
        Assert.Single(dead);
        Assert.Equal("job-3", dead[0].Value);
        Assert.Equal("rejected by consumer", dead[0].Headers["bigpipe.dlq.reason"]);
    }

    [Fact]
    public async Task Sse_stream_applies_server_side_filter()
    {
        var topic = BigPipeFixture.Unique("sse");
        await fx.Admin.CreateTopicAsync(topic, 1);
        var http = new BigPipeHttpClient(fx.HttpUrl);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var received = new List<HttpRecord>();
        var reader = Task.Run(async () =>
        {
            await foreach (var r in http.StreamAsync(topic, "earliest", "header('region') == 'ID'", ct: cts.Token))
            {
                received.Add(r);
                if (received.Count == 2) break;
            }
        });
        await http.ProduceAsync(topic, "a", headers: new Dictionary<string, string> { ["region"] = "ID" });
        await http.ProduceAsync(topic, "b", headers: new Dictionary<string, string> { ["region"] = "SG" });
        await http.ProduceAsync(topic, "c", headers: new Dictionary<string, string> { ["region"] = "ID" });
        await reader;
        Assert.Equal(["a", "c"], received.Select(r => r.Value));
    }
}
