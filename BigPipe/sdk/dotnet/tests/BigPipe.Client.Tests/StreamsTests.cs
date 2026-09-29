using BigPipe.Client.Admin;
using BigPipe.Streams;

namespace BigPipe.Client.Tests;

public sealed record Payment(string MerchantId, decimal Amount, string Status);

public sealed record MerchantTotals(long Count = 0, decimal Amount = 0);

public sealed record Merchant(string Name, string City);

[Collection("bigpipe")]
public class StreamsTests(BigPipeFixture fx)
{
    private static async Task WaitUntil(Func<bool> cond, int seconds = 30)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (!cond() && DateTime.UtcNow < deadline) await Task.Delay(100);
    }

    [Fact]
    public async Task Windowed_aggregate_per_merchant_matches_design_sample()
    {
        var input = BigPipeFixture.Unique("payments");
        var output = input + "-totals";
        await fx.Admin.CreateTopicAsync(input, 3);

        var topology = new StreamsBuilder();
        var totals = topology.Stream<string, Payment>(input)
            .Filter((_, p) => p.Status == "SETTLED")
            .GroupBy((_, p) => p.MerchantId)
            .WindowedBy(TumblingWindow.Of(TimeSpan.FromMinutes(1)).Grace(TimeSpan.FromSeconds(10)))
            .Aggregate(() => new MerchantTotals(), (_, p, acc) => acc with { Count = acc.Count + 1, Amount = acc.Amount + p.Amount },
                Stores.RocksDb("merchant-totals"));
        totals.ToStream().MapValues(t => t.Amount).To(output);

        await using var app = new StreamsApp(topology, new StreamsConfig { ApplicationId = BigPipeFixture.Unique("agg"), Bootstrap = fx.Bootstrap });
        await app.StartAsync();

        var baseTs = new DateTimeOffset(2026, 9, 29, 10, 0, 0, TimeSpan.Zero);
        await using (var p = new ProducerBuilder<string, Payment>().WithBootstrap(fx.Bootstrap).Build())
        {
            var sends = new List<Task>();
            for (var i = 0; i < 30; i++)
            {
                sends.Add(p.SendAsync(input, new Message<string, Payment>
                {
                    Key = $"tx-{i}",
                    Value = new Payment(i % 2 == 0 ? "M-1" : "M-2", 10_000m, i % 5 == 0 ? "PENDING" : "SETTLED"),
                    Timestamp = baseTs.AddSeconds(i),
                }));
            }
            await Task.WhenAll(sends);
        }

        var store = app.Store<Windowed<string>, MerchantTotals>("merchant-totals");
        await WaitUntil(() => store.All().Sum(kv => kv.Value.Count) == 24);
        Assert.Equal(StreamsState.Running, app.State);
        var m1 = store.All().Single(kv => kv.Key.Key == "M-1");
        Assert.Equal(baseTs.ToUnixTimeMilliseconds(), m1.Key.Start);
        Assert.Equal(12, m1.Value.Count); // 15 even ids minus 3 pending (0, 10, 20)
        Assert.Equal(120_000m, m1.Value.Amount);
    }

    [Fact]
    public async Task Stream_table_join_and_changelog_restore()
    {
        var merchants = BigPipeFixture.Unique("merchants");
        var payments = BigPipeFixture.Unique("pay");
        var enriched = payments + "-enriched";
        await fx.Admin.CreateTopicAsync(merchants, 1);
        await fx.Admin.CreateTopicAsync(payments, 1);
        var appId = BigPipeFixture.Unique("join");

        StreamsBuilder Build()
        {
            var b = new StreamsBuilder();
            var table = b.Table<string, Merchant>(merchants);
            var stream = b.Stream<string, Payment>(payments).SelectKey((_, p) => p.MerchantId);
            stream.Join(table, (p, m) => $"{m.Name} ({m.City}) {p.Amount}").To(enriched);
            stream.GroupByKey().Count(Stores.Persistent("payments-per-merchant"));
            return b;
        }

        await using (var mp = new ProducerBuilder<string, Merchant>().WithBootstrap(fx.Bootstrap).Build())
            await mp.SendAsync(merchants, "M-1", new Merchant("Warung Kopi", "Bandung"));

        await using var pp = new ProducerBuilder<string, Payment>().WithBootstrap(fx.Bootstrap).Build();
        await using (var app = new StreamsApp(Build(), new StreamsConfig { ApplicationId = appId, Bootstrap = fx.Bootstrap }))
        {
            await app.StartAsync();
            await Task.Delay(1500); // let the table load first
            await Task.WhenAll(Enumerable.Range(0, 5).Select(_ => pp.SendAsync(payments, "x", new Payment("M-1", 25_000m, "SETTLED"))));
            var counts = app.Store<string, long>("payments-per-merchant");
            await WaitUntil(() => counts.TryGet("M-1", out var c) && c == 5);
            Assert.True(counts.TryGet("M-1", out var n) && n == 5);
            List<HttpRecord> out1 = [];
            await WaitUntil(() => (out1 = fx.Admin.BrowseAsync(enriched).GetAwaiter().GetResult()).Count == 5);
            Assert.All(out1, r => Assert.Equal("Warung Kopi (Bandung) 25000", r.Value));
        }

        // A fresh instance restores the count from the changelog and keeps counting.
        await pp.SendAsync(payments, "x", new Payment("M-1", 1m, "SETTLED"));
        await using var app2 = new StreamsApp(Build(), new StreamsConfig { ApplicationId = appId, Bootstrap = fx.Bootstrap });
        await app2.StartAsync();
        var counts2 = app2.Store<string, long>("payments-per-merchant");
        await WaitUntil(() => counts2.TryGet("M-1", out var c) && c == 6);
        Assert.True(counts2.TryGet("M-1", out var n2) && n2 == 6, $"count after restore = {n2}");
    }

    [Fact]
    public async Task Emit_on_window_close_and_session_windows()
    {
        var input = BigPipeFixture.Unique("clicks");
        var closed = input + "-closed";
        await fx.Admin.CreateTopicAsync(input, 1);
        var b = new StreamsBuilder();
        var grouped = b.Stream<string, string>(input).GroupByKey();
        grouped.WindowedBy(TumblingWindow.Of(TimeSpan.FromSeconds(10))).EmitOnWindowClose().Count()
            .ToStream().Map((k, v) => ($"{k.Key}:{k.Start}", v)).To(closed);
        var sessions = grouped.WindowedBy(SessionWindow.WithInactivityGap(TimeSpan.FromSeconds(5))).Count(Stores.InMemory("sessions"));

        await using var app = new StreamsApp(b, new StreamsConfig { ApplicationId = BigPipeFixture.Unique("win"), Bootstrap = fx.Bootstrap });
        await app.StartAsync();
        var t0 = new DateTimeOffset(2026, 9, 29, 12, 0, 0, TimeSpan.Zero);
        await using (var p = new ProducerBuilder<string, string>().WithBootstrap(fx.Bootstrap).Build())
        {
            foreach (var s in new[] { 0, 1, 2, 6, 30 }) // session 1: 0-6 (gaps <= 5s), session 2: 30
                await p.SendAsync(input, new Message<string, string> { Key = "user-1", Value = "click", Timestamp = t0.AddSeconds(s) });
        }
        var store = app.Store<Windowed<string>, long>(sessions.StoreName);
        await WaitUntil(() => store.Count == 2 && store.All().Sum(x => x.Value) == 5);
        Assert.Equal([1L, 4L], store.All().Select(x => x.Value).OrderBy(x => x));

        List<HttpRecord> closedWindows = [];
        await WaitUntil(() => (closedWindows = fx.Admin.BrowseAsync(closed).GetAwaiter().GetResult()).Count >= 1);
        // Only [0,10) has closed (stream time reached 30s); it saw 4 clicks, emitted once.
        Assert.Single(closedWindows);
        Assert.Equal($"user-1:{t0.ToUnixTimeMilliseconds()}", closedWindows[0].Key);
    }
}
