// BigPipe demo workload: realistic traffic for the Console, Gallery and screenshots.
// Creates topics on all three storage modes, a flow, and consumer groups (one keeps up, one lags).
// Usage: dotnet run --project samples/BigPipe.Demo [-- --bootstrap localhost:9092 --admin http://localhost:9644 --seconds 0]
// Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
using BigPipe.Client;
using BigPipe.Client.Admin;

var bootstrap = Arg("--bootstrap", "localhost:9092");
var adminUrl = Arg("--admin", "http://localhost:9644");
var seconds = int.Parse(Arg("--seconds", "0")); // 0 = run until Ctrl-C

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
if (seconds > 0) cts.CancelAfter(TimeSpan.FromSeconds(seconds));
var ct = cts.Token;

using var admin = new BigPipeAdminClient(adminUrl);
Console.WriteLine("BigPipe demo — Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil");
await admin.EnsureTopicAsync("payments", 6, StorageMode.Local);
await admin.EnsureTopicAsync("orders", 3, StorageMode.Local);
await admin.EnsureTopicAsync("clickstream", 12, StorageMode.Diskless);
await admin.EnsureTopicAsync("iot.telemetry", 6, StorageMode.Diskless);
await admin.EnsureTopicAsync("audit-log", 2, StorageMode.Tiered, new Dictionary<string, string> { ["segment.bytes"] = "65536", ["local.retention.ms"] = "60000" });
try
{
    await admin.DeployFlowAsync(new FlowSpec
    {
        Name = "high-value-payments",
        Input = "payments",
        Output = "payments.high-value",
        Processors =
        [
            FlowProcessor.FilterBy("this.amount > 5000000"),
            FlowProcessor.MapWith("root = this\nroot.card = deleted()\nroot.review = true"),
        ],
    });
}
catch (BigPipeException e)
{
    Console.WriteLine($"flow: {e.Message}");
}

var merchants = new[] { "Warung Kopi Nusantara", "Toko Batik Solo", "Bakmi Jogja", "Sate Madura 99", "Apotek Sehat", "Gadget Store BDG", "Kopi Kenangan", "Martabak Bangka" };
var cities = new[] { "Jakarta", "Bandung", "Surabaya", "Yogyakarta", "Medan", "Makassar", "Denpasar" };
var pages = new[] { "/", "/produk", "/produk/batik-tulis", "/keranjang", "/checkout", "/promo", "/akun" };
// Random.Shared is thread-safe; the produce loops run concurrently.
var rng = Random.Shared;

await using var producer = new ProducerBuilder<string, string>().WithBootstrap(bootstrap).WithCompression(CompressionType.Lz4)
    .WithLinger(TimeSpan.FromMilliseconds(20)).Build();

async Task ProduceLoop(string topic, int perSecond, Func<int, (string Key, string Value, Dictionary<string, string>? Headers)> make)
{
    var n = 0;
    var tick = TimeSpan.FromMilliseconds(100);
    while (!ct.IsCancellationRequested)
    {
        var burst = Math.Max(1, perSecond / 10 + rng.Next(-perSecond / 40 - 1, perSecond / 40 + 2));
        var sends = new List<Task>(burst);
        for (var i = 0; i < burst; i++)
        {
            var (k, v, h) = make(n++);
            sends.Add(h is null ? producer.SendAsync(topic, k, v, ct: ct) : producer.SendAsync(topic, k, v, h, ct));
        }
        try
        {
            await Task.WhenAll(sends);
            await Task.Delay(tick, ct);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception e)
        {
            Console.WriteLine($"{topic}: {e.Message}");
            await Task.Delay(1000, CancellationToken.None);
        }
    }
}

async Task ConsumeLoop(string group, string topic, TimeSpan perRecordDelay)
{
    await using var consumer = new ConsumerBuilder<string, string>().WithBootstrap(bootstrap).WithGroup(group)
        .WithAutoOffsetReset(OffsetReset.Earliest).Build();
    consumer.Subscribe(topic);
    try
    {
        await foreach (var _ in consumer.ConsumeAsync(ct))
            if (perRecordDelay > TimeSpan.Zero) await Task.Delay(perRecordDelay, ct);
    }
    catch (OperationCanceledException)
    {
    }
}

var tasks = new List<Task>
{
    ProduceLoop("payments", 400, i =>
    {
        var amount = rng.Next(10, 1200) * 5_000L + (rng.Next(100) == 0 ? 40_000_000 : 0);
        var city = cities[rng.Next(cities.Length)];
        return ($"tx-{i}", $$"""{"tx":"tx-{{i}}","merchant":"{{merchants[rng.Next(merchants.Length)]}}","city":"{{city}}","amount":{{amount}},"currency":"IDR","card":"4111-****-****-{{rng.Next(1000, 9999)}}","status":"{{(rng.Next(20) == 0 ? "PENDING" : "SETTLED")}}"}""",
            new Dictionary<string, string> { ["region"] = "ID", ["city"] = city });
    }),
    ProduceLoop("orders", 60, i => ($"order-{i}", $$"""{"order":"order-{{i}}","items":{{rng.Next(1, 6)}},"total":{{rng.Next(20, 900) * 1000}},"channel":"{{(rng.Next(2) == 0 ? "app" : "web")}}"}""", null)),
    ProduceLoop("clickstream", 1500, i => ($"user-{rng.Next(5000)}", $$"""{"page":"{{pages[rng.Next(pages.Length)]}}","ms":{{rng.Next(40, 4000)}},"device":"{{(rng.Next(3) == 0 ? "desktop" : "mobile")}}"}""", null)),
    ProduceLoop("iot.telemetry", 300, i =>
    {
        var device = $"truck-{rng.Next(40):D2}";
        var temp = 4 + rng.NextDouble() * 2 + (rng.Next(500) == 0 ? 12 : 0);
        return (device, $$"""{"device":"{{device}}","temp_c":{{temp:0.00}},"speed_kmh":{{rng.Next(0, 90)}},"lat":{{-6.2 + rng.NextDouble() / 10:0.0000}},"lon":{{106.8 + rng.NextDouble() / 10:0.0000}}}""", null);
    }),
    ProduceLoop("audit-log", 30, i => ($"user-{rng.Next(50)}", $$"""{"action":"{{(rng.Next(2) == 0 ? "login" : "update-profile")}}","ok":true}""", null)),
    ConsumeLoop("fraud-detector", "payments", TimeSpan.Zero),
    ConsumeLoop("ledger-writer", "payments", TimeSpan.FromMilliseconds(4)),
    ConsumeLoop("analytics-dashboard", "clickstream", TimeSpan.Zero),
    ConsumeLoop("fleet-monitor", "iot.telemetry", TimeSpan.Zero),
    ConsumeLoop("fulfilment", "orders", TimeSpan.FromMilliseconds(40)),
};
Console.WriteLine($"Producing to {bootstrap}. Open the Console at http://localhost:8080. Ctrl-C to stop.");
await Task.WhenAll(tasks);

string Arg(string name, string fallback)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : fallback;
}
