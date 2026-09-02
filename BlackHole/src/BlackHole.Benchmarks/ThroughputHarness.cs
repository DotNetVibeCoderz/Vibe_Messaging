// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Diagnostics;
using System.Text;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Benchmarks;

/// <summary>
/// Sustained-load measurements that BenchmarkDotNet is the wrong tool for: latency percentiles under
/// a real request stream, aggregate message rate with many connections, and streaming bandwidth.
/// BDN measures one operation very precisely; this measures what a running system looks like.
/// </summary>
public static class ThroughputHarness
{
    /// <param name="stages">
    /// Which sections to run: latency, throughput, fanout, batch, stream. Empty runs all of them.
    /// </param>
    public static async Task RunAsync(IReadOnlyCollection<string>? stages = null)
    {
        bool Wanted(string name) => stages is null || stages.Count == 0 || stages.Contains(name);

        Header();
        if (Wanted("latency")) await RpcLatencyAsync();
        if (Wanted("throughput")) await RpcThroughputAsync();
        if (Wanted("fanout")) await FanOutAsync();
        if (Wanted("batch")) await BatchThroughputAsync();
        if (Wanted("stream")) await StreamThroughputAsync();
        Console.WriteLine();
        Console.WriteLine("Gravicode Studios - led by Kang Fadhil");
    }

    // ---------------------------------------------------------------- latency

    private static async Task RpcLatencyAsync()
    {
        const int warmup = 5_000;
        const int measured = 50_000;

        var options = new TransportOptions { KeepAliveInterval = null };
        await using var server = new BlackHoleServer(0, options);
        server.Rpc.Register("echo", request => request.Payload);
        server.Start();

        await using BlackHoleClient client = await BlackHoleClient.ConnectAsync("127.0.0.1", server.EndPoint.Port, options);
        byte[] payload = Encoding.UTF8.GetBytes("{\"deviceId\":\"tank-3\",\"c\":28.4}");

        for (int i = 0; i < warmup; i++)
            await client.Rpc.CallAsync("echo", payload);

        var samples = new double[measured];
        long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        int gen0Before = GC.CollectionCount(0);

        var total = Stopwatch.StartNew();
        for (int i = 0; i < measured; i++)
        {
            long start = Stopwatch.GetTimestamp();
            await client.Rpc.CallAsync("echo", payload);
            samples[i] = Stopwatch.GetElapsedTime(start).TotalMicroseconds;
        }
        total.Stop();

        long allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        int gen0 = GC.CollectionCount(0) - gen0Before;
        Array.Sort(samples);

        Section("RPC latency, sequential round trips over loopback");
        Row("payload", $"{payload.Length} bytes");
        Row("calls", $"{measured:N0}");
        Row("throughput", $"{measured / total.Elapsed.TotalSeconds:N0} calls/sec");
        Row("mean", $"{samples.Average():N1} us");
        Row("p50", $"{Percentile(samples, 0.50):N1} us");
        Row("p90", $"{Percentile(samples, 0.90):N1} us");
        Row("p99", $"{Percentile(samples, 0.99):N1} us");
        Row("p99.9", $"{Percentile(samples, 0.999):N1} us");
        Row("max", $"{samples[^1]:N1} us");
        Row("allocated", $"{allocated / (double)measured:N0} B/call (client and server in one process)");
        Row("gen0 collections", $"{gen0}");
    }

    // ------------------------------------------------------------- throughput

    private static async Task RpcThroughputAsync()
    {
        const int perConnection = 20_000;
        int[] connectionCounts = [1, 4, 16];

        Section("RPC throughput, concurrent connections");
        Console.WriteLine("  connections   in flight   calls        duration     calls/sec");
        Console.WriteLine("  -----------   ---------   ----------   ----------   ------------");

        foreach (int connections in connectionCounts)
        {
            var options = new TransportOptions { KeepAliveInterval = null };
            await using var server = new BlackHoleServer(0, options);
            server.Rpc.Register("echo", request => request.Payload);
            server.Start();

            var clients = new BlackHoleClient[connections];
            for (int i = 0; i < connections; i++)
                clients[i] = await BlackHoleClient.ConnectAsync("127.0.0.1", server.EndPoint.Port, options);

            byte[] payload = new byte[64];
            const int inFlight = 16;

            // Warm up every connection so the first measured call is not a JIT sample.
            foreach (BlackHoleClient client in clients)
                await client.Rpc.CallAsync("echo", payload);

            var sw = Stopwatch.StartNew();
            await Task.WhenAll(clients.Select(async client =>
            {
                for (int batch = 0; batch < perConnection / inFlight; batch++)
                {
                    var calls = new Task<byte[]>[inFlight];
                    for (int i = 0; i < inFlight; i++)
                        calls[i] = client.Rpc.CallAsync("echo", payload);
                    await Task.WhenAll(calls);
                }
            }));
            sw.Stop();

            long calls = (long)connections * (perConnection / inFlight) * inFlight;
            Console.WriteLine($"  {connections,11}   {inFlight,9}   {calls,10:N0}   " +
                              $"{sw.Elapsed.TotalMilliseconds,8:N0} ms   {calls / sw.Elapsed.TotalSeconds,12:N0}");

            var teardown = Stopwatch.StartNew();
            await Task.WhenAll(clients.Select(c => c.DisposeAsync().AsTask()));
            Trace($"closed {connections} connection(s) in {teardown.ElapsedMilliseconds:N0} ms");
        }
    }

    /// <summary>
    /// Progress line. Long stages must say what they are doing - a benchmark that goes silent for
    /// minutes is indistinguishable from one that has hung.
    /// </summary>
    private static void Trace(string message)
    {
        if (Verbose)
            Console.WriteLine($"    . {message}");
    }

    /// <summary>Print progress lines between measurements.</summary>
    public static bool Verbose { get; set; }

    // ---------------------------------------------------------------- fan-out

    private static async Task FanOutAsync()
    {
        const int subscribers = 50;
        const int publishes = 2_000;

        var options = new TransportOptions { KeepAliveInterval = null };
        await using var server = new BlackHoleServer(0, options);
        server.Start();

        var clients = new BlackHoleClient[subscribers];
        long delivered = 0;
        var allDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        long expected = (long)subscribers * publishes;

        Trace($"connecting {subscribers} subscribers");
        for (int i = 0; i < subscribers; i++)
        {
            clients[i] = await BlackHoleClient.ConnectAsync("127.0.0.1", server.EndPoint.Port, options);
            clients[i].PubSub.Received += (_, _) =>
            {
                if (Interlocked.Increment(ref delivered) == expected)
                    allDelivered.TrySetResult();
            };
            await clients[i].PubSub.SubscribeAsync("plant/floor-1/telemetry");
        }

        while (server.PubSub.SubscriberCount("plant/floor-1/telemetry") < subscribers)
            await Task.Delay(20);
        Trace("all subscriptions registered, publishing");

        byte[] payload = Encoding.UTF8.GetBytes("{\"c\":28.4,\"kPa\":101.7}");
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < publishes; i++)
            await server.PublishAsync("plant/floor-1/telemetry", payload);
        await allDelivered.Task.WaitAsync(TimeSpan.FromSeconds(60));
        sw.Stop();

        Section("Pub/Sub fan-out");
        Row("subscribers", $"{subscribers}");
        Row("publishes", $"{publishes:N0}");
        Row("deliveries", $"{expected:N0}");
        Row("duration", $"{sw.Elapsed.TotalMilliseconds:N0} ms");
        Row("delivery rate", $"{expected / sw.Elapsed.TotalSeconds:N0} messages/sec");

        foreach (BlackHoleClient client in clients)
            await client.DisposeAsync();
    }

    // --------------------------------------------------------------- batching

    private static async Task BatchThroughputAsync()
    {
        const int messages = 200_000;

        Section("Batching, 200,000 small publishes");
        Console.WriteLine("  mode                       duration     messages/sec   socket writes");
        Console.WriteLine("  ------------------------   ----------   ------------   -------------");

        byte[] payload = Encoding.UTF8.GetBytes("28.4");

        await MeasureAsync("one send per message", async (client, received) =>
        {
            for (int i = 0; i < messages; i++)
                await client.SendAsync(new BlackHoleMessage(MessageType.Publish, "t", payload));
            await received(messages);
            return messages;
        });

        await MeasureAsync("batches of 256", async (client, received) =>
        {
            client.Batch.MaxCount = 256;
            client.Batch.MaxBytes = 1 << 20;
            client.Batch.MaxDelay = null;
            for (int i = 0; i < messages; i++)
                await client.Batch.AddAsync(new BlackHoleMessage(MessageType.Publish, "t", payload));
            await client.Batch.FlushAsync();
            await received(messages);
            return client.Batch.BatchesSent;
        });

        await MeasureAsync("batches of 1024", async (client, received) =>
        {
            client.Batch.MaxCount = 1024;
            client.Batch.MaxBytes = 4 << 20;
            client.Batch.MaxDelay = null;
            for (int i = 0; i < messages; i++)
                await client.Batch.AddAsync(new BlackHoleMessage(MessageType.Publish, "t", payload));
            await client.Batch.FlushAsync();
            await received(messages);
            return client.Batch.BatchesSent;
        });

        static async Task MeasureAsync(string label, Func<BlackHoleClient, Func<int, Task>, Task<long>> run)
        {
            var options = new TransportOptions { KeepAliveInterval = null };
            await using var server = new BlackHoleServer(0, options);

            long seen = 0;
            TaskCompletionSource? done = null;
            int target = 0;
            server.ClientConnected += connection =>
                connection.Router.On(MessageType.Publish, (_, _) =>
                {
                    if (Interlocked.Increment(ref seen) == target)
                        done?.TrySetResult();
                });
            server.Start();

            await using BlackHoleClient client = await BlackHoleClient.ConnectAsync("127.0.0.1", server.EndPoint.Port, options);

            var sw = Stopwatch.StartNew();
            long writes = await run(client, async count =>
            {
                target = count;
                done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                if (Interlocked.Read(ref seen) >= count)
                    done.TrySetResult();
                await done.Task.WaitAsync(TimeSpan.FromSeconds(120));
            });
            sw.Stop();

            Console.WriteLine($"  {label,-24}   {sw.Elapsed.TotalMilliseconds,8:N0} ms   " +
                              $"{messages / sw.Elapsed.TotalSeconds,12:N0}   {writes,13:N0}");
        }
    }

    // -------------------------------------------------------------- streaming

    private static async Task StreamThroughputAsync()
    {
        const int totalBytes = 64 * 1024 * 1024;
        int[] chunkSizes = [4096, 16384, 65536];

        Section("Streaming, 64 MiB per transfer");
        Console.WriteLine("  chunk size   duration     throughput      chunks");
        Console.WriteLine("  ----------   ----------   -------------   ---------");

        var payload = new byte[totalBytes];
        Random.Shared.NextBytes(payload);

        foreach (int chunkSize in chunkSizes)
        {
            var options = new TransportOptions { KeepAliveInterval = null };
            await using var server = new BlackHoleServer(0, options);

            var completed = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            server.ClientConnected += connection =>
            {
                connection.Streams.MaxStreamLength = long.MaxValue;
                connection.Streams.Completed += (_, e) => completed.TrySetResult(e.Length);
            };
            server.Start();

            await using BlackHoleClient client = await BlackHoleClient.ConnectAsync("127.0.0.1", server.EndPoint.Port, options);

            var sw = Stopwatch.StartNew();
            await client.OutgoingStreams.SendAsync(
                "bench.bin", payload,
                new StreamDescriptor("bench.bin", totalBytes, "application/octet-stream"),
                chunkSize);
            long received = await completed.Task.WaitAsync(TimeSpan.FromSeconds(120));
            sw.Stop();

            Console.WriteLine($"  {chunkSize / 1024 + " KiB",10}   {sw.Elapsed.TotalMilliseconds,8:N0} ms   " +
                              $"{received / (1024.0 * 1024) / sw.Elapsed.TotalSeconds,10:N0} MiB/s   " +
                              $"{received / chunkSize,9:N0}");
        }
    }

    // ---------------------------------------------------------------- console

    private static double Percentile(double[] sorted, double percentile)
    {
        int index = (int)Math.Ceiling(percentile * sorted.Length) - 1;
        return sorted[Math.Clamp(index, 0, sorted.Length - 1)];
    }

    private static void Header()
    {
        Console.OutputEncoding = Encoding.UTF8;
        Console.WriteLine("==========================================================");
        Console.WriteLine("  BLACKHOLE MESSAGING v3 - THROUGHPUT HARNESS");
        Console.WriteLine("==========================================================");
        Console.WriteLine($"  runtime      : {Environment.Version}");
        Console.WriteLine($"  os           : {Environment.OSVersion.VersionString}");
        Console.WriteLine($"  processors   : {Environment.ProcessorCount}");
        Console.WriteLine($"  server GC    : {System.Runtime.GCSettings.IsServerGC}");
        Console.WriteLine($"  measured     : {DateTimeOffset.Now:yyyy-MM-dd HH:mm}");
        Console.WriteLine("  transport    : TCP over loopback, both ends in one process");
    }

    private static void Section(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"--- {title} ".PadRight(58, '-'));
    }

    private static void Row(string label, string value) =>
        Console.WriteLine($"  {label,-18} {value}");
}
