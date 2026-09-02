// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Diagnostics;
using System.Net;
using System.Text;
using BlackHole.Hosting;
using BlackHole.Transport;

namespace BlackHole.Benchmarks;

/// <summary>
/// The same workload over TCP, Unix domain sockets, named pipes and shared memory.
/// </summary>
/// <remarks>
/// The whole reason the same-machine transports exist is that they should be faster than a loopback
/// socket. This is where that claim is checked rather than assumed - and where the cost of shared
/// memory's polling shows up alongside its latency win.
/// </remarks>
public static class TransportComparison
{
    private sealed record Harness(
        string Name,
        Func<TransportOptions, IListenerHost> CreateListener,
        Func<IListenerHost, TransportOptions, Task<BlackHoleClient>> Connect);

    public static async Task RunAsync(IReadOnlyCollection<string>? only = null)
    {
        Header();

        var harnesses = new List<Harness>
        {
            new("TCP loopback",
                options => new TcpListenerHost(new IPEndPoint(IPAddress.Loopback, 0), options),
                (listener, options) => BlackHoleClient.ConnectAsync(
                    "127.0.0.1", ((TcpListenerHost)listener).EndPoint.Port, options)),

            new("Named pipe",
                options => new NamedPipeListenerHost(UniqueName(), options),
                (listener, options) => BlackHoleClient.ConnectPipeAsync(
                    ((NamedPipeListenerHost)listener).PipeName, options, TimeSpan.FromSeconds(10))),

            new("Shared memory",
                options => new SharedMemoryListenerHost(UniqueName(), 4, options, BenchmarkShared()),
                (listener, options) => BlackHoleClient.ConnectSharedMemoryAsync(
                    ((SharedMemoryListenerHost)listener).Name, 4, TimeSpan.FromSeconds(10), options, BenchmarkShared())),
        };

        if (UnixSocketTransport.IsSupported)
        {
            harnesses.Insert(1, new Harness(
                "Unix socket",
                options => new UnixSocketListenerHost(UnixSocketTransport.TempPath(UniqueName()), options),
                (listener, options) => BlackHoleClient.ConnectUnixAsync(
                    ((UnixSocketListenerHost)listener).SocketPath, options)));
        }
        else
        {
            Console.WriteLine("  (Unix domain sockets are unavailable on this platform; skipping that row)");
            Console.WriteLine();
        }

        if (only is { Count: > 0 })
            harnesses = harnesses.Where(h => only.Any(o => h.Name.Contains(o, StringComparison.OrdinalIgnoreCase))).ToList();

        await LatencyAsync(harnesses);
        await ThroughputAsync(harnesses);
        await StreamingAsync(harnesses);
        await IdleCostAsync(harnesses);

        Console.WriteLine();
        Console.WriteLine("Gravicode Studios - led by Kang Fadhil");
    }

    // ---------------------------------------------------------------- latency

    private static async Task LatencyAsync(IReadOnlyList<Harness> harnesses)
    {
        const int warmup = 2_000;
        const int measured = 20_000;

        Section("RPC latency, sequential round trips, 30-byte payload");
        Console.WriteLine("  transport        p50        p90        p99        mean     calls/sec");
        Console.WriteLine("  --------------   --------   --------   --------   --------   ----------");

        byte[] payload = Encoding.UTF8.GetBytes("{\"deviceId\":\"tank-3\",\"c\":28.4}");

        foreach (Harness harness in harnesses)
        {
            var options = new TransportOptions { KeepAliveInterval = null };
            IListenerHost listener = harness.CreateListener(options);

            await using var server = new BlackHoleServer(listener, options);
            server.Rpc.Register("echo", request => request.Payload);
            server.Start();

            await using BlackHoleClient client = await harness.Connect(listener, options);

            for (int i = 0; i < warmup; i++)
                await client.Rpc.CallAsync("echo", payload);

            var samples = new double[measured];
            var total = Stopwatch.StartNew();
            for (int i = 0; i < measured; i++)
            {
                long start = Stopwatch.GetTimestamp();
                await client.Rpc.CallAsync("echo", payload);
                samples[i] = Stopwatch.GetElapsedTime(start).TotalMicroseconds;
            }
            total.Stop();
            Array.Sort(samples);

            Console.WriteLine(
                $"  {harness.Name,-14}   {Percentile(samples, 0.50),6:N1}us   {Percentile(samples, 0.90),6:N1}us   " +
                $"{Percentile(samples, 0.99),6:N1}us   {samples.Average(),6:N1}us   " +
                $"{measured / total.Elapsed.TotalSeconds,10:N0}");
        }
    }

    // ------------------------------------------------------------- throughput

    private static async Task ThroughputAsync(IReadOnlyList<Harness> harnesses)
    {
        const int messages = 100_000;
        await SettleAsync();

        Section("Publish throughput, 100,000 small messages");
        Console.WriteLine("  transport        one-by-one         batched (256)      speed-up");
        Console.WriteLine("  --------------   ----------------   ----------------   --------");

        byte[] payload = Encoding.UTF8.GetBytes("28.4");

        foreach (Harness harness in harnesses)
        {
            double individual = await MeasurePublishAsync(harness, messages, payload, batchSize: 0);
            double batched = await MeasurePublishAsync(harness, messages, payload, batchSize: 256);

            Console.WriteLine(
                $"  {harness.Name,-14}   {messages / individual,10:N0} msg/s   " +
                $"{messages / batched,10:N0} msg/s   {individual / batched,6:N1}x");
        }
    }

    private static async Task<double> MeasurePublishAsync(
        Harness harness, int messages, byte[] payload, int batchSize)
    {
        var options = new TransportOptions { KeepAliveInterval = null };
        IListenerHost listener = harness.CreateListener(options);

        long seen = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = new BlackHoleServer(listener, options);
        server.ClientConnected += connection =>
            connection.Router.On(Protocol.MessageType.Publish, (_, _) =>
            {
                if (Interlocked.Increment(ref seen) == messages)
                    done.TrySetResult();
            });
        server.Start();

        await using BlackHoleClient client = await harness.Connect(listener, options);

        var sw = Stopwatch.StartNew();
        if (batchSize <= 0)
        {
            for (int i = 0; i < messages; i++)
                await client.PubSub.PublishAsync("t", payload);
        }
        else
        {
            client.Batch.MaxCount = batchSize;
            client.Batch.MaxBytes = 1 << 20;
            client.Batch.MaxDelay = null;
            for (int i = 0; i < messages; i++)
                await client.Batch.AddAsync(new Protocol.BlackHoleMessage(Protocol.MessageType.Publish, "t", payload));
            await client.Batch.FlushAsync();
        }

        await done.Task.WaitAsync(TimeSpan.FromSeconds(120));
        sw.Stop();
        return sw.Elapsed.TotalSeconds;
    }

    // -------------------------------------------------------------- streaming

    private static async Task StreamingAsync(IReadOnlyList<Harness> harnesses)
    {
        const int totalBytes = 32 * 1024 * 1024;
        await SettleAsync();

        Section("Streaming, 32 MiB at a 16 KiB chunk size");
        Console.WriteLine("  transport        duration     throughput");
        Console.WriteLine("  --------------   ----------   -------------");

        var payload = new byte[totalBytes];
        Random.Shared.NextBytes(payload);

        foreach (Harness harness in harnesses)
        {
            var options = new TransportOptions { KeepAliveInterval = null };
            IListenerHost listener = harness.CreateListener(options);

            var completed = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

            await using var server = new BlackHoleServer(listener, options);
            server.ClientConnected += connection =>
            {
                connection.Streams.MaxStreamLength = long.MaxValue;
                connection.Streams.Completed += (_, e) => completed.TrySetResult(e.Length);
            };
            server.Start();

            await using BlackHoleClient client = await harness.Connect(listener, options);

            var sw = Stopwatch.StartNew();
            await client.OutgoingStreams.SendAsync(
                "bench", payload,
                new Protocol.StreamDescriptor("bench.bin", totalBytes, "application/octet-stream"),
                chunkSize: 16 * 1024);
            long received = await completed.Task.WaitAsync(TimeSpan.FromSeconds(180));
            sw.Stop();

            Console.WriteLine(
                $"  {harness.Name,-14}   {sw.Elapsed.TotalMilliseconds,8:N0} ms   " +
                $"{received / (1024.0 * 1024) / sw.Elapsed.TotalSeconds,10:N0} MiB/s");
        }
    }

    // -------------------------------------------------------------- idle cost

    /// <summary>
    /// What an idle connection costs. Shared memory polls where a socket sleeps, and this is the
    /// number that should decide whether you use it for many mostly-quiet links.
    /// </summary>
    private static async Task IdleCostAsync(IReadOnlyList<Harness> harnesses)
    {
        await SettleAsync();
        Section("CPU used by one idle connection, over 2 seconds");
        Console.WriteLine("  transport        cpu time     % of one core");
        Console.WriteLine("  --------------   ----------   -------------");

        foreach (Harness harness in harnesses)
        {
            var options = new TransportOptions { KeepAliveInterval = null };
            IListenerHost listener = harness.CreateListener(options);

            await using var server = new BlackHoleServer(listener, options);
            server.Start();

            await using BlackHoleClient client = await harness.Connect(listener, options);
            await Task.Delay(200); // let the connection settle

            TimeSpan before = Process.GetCurrentProcess().TotalProcessorTime;
            var sw = Stopwatch.StartNew();
            await Task.Delay(2000);
            sw.Stop();
            TimeSpan cpu = Process.GetCurrentProcess().TotalProcessorTime - before;

            Console.WriteLine(
                $"  {harness.Name,-14}   {cpu.TotalMilliseconds,7:N0} ms   " +
                $"{cpu.TotalMilliseconds / sw.Elapsed.TotalMilliseconds * 100,11:N1} %");
        }
    }

    // ---------------------------------------------------------------- console

    /// <summary>
    /// Lets the previous stage unwind before the next one measures.
    /// </summary>
    /// <remarks>
    /// Shared memory gives each connection a dedicated receive thread, and a stage that created
    /// dozens of them leaves those threads winding down while the next stage starts. Measuring
    /// through that contention understated shared-memory streaming by more than tenfold.
    /// </remarks>
    private static async Task SettleAsync()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        await Task.Delay(500);
    }

    private static string UniqueName() => $"bhbench-{Guid.NewGuid():N}"[..20];

    private static SharedMemoryOptions BenchmarkShared() => new()
    {
        RingCapacity = 1024 * 1024,
        SpinCount = 200,
        PollInterval = TimeSpan.FromMilliseconds(1),
    };

    private static double Percentile(double[] sorted, double percentile)
    {
        int index = (int)Math.Ceiling(percentile * sorted.Length) - 1;
        return sorted[Math.Clamp(index, 0, sorted.Length - 1)];
    }

    private static void Header()
    {
        Console.OutputEncoding = Encoding.UTF8;
        Console.WriteLine("==========================================================");
        Console.WriteLine("  BLACKHOLE MESSAGING - TRANSPORT COMPARISON");
        Console.WriteLine("==========================================================");
        Console.WriteLine($"  runtime      : {Environment.Version}");
        Console.WriteLine($"  os           : {Environment.OSVersion.VersionString}");
        Console.WriteLine($"  processors   : {Environment.ProcessorCount}");
        Console.WriteLine($"  server GC    : {System.Runtime.GCSettings.IsServerGC}");
        Console.WriteLine($"  measured     : {DateTimeOffset.Now:yyyy-MM-dd HH:mm}");
        Console.WriteLine("  note         : both ends run in one process, on one machine");
    }

    private static void Section(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"--- {title} ".PadRight(58, '-'));
    }
}
