// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using SocketSignal.Benchmarks.Legacy;

namespace SocketSignal.Benchmarks;

/// <summary>
/// End-to-end round trips over a real loopback WebSocket, for both the v1 and v2 stacks.
/// </summary>
/// <remarks>
/// BenchmarkDotNet is the right tool for the codec micro-benchmarks, but a full RPC round trip is
/// dominated by the loopback socket and the OS scheduler, so it is measured here with a plain
/// stopwatch over a large sample instead. The allocation figure is what to watch: it is the number
/// that decides whether a server holds up at ten thousand connections.
/// </remarks>
public static class ThroughputHarness
{
    public static async Task RunAsync()
    {
        Console.WriteLine();
        Console.WriteLine("=== End-to-end RPC over loopback WebSocket ===");
        Console.WriteLine($"   {RuntimeInformationLine()}");
        Console.WriteLine();

        const int warmup = 500;
        const int iterations = 20_000;

        Result legacy = await MeasureLegacyAsync(warmup, iterations);
        Result current = await MeasureCurrentAsync(warmup, iterations);

        Console.WriteLine($"{"stack",-10} {"calls",10} {"elapsed",12} {"calls/sec",14} {"latency",12} {"allocated",14}");
        Console.WriteLine(new string('-', 78));
        Print("v1", legacy);
        Print("v2", current);
        Console.WriteLine();
        Console.WriteLine($"   throughput  x{current.CallsPerSecond / legacy.CallsPerSecond:0.00}");
        Console.WriteLine($"   allocation  {(1 - (double)current.BytesPerCall / legacy.BytesPerCall) * 100:0.0}% less per call " +
                          $"({legacy.BytesPerCall:0} B -> {current.BytesPerCall:0} B)");
        Console.WriteLine();

        static void Print(string name, Result r) =>
            Console.WriteLine($"{name,-10} {r.Calls,10:N0} {r.Elapsed.TotalSeconds,11:0.000}s " +
                              $"{r.CallsPerSecond,13:N0} {r.MicrosecondsPerCall,10:0.0}us {r.BytesPerCall,11:0} B");
    }

    private readonly record struct Result(int Calls, TimeSpan Elapsed, long BytesAllocated)
    {
        public double CallsPerSecond => Calls / Elapsed.TotalSeconds;
        public double MicrosecondsPerCall => Elapsed.TotalMicroseconds / Calls;
        public double BytesPerCall => (double)BytesAllocated / Calls;
    }

    private static int FreePort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        int port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    // ---------------------------------------------------------------------------------------
    // v2
    // ---------------------------------------------------------------------------------------

    private static async Task<Result> MeasureCurrentAsync(int warmup, int iterations)
    {
        int port = FreePort();
        using var cts = new CancellationTokenSource();

        await using var server = new SocketSignalServer($"http://localhost:{port}/ws/", new SocketSignalOptions
        {
            KeepAliveInterval = Timeout.InfiniteTimeSpan,
        });
        server.Register<int, int, int>("sum", (_, a, b) => ValueTask.FromResult(a + b));
        _ = server.StartAsync(cts.Token);

        await using var client = new SocketSignalClient(new SocketSignalOptions
        {
            KeepAliveInterval = Timeout.InfiniteTimeSpan,
        });
        await client.ConnectAsync(new Uri($"ws://localhost:{port}/ws/"));

        for (int i = 0; i < warmup; i++)
            await client.CallAsync<int>("sum", i, 1);

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var clock = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
            await client.CallAsync<int>("sum", i, 1);
        clock.Stop();
        long after = GC.GetTotalAllocatedBytes(precise: true);

        await cts.CancelAsync();
        return new Result(iterations, clock.Elapsed, after - before);
    }

    // ---------------------------------------------------------------------------------------
    // v1, exactly as it was
    // ---------------------------------------------------------------------------------------

    private static async Task<Result> MeasureLegacyAsync(int warmup, int iterations)
    {
        int port = FreePort();
        using var cts = new CancellationTokenSource();

        var server = new LegacySocketSignalServer($"http://localhost:{port}/ws/");
        server.Register("sum", (_, args) =>
            Task.FromResult<object?>(args[0].GetInt32() + args[1].GetInt32()));
        _ = server.StartAsync(cts.Token);

        var client = new LegacySocketSignalClient();
        await client.ConnectAsync(new Uri($"ws://localhost:{port}/ws/"));

        for (int i = 0; i < warmup; i++)
            await client.CallAsync("sum", i, 1);

        long before = GC.GetTotalAllocatedBytes(precise: true);
        var clock = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            JsonElement? result = await client.CallAsync("sum", i, 1);
            _ = result?.GetInt32();
        }
        clock.Stop();
        long after = GC.GetTotalAllocatedBytes(precise: true);

        await cts.CancelAsync();
        return new Result(iterations, clock.Elapsed, after - before);
    }

    private static string RuntimeInformationLine() =>
        $"{System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription} on " +
        $"{System.Runtime.InteropServices.RuntimeInformation.OSDescription}, " +
        $"{Environment.ProcessorCount} logical cores";
}
