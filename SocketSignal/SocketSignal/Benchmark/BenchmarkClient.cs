using SocketSignal;
using System.Diagnostics;

namespace SocketSignal.Benchmark;

public static class BenchmarkClient
{
    public static async Task RunAsync()
    {
        var client = new SocketSignalClient();
        await client.ConnectAsync(new Uri("ws://localhost:8080/ws/"));

        const int iterations = 1000;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            await client.CallAsync("sum", i, i + 1);
        }
        sw.Stop();

        Console.WriteLine($"Benchmark: {iterations} calls in {sw.ElapsedMilliseconds} ms, avg {sw.Elapsed.TotalMilliseconds / iterations:0.000} ms/call");
    }
}
