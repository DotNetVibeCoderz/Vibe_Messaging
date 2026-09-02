// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using BenchmarkDotNet.Running;
using SocketSignal.Benchmarks;

// dotnet run -c Release --project src/SocketSignal.Benchmarks -- throughput
//   real RPC round trips over loopback, v1 against v2
// dotnet run -c Release --project src/SocketSignal.Benchmarks -- micro
//   BenchmarkDotNet over the codec and dispatch paths

string mode = args.Length > 0 ? args[0].ToLowerInvariant() : "all";

if (mode is "throughput" or "all")
    await ThroughputHarness.RunAsync();

if (mode is "alloc" or "throughput" or "all")
    AllocationReport.Run();

if (mode is "micro" or "all")
{
    BenchmarkRunner.Run(
    [
        BenchmarkConverter.TypeToBenchmarks(typeof(EncodeBenchmarks)),
        BenchmarkConverter.TypeToBenchmarks(typeof(DecodeBenchmarks)),
        BenchmarkConverter.TypeToBenchmarks(typeof(DispatchBenchmarks)),
        BenchmarkConverter.TypeToBenchmarks(typeof(CorrelationIdBenchmarks)),
    ]);
}
