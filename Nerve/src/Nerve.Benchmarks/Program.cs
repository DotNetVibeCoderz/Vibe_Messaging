// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using Nerve.Benchmarks;

if (args.Contains("--quick"))
{
    string? stage = args.SkipWhile(a => a != "--quick").Skip(1).FirstOrDefault(a => !a.StartsWith("--"));
    await ThroughputHarness.RunAsync(stage);
    return;
}

if (args.Contains("--help") || args.Length == 0)
{
    Console.WriteLine("""
        Nerve benchmarks - Gravicode Studios, led by Kang Fadhil.

          --quick [stage]     sustained-load harness, roughly a minute
                              stages: fanout, legacy, concurrent, wildcard
          --micro             BenchmarkDotNet, every suite (several minutes)
          --micro --job short BenchmarkDotNet with fewer iterations
          --filter "*Wild*"   BenchmarkDotNet, one suite

        With no arguments, this help is printed.
        """);
    return;
}

IConfig config = DefaultConfig.Instance;
if (args.Contains("--job") && args.Contains("short"))
    config = config.AddJob(Job.ShortRun);

BenchmarkSwitcher
    .FromTypes([typeof(DispatchBenchmarks), typeof(WildcardBenchmarks), typeof(PatternBenchmarks)])
    .Run(args.Where(a => a != "--micro").ToArray(), config);
