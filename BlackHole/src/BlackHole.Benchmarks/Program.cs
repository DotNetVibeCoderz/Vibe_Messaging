// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

namespace BlackHole.Benchmarks;

internal static class Program
{
    /// <summary>
    /// Two modes, because they answer different questions.
    /// <c>--quick</c> runs the sustained-load harness: latency percentiles, aggregate rates,
    /// streaming bandwidth. Anything else goes to BenchmarkDotNet for precise per-operation timings
    /// and allocation counts.
    /// </summary>
    private static async Task Main(string[] args)
    {
        if (args.Contains("--quick"))
        {
            // Anything after --quick that is not a flag names a stage to run on its own.
            string[] stages = args
                .SkipWhile(a => a != "--quick").Skip(1)
                .TakeWhile(a => !a.StartsWith('-'))
                .ToArray();
            ThroughputHarness.Verbose = !args.Contains("--terse");
            await ThroughputHarness.RunAsync(stages);
            return;
        }

        BenchmarkSwitcher
            .FromAssembly(typeof(Program).Assembly)
            .Run(args, DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator));
    }
}
