// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using System.Diagnostics;
using System.Runtime.InteropServices;
using Nerve.Benchmarks.Baseline;

namespace Nerve.Benchmarks;

/// <summary>
/// Sustained load rather than per-call timing: how many messages a second the hub carries, and how
/// many bytes it asks the GC for while doing it.
/// </summary>
/// <remarks>
/// BenchmarkDotNet answers "what does one publish cost". This answers "what happens when you do it
/// for a while", which is the question the v1 README was really asking with its million-message
/// loop. The two are kept separate because the second one is the noisier of the two.
/// </remarks>
internal static class ThroughputHarness
{
    private const int Messages = 5_000_000;

    public static async Task RunAsync(string? stage)
    {
        Console.WriteLine();
        Console.WriteLine($"Sustained load - {Messages:N0} messages per run");
        Console.WriteLine($"  {RuntimeInformation.FrameworkDescription}, {Environment.ProcessorCount} logical cores");
        Console.WriteLine();

        if (stage is null or "fanout") FanOut();
        if (stage is null or "legacy") await LegacyComparisonAsync();
        if (stage is null or "concurrent") Concurrent();
        if (stage is null or "wildcard") Wildcards();
    }

    /// <summary>One publisher, a growing number of subscribers on the same topic.</summary>
    private static void FanOut()
    {
        Header("Fan-out: one topic, N subscribers, single publisher");

        foreach (int subscribers in new[] { 1, 2, 8, 32 })
        {
            using var hub = new NerveHub();
            long received = 0;
            var tokens = new List<IDisposable>();
            for (int i = 0; i < subscribers; i++) tokens.Add(hub.Subscribe<int>("bench", _ => received++));

            NerveTopic<int> topic = hub.Topic<int>("bench");
            Measure($"{subscribers,3} subscriber(s)", Messages, () =>
            {
                for (int i = 0; i < Messages; i++) topic.Publish(i);
            }, extra: () => $"{received:N0} deliveries");

            foreach (IDisposable token in tokens) token.Dispose();
        }
    }

    /// <summary>The same work through v1 and v2, side by side.</summary>
    private static async Task LegacyComparisonAsync()
    {
        Header("Against v1: one topic, one subscriber");

        var legacy = new LegacyHub();
        long legacyReceived = 0;
        legacy.Subscribe<int>("bench", _ => legacyReceived++);
        Measure("v1  Publish(topic, value)", Messages, () =>
        {
            for (int i = 0; i < Messages; i++) legacy.Publish("bench", i);
        }, extra: () => $"{legacyReceived:N0} received");

        using var hub = new NerveHub();
        long received = 0;
        using IDisposable _ = hub.Subscribe<int>("bench", _ => received++);
        Measure("v2  Publish(topic, value)", Messages, () =>
        {
            for (int i = 0; i < Messages; i++) hub.Publish("bench", i);
        }, extra: () => $"{received:N0} received");

        NerveTopic<int> handle = hub.Topic<int>("bench");
        Measure("v2  handle.Publish(value)", Messages, () =>
        {
            for (int i = 0; i < Messages; i++) handle.Publish(i);
        });

        await Task.CompletedTask;
    }

    /// <summary>Every core publishing to its own topic at once.</summary>
    private static void Concurrent()
    {
        Header("Concurrent publishers, one topic each");

        int workers = Environment.ProcessorCount;
        int perWorker = Messages / workers;

        using var hub = new NerveHub();
        long received = 0;
        var tokens = new List<IDisposable>();
        for (int w = 0; w < workers; w++)
        {
            tokens.Add(hub.Subscribe<int>($"bench/{w}", _ => Interlocked.Increment(ref received)));
        }

        Measure($"{workers} publishers", perWorker * workers, () =>
        {
            Parallel.For(0, workers, w =>
            {
                NerveTopic<int> topic = hub.Topic<int>($"bench/{w}");
                for (int i = 0; i < perWorker; i++) topic.Publish(i);
            });
        }, extra: () => $"{received:N0} deliveries");

        foreach (IDisposable token in tokens) token.Dispose();
    }

    /// <summary>Whether a wildcard subscriber costs anything once the route is resolved.</summary>
    private static void Wildcards()
    {
        Header("Wildcard routing");

        using var exact = new NerveHub();
        long a = 0;
        using IDisposable _1 = exact.Subscribe<int>("agents/task/writer", _ => a++);
        NerveTopic<int> exactTopic = exact.Topic<int>("agents/task/writer");
        Measure("exact subscriber", Messages, () =>
        {
            for (int i = 0; i < Messages; i++) exactTopic.Publish(i);
        });

        using var wild = new NerveHub();
        long b = 0;
        using IDisposable _2 = wild.Subscribe<int>("agents/+/writer", _ => b++);
        NerveTopic<int> wildTopic = wild.Topic<int>("agents/task/writer");
        Measure("one wildcard subscriber", Messages, () =>
        {
            for (int i = 0; i < Messages; i++) wildTopic.Publish(i);
        }, extra: () => $"{b:N0} deliveries");

        Header("Route resolution (first publish to a new topic)");
        using var cold = new NerveHub();
        using IDisposable _3 = cold.Subscribe<int>("agents/#", _ => { });
        const int topics = 100_000;
        Measure($"{topics:N0} distinct topics", topics, () =>
        {
            for (int i = 0; i < topics; i++) cold.Publish($"agents/task/{i}", i);
        });
    }

    // ============================== Measurement ==============================

    private static void Header(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"  {title}");
        Console.WriteLine($"  {new string('-', title.Length)}");
    }

    private static void Measure(string label, int messages, Action work, Func<string>? extra = null)
    {
        // One full warm-up pass so tiering and the branch predictors have settled.
        work();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Process-wide rather than per-thread: the concurrent stage allocates on the pool's threads.
        long before = GC.GetTotalAllocatedBytes(precise: true);
        int gen0 = GC.CollectionCount(0);
        var stopwatch = Stopwatch.StartNew();
        work();
        stopwatch.Stop();
        long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        int collections = GC.CollectionCount(0) - gen0;

        double perSecond = messages / stopwatch.Elapsed.TotalSeconds;
        double nanos = stopwatch.Elapsed.TotalMilliseconds * 1_000_000 / messages;

        Console.WriteLine(
            $"    {label,-26} {perSecond,14:N0} msg/s   {nanos,7:N1} ns/msg   " +
            $"{Bytes(allocated),10} alloc   {collections,3} gen0" +
            (extra is null ? string.Empty : $"   {extra()}"));
    }

    private static string Bytes(long value) => value switch
    {
        < 1024 => $"{value} B",
        < 1024 * 1024 => $"{value / 1024.0:N1} KB",
        _ => $"{value / (1024.0 * 1024):N1} MB",
    };
}
