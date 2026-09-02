// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Text;
using System.Text.Json;
using SocketSignal.Benchmarks.Legacy;
using SocketSignal.Buffers;
using SocketSignal.Dispatch;
using SocketSignal.Protocol;

namespace SocketSignal.Benchmarks;

/// <summary>
/// Bytes allocated per operation, measured over a large run with
/// <see cref="GC.GetTotalAllocatedBytes(bool)"/>.
/// </summary>
/// <remarks>
/// BenchmarkDotNet owns the timing numbers; this owns the allocation numbers. Measuring over a
/// hundred thousand operations is what makes pooled buffers show up honestly: a pool rent looks
/// like an allocation the first time and like nothing every time after, which is exactly the
/// behaviour that matters on a long-lived connection.
/// </remarks>
public static class AllocationReport
{
    private const int Operations = 200_000;

    public static void Run()
    {
        Console.WriteLine();
        Console.WriteLine("=== Bytes allocated per operation ===");
        Console.WriteLine();
        Console.WriteLine($"{"operation",-34} {"v1",13} {"v2",12} {"saved",10}");
        Console.WriteLine(new string('-', 74));

        Report("encode one invoke frame", EncodeLegacy, EncodeCurrent);
        Report("decode one invoke frame", DecodeLegacy, DecodeCurrent);
        Report("find the handler", LookupLegacy, LookupCurrent);
        Report("mint a correlation id", IdLegacy, IdCurrent);
        Console.WriteLine();
    }

    private static void Report(string name, Action legacy, Action current)
    {
        double before = Measure(legacy);
        double after = Measure(current);
        double saved = before == 0 ? 0 : (1 - after / before) * 100;
        Console.WriteLine($"{name,-34} {before,11:0.0} B {after,10:0.0} B {saved,9:0.0}%");
    }

    private static double Measure(Action operation)
    {
        for (int i = 0; i < 2_000; i++) operation();   // warm up, and let the pools fill

        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < Operations; i++) operation();
        long after = GC.GetTotalAllocatedBytes(precise: true);

        return (double)(after - before) / Operations;
    }

    // ---------------------------------------------------------------------------------------

    private static readonly JsonSerializerOptions Options = SocketSignalOptions.Default;
    private static readonly object?[] Args = [5, 7];

    private static readonly byte[] Frame = Encoding.UTF8.GetBytes(
        """{"type":"invoke","id":"3","method":"sum","args":[5,7],"expectReturn":true}""");

    private static readonly byte[] MethodName = "sonar.contacts.update"u8.ToArray();

    private static readonly PooledBufferWriter Buffer = new();
    private static readonly Utf8JsonWriter Writer = new(Buffer, new JsonWriterOptions { SkipValidation = true });

    private static readonly Dictionary<string, HandlerEntry> ByString = new(StringComparer.Ordinal);
    private static readonly Utf8HandlerTable ByUtf8 = new();

    static AllocationReport()
    {
        foreach (string method in new[]
                 {
                     "sum", "echo", "sonar.ping", "sonar.classify", "sonar.contacts.update",
                     "sonar.track", "telemetry", "log", "join", "leave",
                 })
        {
            var handler = new TypedHandler<int>(_ => ValueTask.FromResult(0));
            ByString[method] = handler;
            ByUtf8.Set(method, handler);
        }
    }

    private static void EncodeLegacy()
    {
        var message = new LegacySignalMessage
        {
            Type = "invoke",
            Id = Guid.NewGuid().ToString("N"),
            Method = "sum",
            Args = [JsonSerializer.SerializeToElement(5, Options), JsonSerializer.SerializeToElement(7, Options)],
            ExpectReturn = true,
        };
        _ = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, Options));
    }

    private static void EncodeCurrent()
    {
        Buffer.Reset();
        Writer.Reset(Buffer);
        SignalWriter.WriteInvoke(Writer, 42, expectReturn: true, "sum", Args, Options);
    }

    private static void DecodeLegacy()
    {
        string json = Encoding.UTF8.GetString(Frame);
        LegacySignalMessage? message = JsonSerializer.Deserialize<LegacySignalMessage>(json, Options);
        _ = message?.Args?[0].GetInt32() + message?.Args?[1].GetInt32();
    }

    private static void DecodeCurrent()
    {
        SignalFrame.TryParse(Frame, out SignalFrame frame);
        Utf8JsonReader reader = ArgReader.Open(frame.Args);
        _ = ArgReader.Next<int>(ref reader, Options) + ArgReader.Next<int>(ref reader, Options);
    }

    private static void LookupLegacy() => ByString.TryGetValue(Encoding.UTF8.GetString(MethodName), out _);

    private static void LookupCurrent() => _ = ByUtf8.Find(MethodName);

    private static long _next;

    private static void IdLegacy() => _ = Guid.NewGuid().ToString("N");

    private static void IdCurrent()
    {
        Buffer.Reset();
        Writer.Reset(Buffer);
        SignalWriter.WritePing(Writer, Interlocked.Increment(ref _next));
    }
}
