// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using SocketSignal.Benchmarks.Legacy;
using SocketSignal.Buffers;
using SocketSignal.Dispatch;
using SocketSignal.Protocol;

namespace SocketSignal.Benchmarks;

/// <summary>
/// Encoding one frame. v1 built a <see cref="string"/> with <see cref="JsonSerializer"/> and then
/// copied it into a fresh <c>byte[]</c> with <see cref="Encoding.UTF8"/> - two allocations and two
/// passes over the data for every message. v2 writes UTF-8 straight into a pooled buffer.
/// </summary>
[MemoryDiagnoser(displayGenColumns: false)]
public class EncodeBenchmarks
{
    private readonly PooledBufferWriter _buffer = new();
    private readonly Utf8JsonWriter _writer;
    private readonly JsonSerializerOptions _options = SocketSignalOptions.Default;
    private readonly object?[] _args = [5, 7];

    public EncodeBenchmarks() =>
        _writer = new Utf8JsonWriter(_buffer, new JsonWriterOptions { SkipValidation = true });

    [Benchmark(Baseline = true, Description = "v1: serialize to string, then GetBytes")]
    public byte[] Legacy()
    {
        var message = new LegacySignalMessage
        {
            Type = "invoke",
            Id = Guid.NewGuid().ToString("N"),
            Method = "sum",
            Args = [.. _args.Select(a => JsonSerializer.SerializeToElement(a, _options))],
            ExpectReturn = true,
        };
        string json = JsonSerializer.Serialize(message, _options);
        return Encoding.UTF8.GetBytes(json);
    }

    [Benchmark(Description = "v2: write UTF-8 into a pooled buffer")]
    public int Optimized()
    {
        _buffer.Reset();
        _writer.Reset(_buffer);
        SignalWriter.WriteInvoke(_writer, 42, expectReturn: true, "sum", _args, _options);
        return _buffer.WrittenCount;
    }

    [Benchmark(Description = "v2: single typed argument")]
    public int OptimizedTyped()
    {
        _buffer.Reset();
        _writer.Reset(_buffer);
        SignalWriter.WriteInvokeSingle(_writer, 42, expectReturn: true, "tick", 1234, _options);
        return _buffer.WrittenCount;
    }
}

/// <summary>
/// Decoding one frame. v1 turned the bytes into a UTF-16 string and then bound them to a POCO whose
/// every argument became a <see cref="JsonElement"/> - and therefore a <see cref="JsonDocument"/>.
/// v2 reads the envelope in place and hands the arguments to the handler as raw bytes.
/// </summary>
[MemoryDiagnoser(displayGenColumns: false)]
public class DecodeBenchmarks
{
    private static readonly byte[] Frame = Encoding.UTF8.GetBytes(
        """{"type":"invoke","id":"3","method":"sum","args":[5,7],"expectReturn":true}""");

    private readonly JsonSerializerOptions _options = SocketSignalOptions.Default;

    [Benchmark(Baseline = true, Description = "v1: GetString, then bind to a POCO")]
    public string? Legacy()
    {
        string json = Encoding.UTF8.GetString(Frame);
        LegacySignalMessage? message = JsonSerializer.Deserialize<LegacySignalMessage>(json, _options);
        return message?.Method;
    }

    [Benchmark(Description = "v2: parse in place")]
    public int Optimized()
    {
        SignalFrame.TryParse(Frame, out SignalFrame frame);
        return frame.Method.Length + frame.Args.Length;
    }

    /// <summary>Parse plus decode both arguments into ints - the whole server-side read path.</summary>
    [Benchmark(Description = "v2: parse and read both arguments")]
    public int OptimizedWithArgs()
    {
        SignalFrame.TryParse(Frame, out SignalFrame frame);
        Utf8JsonReader reader = ArgReader.Open(frame.Args);
        return ArgReader.Next<int>(ref reader, _options) + ArgReader.Next<int>(ref reader, _options);
    }
}

/// <summary>
/// Finding the handler for an incoming method name. A string-keyed dictionary has to decode the
/// name to UTF-16 first, which is an allocation per message that nothing ever reads again.
/// </summary>
[MemoryDiagnoser(displayGenColumns: false)]
public class DispatchBenchmarks
{
    private static readonly byte[] Name = "sonar.contacts.update"u8.ToArray();

    private readonly Dictionary<string, HandlerEntry> _byString = new(StringComparer.Ordinal);
    private readonly Utf8HandlerTable _byUtf8 = new();

    [GlobalSetup]
    public void Setup()
    {
        string[] methods =
        [
            "sum", "echo", "sonar.ping", "sonar.classify", "sonar.contacts.update",
            "sonar.track", "telemetry", "log", "join", "leave",
        ];

        foreach (string method in methods)
        {
            var handler = new TypedHandler<int>(_ => ValueTask.FromResult(0));
            _byString[method] = handler;
            _byUtf8.Set(method, handler);
        }
    }

    [Benchmark(Baseline = true, Description = "v1: GetString, then dictionary lookup")]
    public bool Legacy()
    {
        string method = Encoding.UTF8.GetString(Name);
        return _byString.TryGetValue(method, out HandlerEntry? handler) && handler is not null;
    }

    [Benchmark(Description = "v2: probe the UTF-8 table")]
    public bool Optimized() => _byUtf8.Find(Name) is not null;
}

/// <summary>
/// Minting a correlation id. Every call needs one, and v1 spent a 32-character string on each.
/// </summary>
[MemoryDiagnoser(displayGenColumns: false)]
public class CorrelationIdBenchmarks
{
    private readonly PooledBufferWriter _buffer = new();
    private readonly Utf8JsonWriter _writer;
    private long _next;

    public CorrelationIdBenchmarks() =>
        _writer = new Utf8JsonWriter(_buffer, new JsonWriterOptions { SkipValidation = true });

    [Benchmark(Baseline = true, Description = "v1: Guid.ToString(\"N\")")]
    public string Legacy() => Guid.NewGuid().ToString("N");

    [Benchmark(Description = "v2: counter formatted into the frame")]
    public int Optimized()
    {
        _buffer.Reset();
        _writer.Reset(_buffer);
        SignalWriter.WritePing(_writer, Interlocked.Increment(ref _next));
        return _buffer.WrittenCount;
    }
}
