// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;
using BlackHole.Buffers;
using BlackHole.Protocol;

namespace BlackHole.Benchmarks;

/// <summary>
/// Framing in isolation - no sockets, no scheduler. This is where the per-message allocation claim
/// is proved or disproved, so <see cref="MemoryDiagnoserAttribute"/> matters more than the timings.
/// </summary>
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public class CodecBenchmarks
{
    private readonly HeaderCache _cache = new();
    private PooledBufferWriter _writer = null!;
    private byte[] _encodedFrame = null!;
    private BlackHoleMessage _message;

    /// <summary>Payload sizes: a sensor reading, a small document, a stream chunk.</summary>
    [Params(16, 512, 4096)]
    public int PayloadSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var payload = new byte[PayloadSize];
        Random.Shared.NextBytes(payload);
        _message = new BlackHoleMessage(MessageType.Publish, "sensor/tank-3/temperature", payload, 42);

        _writer = new PooledBufferWriter(64 * 1024);

        using var scratch = new PooledBufferWriter(64 * 1024);
        FrameCodec.Write(scratch, _message);
        _encodedFrame = scratch.ToArray();

        _cache.Prime("sensor/tank-3/temperature");
    }

    [GlobalCleanup]
    public void Cleanup() => _writer.Dispose();

    [Benchmark(Description = "Encode one frame")]
    public int Encode()
    {
        _writer.Reset();
        return FrameCodec.Write(_writer, _message);
    }

    [Benchmark(Description = "Decode one frame")]
    public int Decode()
    {
        var buffer = new ReadOnlySequence<byte>(_encodedFrame);
        FrameCodec.TryRead(ref buffer, _cache, FrameCodec.DefaultMaxFrameLength,
            out BlackHoleMessage message, out byte[]? rented);
        if (rented is not null)
            ArrayPool<byte>.Shared.Return(rented);
        return message.Payload.Length;
    }

    [Benchmark(Description = "Encode then decode")]
    public int RoundTrip()
    {
        _writer.Reset();
        FrameCodec.Write(_writer, _message);

        var buffer = new ReadOnlySequence<byte>(_writer.WrittenMemory);
        FrameCodec.TryRead(ref buffer, _cache, FrameCodec.DefaultMaxFrameLength,
            out BlackHoleMessage message, out byte[]? rented);
        if (rented is not null)
            ArrayPool<byte>.Shared.Return(rented);
        return message.Payload.Length;
    }
}

/// <summary>
/// The header cache against a plain UTF-8 decode. Every received message pays this cost, so the gap
/// here is multiplied by the whole message rate.
/// </summary>
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public class HeaderDecodeBenchmarks
{
    private readonly HeaderCache _cache = new();
    private byte[] _header = null!;

    [GlobalSetup]
    public void Setup()
    {
        _header = Encoding.UTF8.GetBytes("sensor/tank-3/temperature");
        _cache.GetString(_header);
    }

    [Benchmark(Baseline = true, Description = "Encoding.UTF8.GetString")]
    public string PlainDecode() => Encoding.UTF8.GetString(_header);

    [Benchmark(Description = "HeaderCache.GetString")]
    public string CachedDecode() => _cache.GetString(_header);
}

/// <summary>
/// Batch packing and unpacking. The interesting number is the per-message cost once the envelope
/// amortises the frame overhead.
/// </summary>
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public class BatchCodecBenchmarks
{
    private BlackHoleMessage[] _messages = null!;
    private PooledBufferWriter _writer = null!;
    private byte[] _encodedBatch = null!;
    private readonly HeaderCache _cache = new();

    [Params(16, 256)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var payload = Encoding.UTF8.GetBytes("28.4");
        _messages = new BlackHoleMessage[MessageCount];
        for (int i = 0; i < MessageCount; i++)
            _messages[i] = new BlackHoleMessage(MessageType.Publish, "sensor/tank-3/temperature", payload, i);

        _writer = new PooledBufferWriter(256 * 1024);
        using var scratch = new PooledBufferWriter(256 * 1024);
        foreach (BlackHoleMessage message in _messages)
            FrameCodec.Write(scratch, message);
        _encodedBatch = scratch.ToArray();
        _cache.Prime("sensor/tank-3/temperature");
    }

    [GlobalCleanup]
    public void Cleanup() => _writer.Dispose();

    [Benchmark(Description = "Pack a batch envelope")]
    public int Pack()
    {
        _writer.Reset();
        foreach (BlackHoleMessage message in _messages)
            FrameCodec.Write(_writer, message);
        return _writer.WrittenCount;
    }

    [Benchmark(Description = "Unpack a batch envelope")]
    public int Unpack()
    {
        var buffer = new ReadOnlySequence<byte>(_encodedBatch);
        int count = 0;
        while (FrameCodec.TryRead(ref buffer, _cache, int.MaxValue, out BlackHoleMessage _, out byte[]? rented))
        {
            count++;
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
        return count;
    }
}

/// <summary>Topic matching, which the broker runs once per wildcard filter per publish.</summary>
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public class TopicFilterBenchmarks
{
    private const string Topic = "sensor/tank-3/temperature";

    [Benchmark(Description = "Exact match")]
    public bool Exact() => Patterns.TopicFilter.Matches("sensor/tank-3/temperature", Topic);

    [Benchmark(Description = "Single-segment wildcard")]
    public bool SingleWildcard() => Patterns.TopicFilter.Matches("sensor/+/temperature", Topic);

    [Benchmark(Description = "Multi-segment wildcard")]
    public bool MultiWildcard() => Patterns.TopicFilter.Matches("sensor/#", Topic);

    [Benchmark(Description = "Non-matching filter")]
    public bool NoMatch() => Patterns.TopicFilter.Matches("sensor/+/humidity", Topic);
}
