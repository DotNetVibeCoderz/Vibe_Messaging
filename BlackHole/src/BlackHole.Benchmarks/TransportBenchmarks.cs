// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Text;
using BenchmarkDotNet.Attributes;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.Benchmarks;

/// <summary>
/// End to end over a loopback socket. Slower and noisier than <see cref="CodecBenchmarks"/>, but
/// these are the numbers an application actually sees.
/// </summary>
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
[MinIterationCount(5)]
[MaxIterationCount(20)]
public class RpcBenchmarks
{
    private BlackHoleServer _server = null!;
    private BlackHoleClient _client = null!;
    private byte[] _payload = null!;

    /// <summary>Sensor reading, JSON document, small file.</summary>
    [Params(16, 1024)]
    public int PayloadSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var options = new TransportOptions { KeepAliveInterval = null };

        _server = new BlackHoleServer(0, options);
        _server.Rpc.Register("echo", request => request.Payload);
        _server.Start();

        _client = BlackHoleClient.ConnectAsync("127.0.0.1", _server.EndPoint.Port, options).GetAwaiter().GetResult();

        _payload = new byte[PayloadSize];
        Random.Shared.NextBytes(_payload);
        _client.Rpc.CallAsync("echo", _payload).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _server.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark(Description = "One RPC round trip")]
    public async Task<int> RoundTrip()
    {
        byte[] result = await _client.Rpc.CallAsync("echo", _payload);
        return result.Length;
    }

    [Benchmark(Description = "32 concurrent RPC round trips")]
    public async Task<int> Pipelined()
    {
        var calls = new Task<byte[]>[32];
        for (int i = 0; i < calls.Length; i++)
            calls[i] = _client.Rpc.CallAsync("echo", _payload);
        byte[][] results = await Task.WhenAll(calls);
        return results.Length;
    }
}

/// <summary>
/// The point of batching: how much a hundred small publishes cost sent one at a time versus packed
/// into one envelope.
/// </summary>
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
[MinIterationCount(5)]
[MaxIterationCount(20)]
public class PublishBenchmarks
{
    private const int MessageCount = 100;

    private BlackHoleServer _server = null!;
    private BlackHoleClient _client = null!;
    private BlackHoleMessage[] _messages = null!;
    private byte[] _payload = null!;

    [GlobalSetup]
    public void Setup()
    {
        var options = new TransportOptions { KeepAliveInterval = null };
        _server = new BlackHoleServer(0, options);
        _server.Start();
        _client = BlackHoleClient.ConnectAsync("127.0.0.1", _server.EndPoint.Port, options).GetAwaiter().GetResult();

        _payload = Encoding.UTF8.GetBytes("28.4");
        _messages = new BlackHoleMessage[MessageCount];
        for (int i = 0; i < MessageCount; i++)
            _messages[i] = new BlackHoleMessage(MessageType.Publish, "sensor/tank-3/temperature", _payload, i);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _server.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true, Description = "100 publishes, one send each")]
    public async Task Individually()
    {
        foreach (BlackHoleMessage message in _messages)
            await _client.SendAsync(message);
    }

    [Benchmark(Description = "100 publishes, write then one flush")]
    public async Task WriteThenFlush()
    {
        foreach (BlackHoleMessage message in _messages)
            await _client.Transport.WriteAsync(message);
        await _client.Transport.FlushAsync();
    }

    [Benchmark(Description = "100 publishes in one batch envelope")]
    public async Task Batched() => await _client.Batch.SendBatchAsync(_messages);
}

/// <summary>Streaming throughput at a few chunk sizes, to show where the flush threshold pays off.</summary>
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
[MinIterationCount(3)]
[MaxIterationCount(10)]
public class StreamingBenchmarks
{
    private BlackHoleServer _server = null!;
    private BlackHoleClient _client = null!;
    private byte[] _payload = null!;

    /// <summary>4 MiB per transfer.</summary>
    private const int TotalBytes = 4 * 1024 * 1024;

    [Params(4096, 16384, 65536)]
    public int ChunkSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var options = new TransportOptions { KeepAliveInterval = null };
        _server = new BlackHoleServer(0, options);
        _server.ClientConnected += connection => connection.Streams.MaxStreamLength = long.MaxValue;
        _server.Start();
        _client = BlackHoleClient.ConnectAsync("127.0.0.1", _server.EndPoint.Port, options).GetAwaiter().GetResult();

        _payload = new byte[TotalBytes];
        Random.Shared.NextBytes(_payload);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _server.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark(Description = "Stream 4 MiB")]
    public async Task<long> Stream4MiB() =>
        await _client.OutgoingStreams.SendAsync(
            $"bench-{Guid.NewGuid():N}", _payload,
            new StreamDescriptor("bench.bin", TotalBytes, "application/octet-stream"),
            ChunkSize);
}
