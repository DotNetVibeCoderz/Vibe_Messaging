// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Buffers;
using System.Text;
using BlackHole.Buffers;
using BlackHole.Patterns;
using BlackHole.Protocol;
using Xunit;

namespace BlackHole.Tests;

public class FrameCodecTests
{
    private static readonly HeaderCache Cache = new();

    [Fact]
    public void RoundTripsEveryField()
    {
        var original = new BlackHoleMessage(
            MessageType.RpcRequest, "sensor/tank-3/temperature",
            Encoding.UTF8.GetBytes("28.4"), correlationId: 987654321, flags: MessageFlags.NoReply);

        using var writer = new PooledBufferWriter();
        int written = FrameCodec.Write(writer, original);

        var buffer = new ReadOnlySequence<byte>(writer.WrittenMemory);
        Assert.True(FrameCodec.TryRead(ref buffer, Cache, FrameCodec.DefaultMaxFrameLength,
            out BlackHoleMessage parsed, out byte[]? rented));

        Assert.Null(rented);
        Assert.Equal(written, writer.WrittenCount);
        Assert.Equal(original.Type, parsed.Type);
        Assert.Equal(original.Flags, parsed.Flags);
        Assert.Equal(original.Header, parsed.Header);
        Assert.Equal(original.CorrelationId, parsed.CorrelationId);
        Assert.Equal("28.4", parsed.PayloadAsString());
        Assert.True(buffer.IsEmpty);
    }

    [Fact]
    public void HandlesEmptyHeaderAndPayload()
    {
        using var writer = new PooledBufferWriter();
        FrameCodec.Write(writer, new BlackHoleMessage(MessageType.Ping));

        var buffer = new ReadOnlySequence<byte>(writer.WrittenMemory);
        Assert.True(FrameCodec.TryRead(ref buffer, Cache, FrameCodec.DefaultMaxFrameLength, out BlackHoleMessage parsed, out _));
        Assert.Equal(MessageType.Ping, parsed.Type);
        Assert.Equal(string.Empty, parsed.Header);
        Assert.True(parsed.Payload.IsEmpty);
    }

    [Fact]
    public void ReturnsFalseUntilTheWholeFrameArrives()
    {
        using var writer = new PooledBufferWriter();
        FrameCodec.Write(writer, new BlackHoleMessage(MessageType.Publish, "topic", Encoding.UTF8.GetBytes("body")));
        ReadOnlyMemory<byte> full = writer.WrittenMemory;

        for (int prefix = 0; prefix < full.Length; prefix++)
        {
            var partial = new ReadOnlySequence<byte>(full[..prefix]);
            Assert.False(FrameCodec.TryRead(ref partial, Cache, FrameCodec.DefaultMaxFrameLength, out _, out _));
        }

        var complete = new ReadOnlySequence<byte>(full);
        Assert.True(FrameCodec.TryRead(ref complete, Cache, FrameCodec.DefaultMaxFrameLength, out _, out _));
    }

    [Fact]
    public void ParsesAPayloadSplitAcrossSegments()
    {
        byte[] payload = new byte[8192];
        Random.Shared.NextBytes(payload);

        using var writer = new PooledBufferWriter();
        FrameCodec.Write(writer, new BlackHoleMessage(MessageType.StreamChunk, "video", payload, 7));

        // Two segments, split mid payload: this is the path that rents a copy.
        ReadOnlyMemory<byte> full = writer.WrittenMemory;
        ReadOnlySequence<byte> sequence = Segment.Build(full, 100);

        Assert.True(FrameCodec.TryRead(ref sequence, Cache, FrameCodec.DefaultMaxFrameLength,
            out BlackHoleMessage parsed, out byte[]? rented));

        Assert.NotNull(rented);
        Assert.Equal(7, parsed.CorrelationId);
        Assert.True(payload.AsSpan().SequenceEqual(parsed.Payload.Span));
        ArrayPool<byte>.Shared.Return(rented!);
    }

    [Fact]
    public void RejectsAFrameLongerThanTheLimit()
    {
        using var writer = new PooledBufferWriter();
        FrameCodec.Write(writer, new BlackHoleMessage(MessageType.Publish, "t", new byte[4096]));
        var buffer = new ReadOnlySequence<byte>(writer.WrittenMemory);

        Assert.Throws<BlackHoleProtocolException>(() =>
        {
            ReadOnlySequence<byte> local = buffer;
            FrameCodec.TryRead(ref local, Cache, maxFrameLength: 128, out _, out _);
        });
    }

    [Fact]
    public void RejectsAnImpossibleLengthPrefix()
    {
        var garbage = new byte[] { 2, 0, 0, 0, 1, 2 };
        Assert.Throws<BlackHoleProtocolException>(() =>
        {
            var buffer = new ReadOnlySequence<byte>(garbage);
            FrameCodec.TryRead(ref buffer, Cache, FrameCodec.DefaultMaxFrameLength, out _, out _);
        });
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public static ReadOnlySequence<byte> Build(ReadOnlyMemory<byte> data, int firstLength)
        {
            var first = new Segment { Memory = data[..firstLength] };
            var second = new Segment { Memory = data[firstLength..], RunningIndex = firstLength };
            first.Next = second;
            return new ReadOnlySequence<byte>(first, 0, second, second.Memory.Length);
        }
    }
}

public class HeaderCacheTests
{
    [Fact]
    public void ReturnsTheSameInstanceForRepeatedHeaders()
    {
        var cache = new HeaderCache(64);
        byte[] bytes = Encoding.UTF8.GetBytes("sensor/pump-1/pressure");

        string first = cache.GetString(bytes);
        string second = cache.GetString(bytes);

        Assert.Equal("sensor/pump-1/pressure", first);
        Assert.Same(first, second);
        Assert.Equal(1, cache.Hits);
        Assert.Equal(1, cache.Misses);
    }

    [Fact]
    public void DecodesDistinctHeadersCorrectlyDespiteSlotReuse()
    {
        var cache = new HeaderCache(8);
        for (int i = 0; i < 200; i++)
        {
            string header = $"topic/{i}";
            Assert.Equal(header, cache.GetString(Encoding.UTF8.GetBytes(header)));
        }
    }

    [Fact]
    public void HandlesNonAsciiHeaders()
    {
        var cache = new HeaderCache();
        const string header = "suhu/tangki/derajat-°C";
        Assert.Equal(header, cache.GetString(Encoding.UTF8.GetBytes(header)));
    }
}

public class TopicFilterTests
{
    [Theory]
    [InlineData("sensor/tank-3/temp", "sensor/tank-3/temp", true)]
    [InlineData("sensor/+/temp", "sensor/tank-3/temp", true)]
    [InlineData("sensor/+/temp", "sensor/tank-3/humidity", false)]
    [InlineData("sensor/+/temp", "sensor/a/b/temp", false)]
    [InlineData("sensor/#", "sensor/tank-3/temp", true)]
    [InlineData("sensor/#", "sensor", false)]
    [InlineData("#", "anything/at/all", true)]
    [InlineData("sensor/tank-3/temp", "sensor/tank-3", false)]
    [InlineData("sensor/tank-3", "sensor/tank-3/temp", false)]
    [InlineData("+/+/temp", "sensor/tank-3/temp", true)]
    public void MatchesLikeMqtt(string filter, string topic, bool expected) =>
        Assert.Equal(expected, TopicFilter.Matches(filter, topic));
}

public class PooledBufferWriterTests
{
    [Fact]
    public void GrowsAndKeepsContent()
    {
        using var writer = new PooledBufferWriter(16);
        var payload = new byte[10_000];
        Random.Shared.NextBytes(payload);

        for (int offset = 0; offset < payload.Length; offset += 500)
            writer.Write(payload.AsSpan(offset, Math.Min(500, payload.Length - offset)));

        Assert.Equal(payload.Length, writer.WrittenCount);
        Assert.True(payload.AsSpan().SequenceEqual(writer.WrittenSpan));
    }

    [Fact]
    public void ResetKeepsTheRentedArray()
    {
        using var writer = new PooledBufferWriter(1024);
        writer.Write(new byte[512]);
        int capacity = writer.Capacity;

        writer.Reset();

        Assert.Equal(0, writer.WrittenCount);
        Assert.Equal(capacity, writer.Capacity);
    }
}

public class StreamDescriptorTests
{
    [Fact]
    public void RoundTrips()
    {
        var original = new StreamDescriptor("kalibrasi-2026.csv", 1_048_576, "text/csv");
        StreamDescriptor parsed = StreamDescriptor.Decode(original.Encode());

        Assert.Equal(original, parsed);
        Assert.True(parsed.HasLength);
    }

    [Fact]
    public void DecodesAnEmptyPayloadAsUnknown()
    {
        StreamDescriptor parsed = StreamDescriptor.Decode(ReadOnlySpan<byte>.Empty);
        Assert.False(parsed.HasLength);
    }
}
