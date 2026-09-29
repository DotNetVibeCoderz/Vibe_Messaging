using System.Text;
using BigPipe.Client.Protocol;

namespace BigPipe.Client.Tests;

public class ProtocolTests
{
    [Fact]
    public void Crc32C_matches_reference_vector() =>
        Assert.Equal(0xE3069283u, Crc32C.Compute(Encoding.ASCII.GetBytes("123456789")));

    [Theory]
    [InlineData("21", -973932308)]
    [InlineData("foobar", -790332482)]
    [InlineData("a-little-bit-long-string", -985981536)]
    [InlineData("a-little-bit-longer-string", -1486304829)]
    [InlineData("abc", 479470107)]
    public void Murmur2_matches_kafka(string input, int expected) =>
        Assert.Equal(expected, Murmur2.Hash(Encoding.UTF8.GetBytes(input)));

    [Theory]
    [InlineData(CompressionType.None)]
    [InlineData(CompressionType.Gzip)]
    [InlineData(CompressionType.Snappy)]
    [InlineData(CompressionType.Lz4)]
    [InlineData(CompressionType.Zstd)]
    public void Record_batch_roundtrip(CompressionType codec)
    {
        var b = new RecordBatchBuilder(codec);
        for (var i = 0; i < 50; i++)
        {
            var v = Encoding.UTF8.GetBytes($"{{\"n\":{i}}}");
            b.Add(Encoding.UTF8.GetBytes($"k{i}"), false, v, false, [Header.Of("h", "x")], 1000 + i);
        }
        var bytes = b.Build();
        var output = new List<RawRecord>();
        RecordBatchReader.Decode(bytes, 10, output, out var next);
        Assert.Equal(40, output.Count);
        Assert.Equal(50, next);
        Assert.Equal(10, output[0].Offset);
        Assert.Equal(1010, output[0].Timestamp);
        Assert.Equal("x", output[0].Headers[0].GetString());
        Assert.Equal("{\"n\":49}", Encoding.UTF8.GetString(output[^1].Value!.Value.Span));
    }

    [Fact]
    public void Assignors_cover_every_partition_once()
    {
        var subs = new Dictionary<string, List<string>> { ["a"] = ["t1", "t2"], ["b"] = ["t1", "t2"], ["c"] = ["t1"] };
        var counts = new Dictionary<string, int> { ["t1"] = 7, ["t2"] = 3 };
        foreach (var plan in new[] { ConsumerProtocol.Range(subs, counts), ConsumerProtocol.RoundRobin(subs, counts) })
        {
            var all = plan.Values.SelectMany(x => x).ToList();
            Assert.Equal(10, all.Count);
            Assert.Equal(10, all.Distinct().Count());
            Assert.DoesNotContain(plan["c"], p => p.Topic == "t2");
        }
        var encoded = ConsumerProtocol.EncodeAssignment([new("t1", 0), new("t1", 3)]);
        Assert.Equal([new TopicPartition("t1", 0), new TopicPartition("t1", 3)], ConsumerProtocol.DecodeAssignment(encoded));
    }
}
