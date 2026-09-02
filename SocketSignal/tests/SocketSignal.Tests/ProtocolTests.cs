// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Text;
using System.Text.Json;
using SocketSignal.Buffers;
using SocketSignal.Dispatch;
using SocketSignal.Protocol;
using Xunit;

namespace SocketSignal.Tests;

public class ProtocolTests
{
    private static string Encode(Action<Utf8JsonWriter> write)
    {
        using var buffer = new PooledBufferWriter();
        using var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { SkipValidation = true });
        write(writer);
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    [Fact]
    public void Parses_an_invoke_with_arguments()
    {
        ReadOnlySpan<byte> json = """
            {"type":"invoke","id":"7","method":"sum","args":[5,7],"expectReturn":true}
            """u8;

        Assert.True(SignalFrame.TryParse(json, out SignalFrame frame));
        Assert.Equal(MessageType.Invoke, frame.Type);
        Assert.Equal("7", Encoding.UTF8.GetString(frame.Id));
        Assert.Equal("sum", Encoding.UTF8.GetString(frame.Method));
        Assert.Equal("[5,7]", Encoding.UTF8.GetString(frame.Args));
        Assert.True(frame.ExpectReturn);
        Assert.False(frame.HasError);
    }

    [Fact]
    public void Parses_a_result_and_an_error()
    {
        Assert.True(SignalFrame.TryParse("""{"type":"result","id":"7","result":{"a":1}}"""u8, out SignalFrame ok));
        Assert.Equal(MessageType.Result, ok.Type);
        Assert.Equal("""{"a":1}""", Encoding.UTF8.GetString(ok.Result));
        Assert.False(ok.HasError);

        Assert.True(SignalFrame.TryParse("""{"type":"result","id":"7","error":"boom"}"""u8, out SignalFrame bad));
        Assert.True(bad.HasError);
        Assert.Equal("boom", Encoding.UTF8.GetString(bad.Error));
    }

    [Fact]
    public void Ignores_properties_it_does_not_know()
    {
        // The protocol has to be able to grow without old peers falling over.
        ReadOnlySpan<byte> json = """
            {"type":"invoke","trace":{"span":"abc"},"method":"ping","args":[],"future":[1,2,3]}
            """u8;

        Assert.True(SignalFrame.TryParse(json, out SignalFrame frame));
        Assert.Equal(MessageType.Invoke, frame.Type);
        Assert.Equal("ping", Encoding.UTF8.GetString(frame.Method));
    }

    [Fact]
    public void Rejects_junk()
    {
        Assert.False(SignalFrame.TryParse("not json at all"u8, out _));
        Assert.False(SignalFrame.TryParse("[1,2,3]"u8, out _));
    }

    [Fact]
    public void Accepts_a_numeric_id_from_a_hand_rolled_client()
    {
        Assert.True(SignalFrame.TryParse("""{"type":"result","id":42,"result":1}"""u8, out SignalFrame frame));
        Assert.Equal("42", Encoding.UTF8.GetString(frame.Id));
    }

    [Fact]
    public void Writes_an_invoke_a_browser_can_read()
    {
        string json = Encode(w => SignalWriter.WriteInvoke(
            w, callId: 3, expectReturn: true, "sum", [5, 7], SocketSignalOptions.Default));

        Assert.Equal("""{"type":"invoke","id":"3","method":"sum","args":[5,7],"expectReturn":true}""", json);
    }

    [Fact]
    public void Omits_expectReturn_when_no_reply_is_wanted()
    {
        string json = Encode(w => SignalWriter.WriteInvoke(
            w, callId: 1, expectReturn: false, "log", ["hi"], SocketSignalOptions.Default));

        Assert.DoesNotContain("expectReturn", json);
    }

    [Fact]
    public void Round_trips_what_it_writes()
    {
        string json = Encode(w => SignalWriter.WriteResult(w, "9"u8, new { ok = true }, SocketSignalOptions.Default));

        Assert.True(SignalFrame.TryParse(Encoding.UTF8.GetBytes(json), out SignalFrame frame));
        Assert.Equal(MessageType.Result, frame.Type);
        Assert.Equal("9", Encoding.UTF8.GetString(frame.Id));
        Assert.Equal("""{"ok":true}""", Encoding.UTF8.GetString(frame.Result));
    }

    [Fact]
    public void Handler_table_finds_by_utf8_name()
    {
        var table = new Utf8HandlerTable();
        var handler = new TypedHandler<int>(_ => ValueTask.FromResult(1));

        table.Set("sonar.classify", handler);
        table.Set("sum", handler);

        Assert.Same(handler, table.Find("sum"u8));
        Assert.Same(handler, table.Find("sonar.classify"u8));
        Assert.Null(table.Find("nope"u8));
        Assert.Null(table.Find("su"u8));
        Assert.Null(table.Find("sums"u8));

        Assert.True(table.Remove("sum"));
        Assert.Null(table.Find("sum"u8));
    }

    [Fact]
    public void Handler_table_survives_a_rebuild_past_its_initial_capacity()
    {
        var table = new Utf8HandlerTable();
        for (int i = 0; i < 200; i++)
            table.Set($"method{i}", new TypedHandler<int>(_ => ValueTask.FromResult(i)));

        for (int i = 0; i < 200; i++)
            Assert.NotNull(table.Find(Encoding.UTF8.GetBytes($"method{i}")));

        Assert.Equal(200, table.Count);
    }

    [Fact]
    public void Pooled_writer_grows_and_keeps_its_contents()
    {
        using var buffer = new PooledBufferWriter(8);
        var payload = new byte[5000];
        Random.Shared.NextBytes(payload);

        payload.AsSpan().CopyTo(buffer.GetSpan(payload.Length));
        buffer.Advance(payload.Length);

        Assert.Equal(payload.Length, buffer.WrittenCount);
        Assert.True(buffer.WrittenSpan.SequenceEqual(payload));

        buffer.Reset();
        Assert.Equal(0, buffer.WrittenCount);
    }
}
