// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Net;
using System.Text;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;
using Xunit;

namespace BlackHole.Tests;

/// <summary>
/// One suite, run unchanged over every transport.
/// </summary>
/// <remarks>
/// <para>
/// This is the point of the whole design: TCP, Unix domain sockets, named pipes and shared memory
/// share one framing implementation and one set of patterns, so they must behave identically. A
/// test that passes on TCP and fails on shared memory means the abstraction leaked.
/// </para>
/// <para>
/// A transport the platform cannot provide returns early rather than failing. xunit 2.x has no
/// runtime skip, so <see cref="UnsupportedReason"/> is asserted separately by
/// <see cref="TransportSupportTests"/> - that way an unavailable transport is visible rather than
/// quietly counted as a pass.
/// </para>
/// </remarks>
public abstract class TransportContractTests : IAsyncLifetime
{
    private BlackHoleServer? _server;

    /// <summary>The listener under test, once started.</summary>
    protected IListenerHost Listener { get; private set; } = null!;

    /// <summary>The server under test, once started.</summary>
    protected BlackHoleServer Server => _server!;

    /// <summary>Why this transport cannot run here, or null when it can.</summary>
    protected virtual string? UnsupportedReason => null;

    /// <summary>Creates the listener under test.</summary>
    protected abstract IListenerHost CreateListener(TransportOptions options);

    /// <summary>Connects a client to the listener this fixture started.</summary>
    protected abstract Task<BlackHoleClient> ConnectAsync(Action<BlackHoleClient>? configure = null);

    /// <summary>An endpoint name unique to this test run, so parallel runs never collide.</summary>
    protected string Unique { get; } = $"bh{Guid.NewGuid():N}"[..20];

    protected static TransportOptions FastOptions() => new()
    {
        // Keepalive off: these tests are short and a stray Ping only adds noise.
        KeepAliveInterval = null,
    };

    public Task InitializeAsync()
    {
        if (UnsupportedReason is not null)
            return Task.CompletedTask;

        TransportOptions options = FastOptions();
        Listener = CreateListener(options);
        _server = new BlackHoleServer(Listener, options);

        _server.Rpc
            .Register("echo", request => request.Payload)
            .RegisterText("upper", text => text.ToUpperInvariant())
            .Register("boom", _ => throw new InvalidOperationException("handler failed"));

        _server.Start();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_server is not null)
            await _server.DisposeAsync();
    }

    [Fact]
    public async Task RpcEchoesTheExactPayload()
    {
        if (UnsupportedReason is not null) return;
        await using BlackHoleClient client = await ConnectAsync();

        byte[] payload = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();

        Assert.Equal(payload, await client.Rpc.CallAsync("echo", payload, TimeSpan.FromSeconds(20)));
    }

    [Fact]
    public async Task RpcKeepsManyInFlightCallsApart()
    {
        if (UnsupportedReason is not null) return;
        await using BlackHoleClient client = await ConnectAsync();

        string[] results = await Task.WhenAll(
            Enumerable.Range(0, 100).Select(i =>
                client.Rpc.CallTextAsync("upper", $"call-{i}", TimeSpan.FromSeconds(30))));

        for (int i = 0; i < results.Length; i++)
            Assert.Equal($"CALL-{i}", results[i]);
    }

    [Fact]
    public async Task RpcSurfacesAHandlerFailure()
    {
        if (UnsupportedReason is not null) return;
        await using BlackHoleClient client = await ConnectAsync();

        RpcException error = await Assert.ThrowsAsync<RpcException>(
            () => client.Rpc.CallAsync("boom", timeout: TimeSpan.FromSeconds(20)));

        Assert.Contains("handler failed", error.Message);
    }

    [Fact]
    public async Task RpcRejectsAnUnknownMethod()
    {
        if (UnsupportedReason is not null) return;
        await using BlackHoleClient client = await ConnectAsync();

        RpcException error = await Assert.ThrowsAsync<RpcException>(
            () => client.Rpc.CallAsync("no-such-method", timeout: TimeSpan.FromSeconds(20)));

        Assert.Contains("Unknown method", error.Message);
    }

    [Fact]
    public async Task PublishReachesASubscriber()
    {
        if (UnsupportedReason is not null) return;

        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using BlackHoleClient subscriber = await ConnectAsync(
            c => c.PubSub.Received += (_, payload) => received.TrySetResult(Encoding.UTF8.GetString(payload.Span)));
        await subscriber.PubSub.SubscribeAsync("sensor/+/temperature");
        await WaitForSubscribersAsync("sensor/+/temperature", 1);

        await using BlackHoleClient publisher = await ConnectAsync();
        await publisher.PubSub.PublishAsync("sensor/tank-3/temperature", "28.4");

        Assert.Equal("28.4", await received.Task.WaitAsync(TimeSpan.FromSeconds(20)));
    }

    [Fact]
    public async Task StreamsReassembleLargeBodiesByteForByte()
    {
        if (UnsupportedReason is not null) return;

        var payload = new byte[256 * 1024];
        Random.Shared.NextBytes(payload);

        var completed = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        Server.ClientConnected += connection =>
            connection.Streams.Completed += (_, e) => completed.TrySetResult(e.Data.ToArray());

        await using BlackHoleClient client = await ConnectAsync();
        long sent = await client.OutgoingStreams.SendAsync(
            "firmware", payload,
            new StreamDescriptor("firmware.bin", payload.Length, "application/octet-stream"),
            chunkSize: 16 * 1024);

        Assert.Equal(payload.Length, sent);
        Assert.Equal(payload, await completed.Task.WaitAsync(TimeSpan.FromSeconds(60)));
    }

    [Fact]
    public async Task BatchedMessagesArriveIndividuallyRouted()
    {
        if (UnsupportedReason is not null) return;

        const int count = 200;
        var seen = new List<string>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Server.ClientConnected += connection =>
            connection.Router.On(MessageType.Publish, (_, message) =>
            {
                lock (seen)
                {
                    seen.Add(message.Header);
                    if (seen.Count == count) done.TrySetResult();
                }
            });

        await using BlackHoleClient client = await ConnectAsync();
        await client.Batch.SendBatchAsync(
            Enumerable.Range(0, count)
                .Select(i => new BlackHoleMessage(MessageType.Publish, $"log/{i}", Encoding.UTF8.GetBytes($"line {i}")))
                .ToArray());

        await done.Task.WaitAsync(TimeSpan.FromSeconds(30));
        lock (seen)
        {
            Assert.Equal(count, seen.Count);
            Assert.Equal("log/0", seen[0]);
        }
    }

    [Fact]
    public async Task StatisticsCountBothDirections()
    {
        if (UnsupportedReason is not null) return;
        await using BlackHoleClient client = await ConnectAsync();

        for (int i = 0; i < 10; i++)
            await client.Rpc.CallTextAsync("upper", "abc", TimeSpan.FromSeconds(20));

        Assert.Equal(10, client.Statistics.MessagesSent);
        Assert.Equal(10, client.Statistics.MessagesReceived);
        Assert.True(client.Statistics.BytesSent > 0);
    }

    /// <summary>The wiring race, checked on every transport rather than only on TCP.</summary>
    [Fact]
    public async Task ASubscribeSentImmediatelyOnConnectIsNeverDropped()
    {
        if (UnsupportedReason is not null) return;

        var connected = new List<BlackHoleClient>();
        try
        {
            for (int i = 0; i < 4; i++)
            {
                BlackHoleClient client = await ConnectAsync();
                connected.Add(client);
                await client.PubSub.SubscribeAsync("race/check");
            }

            await WaitForSubscribersAsync("race/check", 4);
            Assert.Equal(4, Server.PubSub.SubscriberCount("race/check"));
        }
        finally
        {
            foreach (BlackHoleClient client in connected)
                await client.DisposeAsync();
        }
    }

    [Fact]
    public void TheServerReportsItsEndpoint()
    {
        if (UnsupportedReason is not null) return;
        Assert.False(string.IsNullOrWhiteSpace(Server.Endpoint));
    }

    [Fact]
    public async Task ADisconnectedClientIsForgotten()
    {
        if (UnsupportedReason is not null) return;

        BlackHoleClient client = await ConnectAsync();
        await client.PubSub.SubscribeAsync("ephemeral");
        await WaitForSubscribersAsync("ephemeral", 1);

        await client.DisposeAsync();
        await WaitForSubscribersAsync("ephemeral", 0);

        Assert.Equal(0, Server.PubSub.SubscriberCount("ephemeral"));
    }

    private async Task WaitForSubscribersAsync(string filter, int expected)
    {
        for (int attempt = 0; attempt < 400 && Server.PubSub.SubscriberCount(filter) != expected; attempt++)
            await Task.Delay(25);
    }
}

public sealed class TcpContractTests : TransportContractTests
{
    protected override IListenerHost CreateListener(TransportOptions options) =>
        new TcpListenerHost(new IPEndPoint(IPAddress.Loopback, 0), options);

    protected override Task<BlackHoleClient> ConnectAsync(Action<BlackHoleClient>? configure = null) =>
        BlackHoleClient.ConnectAsync(
            "127.0.0.1", ((TcpListenerHost)Listener).EndPoint.Port, FastOptions(), configure: configure);
}

public sealed class UnixSocketContractTests : TransportContractTests
{
    protected override string? UnsupportedReason =>
        UnixSocketTransport.IsSupported ? null : "Unix domain sockets need Windows 10 build 17063 or later.";

    protected override IListenerHost CreateListener(TransportOptions options) =>
        new UnixSocketListenerHost(UnixSocketTransport.TempPath(Unique), options);

    protected override Task<BlackHoleClient> ConnectAsync(Action<BlackHoleClient>? configure = null) =>
        BlackHoleClient.ConnectUnixAsync(
            ((UnixSocketListenerHost)Listener).SocketPath, FastOptions(), configure: configure);
}

public sealed class NamedPipeContractTests : TransportContractTests
{
    protected override IListenerHost CreateListener(TransportOptions options) =>
        new NamedPipeListenerHost(Unique, options);

    protected override Task<BlackHoleClient> ConnectAsync(Action<BlackHoleClient>? configure = null) =>
        BlackHoleClient.ConnectPipeAsync(
            Unique, FastOptions(), TimeSpan.FromSeconds(20), configure: configure);
}

public sealed class SharedMemoryContractTests : TransportContractTests
{
    // Small rings and a short spin: these tests move little data and run many endpoints at once, so
    // a 1 MiB default per slot would be pure waste.
    private static SharedMemoryOptions Shared() => new()
    {
        RingCapacity = 64 * 1024,
        SpinCount = 50,
        YieldDuration = TimeSpan.FromMilliseconds(2),
        PollInterval = TimeSpan.FromMilliseconds(1),
    };

    protected override IListenerHost CreateListener(TransportOptions options) =>
        new SharedMemoryListenerHost(Unique, slots: 6, options, Shared());

    protected override Task<BlackHoleClient> ConnectAsync(Action<BlackHoleClient>? configure = null) =>
        BlackHoleClient.ConnectSharedMemoryAsync(
            Unique, slots: 6, TimeSpan.FromSeconds(20), FastOptions(), Shared(), configure: configure);
}

/// <summary>
/// Makes platform support visible instead of letting an unavailable transport pass silently.
/// </summary>
public class TransportSupportTests
{
    [Fact]
    public void UnixDomainSocketsAreAvailableOnThisPlatform()
    {
        // Every OS this library targets supports them: Linux and macOS always, Windows since
        // build 17063. A failure here means the CI image is older than that.
        Assert.True(UnixSocketTransport.IsSupported,
            "Unix domain sockets are unavailable; the transport contract suite skipped its UDS run.");
    }
}
