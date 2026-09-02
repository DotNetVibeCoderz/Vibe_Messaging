// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Text;
using BlackHole.Hosting;
using BlackHole.Patterns;
using BlackHole.Protocol;
using BlackHole.Transport;
using Xunit;

namespace BlackHole.Tests;

/// <summary>
/// Real sockets on loopback. Slower than a fake transport, but these are the tests that would have
/// caught the v2 framing duplication, so they earn their runtime.
/// </summary>
public class EndToEndTests : IAsyncLifetime
{
    private BlackHoleServer _server = null!;
    private int _port;

    private static TransportOptions FastOptions() => new()
    {
        // Keepalive off: these tests are short and a stray Ping only adds noise.
        KeepAliveInterval = null,
    };

    public Task InitializeAsync()
    {
        _server = new BlackHoleServer(0, FastOptions());
        _server.Rpc
            .Register("echo", request => request.Payload)
            .RegisterText("upper", text => text.ToUpperInvariant())
            .Register("boom", _ => throw new InvalidOperationException("sensor offline"));
        _server.Start();
        _port = _server.EndPoint.Port;
        return Task.CompletedTask;
    }

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private Task<BlackHoleClient> ConnectAsync() =>
        BlackHoleClient.ConnectAsync("127.0.0.1", _port, FastOptions());

    [Fact]
    public async Task RpcEchoesTheExactPayload()
    {
        await using BlackHoleClient client = await ConnectAsync();
        byte[] payload = Encoding.UTF8.GetBytes("Halo BlackHole!");

        byte[] result = await client.Rpc.CallAsync("echo", payload);

        Assert.Equal(payload, result);
    }

    [Fact]
    public async Task RpcKeepsManyInFlightCallsApart()
    {
        await using BlackHoleClient client = await ConnectAsync();

        Task<string>[] calls = Enumerable.Range(0, 200)
            .Select(i => client.Rpc.CallTextAsync("upper", $"call-{i}"))
            .ToArray();
        string[] results = await Task.WhenAll(calls);

        for (int i = 0; i < results.Length; i++)
            Assert.Equal($"CALL-{i}", results[i]);
    }

    [Fact]
    public async Task RpcSurfacesAHandlerFailure()
    {
        await using BlackHoleClient client = await ConnectAsync();

        RpcException error = await Assert.ThrowsAsync<RpcException>(() => client.Rpc.CallAsync("boom"));

        Assert.Equal("boom", error.Method);
        Assert.Contains("sensor offline", error.Message);
    }

    [Fact]
    public async Task RpcRejectsAnUnknownMethodInsteadOfHanging()
    {
        await using BlackHoleClient client = await ConnectAsync();

        RpcException error = await Assert.ThrowsAsync<RpcException>(
            () => client.Rpc.CallAsync("no-such-method", timeout: TimeSpan.FromSeconds(5)));

        Assert.Contains("Unknown method", error.Message);
    }

    [Fact]
    public async Task RpcTimesOutRatherThanWaitingForever()
    {
        _server.Rpc.Register("slow", async (_, ct) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(30), ct);
            return ReadOnlyMemory<byte>.Empty;
        });

        await using BlackHoleClient client = await ConnectAsync();

        await Assert.ThrowsAsync<RpcException>(
            () => client.Rpc.CallAsync("slow", timeout: TimeSpan.FromMilliseconds(250)));
    }

    [Fact]
    public async Task PublishReachesEverySubscriber()
    {
        await using BlackHoleClient publisher = await ConnectAsync();
        await using BlackHoleClient subscriberA = await ConnectAsync();
        await using BlackHoleClient subscriberB = await ConnectAsync();

        var receivedA = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var receivedB = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        subscriberA.PubSub.Received += (topic, payload) => receivedA.TrySetResult(Encoding.UTF8.GetString(payload.Span));
        subscriberB.PubSub.Received += (topic, payload) => receivedB.TrySetResult(Encoding.UTF8.GetString(payload.Span));

        await subscriberA.PubSub.SubscribeAsync("plant/floor-1/alerts");
        await subscriberB.PubSub.SubscribeAsync("plant/floor-1/alerts");
        await WaitForSubscribersAsync("plant/floor-1/alerts", 2);

        await publisher.PubSub.PublishAsync("plant/floor-1/alerts", "pump overheating");

        Assert.Equal("pump overheating", await receivedA.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("pump overheating", await receivedB.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task WildcardSubscriptionsMatchTopicSegments()
    {
        await using BlackHoleClient publisher = await ConnectAsync();
        await using BlackHoleClient subscriber = await ConnectAsync();

        var topics = new List<string>();
        var received = new SemaphoreSlim(0);
        subscriber.PubSub.Received += (topic, _) => { lock (topics) topics.Add(topic); received.Release(); };

        await subscriber.PubSub.SubscribeAsync("sensor/+/temperature");
        await WaitForSubscribersAsync("sensor/+/temperature", 1);

        await publisher.PubSub.PublishAsync("sensor/tank-3/temperature", "28.4");
        await publisher.PubSub.PublishAsync("sensor/tank-9/temperature", "31.0");
        await publisher.PubSub.PublishAsync("sensor/tank-3/humidity", "62");

        Assert.True(await received.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.True(await received.WaitAsync(TimeSpan.FromSeconds(5)));
        await Task.Delay(200); // Give the non-matching publish a chance to arrive if it wrongly matched.

        lock (topics)
        {
            Assert.Equal(2, topics.Count);
            Assert.DoesNotContain("sensor/tank-3/humidity", topics);
        }
    }

    [Fact]
    public async Task UnsubscribeStopsDelivery()
    {
        await using BlackHoleClient publisher = await ConnectAsync();
        await using BlackHoleClient subscriber = await ConnectAsync();

        int count = 0;
        subscriber.PubSub.Received += (_, _) => Interlocked.Increment(ref count);

        await subscriber.PubSub.SubscribeAsync("news");
        await WaitForSubscribersAsync("news", 1);
        await publisher.PubSub.PublishAsync("news", "one");
        await Task.Delay(300);

        await subscriber.PubSub.UnsubscribeAsync("news");
        await WaitForSubscribersAsync("news", 0);
        await publisher.PubSub.PublishAsync("news", "two");
        await Task.Delay(300);

        Assert.Equal(1, Volatile.Read(ref count));
    }

    [Fact]
    public async Task ADisconnectedSubscriberIsForgotten()
    {
        await using BlackHoleClient publisher = await ConnectAsync();
        BlackHoleClient subscriber = await ConnectAsync();

        await subscriber.PubSub.SubscribeAsync("ephemeral");
        await WaitForSubscribersAsync("ephemeral", 1);

        await subscriber.DisposeAsync();
        await WaitForSubscribersAsync("ephemeral", 0);

        Assert.Equal(0, _server.PubSub.SubscriberCount("ephemeral"));
    }

    [Fact]
    public async Task StreamsReassembleLargeBodiesByteForByte()
    {
        var payload = new byte[1024 * 1024];
        Random.Shared.NextBytes(payload);

        var completed = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var progressSeen = 0;
        _server.ClientConnected += connection =>
        {
            connection.Streams.Progress += (_, _, _) => Interlocked.Increment(ref progressSeen);
            connection.Streams.Completed += (_, args) => completed.TrySetResult(args.Data.ToArray());
        };

        await using BlackHoleClient client = await ConnectAsync();
        long sent = await client.OutgoingStreams.SendAsync(
            "firmware-2026.bin", payload,
            new StreamDescriptor("firmware-2026.bin", payload.Length, "application/octet-stream"),
            chunkSize: 8192);

        byte[] received = await completed.Task.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(payload.Length, sent);
        Assert.Equal(payload, received);
        Assert.True(progressSeen > 100, $"expected chunk-level progress, saw {progressSeen}");
    }

    [Fact]
    public async Task BatchedMessagesArriveIndividuallyRouted()
    {
        const int count = 500;
        var received = new List<string>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _server.ClientConnected += connection =>
            connection.Router.On(MessageType.Publish, (_, message) =>
            {
                lock (received)
                {
                    received.Add(message.Header);
                    if (received.Count == count) done.TrySetResult();
                }
            });

        await using BlackHoleClient client = await ConnectAsync();

        var batch = Enumerable.Range(0, count)
            .Select(i => new BlackHoleMessage(MessageType.Publish, $"log/entry/{i}", Encoding.UTF8.GetBytes($"line {i}")))
            .ToArray();
        await client.Batch.SendBatchAsync(batch);

        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        lock (received)
        {
            Assert.Equal(count, received.Count);
            Assert.Equal("log/entry/0", received[0]);
            Assert.Equal($"log/entry/{count - 1}", received[^1]);
        }
    }

    [Fact]
    public async Task AutoBatchingFlushesOnItsOwn()
    {
        var seen = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _server.ClientConnected += connection =>
            connection.Router.On(MessageType.Publish, (_, _) =>
            {
                if (Interlocked.Increment(ref seen) == 10) done.TrySetResult();
            });

        await using BlackHoleClient client = await ConnectAsync();
        client.Batch.MaxCount = 1000;             // Count and size cannot trigger:
        client.Batch.MaxBytes = 1 << 20;          // only the delay timer can.
        client.Batch.MaxDelay = TimeSpan.FromMilliseconds(50);
        client.Batch.Start();

        for (int i = 0; i < 10; i++)
            await client.Batch.AddAsync(new BlackHoleMessage(MessageType.Publish, "telemetry", "x"u8.ToArray()));

        await done.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(10, Volatile.Read(ref seen));
    }

    [Fact]
    public async Task ServerCanCallBackIntoTheClient()
    {
        await using BlackHoleClient client = await ConnectAsync();
        client.Handlers.RegisterText("device/status", _ => "ok:tank-3");

        BlackHoleConnection connection = await WaitForConnectionAsync();
        var serverSideCaller = new RpcClient(connection.Transport);
        connection.Router.On(MessageType.RpcResponse, serverSideCaller.HandleAsync);

        string status = await serverSideCaller.CallTextAsync("device/status", "ping");

        Assert.Equal("ok:tank-3", status);
    }

    [Fact]
    public async Task StatisticsCountBothDirections()
    {
        await using BlackHoleClient client = await ConnectAsync();
        for (int i = 0; i < 25; i++)
            await client.Rpc.CallTextAsync("upper", "abc");

        Assert.Equal(25, client.Statistics.MessagesSent);
        Assert.Equal(25, client.Statistics.MessagesReceived);
        Assert.True(client.Statistics.BytesSent > 0);
    }

    /// <summary>
    /// A client that subscribes the instant it connects must not lose that subscription. The
    /// transport used to start reading before the server had installed its dispatcher, so a message
    /// landing in that window was dropped - rarely when idle, reliably under load.
    /// </summary>
    [Fact]
    public async Task ASubscribeSentImmediatelyOnConnectIsNeverDropped()
    {
        const int clients = 40;
        var connected = new List<BlackHoleClient>();

        try
        {
            for (int i = 0; i < clients; i++)
            {
                BlackHoleClient client = await ConnectAsync();
                connected.Add(client);
                await client.PubSub.SubscribeAsync("race/check");
            }

            for (int attempt = 0; attempt < 200 && _server.PubSub.SubscriberCount("race/check") < clients; attempt++)
                await Task.Delay(25);

            Assert.Equal(clients, _server.PubSub.SubscriberCount("race/check"));
        }
        finally
        {
            foreach (BlackHoleClient client in connected)
                await client.DisposeAsync();
        }
    }

    /// <summary>
    /// The same window on the client side. A server that pushes the instant it accepts can beat a
    /// handler attached after <c>ConnectAsync</c> returns, so the <c>configure</c> callback exists
    /// to attach handlers before the receive loop starts.
    /// </summary>
    [Fact]
    public async Task AServerPushRightAfterAcceptReachesTheClient()
    {
        var pushed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Push the moment the connection is accepted, before the client could possibly ask for it.
        _server.ClientConnected += connection => _ = Task.Run(async () =>
            await connection.SendAsync(new BlackHoleMessage(
                MessageType.Publish, "boot/greeting", Encoding.UTF8.GetBytes("welcome"))));

        await using BlackHoleClient client = await BlackHoleClient.ConnectAsync(
            "127.0.0.1", _port, FastOptions(),
            configure: c => c.PubSub.Received += (_, payload) =>
                pushed.TrySetResult(Encoding.UTF8.GetString(payload.Span)));

        Assert.Equal("welcome", await pushed.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AnOversizedFrameFailsTheConnectionInsteadOfAllocating()
    {
        var strict = FastOptions();
        strict.MaxFrameLength = 1024;

        await using var strictServer = new BlackHoleServer(0, strict);
        strictServer.Start();

        var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        strictServer.ClientDisconnected += (_, _) => closed.TrySetResult();

        await using BlackHoleClient client = await BlackHoleClient.ConnectAsync("127.0.0.1", strictServer.EndPoint.Port, FastOptions());
        await client.PubSub.PublishAsync("big", new byte[8192]);

        await closed.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private async Task WaitForSubscribersAsync(string filter, int expected)
    {
        for (int i = 0; i < 100; i++)
        {
            if (_server.PubSub.SubscriberCount(filter) == expected)
                return;
            await Task.Delay(25);
        }
        Assert.Equal(expected, _server.PubSub.SubscriberCount(filter));
    }

    private async Task<BlackHoleConnection> WaitForConnectionAsync()
    {
        for (int i = 0; i < 100; i++)
        {
            BlackHoleConnection? connection = _server.Connections.FirstOrDefault();
            if (connection is not null)
                return connection;
            await Task.Delay(25);
        }
        throw new TimeoutException("No connection was accepted.");
    }
}
