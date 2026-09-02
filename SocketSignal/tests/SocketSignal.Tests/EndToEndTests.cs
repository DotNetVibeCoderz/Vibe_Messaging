// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Net;
using System.Text.Json;
using Xunit;

namespace SocketSignal.Tests;

/// <summary>
/// These run a real HttpListener on a free loopback port and a real WebSocket against it, because
/// the interesting failures in this library are all timing and lifetime failures.
/// </summary>
public class EndToEndTests : IAsyncLifetime
{
    private SocketSignalServer _server = null!;
    private CancellationTokenSource _cts = null!;
    private int _port;

    private static int FreePort()
    {
        var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        int port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    public Task InitializeAsync()
    {
        _port = FreePort();
        _cts = new CancellationTokenSource();
        _server = new SocketSignalServer($"http://localhost:{_port}/ws/", new SocketSignalOptions
        {
            CallTimeout = TimeSpan.FromSeconds(5),
            KeepAliveInterval = Timeout.InfiniteTimeSpan,
        });
        _ = _server.StartAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _cts.CancelAsync();
        await _server.DisposeAsync();
        _cts.Dispose();
    }

    private async Task<SocketSignalClient> ConnectAsync(SocketSignalOptions? options = null)
    {
        var client = new SocketSignalClient(options ?? new SocketSignalOptions
        {
            CallTimeout = TimeSpan.FromSeconds(5),
            KeepAliveInterval = Timeout.InfiniteTimeSpan,
        });
        await client.ConnectAsync(new Uri($"ws://localhost:{_port}/ws/"));
        return client;
    }

    [Fact]
    public async Task Client_calls_server_and_gets_a_typed_result()
    {
        _server.Register<int, int, int>("sum", (_, a, b) => ValueTask.FromResult(a + b));

        await using SocketSignalClient client = await ConnectAsync();
        Assert.Equal(12, await client.CallAsync<int>("sum", 5, 7));
    }

    [Fact]
    public async Task Client_calls_server_with_the_v1_untyped_api()
    {
        _server.Register("echo", async (_, args) =>
        {
            await Task.Yield();
            return $"echo:{args[0].GetString()}";
        });

        await using SocketSignalClient client = await ConnectAsync();
        JsonElement? result = await client.CallAsync("echo", "hello");
        Assert.Equal("echo:hello", result?.GetString());
    }

    [Fact]
    public async Task Welcome_hands_the_client_its_id()
    {
        await using SocketSignalClient client = await ConnectAsync();
        Assert.False(string.IsNullOrEmpty(client.ClientId));
        Assert.Equal(1, _server.ClientCount);
        Assert.Contains(_server.Clients, c => c.Id == client.ClientId);
    }

    [Fact]
    public async Task Server_broadcasts_to_every_client()
    {
        var first = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using SocketSignalClient a = await ConnectAsync();
        await using SocketSignalClient b = await ConnectAsync();
        a.On<string, bool>("hello", text => { first.TrySetResult(text!); return ValueTask.FromResult(true); });
        b.On<string, bool>("hello", text => { second.TrySetResult(text!); return ValueTask.FromResult(true); });

        await WaitForClientsAsync(2);
        await _server.BroadcastAsync("hello", "all hands");

        Assert.Equal("all hands", await first.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("all hands", await second.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Server_sends_to_a_group_and_leaves_everyone_else_alone()
    {
        var inGroup = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        int outsiderCalls = 0;

        await using SocketSignalClient member = await ConnectAsync();
        await using SocketSignalClient outsider = await ConnectAsync();
        member.On<string, bool>("alert", t => { inGroup.TrySetResult(t!); return ValueTask.FromResult(true); });
        outsider.On<string, bool>("alert", _ => { Interlocked.Increment(ref outsiderCalls); return ValueTask.FromResult(true); });

        await WaitForClientsAsync(2);
        _server.AddToGroup("watch", member.ClientId!);

        await _server.SendToGroupAsync("watch", "alert", "contact bearing 041");

        Assert.Equal("contact bearing 041", await inGroup.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(0, Volatile.Read(ref outsiderCalls));
        Assert.Contains("watch", _server.GroupsOf(member.ClientId!));
    }

    [Fact]
    public async Task Server_calls_a_client_and_gets_a_return_value()
    {
        // The v1 gap: InvokeClientAndWaitAsync existed but was internal and unreachable.
        await using SocketSignalClient client = await ConnectAsync();
        client.On<int, int>("double", n => ValueTask.FromResult(n * 2));

        await WaitForClientsAsync(1);
        int answer = await _server.CallClientAsync<int>(client.ClientId!, "double", 21);

        Assert.Equal(42, answer);
    }

    [Fact]
    public async Task A_throwing_handler_becomes_an_exception_on_the_caller()
    {
        _server.Register<string, string>("explode", (_, _) => throw new InvalidOperationException("reactor offline"));

        await using SocketSignalClient client = await ConnectAsync();
        var error = await Assert.ThrowsAsync<SignalInvocationException>(
            async () => await client.CallAsync<string>("explode", "now"));

        Assert.Equal("explode", error.Method);
        Assert.Contains("reactor offline", error.RemoteMessage);
    }

    [Fact]
    public async Task An_unknown_method_reports_itself_as_missing()
    {
        await using SocketSignalClient client = await ConnectAsync();
        await Assert.ThrowsAsync<MethodNotFoundException>(
            async () => await client.CallAsync<int>("nothing.here", 1));
    }

    [Fact]
    public async Task A_call_that_never_answers_times_out_instead_of_hanging()
    {
        // v1 parked the caller forever. This is the regression test for that.
        _server.Register<int>("silence", async _ =>
        {
            await Task.Delay(TimeSpan.FromSeconds(30));
            return 0;
        });

        await using SocketSignalClient client = await ConnectAsync(new SocketSignalOptions
        {
            CallTimeout = TimeSpan.FromMilliseconds(300),
            KeepAliveInterval = Timeout.InfiniteTimeSpan,
        });

        await Assert.ThrowsAsync<SignalTimeoutException>(async () => await client.CallAsync<int>("silence"));
    }

    [Fact]
    public async Task Losing_the_socket_fails_calls_in_flight()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _server.Register<int>("hold", async c =>
        {
            started.TrySetResult();
            await Task.Delay(TimeSpan.FromSeconds(30));
            return 0;
        });

        SocketSignalClient client = await ConnectAsync(new SocketSignalOptions
        {
            CallTimeout = TimeSpan.FromSeconds(30),
            KeepAliveInterval = Timeout.InfiniteTimeSpan,
        });

        Task<int> call = client.CallAsync<int>("hold").AsTask();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await client.DisconnectAsync();

        await Assert.ThrowsAsync<SignalConnectionClosedException>(async () => await call);
        await client.DisposeAsync();
    }

    [Fact]
    public async Task Fire_and_forget_does_not_wait_for_the_handler()
    {
        var seen = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        _server.Register<string, bool>("log", (_, text) => { seen.TrySetResult(text!); return ValueTask.FromResult(true); });

        await using SocketSignalClient client = await ConnectAsync();
        await client.SendAsync("log", "written to the deck log");

        Assert.Equal("written to the deck log", await seen.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Handlers_run_concurrently_rather_than_head_of_line_blocking()
    {
        _server.Register<int, int>("slow", async (_, n) =>
        {
            await Task.Delay(200);
            return n;
        });

        await using SocketSignalClient client = await ConnectAsync();

        var clock = System.Diagnostics.Stopwatch.StartNew();
        int[] answers = await Task.WhenAll(Enumerable.Range(0, 8).Select(i => client.CallAsync<int>("slow", i).AsTask()));
        clock.Stop();

        Assert.Equal([0, 1, 2, 3, 4, 5, 6, 7], answers);
        // Eight 200 ms handlers run in parallel finish nowhere near 1.6 s.
        Assert.True(clock.ElapsedMilliseconds < 1200, $"took {clock.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async Task Complex_types_survive_the_round_trip()
    {
        _server.Register<Contact, Contact>("reflect", (_, contact) =>
            ValueTask.FromResult(contact! with { Bearing = contact.Bearing + 180 }));

        await using SocketSignalClient client = await ConnectAsync();
        var sent = new Contact("M-12", 41.5, 3200, "submerged");
        Contact? back = await client.CallAsync<Contact>("reflect", sent);

        Assert.NotNull(back);
        Assert.Equal("M-12", back.Id);
        Assert.Equal(221.5, back.Bearing);
        Assert.Equal("submerged", back.Classification);
    }

    [Fact]
    public async Task Disconnect_is_reported_and_the_client_is_dropped()
    {
        var gone = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        _server.ClientDisconnected += (_, reason) => gone.TrySetResult(reason);

        SocketSignalClient client = await ConnectAsync();
        await WaitForClientsAsync(1);
        await client.DisconnectAsync();

        await gone.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitForClientsAsync(0);
        await client.DisposeAsync();
    }

    [Fact]
    public async Task Authentication_can_turn_a_connection_away()
    {
        _server.Authenticate = context =>
            ValueTask.FromResult(context.Request.QueryString["token"] == "let-me-in");

        var rejected = new SocketSignalClient();
        await Assert.ThrowsAnyAsync<Exception>(
            async () => await rejected.ConnectAsync(new Uri($"ws://localhost:{_port}/ws/?token=wrong")));
        await rejected.DisposeAsync();

        await using var accepted = new SocketSignalClient();
        await accepted.ConnectAsync(new Uri($"ws://localhost:{_port}/ws/?token=let-me-in"));
        Assert.True(accepted.IsConnected);

        _server.Authenticate = null;
    }

    [Fact]
    public async Task Statistics_count_what_crossed_the_wire()
    {
        _server.Register<int, int>("id", (_, n) => ValueTask.FromResult(n));

        await using SocketSignalClient client = await ConnectAsync();
        await client.CallAsync<int>("id", 1);

        Assert.True(client.Statistics.FramesSent >= 1);
        Assert.True(client.Statistics.BytesReceived > 0);
        Assert.Equal(1, client.Statistics.CallsCompleted);
    }

    private async Task WaitForClientsAsync(int expected)
    {
        for (int i = 0; i < 100 && _server.ClientCount != expected; i++)
            await Task.Delay(20);
        Assert.Equal(expected, _server.ClientCount);
    }

    private sealed record Contact(string Id, double Bearing, double RangeMetres, string Classification);
}
