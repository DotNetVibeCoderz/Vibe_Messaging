// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Xunit;

namespace Nerve.Tests;

public class RequestReplyTests
{
    [Fact]
    public async Task AResponderAnswersTheCaller()
    {
        using var hub = new NerveHub();
        using IDisposable _ = hub.Respond<string, int>("len", text => text.Length);

        int length = await hub.RequestAsync<string, int>("len", "gravicode");

        Assert.Equal(9, length);
    }

    [Fact]
    public async Task AnAsynchronousResponderAnswersTheCaller()
    {
        using var hub = new NerveHub();
        using IDisposable _ = hub.Respond<int, int>("double", async (v, token) =>
        {
            await Task.Delay(10, token);
            return v * 2;
        });

        Assert.Equal(84, await hub.RequestAsync<int, int>("double", 42));
    }

    [Fact]
    public async Task ARespondersFailureSurfacesAtTheCallSite()
    {
        using var hub = new NerveHub();
        using IDisposable _ = hub.Respond<int, int>("boom", _ => throw new InvalidOperationException("no"));

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => hub.RequestAsync<int, int>("boom", 1));

        Assert.Equal("no", thrown.Message);
    }

    [Fact]
    public async Task AMissingResponderIsReportedImmediately()
    {
        using var hub = new NerveHub();

        await Assert.ThrowsAsync<NerveNoResponderException>(
            () => hub.RequestAsync<int, int>("nobody", 1, TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task ASilentResponderTimesOut()
    {
        using var hub = new NerveHub();
        using IDisposable _ = hub.Subscribe<NerveRequest<int, int>>("quiet", _ => { });

        await Assert.ThrowsAsync<TimeoutException>(
            () => hub.RequestAsync<int, int>("quiet", 1, TimeSpan.FromMilliseconds(80)));
    }

    [Fact]
    public async Task RespondersCanBeRegisteredOnAWildcard()
    {
        using var hub = new NerveHub();
        using IDisposable _ = hub.Respond<string, string>("agents/+/ping", name => "pong:" + name);

        Assert.Equal("pong:writer", await hub.RequestAsync<string, string>("agents/writer/ping", "writer"));
        Assert.Equal("pong:critic", await hub.RequestAsync<string, string>("agents/critic/ping", "critic"));
    }

    [Fact]
    public async Task TheFirstReplyWins()
    {
        using var hub = new NerveHub();
        using IDisposable _1 = hub.Respond<int, string>("race", _ => "first");
        using IDisposable _2 = hub.Respond<int, string>("race", _ => "second");

        Assert.Equal("first", await hub.RequestAsync<int, string>("race", 1));
    }
}

public class StreamingTests
{
    [Fact]
    public async Task AStreamYieldsWhatIsPublished()
    {
        using var hub = new NerveHub();
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var received = new List<int>();
        Task consumer = Task.Run(async () =>
        {
            await foreach (int value in hub.StreamAsync<int>("ticks", cancellationToken: stop.Token))
            {
                received.Add(value);
                if (received.Count == 3) await stop.CancelAsync();
            }
        }, CancellationToken.None);

        while (!hub.HasSubscribers<int>("ticks")) await Task.Delay(5);
        for (int i = 1; i <= 3; i++) await hub.PublishAsync("ticks", i);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => consumer);
        Assert.Equal([1, 2, 3], received);
    }

    [Fact]
    public async Task AStreamUnsubscribesWhenTheLoopEnds()
    {
        using var hub = new NerveHub();
        using var stop = new CancellationTokenSource();

        Task consumer = Task.Run(async () =>
        {
            try
            {
                await foreach (int _ in hub.StreamAsync<int>("ticks", cancellationToken: stop.Token)) { }
            }
            catch (OperationCanceledException) { }
        }, CancellationToken.None);

        while (!hub.HasSubscribers<int>("ticks")) await Task.Delay(5);
        await stop.CancelAsync();
        await consumer;

        Assert.False(hub.HasSubscribers<int>("ticks"));
    }

    [Fact]
    public async Task ASlowConsumerDropsRatherThanBlocksThePublisher()
    {
        using var hub = new NerveHub();
        using var stop = new CancellationTokenSource();

        Task consumer = Task.Run(async () =>
        {
            try
            {
                await foreach (int _ in hub.StreamAsync<int>("flood", capacity: 4, cancellationToken: stop.Token))
                    await Task.Delay(50, stop.Token);
            }
            catch (OperationCanceledException) { }
        }, CancellationToken.None);

        while (!hub.HasSubscribers<int>("flood")) await Task.Delay(5);
        for (int i = 0; i < 500; i++) await hub.PublishAsync("flood", i);

        await stop.CancelAsync();
        await consumer;

        Assert.True(hub.GetStatistics().StreamDrops > 0, "a four-slot buffer should have dropped something");
    }

    [Fact]
    public async Task WaitForReturnsTheNextMessage()
    {
        using var hub = new NerveHub();
        Task<string> waiter = hub.WaitForAsync<string>("ready", timeout: TimeSpan.FromSeconds(5));

        while (!hub.HasSubscribers<string>("ready")) await Task.Delay(5);
        await hub.PublishAsync("ready", "go");

        Assert.Equal("go", await waiter);
    }

    [Fact]
    public async Task WaitForHonoursItsPredicate()
    {
        using var hub = new NerveHub();
        Task<int> waiter = hub.WaitForAsync<int>("t", v => v > 100, TimeSpan.FromSeconds(5));

        while (!hub.HasSubscribers<int>("t")) await Task.Delay(5);
        foreach (int v in new[] { 1, 50, 200, 300 }) await hub.PublishAsync("t", v);

        Assert.Equal(200, await waiter);
    }

    [Fact]
    public async Task WaitForTimesOut()
    {
        using var hub = new NerveHub();

        await Assert.ThrowsAsync<TimeoutException>(
            () => hub.WaitForAsync<int>("silent", timeout: TimeSpan.FromMilliseconds(50)));
    }
}

public class ErrorTests
{
    [Fact]
    public async Task OneBrokenHandlerDoesNotStopTheOthers()
    {
        using var hub = new NerveHub();
        int reached = 0;

        using IDisposable _1 = hub.Subscribe<int>("t", _ => throw new InvalidOperationException("boom"));
        using IDisposable _2 = hub.Subscribe<int>("t", _ => reached++);

        await hub.PublishAsync("t", 1);

        Assert.Equal(1, reached);
        Assert.Equal(1, hub.GetStatistics().Errors);
    }

    [Fact]
    public async Task FailuresAreReportedWithTheirTopicAndFilter()
    {
        using var hub = new NerveHub();
        NerveError? reported = null;
        hub.HandlerError += error => reported = error;

        using IDisposable _ = hub.Subscribe<int>("sensor/+/temp", _ => throw new InvalidOperationException("boom"));
        await hub.PublishAsync("sensor/tank-3/temp", 1);

        Assert.NotNull(reported);
        Assert.Equal("sensor/tank-3/temp", reported!.Value.Topic);
        Assert.Equal("sensor/+/temp", reported.Value.SubscriptionFilter);
        Assert.Equal(typeof(int), reported.Value.MessageType);
        Assert.Equal("boom", reported.Value.Exception.Message);
    }

    [Fact]
    public async Task AnAsynchronousFailureIsReportedToo()
    {
        using var hub = new NerveHub();
        var reported = new List<NerveError>();
        hub.HandlerError += reported.Add;

        using IDisposable _ = hub.Subscribe<int>("t", async _ =>
        {
            await Task.Delay(10);
            throw new InvalidOperationException("late");
        });

        await hub.PublishAsync("t", 1);

        Assert.Single(reported);
        Assert.Equal("late", reported[0].Exception.Message);
    }

    [Fact]
    public async Task PropagateSurfacesTheFailureToThePublisher()
    {
        using var hub = new NerveHub(new NerveOptions { ErrorBehavior = HandlerErrorBehavior.Propagate });
        using IDisposable _ = hub.Subscribe<int>("t", _ => throw new InvalidOperationException("boom"));

        var thrown = await Assert.ThrowsAsync<NerveHandlerException>(async () => await hub.PublishAsync("t", 1));

        Assert.Equal("t", thrown.Topic);
        Assert.IsType<InvalidOperationException>(thrown.InnerException);
    }

    [Fact]
    public async Task TheConstructorErrorHookSeesFailuresToo()
    {
        var reported = new List<NerveError>();
        using var hub = new NerveHub(new NerveOptions { OnError = reported.Add });

        using IDisposable _ = hub.Subscribe<int>("t", _ => throw new InvalidOperationException("boom"));
        await hub.PublishAsync("t", 1);

        Assert.Single(reported);
    }
}

public class StatisticsTests
{
    [Fact]
    public async Task CountsPublishesDeliveriesAndRoutes()
    {
        using var hub = new NerveHub();
        using IDisposable _1 = hub.Subscribe<int>("a", _ => { });
        using IDisposable _2 = hub.Subscribe<int>("a", _ => { });

        await hub.PublishAsync("a", 1);
        await hub.PublishAsync("a", 2);
        await hub.PublishAsync("b", 3);

        NerveStatistics stats = hub.GetStatistics();

        Assert.Equal(3, stats.Published);
        Assert.Equal(4, stats.Delivered);       // two messages, two subscribers each
        Assert.Equal(1, stats.Unrouted);
        Assert.Equal(2, stats.Routes);
        Assert.Equal(2, stats.Subscriptions);
    }

    [Fact]
    public async Task CountersStayAtZeroWhenCollectionIsOff()
    {
        using var hub = new NerveHub(new NerveOptions { CollectStatistics = false });
        using IDisposable _ = hub.Subscribe<int>("a", _ => { });

        await hub.PublishAsync("a", 1);

        NerveStatistics stats = hub.GetStatistics();
        Assert.Equal(0, stats.Published);
        Assert.Equal(0, stats.Delivered);
        Assert.Equal(1, stats.Subscriptions);   // structural counts are always available
    }

    [Fact]
    public async Task RetainedTopicsAreCounted()
    {
        using var hub = new NerveHub();
        await hub.PublishRetainedAsync("a", 1);
        await hub.PublishRetainedAsync("b", 2);
        await hub.PublishAsync("c", 3);

        Assert.Equal(2, hub.GetStatistics().Retained);
    }
}
