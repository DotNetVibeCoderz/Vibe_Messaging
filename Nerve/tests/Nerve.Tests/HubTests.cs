// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Xunit;

namespace Nerve.Tests;

public class HubTests
{
    [Fact]
    public async Task DeliversToEverySubscriber()
    {
        using var hub = new NerveHub();
        var seen = new List<string>();

        using IDisposable a = hub.Subscribe<string>("chat/general", m => seen.Add("a:" + m));
        using IDisposable b = hub.Subscribe<string>("chat/general", m => seen.Add("b:" + m));

        await hub.PublishAsync("chat/general", "halo");

        Assert.Equal(["a:halo", "b:halo"], seen);
    }

    [Fact]
    public async Task SubscribersRunInRegistrationOrder()
    {
        using var hub = new NerveHub();
        var order = new List<int>();

        using IDisposable _1 = hub.Subscribe<int>("t", _ => order.Add(1));
        using IDisposable _2 = hub.Subscribe<int>("t", _ => order.Add(2));
        using IDisposable _3 = hub.Subscribe<int>("t", _ => order.Add(3));

        await hub.PublishAsync("t", 0);

        Assert.Equal([1, 2, 3], order);
    }

    [Fact]
    public async Task RoutesByTypeAsWellAsTopic()
    {
        using var hub = new NerveHub();
        int ints = 0, strings = 0;

        using IDisposable _1 = hub.Subscribe<int>("shared", _ => ints++);
        using IDisposable _2 = hub.Subscribe<string>("shared", _ => strings++);

        await hub.PublishAsync("shared", 42);

        Assert.Equal(1, ints);
        Assert.Equal(0, strings);
    }

    [Fact]
    public async Task SynchronousHandlersHaveRunBeforePublishReturns()
    {
        using var hub = new NerveHub();
        int seen = 0;

        using IDisposable _ = hub.Subscribe<int>("t", v => seen = v);
        hub.Publish("t", 7);

        Assert.Equal(7, seen);
        await Task.CompletedTask;
    }

    [Fact]
    public async Task PublishAsyncAwaitsAsynchronousHandlers()
    {
        using var hub = new NerveHub();
        bool finished = false;

        using IDisposable _ = hub.Subscribe<int>("t", async _ =>
        {
            await Task.Delay(20);
            finished = true;
        });

        await hub.PublishAsync("t", 1);

        Assert.True(finished);
    }

    [Fact]
    public async Task HandlersCanTakeTheCancellationToken()
    {
        using var hub = new NerveHub();
        bool cancelled = false;

        using IDisposable _ = hub.Subscribe<int>("t", (_, token) =>
        {
            cancelled = token.IsCancellationRequested;
            return ValueTask.CompletedTask;
        });

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        await hub.PublishAsync("t", 1, cts.Token);

        Assert.True(cancelled);
    }

    [Fact]
    public async Task DisposingASubscriptionStopsDelivery()
    {
        using var hub = new NerveHub();
        int count = 0;

        IDisposable subscription = hub.Subscribe<int>("t", _ => count++);
        await hub.PublishAsync("t", 1);
        subscription.Dispose();
        await hub.PublishAsync("t", 1);

        Assert.Equal(1, count);
    }

    [Fact]
    public void DisposingASubscriptionTwiceIsHarmless()
    {
        using var hub = new NerveHub();
        IDisposable subscription = hub.Subscribe<int>("t", _ => { });

        subscription.Dispose();
        subscription.Dispose();

        Assert.Equal(0, hub.SubscriberCount<int>("t"));
    }

    [Fact]
    public async Task ASubscriberDisposedMidDispatchStopsImmediately()
    {
        // The dispatch loop walks a snapshot, so a handler that unsubscribes a later one has to be
        // honoured by the Active check rather than by rebuilding the array.
        using var hub = new NerveHub();
        int second = 0;
        IDisposable? later = null;

        using IDisposable first = hub.Subscribe<int>("t", _ => later!.Dispose());
        later = hub.Subscribe<int>("t", _ => second++);

        await hub.PublishAsync("t", 1);

        Assert.Equal(0, second);
    }

    [Fact]
    public async Task PublishingToNothingIsFine()
    {
        using var hub = new NerveHub();
        await hub.PublishAsync("nobody/listening", 1);

        Assert.Equal(1, hub.GetStatistics().Unrouted);
    }

    [Fact]
    public async Task SubscribeOnceFiresOnceAndUnsubscribes()
    {
        using var hub = new NerveHub();
        int count = 0;

        using IDisposable _ = hub.SubscribeOnce<int>("t", _ => count++);
        await hub.PublishAsync("t", 1);
        await hub.PublishAsync("t", 2);

        Assert.Equal(1, count);
        Assert.Equal(0, hub.SubscriberCount<int>("t"));
    }

    [Fact]
    public async Task APredicateFiltersBeforeTheHandler()
    {
        using var hub = new NerveHub();
        var seen = new List<int>();

        using IDisposable _ = hub.Subscribe<int>("t", v => v > 10, seen.Add);
        foreach (int v in new[] { 5, 20, 8, 30 }) await hub.PublishAsync("t", v);

        Assert.Equal([20, 30], seen);
    }

    [Fact]
    public async Task ATopicHandlePublishesToTheSameRoute()
    {
        using var hub = new NerveHub();
        int seen = 0;

        using IDisposable _ = hub.Subscribe<int>("sensor/tank-3", v => seen = v);

        NerveTopic<int> topic = hub.Topic<int>("sensor/tank-3");
        Assert.True(topic.HasSubscribers);
        Assert.Equal("sensor/tank-3", topic.Name);

        await topic.PublishAsync(99);

        Assert.Equal(99, seen);
    }

    [Fact]
    public void PublishingToAWildcardTopicThrows()
    {
        using var hub = new NerveHub();
        Assert.Throws<ArgumentException>(() => hub.Publish("sensor/+/temp", 1));
    }

    [Fact]
    public void SubscribingToAMalformedFilterThrows()
    {
        using var hub = new NerveHub();
        Assert.Throws<ArgumentException>(() => hub.Subscribe<int>("a/#/b", _ => { }));
    }

    [Fact]
    public void UsingADisposedHubThrows()
    {
        var hub = new NerveHub();
        hub.Dispose();

        Assert.Throws<ObjectDisposedException>(() => hub.Subscribe<int>("t", _ => { }));
    }

    [Fact]
    public async Task ConcurrentPublishersAllGetThrough()
    {
        using var hub = new NerveHub();
        int received = 0;

        using IDisposable _ = hub.Subscribe<int>("load", _ => Interlocked.Increment(ref received));

        await Parallel.ForEachAsync(Enumerable.Range(0, 8), async (worker, _) =>
        {
            for (int i = 0; i < 10_000; i++) await hub.PublishAsync("load", i);
        });

        Assert.Equal(80_000, received);
    }

    [Fact]
    public async Task SubscribingDuringPublishDoesNotLoseMessages()
    {
        // Registration is copy-on-write and routes rebuild lazily; a subscriber added while
        // another thread is publishing has to start receiving without a restart.
        using var hub = new NerveHub();
        int received = 0;
        using var stop = new CancellationTokenSource();

        Task publisher = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested) await hub.PublishAsync("churn", 1);
        });

        var subscriptions = new List<IDisposable>();
        for (int i = 0; i < 50; i++)
        {
            subscriptions.Add(hub.Subscribe<int>("churn", _ => Interlocked.Increment(ref received)));
            await Task.Delay(1);
        }

        await stop.CancelAsync();
        await publisher;
        foreach (IDisposable subscription in subscriptions) subscription.Dispose();

        Assert.True(received > 0, "the late subscribers never received anything");
    }
}
