// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Xunit;

namespace Nerve.Tests;

public class WildcardTests
{
    [Fact]
    public async Task ASingleLevelWildcardReceivesFromEveryMatchingTopic()
    {
        using var hub = new NerveHub();
        var seen = new List<double>();

        using IDisposable _ = hub.Subscribe<double>("sensor/+/temperature", seen.Add);

        await hub.PublishAsync("sensor/tank-3/temperature", 28.4);
        await hub.PublishAsync("sensor/tank-9/temperature", 31.0);
        await hub.PublishAsync("sensor/tank-3/pressure", 1.2);

        Assert.Equal([28.4, 31.0], seen);
    }

    [Fact]
    public async Task AMultiLevelWildcardReachesEveryDepth()
    {
        using var hub = new NerveHub();
        int seen = 0;

        using IDisposable _ = hub.Subscribe<int>("agents/#", _ => seen++);

        await hub.PublishAsync("agents", 1);
        await hub.PublishAsync("agents/task/writer", 1);
        await hub.PublishAsync("agents/task/writer/retry/3", 1);
        await hub.PublishAsync("other/task", 1);

        Assert.Equal(3, seen);
    }

    [Fact]
    public async Task ExactSubscribersRunBeforeWildcardOnes()
    {
        using var hub = new NerveHub();
        var order = new List<string>();

        using IDisposable _1 = hub.Subscribe<int>("a/#", _ => order.Add("wildcard"));
        using IDisposable _2 = hub.Subscribe<int>("a/b", _ => order.Add("exact"));

        await hub.PublishAsync("a/b", 1);

        Assert.Equal(["exact", "wildcard"], order);
    }

    [Fact]
    public async Task AWildcardCoversTopicsThatDidNotExistYet()
    {
        using var hub = new NerveHub();
        var seen = new List<string>();

        using IDisposable _ = hub.Subscribe<string>("agents/result/+", seen.Add);

        // The route for this topic is created by the publish itself, well after the subscribe.
        await hub.PublishAsync("agents/result/analyst", "done");

        Assert.Equal(["done"], seen);
    }

    [Fact]
    public async Task DisposingAWildcardStopsEveryTopicItCovered()
    {
        using var hub = new NerveHub();
        int seen = 0;

        IDisposable subscription = hub.Subscribe<int>("a/+", _ => seen++);
        await hub.PublishAsync("a/one", 1);
        await hub.PublishAsync("a/two", 1);
        subscription.Dispose();
        await hub.PublishAsync("a/one", 1);
        await hub.PublishAsync("a/three", 1);

        Assert.Equal(2, seen);
    }

    [Fact]
    public async Task OverlappingWildcardsEachDeliverOnce()
    {
        using var hub = new NerveHub();
        int broad = 0, narrow = 0, exact = 0;

        using IDisposable _1 = hub.Subscribe<int>("#", _ => broad++);
        using IDisposable _2 = hub.Subscribe<int>("a/+", _ => narrow++);
        using IDisposable _3 = hub.Subscribe<int>("a/b", _ => exact++);

        await hub.PublishAsync("a/b", 1);

        Assert.Equal(1, broad);
        Assert.Equal(1, narrow);
        Assert.Equal(1, exact);
    }
}

public class RetainedTests
{
    [Fact]
    public async Task ANewSubscriberReceivesTheRetainedMessage()
    {
        using var hub = new NerveHub();
        await hub.PublishRetainedAsync("config/theme", "dark");

        string? seen = null;
        using IDisposable _ = hub.Subscribe<string>("config/theme", v => seen = v);

        Assert.Equal("dark", seen);
    }

    [Fact]
    public async Task OnlyTheLatestRetainedMessageSurvives()
    {
        using var hub = new NerveHub();
        await hub.PublishRetainedAsync("config/theme", "dark");
        await hub.PublishRetainedAsync("config/theme", "light");

        string? seen = null;
        using IDisposable _ = hub.Subscribe<string>("config/theme", v => seen = v);

        Assert.Equal("light", seen);
    }

    [Fact]
    public async Task AWildcardSubscriberIsGivenEveryMatchingRetainedTopic()
    {
        using var hub = new NerveHub();
        await hub.PublishRetainedAsync("roster/writer", "idle");
        await hub.PublishRetainedAsync("roster/critic", "idle");
        await hub.PublishAsync("roster/analyst", "busy");   // not retained

        var seen = new List<string>();
        using IDisposable _ = hub.Subscribe<string>("roster/+", seen.Add);

        Assert.Equal(2, seen.Count);
        Assert.All(seen, v => Assert.Equal("idle", v));
    }

    [Fact]
    public async Task ClearingStopsTheReplay()
    {
        using var hub = new NerveHub();
        await hub.PublishRetainedAsync("config/theme", "dark");
        hub.ClearRetained<string>("config/theme");

        string? seen = null;
        using IDisposable _ = hub.Subscribe<string>("config/theme", v => seen = v);

        Assert.Null(seen);
    }

    [Fact]
    public async Task RetainedValuesCanBeReadWithoutSubscribing()
    {
        using var hub = new NerveHub();
        Assert.False(hub.TryGetRetained<int>("counter", out _));

        await hub.PublishRetainedAsync("counter", 12);

        Assert.True(hub.TryGetRetained<int>("counter", out int value));
        Assert.Equal(12, value);
    }
}
