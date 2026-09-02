// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Nerve.Routing;
using Xunit;

namespace Nerve.Tests;

/// <summary>
/// The matcher is the one piece of Nerve with a specification written by somebody else - MQTT's -
/// so these cases are taken from it rather than invented.
/// </summary>
public class TopicFilterTests
{
    [Theory]
    [InlineData("sport/tennis/player1", "sport/tennis/player1")]
    [InlineData("sport/tennis/+", "sport/tennis/player1")]
    [InlineData("sport/+/player1", "sport/tennis/player1")]
    [InlineData("+/tennis/player1", "sport/tennis/player1")]
    [InlineData("+/+/+", "sport/tennis/player1")]
    [InlineData("sport/tennis/#", "sport/tennis/player1")]
    [InlineData("sport/tennis/#", "sport/tennis/player1/ranking")]
    [InlineData("sport/#", "sport")]
    [InlineData("#", "sport/tennis/player1")]
    [InlineData("#", "a")]
    [InlineData("sport/tennis/+", "sport/tennis/")]
    [InlineData("agents/result/+", "agents/result/writer")]
    public void Covers(string filter, string topic) => Assert.True(TopicFilter.Matches(filter, topic));

    [Theory]
    [InlineData("sport/tennis/player1", "sport/tennis/player2")]
    [InlineData("sport/tennis/+", "sport/tennis/player1/ranking")]
    [InlineData("sport/+", "sport")]
    [InlineData("sport/tennis", "sport/tennis/player1")]
    [InlineData("sport/tennis/player1", "sport/tennis")]
    [InlineData("+/tennis", "sport/badminton")]
    [InlineData("agents/result/+", "agents/task/writer")]
    public void DoesNotCover(string filter, string topic) => Assert.False(TopicFilter.Matches(filter, topic));

    [Fact]
    public void MatchingIsCaseSensitive() => Assert.False(TopicFilter.Matches("Sport/#", "sport/tennis"));

    [Theory]
    [InlineData("sport/#/player", "'#' is only legal as the last level")]
    [InlineData("sport/tennis#", "'#' must occupy a whole level")]
    [InlineData("sport/+tennis", "'+' must occupy a whole level")]
    public void RejectsMalformedFilters(string filter, string reason)
    {
        var thrown = Assert.Throws<ArgumentException>(() => TopicFilter.ValidateFilter(filter));
        Assert.Contains(reason, thrown.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("sport/#")]
    [InlineData("sport/+/x")]
    [InlineData("#")]
    [InlineData("+")]
    public void AcceptsWellFormedFilters(string filter) => TopicFilter.ValidateFilter(filter);

    [Fact]
    public void PublishingToAWildcardIsRejected()
    {
        Assert.Throws<ArgumentException>(() => TopicFilter.ValidateTopic("sensor/+/temp"));
        Assert.Throws<ArgumentException>(() => TopicFilter.ValidateTopic("sensor/#"));
    }

    [Fact]
    public void WildcardDetection()
    {
        Assert.True(TopicFilter.IsWildcard("a/+/b"));
        Assert.True(TopicFilter.IsWildcard("a/#"));
        Assert.False(TopicFilter.IsWildcard("a/b/c"));
    }
}
