// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve.Routing;

/// <summary>
/// MQTT-style topic filter matching: <c>+</c> stands for exactly one level, <c>#</c> for the
/// remaining levels and only as the final one.
/// </summary>
/// <remarks>
/// Matching walks both strings as spans and never allocates - no <c>Split</c>, no substrings. A
/// filter is only ever matched against a topic when a route is first resolved, so this runs once
/// per distinct topic rather than once per message.
/// </remarks>
public static class TopicFilter
{
    /// <summary>The level separator, matching MQTT.</summary>
    public const char Separator = '/';

    /// <summary>Single-level wildcard.</summary>
    public const char SingleLevel = '+';

    /// <summary>Multi-level wildcard. Legal only as the last level.</summary>
    public const char MultiLevel = '#';

    /// <summary>True when <paramref name="filter"/> contains a wildcard character.</summary>
    public static bool IsWildcard(string filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return filter.AsSpan().IndexOfAny(SingleLevel, MultiLevel) >= 0;
    }

    /// <summary>Throws if <paramref name="filter"/> is not a legal subscription filter.</summary>
    /// <param name="filter">The filter to check.</param>
    /// <param name="paramName">Reported as the offending parameter.</param>
    public static void ValidateFilter(string filter, string paramName = "topicFilter")
    {
        ArgumentException.ThrowIfNullOrEmpty(filter, paramName);

        ReadOnlySpan<char> rest = filter;
        while (!rest.IsEmpty)
        {
            int slash = rest.IndexOf(Separator);
            ReadOnlySpan<char> level = slash < 0 ? rest : rest[..slash];

            if (level.IndexOf(MultiLevel) >= 0)
            {
                if (level.Length != 1)
                    throw new ArgumentException($"'#' must occupy a whole level: '{filter}'.", paramName);
                if (slash >= 0)
                    throw new ArgumentException($"'#' is only legal as the last level: '{filter}'.", paramName);
            }
            else if (level.IndexOf(SingleLevel) >= 0 && level.Length != 1)
            {
                throw new ArgumentException($"'+' must occupy a whole level: '{filter}'.", paramName);
            }

            if (slash < 0) break;
            rest = rest[(slash + 1)..];
        }
    }

    /// <summary>Throws if <paramref name="topic"/> cannot be published to.</summary>
    /// <param name="topic">The topic to check.</param>
    /// <param name="paramName">Reported as the offending parameter.</param>
    public static void ValidateTopic(string topic, string paramName = "topic")
    {
        ArgumentException.ThrowIfNullOrEmpty(topic, paramName);
        if (topic.AsSpan().IndexOfAny(SingleLevel, MultiLevel) >= 0)
            throw new ArgumentException($"A published topic cannot contain a wildcard: '{topic}'.", paramName);
    }

    /// <summary>True when <paramref name="topic"/> is covered by <paramref name="filter"/>.</summary>
    /// <param name="filter">A subscription filter, possibly containing wildcards.</param>
    /// <param name="topic">A concrete topic.</param>
    public static bool Matches(string filter, string topic)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentNullException.ThrowIfNull(topic);
        return Matches(filter.AsSpan(), topic.AsSpan());
    }

    /// <summary>True when <paramref name="topic"/> is covered by <paramref name="filter"/>.</summary>
    /// <param name="filter">A subscription filter, possibly containing wildcards.</param>
    /// <param name="topic">A concrete topic.</param>
    public static bool Matches(ReadOnlySpan<char> filter, ReadOnlySpan<char> topic)
    {
        while (true)
        {
            // '#' swallows whatever is left, including nothing at all: "a/#" covers "a".
            if (filter.Length == 1 && filter[0] == MultiLevel) return true;

            int f = filter.IndexOf(Separator);
            int t = topic.IndexOf(Separator);

            ReadOnlySpan<char> filterLevel = f < 0 ? filter : filter[..f];
            ReadOnlySpan<char> topicLevel = t < 0 ? topic : topic[..t];

            bool single = filterLevel.Length == 1 && filterLevel[0] == SingleLevel;
            if (!single && !filterLevel.SequenceEqual(topicLevel)) return false;

            if (f < 0) return t < 0;                        // filter exhausted: the topic must be too
            if (t < 0)
            {
                // The topic ran out first. Only a trailing "/#" still covers it.
                ReadOnlySpan<char> tail = filter[(f + 1)..];
                return tail.Length == 1 && tail[0] == MultiLevel;
            }

            filter = filter[(f + 1)..];
            topic = topic[(t + 1)..];
        }
    }
}
