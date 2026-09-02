// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using System.Diagnostics;

namespace Nerve.AgentSim.Agents;

/// <summary>
/// A sub-agent that does one kind of work. It knows the hub and its own specialty, and nothing else
/// - not the orchestrator, not its peers, not the panel watching it.
/// </summary>
/// <remarks>
/// <para>
/// Work arrives through <see cref="INerveHub.StreamAsync{T}"/> rather than a plain subscription,
/// which is what gives the specialist a thread of its own. A subscription handler runs on the
/// publisher's thread, so an agent that slept inside one would be sleeping on the orchestrator's
/// thread and the whole simulation would run one task at a time. The stream buffers instead, and
/// the six specialists genuinely work in parallel.
/// </para>
/// <para>
/// The status published on <c>agents/roster/*</c> is retained, so the panel can be opened at any
/// point and immediately sees a full roster rather than an empty one that fills in as work happens.
/// </para>
/// </remarks>
public sealed class Specialist
{
    private readonly INerveHub _hub;
    private readonly Random _random;
    private readonly string _resultTopic;
    private readonly IDisposable _capabilityResponder;

    private int _queued;
    private int _completed;

    /// <summary>Creates a specialist and registers what it can do.</summary>
    /// <param name="hub">The bus. The only thing this agent shares with the others.</param>
    /// <param name="specialty">What kind of work it takes on.</param>
    /// <param name="seed">Seeds the latency and confidence draws, so a run can be reproduced.</param>
    public Specialist(INerveHub hub, Specialty specialty, int seed)
    {
        _hub = hub;
        Specialty = specialty;
        _random = new Random(seed);
        _resultTopic = Topics.ResultFrom(specialty);

        // Answering "what are you for" over request/reply means the orchestrator can discover the
        // roster instead of being told it at construction time.
        _capabilityResponder = hub.Respond<string, Capability>(
            Topics.CapabilityOf(specialty),
            _ => new Capability(specialty, Profile.Headline, Profile.TypicalMs));
    }

    /// <summary>What kind of work this agent takes on.</summary>
    public Specialty Specialty { get; }

    private AgentProfile Profile => AgentProfile.For(Specialty);

    /// <summary>
    /// Drains this specialist's queue until cancelled. One sub-task at a time, in the order it
    /// arrived - which is what makes the queue depth on screen mean something.
    /// </summary>
    /// <param name="cancellationToken">Stops the agent.</param>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        await PublishStatusAsync(busy: false, cancellationToken).ConfigureAwait(false);

        try
        {
            await foreach (SubTask task in _hub
                .StreamAsync<SubTask>(Topics.TaskFor(Specialty), capacity: 256, cancellationToken)
                .ConfigureAwait(false))
            {
                Interlocked.Increment(ref _queued);
                await PublishStatusAsync(busy: true, cancellationToken).ConfigureAwait(false);

                SubResult result = await WorkAsync(task, cancellationToken).ConfigureAwait(false);

                Interlocked.Decrement(ref _queued);
                Interlocked.Increment(ref _completed);

                await _hub.PublishAsync(_resultTopic, result, cancellationToken).ConfigureAwait(false);
                await PublishStatusAsync(busy: false, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        finally
        {
            _capabilityResponder.Dispose();
        }
    }

    private async Task<SubResult> WorkAsync(SubTask task, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        int duration = _random.Next(Profile.MinimumMs, Profile.MaximumMs);
        await Task.Delay(duration, cancellationToken).ConfigureAwait(false);

        double confidence = 0.62 + (_random.NextDouble() * 0.36);
        string finding = Profile.Findings[_random.Next(Profile.Findings.Length)];

        return new SubResult(
            task.MissionId, task.Step, Specialty, finding, confidence, (int)stopwatch.ElapsedMilliseconds);
    }

    private ValueTask PublishStatusAsync(bool busy, CancellationToken cancellationToken) =>
        _hub.PublishRetainedAsync(
            Topics.RosterFor(Specialty),
            new AgentStatus(Specialty, Volatile.Read(ref _queued), Volatile.Read(ref _completed), busy),
            cancellationToken);
}

/// <summary>
/// The fixed character of one specialist: what it says it does, how long it takes, and the shape of
/// the answers it gives back.
/// </summary>
/// <param name="Display">The name on the panel.</param>
/// <param name="Headline">What it answers when asked about its capability.</param>
/// <param name="MinimumMs">Fastest it finishes a sub-task.</param>
/// <param name="MaximumMs">Slowest it finishes a sub-task.</param>
/// <param name="Findings">The answers it draws from.</param>
public sealed record AgentProfile(
    string Display, string Headline, int MinimumMs, int MaximumMs, string[] Findings)
{
    /// <summary>Roughly how long this specialist takes.</summary>
    public int TypicalMs => (MinimumMs + MaximumMs) / 2;

    /// <summary>The profile for one specialty.</summary>
    /// <param name="specialty">Which one.</param>
    public static AgentProfile For(Specialty specialty) => specialty switch
    {
        Specialty.Researcher => new AgentProfile(
            "Researcher", "finds prior art and primary sources", 420, 1_150,
            [
                "found 4 primary sources, 2 contradict the premise",
                "prior art exists; closest match is three years old",
                "no primary source supports the claim as written",
                "traced the figure back to a single 2019 measurement",
            ]),

        Specialty.Analyst => new AgentProfile(
            "Analyst", "turns measurements into a finding", 300, 900,
            [
                "p95 moved 18% the wrong way after the change",
                "the variance is in the tail, not the mean",
                "two of the six cohorts explain the whole delta",
                "the effect disappears once you control for volume",
            ]),

        Specialty.Engineer => new AgentProfile(
            "Engineer", "reads and writes the code", 520, 1_400,
            [
                "the allocation is in the framing loop, not the parser",
                "reproduced on the second run; it is a race, not a leak",
                "one lock removed, the hot path is now allocation-free",
                "the fix is four lines; the test to prove it is forty",
            ]),

        Specialty.Writer => new AgentProfile(
            "Writer", "drafts the prose", 380, 1_000,
            [
                "drafted at 340 words, one claim per paragraph",
                "rewrote the opening; the old one buried the finding",
                "cut the summary in half without losing a fact",
                "draft ready, two figures still need captions",
            ]),

        Specialty.Critic => new AgentProfile(
            "Critic", "reviews what the others produced", 260, 700,
            [
                "the argument holds; the second exhibit does not",
                "two unsupported claims, both in the conclusion",
                "accurate but unreadable at this length",
                "approved with one correction to the numbers",
            ]),

        Specialty.Translator => new AgentProfile(
            "Translator", "moves text between Bahasa Indonesia and English", 340, 950,
            [
                "translated in full; three technical terms kept in English",
                "the idiom in paragraph two has no direct equivalent",
                "reads naturally in Bahasa Indonesia, register unchanged",
                "back-translation matches the source on every claim",
            ]),

        _ => throw new ArgumentOutOfRangeException(nameof(specialty)),
    };
}
