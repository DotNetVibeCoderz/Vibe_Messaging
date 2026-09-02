// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.Diagnostics;

namespace Nerve.AgentSim.Agents;

/// <summary>
/// The agent that takes an instruction, decides who should work on it, dispatches the pieces, and
/// folds the answers back into one result.
/// </summary>
/// <remarks>
/// <para>
/// It never calls a specialist. It publishes to <c>agents/task/{specialty}</c> and subscribes to
/// <c>agents/result/+</c>, so the roster can grow or shrink without a line changing here - a
/// seventh specialist would start receiving work the moment it subscribed to its own topic.
/// </para>
/// <para>
/// Missions arrive on a stream so planning happens on the orchestrator's own thread, but results
/// are taken through a plain subscription: folding an answer into a dictionary is a few
/// microseconds of work, and running it inline on the specialist's thread is cheaper than handing
/// it to another one.
/// </para>
/// </remarks>
public sealed class Orchestrator
{
    private readonly INerveHub _hub;
    private readonly ConcurrentDictionary<int, MissionState> _inFlight = new();
    private IDisposable? _results;

    /// <summary>Creates the orchestrator.</summary>
    /// <param name="hub">The bus.</param>
    public Orchestrator(INerveHub hub) => _hub = hub;

    /// <summary>Missions accepted but not yet aggregated.</summary>
    public int InFlight => _inFlight.Count;

    /// <summary>Runs until cancelled.</summary>
    /// <param name="cancellationToken">Stops the orchestrator.</param>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _results = _hub.Subscribe<SubResult>(Topics.AnyResult, Fold);

        try
        {
            await foreach (Mission mission in _hub
                .StreamAsync<Mission>(Topics.MissionInbox, capacity: 128, cancellationToken)
                .ConfigureAwait(false))
            {
                await DispatchAsync(mission, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        finally
        {
            _results?.Dispose();
        }
    }

    private async Task DispatchAsync(Mission mission, CancellationToken cancellationToken)
    {
        IReadOnlyList<PlannedStep> plan = Plan(mission.Instruction);
        _inFlight[mission.Id] = new MissionState(mission, plan.Count);

        for (int i = 0; i < plan.Count; i++)
        {
            PlannedStep step = plan[i];
            var task = new SubTask(mission.Id, i + 1, plan.Count, step.Specialty, step.Brief);
            await _hub.PublishAsync(Topics.TaskFor(step.Specialty), task, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Folds one specialist's answer into its mission, and publishes the digest once the last one
    /// lands.
    /// </summary>
    private void Fold(SubResult result)
    {
        if (!_inFlight.TryGetValue(result.MissionId, out MissionState? state)) return;
        if (!state.Accept(result)) return;

        _inFlight.TryRemove(result.MissionId, out _);

        IReadOnlyList<SubResult> parts = state.Ordered();
        var digest = new MissionDigest(
            state.Mission.Id,
            state.Mission.Instruction,
            parts,
            parts.Min(p => p.Confidence),
            (int)state.Elapsed.TotalMilliseconds);

        _hub.Publish(Topics.MissionComplete, digest);
    }

    // ================================= Planning =================================

    /// <summary>
    /// Works out who should see an instruction, from the instruction itself.
    /// </summary>
    /// <param name="instruction">What was asked for.</param>
    /// <returns>The steps to dispatch, in order.</returns>
    /// <remarks>
    /// Keyword matching, deliberately: the point of the simulation is the coordination, and a plan
    /// that visibly changes with the wording makes the dispatch legible on screen. A real
    /// orchestrator would put a model here and change nothing else.
    /// </remarks>
    public static IReadOnlyList<PlannedStep> Plan(string instruction)
    {
        string text = instruction.ToLowerInvariant();
        var steps = new List<PlannedStep>();

        if (ContainsAny(text, "benchmark", "latency", "throughput", "measure", "profile", "allocation"))
        {
            steps.Add(new PlannedStep(Specialty.Analyst, "quantify the change against the last run"));
            steps.Add(new PlannedStep(Specialty.Engineer, "locate the cost in the code"));
        }

        if (ContainsAny(text, "translate", "bahasa", "indonesian", "localise", "localize"))
        {
            steps.Add(new PlannedStep(Specialty.Translator, "produce the Bahasa Indonesia version"));
        }

        if (ContainsAny(text, "bug", "crash", "regression", "fix", "refactor", "leak", "race"))
        {
            steps.Add(new PlannedStep(Specialty.Engineer, "reproduce, then narrow it to one change"));
        }

        if (ContainsAny(text, "survey", "compare", "research", "evaluate", "prior art", "sources"))
        {
            steps.Add(new PlannedStep(Specialty.Researcher, "gather sources and check the premise"));
        }

        if (ContainsAny(text, "draft", "brief", "announce", "write", "guide", "readme", "post", "summary"))
        {
            steps.Add(new PlannedStep(Specialty.Writer, "draft it, one claim per paragraph"));
        }

        // Nothing matched: fall back to the pair that can start on anything.
        if (steps.Count == 0)
        {
            steps.Add(new PlannedStep(Specialty.Researcher, "establish what is actually being asked"));
            steps.Add(new PlannedStep(Specialty.Writer, "put the answer into words"));
        }

        // Every plan ends with a review. It is the one step that is not about the instruction.
        steps.Add(new PlannedStep(Specialty.Critic, "review the assembled result"));

        return steps;
    }

    private static bool ContainsAny(string text, params string[] needles)
    {
        foreach (string needle in needles)
            if (text.Contains(needle, StringComparison.Ordinal)) return true;
        return false;
    }

    // ============================== Mission state ==============================

    private sealed class MissionState(Mission mission, int expected)
    {
        private readonly ConcurrentDictionary<int, SubResult> _parts = new();
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        private int _completed;

        public Mission Mission { get; } = mission;

        public TimeSpan Elapsed => _stopwatch.Elapsed;

        /// <summary>Records one answer. True when this was the answer that completed the mission.</summary>
        public bool Accept(SubResult result)
        {
            if (!_parts.TryAdd(result.Step, result)) return false;
            return Interlocked.Increment(ref _completed) == expected;
        }

        public IReadOnlyList<SubResult> Ordered() =>
            [.. _parts.Values.OrderBy(p => p.Step)];
    }
}

/// <summary>One step of a plan: who to ask, and for what.</summary>
/// <param name="Specialty">Who the step is addressed to.</param>
/// <param name="Brief">What they are being asked for.</param>
public sealed record PlannedStep(Specialty Specialty, string Brief);
