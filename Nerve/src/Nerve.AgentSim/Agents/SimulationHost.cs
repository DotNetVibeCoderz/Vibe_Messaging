// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve.AgentSim.Agents;

/// <summary>
/// Owns the hub, the orchestrator and the six specialists, and hands the panel a way to feed
/// instructions in.
/// </summary>
/// <remarks>
/// This is the whole wiring of the simulation. Note what is not here: no agent is passed to another
/// agent, and the panel is passed to none of them. Every arrow on screen is a topic.
/// </remarks>
public sealed class SimulationHost : IAsyncDisposable
{
    private readonly CancellationTokenSource _stopping = new();
    private readonly List<Task> _agents = [];
    private readonly Random _random = new();
    private int _missionId;

    /// <summary>Creates the hub and starts every agent.</summary>
    public SimulationHost()
    {
        Hub = new NerveHub(new NerveOptions
        {
            // A specialist that throws should not take the orchestrator's dispatch loop with it.
            ErrorBehavior = HandlerErrorBehavior.Isolate,
        });

        Orchestrator = new Orchestrator(Hub);
        _agents.Add(Orchestrator.RunAsync(_stopping.Token));

        int seed = Environment.TickCount;
        foreach (Specialty specialty in Enum.GetValues<Specialty>())
        {
            var specialist = new Specialist(Hub, specialty, seed++);
            Specialists.Add(specialist);
            _agents.Add(specialist.RunAsync(_stopping.Token));
        }
    }

    /// <summary>The bus every agent and the panel share.</summary>
    public NerveHub Hub { get; }

    /// <summary>The agent that plans and aggregates.</summary>
    public Orchestrator Orchestrator { get; }

    /// <summary>The six sub-agents.</summary>
    public List<Specialist> Specialists { get; } = [];

    /// <summary>Hands a new instruction to the orchestrator.</summary>
    /// <param name="instruction">What to ask for. A random one is drawn when this is null.</param>
    /// <returns>The mission as it was published.</returns>
    public async Task<Mission> DispatchAsync(string? instruction = null)
    {
        var mission = new Mission(
            Interlocked.Increment(ref _missionId),
            instruction ?? MissionCatalog.Random(_random),
            DateTime.Now);

        await Hub.PublishAsync(Topics.MissionInbox, mission).ConfigureAwait(false);
        return mission;
    }

    /// <summary>Asks every specialist what it is for, over request/reply.</summary>
    /// <param name="cancellationToken">Abandons the round.</param>
    /// <returns>One answer per specialist, in roster order.</returns>
    public async Task<IReadOnlyList<Capability>> SurveyAsync(CancellationToken cancellationToken = default)
    {
        var answers = new List<Capability>();
        foreach (Specialty specialty in Enum.GetValues<Specialty>())
        {
            answers.Add(await Hub.RequestAsync<string, Capability>(
                Topics.CapabilityOf(specialty), "who are you", TimeSpan.FromSeconds(2), cancellationToken)
                .ConfigureAwait(false));
        }

        return answers;
    }

    /// <summary>Stops every agent and disposes the hub.</summary>
    public async ValueTask DisposeAsync()
    {
        await _stopping.CancelAsync().ConfigureAwait(false);

        try
        {
            await Task.WhenAll(_agents).WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Cancellation on the way out; nothing here is worth reporting.
        }

        _stopping.Dispose();
        Hub.Dispose();
    }
}

/// <summary>The instructions the simulator draws from.</summary>
/// <remarks>
/// Written as things somebody would actually ask a team for, because the orchestrator plans from
/// the words in them - a list of "Task 1, Task 2" would make every plan identical.
/// </remarks>
public static class MissionCatalog
{
    private static readonly string[] Instructions =
    [
        "Benchmark the ingest path against last quarter and say whether we regressed",
        "Draft the release brief for the v2 messaging rewrite",
        "Translate the onboarding guide into Bahasa Indonesia",
        "Find out why the nightly job crashed on Tuesday and fix it",
        "Compare our wildcard matching against the three MQTT brokers we support",
        "Write the migration guide for teams still on v1",
        "Profile the allocation spike reported by the gateway team",
        "Evaluate whether the retained-message feature is worth the memory",
        "Draft an announcement post for the NuGet release",
        "Research prior art on in-process event buses before we commit to the design",
        "Measure throughput with 32 subscribers and summarise the finding",
        "Fix the race in the subscription churn test and prove it stays fixed",
        "Localise the panel labels and check the register is right",
        "Survey what the team actually uses request/reply for",
        "Write the README section on wildcards, with one worked example",
        "Investigate the memory leak in the streaming consumer",
    ];

    /// <summary>Draws one instruction.</summary>
    /// <param name="random">The source of randomness.</param>
    public static string Random(Random random) => Instructions[random.Next(Instructions.Length)];

    /// <summary>Every instruction, for a scripted run.</summary>
    public static IReadOnlyList<string> All => Instructions;
}
