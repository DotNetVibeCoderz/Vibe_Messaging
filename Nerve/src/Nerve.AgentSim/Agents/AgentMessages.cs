// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve.AgentSim.Agents;

/// <summary>The six specialists the orchestrator can delegate to.</summary>
public enum Specialty
{
    /// <summary>Gathers sources and prior art.</summary>
    Researcher,

    /// <summary>Turns numbers into a finding.</summary>
    Analyst,

    /// <summary>Reads and writes the code.</summary>
    Engineer,

    /// <summary>Drafts the prose.</summary>
    Writer,

    /// <summary>Reviews what the others produced.</summary>
    Critic,

    /// <summary>Moves text between Bahasa Indonesia and English.</summary>
    Translator,
}

/// <summary>Every topic in the simulation, in one place.</summary>
/// <remarks>
/// The agents share nothing but these strings. No agent holds a reference to another, and the
/// panel holds a reference to none of them - it subscribes to the same traffic the agents do.
/// </remarks>
public static class Topics
{
    /// <summary>Where a new instruction is handed to the orchestrator.</summary>
    public const string MissionInbox = "agents/mission/inbox";

    /// <summary>Where the orchestrator publishes an aggregated mission.</summary>
    public const string MissionComplete = "agents/mission/complete";

    /// <summary>Covers every specialist's work queue.</summary>
    public const string AnyTask = "agents/task/+";

    /// <summary>Covers every specialist's answers.</summary>
    public const string AnyResult = "agents/result/+";

    /// <summary>Covers every specialist's retained status.</summary>
    public const string AnyRoster = "agents/roster/+";

    /// <summary>Answered by each specialist, so the orchestrator can ask what it can do.</summary>
    public const string Capability = "agents/+/capability";

    /// <summary>The work queue for one specialist.</summary>
    /// <param name="specialty">Whose queue.</param>
    public static string TaskFor(Specialty specialty) => $"agents/task/{Slug(specialty)}";

    /// <summary>Where one specialist publishes its answers.</summary>
    /// <param name="specialty">Whose answers.</param>
    public static string ResultFrom(Specialty specialty) => $"agents/result/{Slug(specialty)}";

    /// <summary>Where one specialist retains its current status.</summary>
    /// <param name="specialty">Whose status.</param>
    public static string RosterFor(Specialty specialty) => $"agents/roster/{Slug(specialty)}";

    /// <summary>Where one specialist answers capability requests.</summary>
    /// <param name="specialty">Who to ask.</param>
    public static string CapabilityOf(Specialty specialty) => $"agents/{Slug(specialty)}/capability";

    /// <summary>The lower-case topic segment for a specialty.</summary>
    /// <param name="specialty">The specialty.</param>
    public static string Slug(Specialty specialty) => specialty.ToString().ToLowerInvariant();
}

/// <summary>An instruction handed to the orchestrator.</summary>
/// <param name="Id">Sequence number, and what every sub-task carries back.</param>
/// <param name="Instruction">What was asked for, in the words it was asked in.</param>
/// <param name="Accepted">When the orchestrator received it.</param>
public sealed record Mission(int Id, string Instruction, DateTime Accepted);

/// <summary>One piece of a mission, addressed to one specialist.</summary>
/// <param name="MissionId">The mission this belongs to.</param>
/// <param name="Step">Position in the plan, from one.</param>
/// <param name="Steps">How many steps the plan has in total.</param>
/// <param name="Specialty">Who is being asked.</param>
/// <param name="Brief">What that specialist is being asked for.</param>
public sealed record SubTask(int MissionId, int Step, int Steps, Specialty Specialty, string Brief);

/// <summary>One specialist's answer.</summary>
/// <param name="MissionId">The mission this belongs to.</param>
/// <param name="Step">Position in the plan, from one.</param>
/// <param name="Specialty">Who answered.</param>
/// <param name="Finding">What they came back with.</param>
/// <param name="Confidence">How sure they are, from zero to one.</param>
/// <param name="ElapsedMs">How long the work took.</param>
public sealed record SubResult(
    int MissionId, int Step, Specialty Specialty, string Finding, double Confidence, int ElapsedMs);

/// <summary>A mission with every specialist's answer folded back in.</summary>
/// <param name="MissionId">Which mission.</param>
/// <param name="Instruction">What was originally asked.</param>
/// <param name="Parts">The answers, in plan order.</param>
/// <param name="Confidence">The lowest confidence in the set - a chain is as good as its weakest link.</param>
/// <param name="ElapsedMs">Wall-clock time from acceptance to aggregation.</param>
public sealed record MissionDigest(
    int MissionId, string Instruction, IReadOnlyList<SubResult> Parts, double Confidence, int ElapsedMs);

/// <summary>A specialist's current state, retained so a late observer sees the roster at once.</summary>
/// <param name="Specialty">Who this describes.</param>
/// <param name="Queued">Sub-tasks accepted but not yet answered.</param>
/// <param name="Completed">Sub-tasks answered since the simulation started.</param>
/// <param name="Busy">Whether the specialist is working right now.</param>
public sealed record AgentStatus(Specialty Specialty, int Queued, int Completed, bool Busy);

/// <summary>What a specialist says it is for, when asked.</summary>
/// <param name="Specialty">Who answered.</param>
/// <param name="Headline">A one-line description of the work it takes on.</param>
/// <param name="TypicalMs">Roughly how long a sub-task takes it.</param>
public sealed record Capability(Specialty Specialty, string Headline, int TypicalMs);
