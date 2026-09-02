// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.ObjectModel;
using Avalonia.Media;
using Nerve.AgentSim.Agents;

namespace Nerve.AgentSim.ViewModels;

/// <summary>One instruction, its plan, and the answers as they come back.</summary>
public sealed class MissionViewModel : Observable
{
    private int _answered;
    private bool _complete;
    private int _elapsedMs;
    private double _confidence;

    /// <summary>Creates the view model for a mission the orchestrator has accepted.</summary>
    /// <param name="mission">The mission as it was published.</param>
    public MissionViewModel(Mission mission)
    {
        Id = mission.Id;
        Instruction = mission.Instruction;
        Accepted = mission.Accepted;
    }

    /// <summary>The mission's sequence number.</summary>
    public int Id { get; }

    /// <summary>What was asked for.</summary>
    public string Instruction { get; }

    /// <summary>When the orchestrator accepted it.</summary>
    public DateTime Accepted { get; }

    /// <summary>The plan, in dispatch order.</summary>
    public ObservableCollection<StepViewModel> Steps { get; } = [];

    /// <summary>The mission's number, formatted the way it appears on the card.</summary>
    public string Label => $"M{Id:000}";

    /// <summary>When it was accepted, to the second.</summary>
    public string AcceptedAt => Accepted.ToString("HH:mm:ss");

    /// <summary>Answers received so far.</summary>
    public int Answered
    {
        get => _answered;
        private set { if (Set(ref _answered, value)) Raise(nameof(Progress)); }
    }

    /// <summary>True once every step has answered and the digest has arrived.</summary>
    public bool Complete
    {
        get => _complete;
        private set
        {
            if (!Set(ref _complete, value)) return;
            Raise(nameof(State));
            Raise(nameof(StateBrush));
        }
    }

    /// <summary>How long the whole mission took, once it is finished.</summary>
    public int ElapsedMs
    {
        get => _elapsedMs;
        private set { if (Set(ref _elapsedMs, value)) Raise(nameof(Summary)); }
    }

    /// <summary>The weakest confidence among the answers.</summary>
    public double Confidence
    {
        get => _confidence;
        private set { if (Set(ref _confidence, value)) Raise(nameof(Summary)); }
    }

    /// <summary>Where the mission is: dispatched, working, or done.</summary>
    public string State => _complete ? "aggregated" : Steps.Count == 0 ? "planning" : $"{_answered} of {Steps.Count}";

    /// <summary>Cresyl while in flight, verdigris once aggregated.</summary>
    public IBrush StateBrush => new SolidColorBrush(_complete
        ? Color.FromRgb(0x0E, 0x7C, 0x6B)
        : Color.FromRgb(0x6B, 0x2F, 0xA0));

    /// <summary>How far through the plan this mission is, from zero to one.</summary>
    public double Progress => Steps.Count == 0 ? 0 : (double)_answered / Steps.Count;

    /// <summary>The line under the instruction on a finished card.</summary>
    public string Summary => _complete
        ? $"{_elapsedMs} ms   confidence {_confidence:P0}"
        : "in flight";

    /// <summary>Adds a step the orchestrator dispatched.</summary>
    /// <param name="task">The sub-task as it was published.</param>
    public void AddStep(SubTask task)
    {
        Steps.Add(new StepViewModel(task));
        Raise(nameof(State));
        Raise(nameof(Progress));
    }

    /// <summary>Records a specialist's answer against its step.</summary>
    /// <param name="result">The answer as it was published.</param>
    public void Apply(SubResult result)
    {
        foreach (StepViewModel step in Steps)
        {
            if (step.Step != result.Step) continue;
            step.Apply(result);
            Answered++;
            Raise(nameof(State));
            return;
        }
    }

    /// <summary>Marks the mission finished with the orchestrator's digest.</summary>
    /// <param name="digest">The aggregated result.</param>
    public void Finish(MissionDigest digest)
    {
        ElapsedMs = digest.ElapsedMs;
        Confidence = digest.Confidence;
        Complete = true;
    }
}

/// <summary>One step of a plan, before and after the specialist answers.</summary>
public sealed class StepViewModel : Observable
{
    private string _finding = "waiting";
    private double _confidence;
    private int _elapsedMs;
    private bool _done;

    /// <summary>Creates the view model for a dispatched sub-task.</summary>
    /// <param name="task">The sub-task as it was published.</param>
    public StepViewModel(SubTask task)
    {
        Step = task.Step;
        Specialty = task.Specialty;
        Display = AgentProfile.For(task.Specialty).Display;
        Brief = task.Brief;
        Stain = new SolidColorBrush(AgentViewModel.StainFor(task.Specialty));
    }

    /// <summary>Position in the plan, from one.</summary>
    public int Step { get; }

    /// <summary>Who was asked.</summary>
    public Specialty Specialty { get; }

    /// <summary>Their name.</summary>
    public string Display { get; }

    /// <summary>What they were asked for.</summary>
    public string Brief { get; }

    /// <summary>Their stain.</summary>
    public IBrush Stain { get; }

    /// <summary>What they came back with.</summary>
    public string Finding
    {
        get => _finding;
        private set => Set(ref _finding, value);
    }

    /// <summary>True once they have answered.</summary>
    public bool Done
    {
        get => _done;
        private set
        {
            if (!Set(ref _done, value)) return;
            Raise(nameof(Trailer));
            Raise(nameof(PipOpacity));
        }
    }

    /// <summary>Full once this step has answered, ghosted while it is still out.</summary>
    public double PipOpacity => _done ? 1.0 : 0.22;

    /// <summary>The line under the finding.</summary>
    public string Trailer => _done ? $"{_elapsedMs} ms   {_confidence:P0}" : "working";

    /// <summary>Records the answer.</summary>
    /// <param name="result">The answer as it was published.</param>
    public void Apply(SubResult result)
    {
        Finding = result.Finding;
        _confidence = result.Confidence;
        _elapsedMs = result.ElapsedMs;
        Done = true;
    }
}

/// <summary>One line in the synapse log.</summary>
/// <param name="Time">When the panel observed the message.</param>
/// <param name="Topic">The topic it was published to.</param>
/// <param name="Detail">A one-line description of the payload.</param>
/// <param name="Accent">The stain of whoever it belongs to.</param>
public sealed record LogLine(string Time, string Topic, string Detail, IBrush Accent);
