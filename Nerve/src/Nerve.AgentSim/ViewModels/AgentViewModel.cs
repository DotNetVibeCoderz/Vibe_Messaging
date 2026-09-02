// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia.Media;
using Nerve.AgentSim.Agents;

namespace Nerve.AgentSim.ViewModels;

/// <summary>One specialist as the panel sees it: a stain, a queue depth, and what it last said.</summary>
/// <remarks>
/// Everything on here arrives from <c>agents/roster/+</c> and <c>agents/result/+</c>. The panel
/// holds no reference to the <see cref="Specialist"/> it describes.
/// </remarks>
public sealed class AgentViewModel : Observable
{
    private int _queued;
    private int _completed;
    private bool _busy;
    private string _lastFinding = "idle";
    private double _pulse;

    /// <summary>Creates the view model for one specialty.</summary>
    /// <param name="specialty">Which specialist.</param>
    public AgentViewModel(Specialty specialty)
    {
        Specialty = specialty;
        Display = AgentProfile.For(specialty).Display;
        Headline = AgentProfile.For(specialty).Headline;
        Stain = StainFor(specialty);
        Topic = Topics.TaskFor(specialty);
    }

    /// <summary>Which specialist this describes.</summary>
    public Specialty Specialty { get; }

    /// <summary>The name shown beside its terminal.</summary>
    public string Display { get; }

    /// <summary>What it says it is for.</summary>
    public string Headline { get; }

    /// <summary>The topic it takes work from.</summary>
    public string Topic { get; }

    /// <summary>The one colour that identifies this agent everywhere on the panel.</summary>
    public Color Stain { get; }

    /// <summary>A brush over <see cref="Stain"/>, for binding.</summary>
    public IBrush StainBrush => new SolidColorBrush(Stain);

    /// <summary>Sub-tasks accepted but not yet answered.</summary>
    public int Queued
    {
        get => _queued;
        set { if (Set(ref _queued, value)) Raise(nameof(Caption)); }
    }

    /// <summary>Sub-tasks answered since the run started.</summary>
    public int Completed
    {
        get => _completed;
        set { if (Set(ref _completed, value)) Raise(nameof(Caption)); }
    }

    /// <summary>Whether it is working right now.</summary>
    public bool Busy
    {
        get => _busy;
        set => Set(ref _busy, value);
    }

    /// <summary>What it last came back with.</summary>
    public string LastFinding
    {
        get => _lastFinding;
        set => Set(ref _lastFinding, value);
    }

    /// <summary>The line under the agent's name.</summary>
    public string Caption => $"queue {_queued}  done {_completed}";

    /// <summary>
    /// Decays from one to zero after an answer, so the terminal flares and settles. Advanced by the
    /// arbor's own clock rather than by a message.
    /// </summary>
    public double Pulse
    {
        get => _pulse;
        set => _pulse = value;
    }

    /// <summary>The stain that identifies a specialty. Matches the palette in Theme.axaml.</summary>
    /// <param name="specialty">Which specialist.</param>
    public static Color StainFor(Specialty specialty) => specialty switch
    {
        Specialty.Researcher => Color.FromRgb(0x1B, 0x4F, 0x8C),   // Prussian
        Specialty.Analyst => Color.FromRgb(0x0E, 0x7C, 0x6B),      // Verdigris
        Specialty.Engineer => Color.FromRgb(0x9A, 0x6B, 0x0A),     // Ochre
        Specialty.Writer => Color.FromRgb(0x6B, 0x2F, 0xA0),       // Cresyl
        Specialty.Critic => Color.FromRgb(0xBE, 0x12, 0x50),       // Carmine
        Specialty.Translator => Color.FromRgb(0xA8, 0x45, 0x1E),   // Sienna
        _ => Color.FromRgb(0x5B, 0x6D, 0x79),
    };
}
