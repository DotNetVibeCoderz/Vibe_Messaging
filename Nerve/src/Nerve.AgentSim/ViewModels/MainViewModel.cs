// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using Avalonia.Media;
using Avalonia.Threading;
using Nerve.AgentSim.Agents;
using Nerve.AgentSim.Controls;

namespace Nerve.AgentSim.ViewModels;

/// <summary>
/// The panel. It observes the same topics the agents publish on and holds a reference to none of
/// them.
/// </summary>
/// <remarks>
/// <para>
/// The subscriptions here run on whichever agent thread published the message, so none of them
/// touch a bound collection. They drop the message into <see cref="_inbox"/>, and a 33 ms timer on
/// the UI thread drains it and applies one coalesced batch per frame. Binding a collection straight
/// to a handler would be the same mistake as updating a UI from a socket read loop.
/// </para>
/// <para>
/// The roster arrives without being asked for: specialists retain their status, so the six terminals
/// are populated the moment the panel subscribes, however long after start-up that is.
/// </para>
/// </remarks>
public sealed class MainViewModel : Observable, IDisposable
{
    private const int MaxMissions = 40;
    private const int MaxLogLines = 220;
    private const int MaxDrainPerFrame = 400;

    private readonly SimulationHost _host;
    private readonly ConcurrentQueue<object> _inbox = new();
    private readonly Dictionary<int, MissionViewModel> _byId = [];
    private readonly List<IDisposable> _subscriptions = [];
    private readonly Dictionary<Specialty, int> _agentIndex = [];
    private readonly DispatcherTimer _frame;
    private readonly DispatcherTimer _autopilot;

    private MissionViewModel? _selected;
    private string _statistics = string.Empty;
    private string _rate = string.Empty;
    private bool _autopilotOn;
    private long _lastPublished;
    private DateTime _lastRateAt = DateTime.UtcNow;

    /// <summary>Wires the panel to a running simulation.</summary>
    /// <param name="host">The simulation to observe and feed.</param>
    public MainViewModel(SimulationHost host)
    {
        _host = host;

        var agents = new List<AgentViewModel>();
        foreach (Specialty specialty in Enum.GetValues<Specialty>())
        {
            _agentIndex[specialty] = agents.Count;
            agents.Add(new AgentViewModel(specialty));
        }

        Agents = agents;
        Field = new ArborField(agents);

        Dispatch = new RelayCommand(() => _ = _host.DispatchAsync());
        DispatchBurst = new RelayCommand(() => _ = BurstAsync(4));
        ToggleAutopilot = new RelayCommand(SwitchAutopilot);
        Clear = new RelayCommand(ClearBoard);

        Observe();

        _frame = new DispatcherTimer(TimeSpan.FromMilliseconds(33), DispatcherPriority.Background, (_, _) => Drain());
        _frame.Start();

        _autopilot = new DispatcherTimer(TimeSpan.FromMilliseconds(1700), DispatcherPriority.Background,
            (_, _) => _ = _host.DispatchAsync());
    }

    /// <summary>The six specialists, in terminal order.</summary>
    public IReadOnlyList<AgentViewModel> Agents { get; }

    /// <summary>What the arbor draws.</summary>
    public ArborField Field { get; }

    /// <summary>Missions, newest first.</summary>
    public ObservableCollection<MissionViewModel> Missions { get; } = [];

    /// <summary>Recent traffic, newest first.</summary>
    public ObservableCollection<LogLine> Log { get; } = [];

    /// <summary>Hands one instruction to the orchestrator.</summary>
    public RelayCommand Dispatch { get; }

    /// <summary>Hands four instructions over, to show the specialists working in parallel.</summary>
    public RelayCommand DispatchBurst { get; }

    /// <summary>Starts or stops a mission every 1.7 seconds.</summary>
    public RelayCommand ToggleAutopilot { get; }

    /// <summary>Empties the board without stopping the agents.</summary>
    public RelayCommand Clear { get; }

    /// <summary>The mission whose plan is shown on the right.</summary>
    public MissionViewModel? Selected
    {
        get => _selected;
        set => Set(ref _selected, value);
    }

    /// <summary>The hub's own counters, refreshed on the frame timer.</summary>
    public string Statistics
    {
        get => _statistics;
        private set => Set(ref _statistics, value);
    }

    /// <summary>Messages per second across the whole hub.</summary>
    public string Rate
    {
        get => _rate;
        private set => Set(ref _rate, value);
    }

    /// <summary>Whether missions are being fed in automatically.</summary>
    public bool AutopilotOn
    {
        get => _autopilotOn;
        private set { if (Set(ref _autopilotOn, value)) Raise(nameof(AutopilotLabel)); }
    }

    /// <summary>The autopilot button's label.</summary>
    public string AutopilotLabel => _autopilotOn ? "Stop autopilot" : "Start autopilot";

    // ============================== Observation ==============================

    /// <summary>
    /// Subscribes to the traffic. Five wildcard subscriptions are the panel's entire connection to
    /// the simulation.
    /// </summary>
    private void Observe()
    {
        _subscriptions.Add(_host.Hub.Subscribe<Mission>(Topics.MissionInbox, _inbox.Enqueue));
        _subscriptions.Add(_host.Hub.Subscribe<SubTask>(Topics.AnyTask, _inbox.Enqueue));
        _subscriptions.Add(_host.Hub.Subscribe<SubResult>(Topics.AnyResult, _inbox.Enqueue));
        _subscriptions.Add(_host.Hub.Subscribe<MissionDigest>(Topics.MissionComplete, _inbox.Enqueue));
        _subscriptions.Add(_host.Hub.Subscribe<AgentStatus>(Topics.AnyRoster, _inbox.Enqueue));
    }

    /// <summary>Applies one frame's worth of traffic to the bound collections.</summary>
    private void Drain()
    {
        int drained = 0;
        while (drained++ < MaxDrainPerFrame && _inbox.TryDequeue(out object? message))
        {
            switch (message)
            {
                case Mission mission: OnMission(mission); break;
                case SubTask task: OnTask(task); break;
                case SubResult result: OnResult(result); break;
                case MissionDigest digest: OnDigest(digest); break;
                case AgentStatus status: OnStatus(status); break;
            }
        }

        RefreshCounters();
    }

    private void OnMission(Mission mission)
    {
        var view = new MissionViewModel(mission);
        _byId[mission.Id] = view;
        Missions.Insert(0, view);
        Selected ??= view;

        while (Missions.Count > MaxMissions)
        {
            MissionViewModel oldest = Missions[^1];
            Missions.RemoveAt(Missions.Count - 1);
            _byId.Remove(oldest.Id);
            if (ReferenceEquals(Selected, oldest)) Selected = Missions.FirstOrDefault();
        }

        Write(Topics.MissionInbox, $"{view.Label}  {mission.Instruction}", Brushes.Cresyl);
    }

    private void OnTask(SubTask task)
    {
        if (_byId.TryGetValue(task.MissionId, out MissionViewModel? mission)) mission.AddStep(task);

        if (_agentIndex.TryGetValue(task.Specialty, out int index)) Field.Emit(index, outbound: true);

        Write(Topics.TaskFor(task.Specialty),
            $"M{task.MissionId:000} step {task.Step}/{task.Steps}  {task.Brief}",
            new SolidColorBrush(AgentViewModel.StainFor(task.Specialty)));
    }

    private void OnResult(SubResult result)
    {
        if (_byId.TryGetValue(result.MissionId, out MissionViewModel? mission)) mission.Apply(result);

        if (_agentIndex.TryGetValue(result.Specialty, out int index))
        {
            Field.Emit(index, outbound: false);
            Field.FlareSoma();
            Agents[index].LastFinding = result.Finding;
        }

        Write(Topics.ResultFrom(result.Specialty),
            $"M{result.MissionId:000}  {result.Finding}",
            new SolidColorBrush(AgentViewModel.StainFor(result.Specialty)));
    }

    private void OnDigest(MissionDigest digest)
    {
        if (_byId.TryGetValue(digest.MissionId, out MissionViewModel? mission)) mission.Finish(digest);

        Write(Topics.MissionComplete,
            $"M{digest.MissionId:000} aggregated from {digest.Parts.Count} agents in {digest.ElapsedMs} ms",
            Brushes.Verdigris);
    }

    private void OnStatus(AgentStatus status)
    {
        if (!_agentIndex.TryGetValue(status.Specialty, out int index)) return;

        AgentViewModel agent = Agents[index];
        agent.Queued = status.Queued;
        agent.Completed = status.Completed;
        agent.Busy = status.Busy;
    }

    private void Write(string topic, string detail, IBrush accent)
    {
        Log.Insert(0, new LogLine(DateTime.Now.ToString("HH:mm:ss.ff"), topic, detail, accent));
        while (Log.Count > MaxLogLines) Log.RemoveAt(Log.Count - 1);
    }

    private void RefreshCounters()
    {
        NerveStatistics stats = _host.Hub.GetStatistics();
        Statistics =
            $"published {stats.Published:N0}   delivered {stats.Delivered:N0}   " +
            $"routes {stats.Routes}   subscriptions {stats.Subscriptions}   retained {stats.Retained}   " +
            $"errors {stats.Errors}";

        DateTime now = DateTime.UtcNow;
        double seconds = (now - _lastRateAt).TotalSeconds;
        if (seconds < 1) return;

        long published = stats.Published;
        Rate = $"{(published - _lastPublished) / seconds:N0} msg/s";
        _lastPublished = published;
        _lastRateAt = now;
    }

    // ================================ Commands ================================

    private async Task BurstAsync(int count)
    {
        for (int i = 0; i < count; i++)
        {
            await _host.DispatchAsync().ConfigureAwait(false);
            await Task.Delay(90).ConfigureAwait(false);
        }
    }

    private void SwitchAutopilot()
    {
        AutopilotOn = !AutopilotOn;
        if (AutopilotOn) _autopilot.Start();
        else _autopilot.Stop();
    }

    private void ClearBoard()
    {
        Missions.Clear();
        Log.Clear();
        _byId.Clear();
        Field.Clear();
        Selected = null;
    }

    /// <summary>
    /// Runs a scripted set of missions. Used by <c>--demo</c> and by the screenshot pass, so the
    /// picture in the documentation is of the panel doing real work rather than a staged one.
    /// </summary>
    /// <param name="missions">How many instructions to feed in.</param>
    public async Task RunDemoAsync(int missions = 6)
    {
        for (int i = 0; i < missions; i++)
        {
            await _host.DispatchAsync(MissionCatalog.All[i % MissionCatalog.All.Count]).ConfigureAwait(false);
            await Task.Delay(260).ConfigureAwait(false);
        }
    }

    /// <summary>Stops the timers and unsubscribes from the hub.</summary>
    public void Dispose()
    {
        _frame.Stop();
        _autopilot.Stop();
        foreach (IDisposable subscription in _subscriptions) subscription.Dispose();
        _subscriptions.Clear();
    }
}

/// <summary>The two stains the panel uses for things that belong to no single agent.</summary>
internal static class Brushes
{
    public static readonly IBrush Cresyl = new SolidColorBrush(Color.FromRgb(0x6B, 0x2F, 0xA0));
    public static readonly IBrush Verdigris = new SolidColorBrush(Color.FromRgb(0x0E, 0x7C, 0x6B));
}
