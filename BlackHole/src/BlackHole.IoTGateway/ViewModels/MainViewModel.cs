// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Collections.ObjectModel;
using Avalonia.Media;
using Avalonia.Threading;
using BlackHole.IoTGateway.Controls;
using BlackHole.IoTGateway.Simulation;

namespace BlackHole.IoTGateway.ViewModels;

/// <summary>One line in the activity list, already formatted for display.</summary>
public sealed class ActivityItem
{
    public required string Time { get; init; }
    public required string Source { get; init; }
    public required string Detail { get; init; }
    public required IBrush Marker { get; init; }
}

/// <summary>
/// The panel's state: a running gateway, the devices attached to it, and the figures on the rails.
/// </summary>
/// <remarks>
/// Two clocks drive this. Devices publish on their own schedules into lock-free buffers, and a
/// single 33 ms dispatcher timer pulls everything the UI shows out of those buffers at once. That
/// keeps the render cost flat whether there are four devices at 2 Hz or forty at 200 Hz.
/// </remarks>
public sealed class MainViewModel : Observable, IAsyncDisposable
{
    private static readonly string[] Areas = ["floor-1", "floor-2", "utility", "outdoor"];

    private readonly GatewayHost _gateway = new();
    private readonly DispatcherTimer _render;
    private readonly Dictionary<string, DeviceViewModel> _byTopic = new(StringComparer.Ordinal);
    private readonly List<ChartChannel> _channels = [];
    private readonly Random _random = new(20260902);

    private int _nextDeviceNumber = 1;
    private long _lastMessageCount;
    private long _lastSampleTicks = Environment.TickCount64;
    private double _messagesPerSecond;
    private DeviceViewModel? _selected;
    private string _status = "Gateway is stopped. Start it, then add devices.";
    private string _commandResult = string.Empty;
    private bool _isRunning;
    private int _sampleRateHz = 8;

    public MainViewModel()
    {
        StartGateway = new AsyncCommand(StartGatewayAsync, () => !IsRunning);
        StopGateway = new AsyncCommand(StopGatewayAsync, () => IsRunning);
        AddDevice = new AsyncCommand(() => AddDeviceAsync(null), () => IsRunning);
        AddTen = new AsyncCommand(AddTenAsync, () => IsRunning);
        RemoveDevice = new AsyncCommand(RemoveSelectedAsync, () => Selected is not null);
        ToggleStream = new AsyncCommand(ToggleSelectedAsync, () => Selected is not null);
        ForceExcursion = new AsyncCommand(ForceExcursionAsync, () => Selected is not null);
        Identify = new AsyncCommand(IdentifyAsync, () => Selected is not null && IsRunning);
        Calibrate = new AsyncCommand(CalibrateAsync, () => Selected is not null && IsRunning);
        UploadFirmware = new AsyncCommand(UploadFirmwareAsync, () => Selected is not null && IsRunning);

        _gateway.ReadingReceived += OnReadingReceived;

        _render = new DispatcherTimer(DispatcherPriority.Render) { Interval = TimeSpan.FromMilliseconds(33) };
        _render.Tick += (_, _) => Render();
    }

    // ------------------------------------------------------------- collections

    /// <summary>Devices on the plant floor, in the order they were added.</summary>
    public ObservableCollection<DeviceViewModel> Devices { get; } = [];

    /// <summary>Recent gateway activity, newest first.</summary>
    public ObservableCollection<ActivityItem> Activity { get; } = [];

    /// <summary>Pens for the traffic ribbon; the chart reads this list directly.</summary>
    public IReadOnlyList<ChartChannel> Channels => _channels;

    /// <summary>Sensor kinds a new device can be, for the picker.</summary>
    public IReadOnlyList<SensorProfile> SensorKinds { get; } = SensorProfile.Catalogue;

    /// <summary>Kind used when adding the next device.</summary>
    public SensorProfile SelectedKind { get; set; } = SensorProfile.For(SensorKind.Temperature);

    // ----------------------------------------------------------------- commands

    public AsyncCommand StartGateway { get; }
    public AsyncCommand StopGateway { get; }
    public AsyncCommand AddDevice { get; }
    public AsyncCommand AddTen { get; }
    public AsyncCommand RemoveDevice { get; }
    public AsyncCommand ToggleStream { get; }
    public AsyncCommand ForceExcursion { get; }
    public AsyncCommand Identify { get; }
    public AsyncCommand Calibrate { get; }
    public AsyncCommand UploadFirmware { get; }

    // ------------------------------------------------------------------- state

    /// <summary>True while the gateway is accepting.</summary>
    public bool IsRunning
    {
        get => _isRunning;
        private set
        {
            if (Set(ref _isRunning, value))
            {
                Raise(nameof(GatewayStateLabel));
                Raise(nameof(GatewayStateBrush));
                RefreshCommands();
            }
        }
    }

    /// <summary>Word shown beside the status lamp.</summary>
    public string GatewayStateLabel => IsRunning ? $"running on port {_gateway.Port}" : "stopped";

    /// <summary>Lamp colour: green when accepting, dim steel when not.</summary>
    public IBrush GatewayStateBrush => IsRunning
        ? new SolidColorBrush(Color.Parse("#56B87F"))
        : new SolidColorBrush(Color.Parse("#3E5064"));

    /// <summary>The one line of guidance under the title.</summary>
    public string Status
    {
        get => _status;
        private set => Set(ref _status, value);
    }

    /// <summary>Reply from the last command sent to a device.</summary>
    public string CommandResult
    {
        get => _commandResult;
        private set => Set(ref _commandResult, value);
    }

    /// <summary>The device whose pen is drawn bold and whose controls are live.</summary>
    public DeviceViewModel? Selected
    {
        get => _selected;
        set
        {
            DeviceViewModel? previous = _selected;
            if (!Set(ref _selected, value))
                return;

            if (previous is not null)
                previous.IsSelected = false;
            if (value is not null)
                value.IsSelected = true;

            foreach (ChartChannel channel in _channels)
                channel.IsEmphasised = value is not null && channel.Label == value.DeviceId;

            Raise(nameof(HasSelection));
            Raise(nameof(SelectionSummary));
            RefreshCommands();
        }
    }

    /// <summary>True when a device row is selected.</summary>
    public bool HasSelection => Selected is not null;

    /// <summary>Header for the device detail panel.</summary>
    public string SelectionSummary => Selected is null
        ? "Select a device to command it"
        : $"{Selected.DeviceId} · {Selected.Measurement}";

    /// <summary>Sample rate applied to every device, in readings per second.</summary>
    public int SampleRateHz
    {
        get => _sampleRateHz;
        set
        {
            if (!Set(ref _sampleRateHz, value))
                return;
            foreach (DeviceViewModel device in Devices)
                device.Device.SampleRateHz = value;
            Raise(nameof(SampleRateLabel));
        }
    }

    /// <summary>Sample rate with its unit, for the slider caption.</summary>
    public string SampleRateLabel => $"{SampleRateHz} Hz per device";

    // ---------------------------------------------------------------- readouts

    public string DeviceCount => Devices.Count.ToString("N0");
    public string ConnectionCount => _gateway.ConnectionCount.ToString("N0");
    public string ReadingsReceived => _gateway.ReadingsReceived.ToString("N0");
    public string MessagesPerSecond => _messagesPerSecond.ToString("N0");
    public string BytesReceived => FormatBytes(_gateway.BytesReceived);
    public string StreamsReceived => _gateway.StreamsReceived.ToString("N0");
    public string StreamBytes => FormatBytes(_gateway.StreamBytes);
    public string CommandsHandled => _gateway.CommandsHandled.ToString("N0");
    public string TopicCount => _gateway.TopicCount.ToString("N0");
    public string AlarmCount => _gateway.Alarms.ToString("N0");

    // ---------------------------------------------------------------- gateway

    private Task StartGatewayAsync()
    {
        _gateway.Start();
        IsRunning = true;
        _render.Start();
        Status = "Gateway is listening. Add devices to see telemetry arrive.";
        return Task.CompletedTask;
    }

    /// <summary>
    /// Brings the panel up already running with a plant floor attached.
    /// </summary>
    /// <remarks>
    /// Used by <c>--demo</c> for screenshots and for showing the panel without a tour of the
    /// buttons. It calls exactly the same commands an operator would, so nothing in this path is a
    /// mock: the gateway really listens and the devices really connect.
    /// </remarks>
    public async Task RunDemoAsync(int deviceCount = 12)
    {
        await StartGatewayAsync();

        for (int i = 0; i < deviceCount; i++)
            await AddDeviceAsync((SensorKind)(i % SensorProfile.Catalogue.Count));

        // One device driven towards its alarm threshold, so the panel shows an excursion rather
        // than a floor of flat traces.
        if (Devices.Count > 2)
        {
            Selected = Devices[2];
            Devices[2].Device.Excursion = true;
        }

        Status = $"{deviceCount} devices streaming into the gateway on port {_gateway.Port}.";
    }

    private async Task StopGatewayAsync()
    {
        _render.Stop();

        foreach (DeviceViewModel device in Devices.ToArray())
            await device.Device.StopAsync();

        Devices.Clear();
        _channels.Clear();
        _byTopic.Clear();
        Selected = null;

        await _gateway.StopAsync();
        IsRunning = false;
        Render();
        Status = "Gateway is stopped. Start it, then add devices.";
    }

    // ---------------------------------------------------------------- devices

    private async Task AddDeviceAsync(SensorKind? kind)
    {
        SensorKind chosen = kind ?? SelectedKind.Kind;
        int number = _nextDeviceNumber++;
        string area = Areas[_random.Next(Areas.Length)];
        var device = new SimulatedDevice(SimulatedDevice.BuildId(chosen, number), area, chosen, seed: number * 7919)
        {
            SampleRateHz = SampleRateHz,
        };

        var viewModel = new DeviceViewModel(device, Devices.Count);
        _byTopic[device.Topic] = viewModel;
        Devices.Add(viewModel);

        _channels.Add(new ChartChannel
        {
            Label = device.DeviceId,
            Pen = viewModel.Pen,
            Samples = viewModel.Samples,
            Minimum = device.Profile.Minimum,
            Maximum = device.Profile.Maximum,
        });

        try
        {
            await device.StartAsync(_gateway.Port);
            Status = $"{device.DeviceId} is publishing to {device.Topic}";
        }
        catch (Exception ex)
        {
            Status = $"{device.DeviceId} could not connect: {ex.Message}";
            _gateway.Log(GatewayEventKind.Fault, device.DeviceId, ex.Message);
        }

        Raise(nameof(DeviceCount));
        RefreshCommands();
    }

    private async Task AddTenAsync()
    {
        // One of each kind, cycling, so the ribbon shows genuinely different signal shapes rather
        // than ten copies of the same trace.
        for (int i = 0; i < 10; i++)
            await AddDeviceAsync((SensorKind)(i % SensorProfile.Catalogue.Count));
    }

    private async Task RemoveSelectedAsync()
    {
        if (Selected is not { } target)
            return;

        await target.Device.StopAsync();
        _byTopic.Remove(target.Topic);
        _channels.RemoveAll(c => c.Label == target.DeviceId);
        Devices.Remove(target);
        Selected = null;

        Status = $"{target.DeviceId} removed.";
        Raise(nameof(DeviceCount));
    }

    private Task ToggleSelectedAsync()
    {
        if (Selected is not { } target)
            return Task.CompletedTask;

        if (target.Device.State == DeviceState.Streaming)
        {
            target.Device.Pause();
            Status = $"{target.DeviceId} paused. It stays connected.";
        }
        else
        {
            target.Device.Resume();
            Status = $"{target.DeviceId} resumed.";
        }
        return Task.CompletedTask;
    }

    private Task ForceExcursionAsync()
    {
        if (Selected is not { } target)
            return Task.CompletedTask;

        target.Device.Excursion = !target.Device.Excursion;
        Status = target.Device.Excursion
            ? $"{target.DeviceId} is being driven towards its alarm threshold."
            : $"{target.DeviceId} is returning to nominal.";
        return Task.CompletedTask;
    }

    // --------------------------------------------------------------- commands

    private async Task IdentifyAsync()
    {
        CommandResult = await _gateway.SendCommandAsync("device/identify", "?");
    }

    private async Task CalibrateAsync()
    {
        if (Selected is not { } target)
            return;

        // Nudge by 5% of the sensor's range, which is a plausible field correction.
        double delta = (target.Maximum - target.Minimum) * 0.05;
        CommandResult = await _gateway.SendCommandAsync("device/calibrate", delta.ToString("F2"));
    }

    private async Task UploadFirmwareAsync()
    {
        if (Selected is not { } target)
            return;

        Status = $"Uploading firmware from {target.DeviceId}...";
        var progress = new Progress<long>(sent =>
            Status = $"{target.DeviceId} firmware: {sent / 1024:N0} KiB sent");

        try
        {
            long sent = await target.Device.UploadFirmwareAsync(4 * 1024 * 1024, progress);
            Status = $"{target.DeviceId} uploaded {sent / 1024:N0} KiB to the gateway.";
        }
        catch (Exception ex)
        {
            Status = $"{target.DeviceId} upload failed: {ex.Message}";
        }
    }

    // ---------------------------------------------------------------- plumbing

    /// <summary>
    /// Runs on the gateway's receive loop. It must stay cheap: buffer the sample and return, and let
    /// the render timer do everything that touches the UI.
    /// </summary>
    private void OnReadingReceived(string topic, Reading reading)
    {
        if (_byTopic.TryGetValue(topic, out DeviceViewModel? device))
            device.Record(reading.Value);
    }

    /// <summary>One frame: refresh rows, recompute rates, drain the log, repaint the charts.</summary>
    private void Render()
    {
        foreach (DeviceViewModel device in Devices)
        {
            ReadingLevel before = device.Level;
            device.Refresh();
            if (device.Level == ReadingLevel.Alarm && before != ReadingLevel.Alarm)
                _gateway.RecordAlarm(device.DeviceId,
                    $"{device.Measurement} reached {device.Display} {device.Unit}");
        }

        long now = Environment.TickCount64;
        long elapsed = now - _lastSampleTicks;
        if (elapsed >= 500)
        {
            long messages = _gateway.MessagesReceived;
            _messagesPerSecond = (messages - _lastMessageCount) * 1000.0 / elapsed;
            _lastMessageCount = messages;
            _lastSampleTicks = now;
        }

        foreach (GatewayEvent item in _gateway.DrainEvents())
        {
            Activity.Insert(0, new ActivityItem
            {
                Time = item.At.ToString("HH:mm:ss"),
                Source = item.Source,
                Detail = item.Detail,
                Marker = MarkerFor(item.Kind),
            });
        }
        while (Activity.Count > 200)
            Activity.RemoveAt(Activity.Count - 1);

        Raise(nameof(ConnectionCount));
        Raise(nameof(ReadingsReceived));
        Raise(nameof(MessagesPerSecond));
        Raise(nameof(BytesReceived));
        Raise(nameof(StreamsReceived));
        Raise(nameof(StreamBytes));
        Raise(nameof(CommandsHandled));
        Raise(nameof(TopicCount));
        Raise(nameof(AlarmCount));

        FrameRendered?.Invoke();
    }

    /// <summary>Raised at the end of each frame so the view can tick its custom-drawn charts.</summary>
    public event Action? FrameRendered;

    private static IBrush MarkerFor(GatewayEventKind kind) => new SolidColorBrush(kind switch
    {
        GatewayEventKind.Connection => Color.Parse("#4A90D9"),
        GatewayEventKind.Telemetry => Color.Parse("#56B87F"),
        GatewayEventKind.Command => Color.Parse("#9B7EDE"),
        GatewayEventKind.Stream => Color.Parse("#45B6C4"),
        GatewayEventKind.Alarm => Color.Parse("#E0A33E"),
        _ => Color.Parse("#E2504A"),
    });

    private void RefreshCommands()
    {
        StartGateway.RaiseCanExecuteChanged();
        StopGateway.RaiseCanExecuteChanged();
        AddDevice.RaiseCanExecuteChanged();
        AddTen.RaiseCanExecuteChanged();
        RemoveDevice.RaiseCanExecuteChanged();
        ToggleStream.RaiseCanExecuteChanged();
        ForceExcursion.RaiseCanExecuteChanged();
        Identify.RaiseCanExecuteChanged();
        Calibrate.RaiseCanExecuteChanged();
        UploadFirmware.RaiseCanExecuteChanged();
    }

    private static string FormatBytes(long bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => $"{bytes / 1024.0:N1} KiB",
        < 1024L * 1024 * 1024 => $"{bytes / (1024.0 * 1024):N1} MiB",
        _ => $"{bytes / (1024.0 * 1024 * 1024):N2} GiB",
    };

    public async ValueTask DisposeAsync()
    {
        _render.Stop();
        foreach (DeviceViewModel device in Devices)
            await device.Device.DisposeAsync();
        await _gateway.DisposeAsync();
    }
}
