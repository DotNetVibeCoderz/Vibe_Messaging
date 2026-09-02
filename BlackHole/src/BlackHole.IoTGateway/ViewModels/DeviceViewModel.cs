// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia.Media;
using BlackHole.IoTGateway.Controls;
using BlackHole.IoTGateway.Simulation;

namespace BlackHole.IoTGateway.ViewModels;

/// <summary>
/// One device row: its identity, its pen, its trace, and the readout beside it.
/// </summary>
/// <remarks>
/// Readings arrive on the gateway's receive loop far faster than the panel repaints, so this class
/// takes them without touching the UI thread - <see cref="TraceBuffer"/> absorbs the samples and
/// <see cref="Refresh"/> publishes a coalesced update on the render timer. Binding straight to the
/// receive loop would peg the dispatcher at a few hundred readings a second.
/// </remarks>
public sealed class DeviceViewModel : Observable
{
    /// <summary>
    /// Pen colours in the order devices claim them, taken from the ink sets multi-channel chart
    /// recorders shipped with. Six is deliberate: past six traces on one chart nothing is readable,
    /// so the seventh device reuses the first pen and is told apart by its row instead.
    /// </summary>
    public static readonly Color[] PenSet =
    [
        Color.Parse("#E2504A"), // red
        Color.Parse("#4A90D9"), // blue
        Color.Parse("#56B87F"), // green
        Color.Parse("#E0A33E"), // amber
        Color.Parse("#9B7EDE"), // violet
        Color.Parse("#45B6C4"), // cyan
    ];

    private double _value;
    private string _display = "--";
    private DeviceState _state;
    private long _published;
    private bool _isSelected;
    private ReadingLevel _level;

    public DeviceViewModel(SimulatedDevice device, int penIndex)
    {
        Device = device;
        Pen = PenSet[penIndex % PenSet.Length];
        PenBrush = new SolidColorBrush(Pen);
        Samples = new TraceBuffer(1024);
        device.Changed += _ => { };
    }

    /// <summary>The device this row drives.</summary>
    public SimulatedDevice Device { get; }

    /// <summary>Trace samples, written from the gateway receive loop.</summary>
    public TraceBuffer Samples { get; }

    /// <summary>This device's ink colour, used in the row, the sparkline and the ribbon alike.</summary>
    public Color Pen { get; }

    /// <summary>Brush form of <see cref="Pen"/>, for XAML.</summary>
    public IBrush PenBrush { get; }

    public string DeviceId => Device.DeviceId;
    public string Area => Device.Area;
    public string Measurement => Device.Profile.Label;
    public string Unit => Device.Profile.Unit;
    public string Topic => Device.Topic;
    public double Minimum => Device.Profile.Minimum;
    public double Maximum => Device.Profile.Maximum;

    /// <summary>Latest value, formatted for the readout.</summary>
    public string Display
    {
        get => _display;
        private set => Set(ref _display, value);
    }

    /// <summary>Latest raw value.</summary>
    public double Value
    {
        get => _value;
        private set => Set(ref _value, value);
    }

    /// <summary>Where the value sits against the sensor's thresholds.</summary>
    public ReadingLevel Level
    {
        get => _level;
        private set
        {
            if (Set(ref _level, value))
            {
                Raise(nameof(LevelBrush));
                Raise(nameof(LevelLabel));
            }
        }
    }

    /// <summary>Colour for the level pip: pen colour when normal, alarm hues otherwise.</summary>
    public IBrush LevelBrush => Level switch
    {
        ReadingLevel.Alarm => new SolidColorBrush(Color.Parse("#E2504A")),
        ReadingLevel.Warning => new SolidColorBrush(Color.Parse("#E0A33E")),
        _ => PenBrush,
    };

    /// <summary>Short word for the level, shown only when it is not normal.</summary>
    public string LevelLabel => Level switch
    {
        ReadingLevel.Alarm => "ALARM",
        ReadingLevel.Warning => "HIGH",
        _ => string.Empty,
    };

    /// <summary>Connection state as the panel shows it.</summary>
    public DeviceState State
    {
        get => _state;
        private set
        {
            if (Set(ref _state, value))
            {
                Raise(nameof(StateLabel));
                Raise(nameof(IsStreaming));
            }
        }
    }

    /// <summary>State word for the row.</summary>
    public string StateLabel => State switch
    {
        DeviceState.Streaming => "streaming",
        DeviceState.Paused => "paused",
        DeviceState.Connecting => "connecting",
        DeviceState.Faulted => "lost",
        _ => "offline",
    };

    /// <summary>True while the device is publishing, which dims the row when false.</summary>
    public bool IsStreaming => State == DeviceState.Streaming;

    /// <summary>Readings this device has published.</summary>
    public long Published
    {
        get => _published;
        private set => Set(ref _published, value);
    }

    /// <summary>Selected rows draw their pen at full weight in the ribbon.</summary>
    public bool IsSelected
    {
        get => _isSelected;
        set => Set(ref _isSelected, value);
    }

    /// <summary>Records a reading. Called on the gateway receive loop, never on the UI thread.</summary>
    public void Record(double value)
    {
        Samples.Add(value);
        Volatile.Write(ref _pendingValue, value);
    }

    private double _pendingValue = double.NaN;

    /// <summary>
    /// Publishes whatever arrived since the last frame. Called on the UI thread by the render timer,
    /// so a device sampling at 200 Hz still costs one property update per frame.
    /// </summary>
    public void Refresh()
    {
        double pending = Volatile.Read(ref _pendingValue);
        if (!double.IsNaN(pending))
        {
            Value = pending;
            Display = pending.ToString(Device.Profile.Kind == SensorKind.Vibration ? "F2" : "F1");
            Level = Device.Level;
        }

        State = Device.State;
        Published = Device.PublishedCount;
    }
}
