// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
namespace BlackHole.IoTGateway.Simulation;

/// <summary>
/// The kinds of instrument the simulator can put on the plant floor.
/// </summary>
/// <remarks>
/// Each kind carries its own plausible range and drift so the traces on screen look like real
/// process data rather than noise: a tank warms and cools slowly, a flow meter is jumpy, a vibration
/// sensor sits near zero until something goes wrong.
/// </remarks>
public enum SensorKind
{
    Temperature,
    Pressure,
    Humidity,
    FlowRate,
    Vibration,
    PowerDraw,
}

/// <summary>Fixed facts about a sensor kind: what it reads, in what units, within what limits.</summary>
public sealed record SensorProfile(
    SensorKind Kind,
    string Label,
    string Unit,
    double Minimum,
    double Maximum,
    double Nominal,
    double Drift,
    double Noise,
    double WarnAbove,
    double AlarmAbove,
    string TopicSegment)
{
    private static readonly SensorProfile[] All =
    [
        new(SensorKind.Temperature, "Temperature", "°C",   0,   140,  62,  0.35, 0.20,  92, 110, "temperature"),
        new(SensorKind.Pressure,    "Pressure",    "kPa",  0,   400, 180,  0.90, 0.60, 300, 350, "pressure"),
        new(SensorKind.Humidity,    "Humidity",    "%RH",  0,   100,  48,  0.25, 0.35,  80,  92, "humidity"),
        new(SensorKind.FlowRate,    "Flow rate",   "L/min",0,   250, 120,  2.20, 1.80, 205, 235, "flow"),
        new(SensorKind.Vibration,   "Vibration",   "mm/s", 0,    30, 2.4,  0.18, 0.22,  12,  20, "vibration"),
        new(SensorKind.PowerDraw,   "Power draw",  "kW",   0,    75,  28,  0.55, 0.45,  58,  68, "power"),
    ];

    /// <summary>The profile for one kind.</summary>
    public static SensorProfile For(SensorKind kind) => All[(int)kind];

    /// <summary>Every profile, in enum order.</summary>
    public static IReadOnlyList<SensorProfile> Catalogue => All;

    /// <summary>Where a reading of this kind sits between its limits, 0 to 1.</summary>
    public double Normalise(double value) => Math.Clamp((value - Minimum) / (Maximum - Minimum), 0, 1);
}

/// <summary>How a reading compares against the sensor's warning and alarm thresholds.</summary>
public enum ReadingLevel
{
    Normal,
    Warning,
    Alarm,
}
