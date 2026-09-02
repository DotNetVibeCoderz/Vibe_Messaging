// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
namespace SocketSignal.SonarDemo.Simulation;

/// <summary>How a contact has been classified. Drives its colour on both instruments.</summary>
public enum Classification
{
    /// <summary>Detected, not yet identified. Magenta - the chart convention for caution.</summary>
    Unidentified,

    /// <summary>On the surface: a merchant, a trawler, a warship.</summary>
    Surface,

    /// <summary>Below the layer.</summary>
    Submerged,

    /// <summary>Biological. Whales and fish shoals return real echoes and are not traffic.</summary>
    Biologic,
}

/// <summary>
/// One echo as it crosses the wire. This is the payload SocketSignal carries from the sonar array
/// to every console in the operators group, twenty times a second.
/// </summary>
/// <param name="Id">Track number. Stable for the life of the contact.</param>
/// <param name="Bearing">Degrees true, 0 at north, clockwise.</param>
/// <param name="RangeKm">Slant range from the array.</param>
/// <param name="SpeedKnots">Speed over ground.</param>
/// <param name="CourseDegrees">The direction it is travelling, not the direction it lies in.</param>
/// <param name="Class">Current classification.</param>
/// <param name="Strength">Return strength, 0 to 1. Drives blip brightness and trace weight.</param>
public sealed record ContactEcho(
    string Id,
    double Bearing,
    double RangeKm,
    double SpeedKnots,
    double CourseDegrees,
    Classification Class,
    double Strength);

/// <summary>
/// One broadcast from the array: where the beam is pointing, and everything it can hear.
/// </summary>
/// <param name="Tick">Monotonic frame counter, so a console can tell a stall from a quiet sea.</param>
/// <param name="BeamBearing">Where the transducer is pointing at the instant of the frame.</param>
/// <param name="Echoes">Every live contact. Small enough to resend in full rather than diff.</param>
public sealed record SweepFrame(long Tick, double BeamBearing, ContactEcho[] Echoes);

/// <summary>The reply to a classify request - the demo's client-to-server call with a return value.</summary>
/// <param name="Id">The track that was classified.</param>
/// <param name="Class">What the array decided it was.</param>
/// <param name="Confidence">0 to 1.</param>
/// <param name="Note">One line for the operator, in the console's voice.</param>
public sealed record ClassificationResult(string Id, Classification Class, double Confidence, string Note);
