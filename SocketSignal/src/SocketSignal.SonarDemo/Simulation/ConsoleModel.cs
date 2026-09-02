// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
namespace SocketSignal.SonarDemo.Simulation;

/// <summary>One remembered position of a contact. The raw material for trails and the waterfall.</summary>
public readonly record struct TrackPoint(double Bearing, double RangeKm, double Strength, DateTime At);

/// <summary>
/// Everything the console knows, assembled from the frames the array sends. Written by the network
/// callback and read by the drawing code on the UI thread, so every mutation is under the lock and
/// every reader takes a snapshot.
/// </summary>
public sealed class ConsoleModel
{
    /// <summary>How much history the bearing-time recorder shows.</summary>
    public static readonly TimeSpan Window = TimeSpan.FromSeconds(120);

    private readonly Lock _gate = new();
    private readonly Dictionary<string, List<TrackPoint>> _history = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ContactEcho> _latest = new(StringComparer.Ordinal);
    private readonly Queue<double> _callRate = new();

    private DateTime _lastSample = DateTime.MinValue;

    /// <summary>Where the beam was in the last frame received.</summary>
    public double BeamBearing { get; private set; }

    /// <summary>When that frame landed, so the console can carry the beam on between frames.</summary>
    public DateTime BeamAt { get; private set; } = DateTime.UtcNow;

    /// <summary>Frame counter from the array. A stalled counter means the link is up but the array is not.</summary>
    public long Tick { get; private set; }

    /// <summary>The track the operator has selected, or null.</summary>
    public string? SelectedId { get; set; }

    /// <summary>Set while a classify call is in flight, so the button can say so.</summary>
    public string? PendingClassifyId { get; set; }

    /// <summary>The last classification the array reported, shown under the contact list.</summary>
    public ClassificationResult? LastClassification { get; set; }

    /// <summary>Takes one frame from the array.</summary>
    public void Apply(SweepFrame frame)
    {
        DateTime now = DateTime.UtcNow;
        lock (_gate)
        {
            BeamBearing = frame.BeamBearing;
            BeamAt = now;
            Tick = frame.Tick;

            // History is sampled well below the frame rate: the recorder needs shape, not every frame.
            bool sample = now - _lastSample >= TimeSpan.FromMilliseconds(250);
            if (sample) _lastSample = now;

            var live = new HashSet<string>(StringComparer.Ordinal);
            foreach (ContactEcho echo in frame.Echoes)
            {
                live.Add(echo.Id);
                _latest[echo.Id] = echo;

                if (!sample) continue;

                if (!_history.TryGetValue(echo.Id, out List<TrackPoint>? points))
                    _history[echo.Id] = points = new List<TrackPoint>(512);

                points.Add(new TrackPoint(echo.Bearing, echo.RangeKm, echo.Strength, now));

                DateTime cutoff = now - Window;
                int stale = 0;
                while (stale < points.Count && points[stale].At < cutoff) stale++;
                if (stale > 0) points.RemoveRange(0, stale);
            }

            // Drop anything the array has stopped holding.
            foreach (string id in _latest.Keys.Where(id => !live.Contains(id)).ToList())
            {
                _latest.Remove(id);
                _history.Remove(id);
            }
        }
    }

    /// <summary>
    /// The beam angle to draw right now. Frames arrive at 20 Hz and the console draws at 60, so
    /// between frames the beam is carried forward at the array's known rate - without this the
    /// sweep visibly steps.
    /// </summary>
    public double InterpolatedBeam(DateTime now)
    {
        lock (_gate)
        {
            double elapsed = (now - BeamAt).TotalSeconds;
            // Never run the beam more than one frame ahead: if the link stalls, the sweep should
            // stall with it rather than spin on a dead picture.
            elapsed = Math.Min(elapsed, 0.1);
            return (BeamBearing + SonarStation.SweepDegreesPerSecond * elapsed) % 360.0;
        }
    }

    /// <summary>Current contacts, newest first by range. Allocates a snapshot - called once a frame.</summary>
    public ContactEcho[] Snapshot()
    {
        lock (_gate)
        {
            ContactEcho[] all = new ContactEcho[_latest.Count];
            _latest.Values.CopyTo(all, 0);
            Array.Sort(all, static (a, b) => a.RangeKm.CompareTo(b.RangeKm));
            return all;
        }
    }

    /// <summary>The remembered track of one contact, oldest first.</summary>
    public TrackPoint[] HistoryOf(string id)
    {
        lock (_gate)
            return _history.TryGetValue(id, out List<TrackPoint>? points) ? [.. points] : [];
    }

    /// <summary>Every contact's history, for the recorder.</summary>
    public (string Id, TrackPoint[] Points, Classification Class)[] AllHistory()
    {
        lock (_gate)
        {
            var result = new (string, TrackPoint[], Classification)[_history.Count];
            int i = 0;
            foreach ((string id, List<TrackPoint> points) in _history)
            {
                Classification kind = _latest.TryGetValue(id, out ContactEcho? echo)
                    ? echo.Class
                    : Classification.Unidentified;
                result[i++] = (id, [.. points], kind);
            }
            return result;
        }
    }

    // ---------------------------------------------------------------------------------------
    // Telemetry
    // ---------------------------------------------------------------------------------------

    /// <summary>Records one frames-per-second sample for the telemetry sparkline.</summary>
    public void RecordRate(double framesPerSecond)
    {
        lock (_gate)
        {
            _callRate.Enqueue(framesPerSecond);
            while (_callRate.Count > 120) _callRate.Dequeue();
        }
    }

    /// <summary>The rate history, oldest first.</summary>
    public double[] RateHistory()
    {
        lock (_gate) return [.. _callRate];
    }
}
