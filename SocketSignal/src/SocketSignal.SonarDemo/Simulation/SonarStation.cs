// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
namespace SocketSignal.SonarDemo.Simulation;

/// <summary>
/// The sonar array, as a SocketSignal server. It owns the sea state and pushes it to every console
/// that has joined the operators group.
/// </summary>
/// <remarks>
/// The console could just as well read this object directly - they are in the same process - but
/// then the demo would prove nothing. Everything the console knows arrives over a WebSocket, so
/// what runs here is what would run on a ship with the consoles two decks up.
/// </remarks>
public sealed class SonarStation : IAsyncDisposable
{
    /// <summary>Beam rotation rate. Six seconds a turn is a realistic surface-search rate.</summary>
    public const double SweepDegreesPerSecond = 60.0;

    /// <summary>Everything past this range is beyond the array.</summary>
    public const double MaxRangeKm = 12.0;

    /// <summary>Inside this range a contact is close quarters and shows in the alarm colour.</summary>
    public const double CloseQuartersKm = 2.5;

    private const string OperatorsGroup = "operators";
    private const int FramesPerSecond = 20;

    private readonly SocketSignalServer _server;
    private readonly List<Track> _tracks = [];
    private readonly Random _random = new(20260902);
    private readonly CancellationTokenSource _cts = new();

    private double _beamBearing;
    private long _tick;
    private Task? _loop;

    public SonarStation(string urlPrefix)
    {
        _server = new SocketSignalServer(urlPrefix, new SocketSignalOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(10),
            CallTimeout = TimeSpan.FromSeconds(10),
        })
        {
            Name = "array-01",
        };

        // A console joins the operators group and starts receiving sweep frames.
        _server.Register<bool>("sonar.attach", client =>
        {
            client.JoinGroup(OperatorsGroup);
            return ValueTask.FromResult(true);
        });

        // The demo's client-to-server call with a return value: the array studies one track and
        // reports back. The delay is the point - a real classification is not instant.
        _server.Register<string, ClassificationResult>("sonar.classify", async (_, id) =>
        {
            await Task.Delay(450, _cts.Token);
            return Classify(id ?? string.Empty);
        });

        // An active ping: everything within range answers at full strength for a few seconds.
        _server.Register<int>("sonar.ping", _ =>
        {
            int illuminated = 0;
            lock (_tracks)
            {
                foreach (Track track in _tracks)
                {
                    if (track.RangeKm > MaxRangeKm) continue;
                    track.IlluminatedUntil = DateTime.UtcNow.AddSeconds(3);
                    illuminated++;
                }
            }
            return ValueTask.FromResult(illuminated);
        });

        SeedSea();
    }

    /// <summary>Where the beam is pointing right now, so the console can start in sync.</summary>
    public double BeamBearing => Volatile.Read(ref _beamBearing);

    /// <summary>Live console count, shown on the status rail.</summary>
    public int ConsoleCount => _server.ClientCount;

    /// <summary>Frames and bytes the array has pushed, for the telemetry strip.</summary>
    public (long Frames, long Bytes) Traffic
    {
        get
        {
            Diagnostics.SignalStatistics stats = _server.Statistics;
            return (stats.FramesSent, stats.BytesSent);
        }
    }

    public Task StartAsync()
    {
        _ = _server.StartAsync(_cts.Token);
        _loop = Task.Run(() => BroadcastLoopAsync(_cts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    // ---------------------------------------------------------------------------------------
    // The sea
    // ---------------------------------------------------------------------------------------

    private void SeedSea()
    {
        // A plausible picture: a couple of merchants, a fast surface contact, something submerged
        // that does not want to be found, and a whale that will be mistaken for it at least once.
        Add("M-01", 041, 8.4, 16, 190, Classification.Surface, 0.82);
        Add("M-02", 118, 6.1, 13, 275, Classification.Surface, 0.74);
        Add("S-07", 203, 9.6, 24, 015, Classification.Unidentified, 0.41);
        Add("S-11", 297, 3.4, 19, 040, Classification.Unidentified, 0.33);
        Add("B-04", 342, 2.9, 4, 250, Classification.Biologic, 0.55);
        Add("M-09", 076, 11.2, 20, 235, Classification.Surface, 0.63);
        Add("F-02", 158, 2.2, 26, 330, Classification.Surface, 0.68);
    }

    private void Add(string id, double bearing, double range, double speed, double course, Classification kind, double strength)
        => _tracks.Add(new Track
        {
            Id = id,
            Bearing = bearing,
            RangeKm = range,
            SpeedKnots = speed,
            CourseDegrees = course,
            Class = kind,
            BaseStrength = strength,
        });

    /// <summary>
    /// Advances every track along its course and pushes a frame to the operators group. Twenty
    /// frames a second is what makes the beam look continuous on the console.
    /// </summary>
    private async Task BroadcastLoopAsync(CancellationToken token)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1.0 / FramesPerSecond));
        DateTime last = DateTime.UtcNow;

        try
        {
            while (await timer.WaitForNextTickAsync(token).ConfigureAwait(false))
            {
                DateTime now = DateTime.UtcNow;
                double elapsed = (now - last).TotalSeconds;
                last = now;

                Volatile.Write(ref _beamBearing, (BeamBearing + SweepDegreesPerSecond * elapsed) % 360.0);

                ContactEcho[] echoes;
                lock (_tracks)
                {
                    foreach (Track track in _tracks)
                        Advance(track, elapsed, now);

                    echoes = new ContactEcho[_tracks.Count];
                    for (int i = 0; i < _tracks.Count; i++)
                        echoes[i] = _tracks[i].ToEcho(now);
                }

                var frame = new SweepFrame(Interlocked.Increment(ref _tick), BeamBearing, echoes);

                // One typed argument, so this hot path never builds an object[] and never boxes.
                await _server.SendToGroupAsync(OperatorsGroup, "sonar.sweep", frame).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
    }

    /// <summary>
    /// Moves a track by its course and speed. Bearing and range are polar, the course is Cartesian,
    /// so the step goes through x/y and back - which is also what makes a contact crossing the bow
    /// swing through bearings quickly while its range barely changes.
    /// </summary>
    private void Advance(Track track, double elapsedSeconds, DateTime now)
    {
        const double KnotsToKmPerSecond = 1.852 / 3600.0;

        double x = track.RangeKm * Math.Sin(track.Bearing * Math.PI / 180.0);
        double y = track.RangeKm * Math.Cos(track.Bearing * Math.PI / 180.0);

        double distance = track.SpeedKnots * KnotsToKmPerSecond * elapsedSeconds;
        x += distance * Math.Sin(track.CourseDegrees * Math.PI / 180.0);
        y += distance * Math.Cos(track.CourseDegrees * Math.PI / 180.0);

        track.RangeKm = Math.Sqrt(x * x + y * y);
        track.Bearing = (Math.Atan2(x, y) * 180.0 / Math.PI + 360.0) % 360.0;

        // A slow wander on the course, so the bearing-time traces are never perfectly straight.
        track.CourseDegrees = (track.CourseDegrees + (_random.NextDouble() - 0.5) * 4.0 * elapsedSeconds + 360.0) % 360.0;

        // Turn a contact that has run past the horizon back towards the array rather than losing it.
        if (track.RangeKm > MaxRangeKm * 0.95)
            track.CourseDegrees = (track.Bearing + 180.0 + (_random.NextDouble() - 0.5) * 40.0) % 360.0;
        else if (track.RangeKm < 0.8)
            track.CourseDegrees = (track.Bearing + (_random.NextDouble() - 0.5) * 40.0) % 360.0;

        track.Strength = Math.Clamp(
            track.BaseStrength + (_random.NextDouble() - 0.5) * 0.12 + (track.IlluminatedUntil > now ? 0.35 : 0),
            0.05, 1.0);
    }

    private ClassificationResult Classify(string id)
    {
        lock (_tracks)
        {
            Track? track = _tracks.FirstOrDefault(t => t.Id == id);
            if (track is null)
                throw new InvalidOperationException($"Track {id} is no longer held.");

            // What the array can tell from what it has: speed and strength do most of the work.
            (Classification kind, double confidence, string note) = track switch
            {
                { SpeedKnots: > 16 } => (Classification.Surface, 0.88, "Blade rate and speed read as a surface vessel making way."),
                { SpeedKnots: < 5, BaseStrength: > 0.5 } => (Classification.Biologic, 0.71, "Broadband, no tonals. Biologic."),
                { BaseStrength: < 0.45 } => (Classification.Submerged, 0.64, "Weak, steady, no surface wake. Holding below the layer."),
                _ => (Classification.Surface, 0.58, "Consistent with a small surface craft."),
            };

            track.Class = kind;
            return new ClassificationResult(id, kind, confidence, note);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        if (_loop is not null)
        {
            try { await _loop.ConfigureAwait(false); } catch { /* cancelled */ }
        }
        await _server.DisposeAsync().ConfigureAwait(false);
        _cts.Dispose();
    }

    /// <summary>Server-side state for a contact. Only the echo shape crosses the wire.</summary>
    private sealed class Track
    {
        public required string Id { get; init; }
        public double Bearing { get; set; }
        public double RangeKm { get; set; }
        public double SpeedKnots { get; set; }
        public double CourseDegrees { get; set; }
        public Classification Class { get; set; }
        public double BaseStrength { get; init; }
        public double Strength { get; set; }
        public DateTime IlluminatedUntil { get; set; }

        public ContactEcho ToEcho(DateTime now) => new(
            Id, Bearing, RangeKm, SpeedKnots, CourseDegrees, Class,
            Strength <= 0 ? BaseStrength : Strength);
    }
}
