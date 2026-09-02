// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Media;
using SocketSignal.SonarDemo.Simulation;

namespace SocketSignal.SonarDemo.Controls;

/// <summary>
/// The plan position indicator: the sea seen from above, with the array at the centre and the beam
/// rotating clockwise from north.
/// </summary>
/// <remarks>
/// Brightness is not decoration here. A contact is at full strength the moment the beam passes it
/// and fades as the beam moves on, exactly as a phosphor tube behaves - so how bright a blip is
/// tells the operator how long ago the array actually heard it.
/// </remarks>
public sealed class PpiScope : Control
{
    /// <summary>Degrees of trailing decay. Just under a full turn, so the picture never fully empties.</summary>
    private const double PersistenceDegrees = 300.0;

    private const double BlipRadius = 4.5;

    public ConsoleModel? Model { get; set; }

    /// <summary>Raised when the operator clicks a blip, with the track id, or null for empty water.</summary>
    public event Action<string?>? ContactPicked;

    public PpiScope()
    {
        ClipToBounds = true;
        Focusable = true;
    }

    protected override void OnPointerPressed(PointerPressedEventArgs e)
    {
        base.OnPointerPressed(e);
        if (Model is null) return;

        Point click = e.GetPosition(this);
        ScopeGeometry geometry = Layout(Bounds.Size);
        string? hit = null;
        double best = 14 * 14;

        foreach (ContactEcho echo in Model.Snapshot())
        {
            if (echo.RangeKm > SonarStation.MaxRangeKm) continue;
            Point p = Project(geometry, echo.Bearing, echo.RangeKm);
            double distance = (p - click).X * (p - click).X + (p - click).Y * (p - click).Y;
            if (distance < best)
            {
                best = distance;
                hit = echo.Id;
            }
        }

        ContactPicked?.Invoke(hit);
    }

    public override void Render(DrawingContext context)
    {
        Size size = Bounds.Size;
        if (size.Width < 40 || size.Height < 40) return;

        ScopeGeometry geometry = Layout(size);
        DrawWater(context, geometry);
        DrawGraticule(context, geometry);

        if (Model is null) return;

        double beam = Model.InterpolatedBeam(DateTime.UtcNow);
        DrawBeam(context, geometry, beam);

        ContactEcho[] echoes = Model.Snapshot();
        foreach (ContactEcho echo in echoes)
            DrawTrail(context, geometry, echo, beam);
        foreach (ContactEcho echo in echoes)
            DrawBlip(context, geometry, echo, beam);

        if (Model.SelectedId is { } selected)
            DrawSelection(context, geometry, echoes, selected);
    }

    // ---------------------------------------------------------------------------------------
    // Geometry
    // ---------------------------------------------------------------------------------------

    private readonly record struct ScopeGeometry(Point Centre, double Radius);

    private static ScopeGeometry Layout(Size size)
    {
        double radius = Math.Min(size.Width, size.Height) / 2 - 26;
        return new ScopeGeometry(new Point(size.Width / 2, size.Height / 2), Math.Max(radius, 10));
    }

    /// <summary>Bearing and range to a point on screen. North is up and bearing runs clockwise.</summary>
    private static Point Project(ScopeGeometry g, double bearing, double rangeKm)
    {
        double r = Math.Min(rangeKm / SonarStation.MaxRangeKm, 1.0) * g.Radius;
        double radians = bearing * Math.PI / 180.0;
        return new Point(g.Centre.X + r * Math.Sin(radians), g.Centre.Y - r * Math.Cos(radians));
    }

    /// <summary>How far behind the beam a bearing sits, 0 at the beam and rising anticlockwise.</summary>
    private static double Behind(double beam, double bearing) => (beam - bearing + 360.0) % 360.0;

    /// <summary>
    /// Blip brightness for a bearing, given where the beam is. The floor matters: a real tube fades
    /// to nothing, but an operator still has to be able to count contacts on the far side of the
    /// sweep, so the decay bottoms out well above zero.
    /// </summary>
    private static double Persistence(double beam, double bearing) =>
        Math.Clamp(1.0 - Behind(beam, bearing) / PersistenceDegrees, 0.30, 1.0);

    // ---------------------------------------------------------------------------------------
    // Layers
    // ---------------------------------------------------------------------------------------

    private static void DrawWater(DrawingContext context, ScopeGeometry g)
    {
        // A faint radial lift towards the centre: near water is quieter, and it keeps the disc from
        // reading as a flat cut-out.
        var water = new RadialGradientBrush
        {
            GradientStops =
            [
                new GradientStop(Color.FromArgb(0x38, 0x14, 0x33, 0x3D), 0),
                new GradientStop(Color.FromArgb(0x12, 0x08, 0x18, 0x22), 1),
            ],
        };
        context.DrawEllipse(water, null, g.Centre, g.Radius, g.Radius);
    }

    private static void DrawGraticule(DrawingContext context, ScopeGeometry g)
    {
        var ring = new Pen(new SolidColorBrush(Color.FromArgb(0x50, 0x3E, 0x7A, 0x6E)), 1);
        var ringFaint = new Pen(new SolidColorBrush(Color.FromArgb(0x28, 0x3E, 0x7A, 0x6E)), 1);
        var spoke = new Pen(new SolidColorBrush(Color.FromArgb(0x22, 0x3E, 0x7A, 0x6E)), 1);
        var chalkFaint = new SolidColorBrush(Color.FromRgb(0x4C, 0x66, 0x72));

        // Range rings, labelled in kilometres on the north-east diagonal where nothing else sits.
        for (int i = 1; i <= 4; i++)
        {
            double fraction = i / 4.0;
            context.DrawEllipse(null, i == 4 ? ring : ringFaint, g.Centre, g.Radius * fraction, g.Radius * fraction);

            double km = SonarStation.MaxRangeKm * fraction;
            var label = Text($"{km:0}", 9, chalkFaint);
            Point at = Project(g, 45, km);
            context.DrawText(label, new Point(at.X + 3, at.Y - label.Height - 1));
        }

        // The close-quarters ring is the one piece of the graticule that means danger.
        double closeFraction = SonarStation.CloseQuartersKm / SonarStation.MaxRangeKm;
        context.DrawEllipse(null,
            new Pen(new SolidColorBrush(Color.FromArgb(0x40, 0xFF, 0x5C, 0x4D)), 1, new DashStyle([3, 3], 0)),
            g.Centre, g.Radius * closeFraction, g.Radius * closeFraction);

        // Bearing spokes every 30 degrees, labelled the way a bearing is spoken: three digits.
        for (int bearing = 0; bearing < 360; bearing += 30)
        {
            Point outer = Project(g, bearing, SonarStation.MaxRangeKm);
            context.DrawLine(spoke, g.Centre, outer);

            var label = Text($"{bearing:000}", 9, chalkFaint);
            double radians = bearing * Math.PI / 180.0;
            var at = new Point(
                g.Centre.X + (g.Radius + 13) * Math.Sin(radians) - label.Width / 2,
                g.Centre.Y - (g.Radius + 13) * Math.Cos(radians) - label.Height / 2);
            context.DrawText(label, at);
        }
    }

    /// <summary>
    /// The beam, drawn as a stack of thin trailing lines rather than one wedge. A real sweep is a
    /// line whose phosphor has not finished decaying, and stepping the alpha down over the trail
    /// reproduces that far better than a gradient fill does.
    /// </summary>
    private static void DrawBeam(DrawingContext context, ScopeGeometry g, double beam)
    {
        const int Steps = 46;
        const double TrailDegrees = 62.0;

        for (int i = Steps; i >= 0; i--)
        {
            double fraction = i / (double)Steps;
            double bearing = beam - TrailDegrees * fraction;
            byte alpha = (byte)(0x66 * Math.Pow(1 - fraction, 2.2));
            if (alpha < 2) continue;

            var pen = new Pen(new SolidColorBrush(Color.FromArgb(alpha, 0x7F, 0xD4, 0xC1)), 1.6);
            context.DrawLine(pen, g.Centre, Project(g, bearing, SonarStation.MaxRangeKm));
        }

        // The leading edge itself, bright and hairline.
        context.DrawLine(
            new Pen(new SolidColorBrush(Color.FromArgb(0xCC, 0xC8, 0xF2, 0xE8)), 1.2),
            g.Centre, Project(g, beam, SonarStation.MaxRangeKm));
    }

    private void DrawTrail(DrawingContext context, ScopeGeometry g, ContactEcho echo, double beam)
    {
        TrackPoint[] points = Model!.HistoryOf(echo.Id);
        if (points.Length < 2) return;

        Color colour = ColourOf(echo);
        DateTime newest = points[^1].At;

        for (int i = 1; i < points.Length; i++)
        {
            // The trail fades with age, not with the sweep: it is a record of where the contact has
            // been, which stays true whether or not the beam is currently on it.
            double age = (newest - points[i].At).TotalSeconds;
            byte alpha = (byte)(0x4A * Math.Clamp(1 - age / 40.0, 0, 1));
            if (alpha < 3) continue;

            context.DrawLine(
                new Pen(new SolidColorBrush(Color.FromArgb(alpha, colour.R, colour.G, colour.B)), 1),
                Project(g, points[i - 1].Bearing, points[i - 1].RangeKm),
                Project(g, points[i].Bearing, points[i].RangeKm));
        }
    }

    private static void DrawBlip(DrawingContext context, ScopeGeometry g, ContactEcho echo, double beam)
    {
        if (echo.RangeKm > SonarStation.MaxRangeKm) return;

        Point at = Project(g, echo.Bearing, echo.RangeKm);
        Color colour = ColourOf(echo);
        double brightness = Persistence(beam, echo.Bearing) * (0.62 + 0.38 * echo.Strength);
        double radius = BlipRadius * (0.7 + 0.5 * echo.Strength);

        // A soft return around a hard core: the halo is the energy, the core is the position.
        context.DrawEllipse(
            new SolidColorBrush(Color.FromArgb((byte)(0x62 * brightness), colour.R, colour.G, colour.B)),
            null, at, radius * 2.4, radius * 2.4);
        context.DrawEllipse(
            new SolidColorBrush(Color.FromArgb((byte)(0xFF * brightness), colour.R, colour.G, colour.B)),
            null, at, radius, radius);
    }

    private static void DrawSelection(DrawingContext context, ScopeGeometry g, ContactEcho[] echoes, string id)
    {
        ContactEcho? echo = echoes.FirstOrDefault(e => e.Id == id);
        if (echo is null) return;

        Point at = Project(g, echo.Bearing, echo.RangeKm);
        var pen = new Pen(new SolidColorBrush(Color.FromArgb(0xE0, 0xDC, 0xE8, 0xE6)), 1);

        // An open crosshair, so the marker never covers the thing it marks.
        const double Gap = 7, Arm = 14;
        context.DrawLine(pen, new Point(at.X - Arm, at.Y), new Point(at.X - Gap, at.Y));
        context.DrawLine(pen, new Point(at.X + Gap, at.Y), new Point(at.X + Arm, at.Y));
        context.DrawLine(pen, new Point(at.X, at.Y - Arm), new Point(at.X, at.Y - Gap));
        context.DrawLine(pen, new Point(at.X, at.Y + Gap), new Point(at.X, at.Y + Arm));

        var label = Text($"{echo.Id}  {echo.Bearing:000.0}  {echo.RangeKm:0.0}km",
            10, new SolidColorBrush(Color.FromRgb(0xDC, 0xE8, 0xE6)));
        context.DrawText(label, new Point(at.X + Arm + 4, at.Y - label.Height / 2));
    }

    internal static Color ColourOf(ContactEcho echo)
    {
        if (echo.RangeKm <= SonarStation.CloseQuartersKm)
            return Color.FromRgb(0xFF, 0x5C, 0x4D);   // the only saturated red on the console

        return echo.Class switch
        {
            Classification.Surface => Color.FromRgb(0xF2, 0xA6, 0x5A),
            Classification.Submerged => Color.FromRgb(0x8F, 0xB8, 0xFF),
            Classification.Biologic => Color.FromRgb(0x7F, 0xD4, 0xC1),
            _ => Color.FromRgb(0xD9, 0x6F, 0xA8),
        };
    }

    internal static FormattedText Text(string value, double size, IBrush brush) =>
        new(value, System.Globalization.CultureInfo.InvariantCulture, FlowDirection.LeftToRight,
            new Typeface(new FontFamily("Cascadia Mono, Consolas, monospace")), size, brush);
}
