// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;
using Avalonia.Controls;
using Avalonia.Media;
using SocketSignal.SonarDemo.Simulation;

namespace SocketSignal.SonarDemo.Controls;

/// <summary>
/// The bearing-time recorder: bearing across, time down, newest at the top.
/// </summary>
/// <remarks>
/// This is the instrument sonar operators actually read, and it is what the scope cannot show. A
/// blip on the scope is a position; a trace here is a history - it says whether a contact is
/// steady, whether it is drawing left, and whether two contacts that look like one are about to
/// separate. A trace that runs straight down is a contact holding its bearing, which is the classic
/// signature of something on a collision course.
/// </remarks>
public sealed class BearingWaterfall : Control
{
    private const double GutterLeft = 0;
    private const double HeaderHeight = 16;

    public ConsoleModel? Model { get; set; }

    public BearingWaterfall() => ClipToBounds = true;

    public override void Render(DrawingContext context)
    {
        Size size = Bounds.Size;
        if (size.Width < 60 || size.Height < 60) return;

        var plot = new Rect(GutterLeft, HeaderHeight, size.Width - GutterLeft, size.Height - HeaderHeight);
        DrawGraticule(context, plot);

        if (Model is null) return;

        DateTime now = DateTime.UtcNow;
        string? selected = Model.SelectedId;

        foreach ((string id, TrackPoint[] points, Classification kind) in Model.AllHistory())
        {
            if (points.Length < 2) continue;
            DrawTrace(context, plot, points, kind, now, isSelected: id == selected);
        }

        DrawBeamMarker(context, plot, Model.InterpolatedBeam(now));
    }

    // ---------------------------------------------------------------------------------------

    private static double BearingToX(Rect plot, double bearing) => plot.X + bearing / 360.0 * plot.Width;

    private static double TimeToY(Rect plot, DateTime at, DateTime now)
    {
        double age = (now - at).TotalSeconds;
        return plot.Y + Math.Clamp(age / ConsoleModel.Window.TotalSeconds, 0, 1) * plot.Height;
    }

    private static void DrawGraticule(DrawingContext context, Rect plot)
    {
        var faint = new SolidColorBrush(Color.FromRgb(0x4C, 0x66, 0x72));
        var gridline = new Pen(new SolidColorBrush(Color.FromArgb(0x1E, 0x3E, 0x7A, 0x6E)), 1);

        // Bearing gridlines every 45 degrees, labelled along the top edge.
        for (int bearing = 0; bearing <= 360; bearing += 45)
        {
            double x = BearingToX(plot, bearing);
            context.DrawLine(gridline, new Point(x, plot.Y), new Point(x, plot.Bottom));

            if (bearing == 360) continue;
            var label = PpiScope.Text($"{bearing:000}", 8.5, faint);
            double at = bearing == 0 ? x + 2 : x - label.Width / 2;
            context.DrawText(label, new Point(at, 2));
        }

        // Time marks down the right edge, so the operator can read how old a trace is.
        for (int seconds = 30; seconds < ConsoleModel.Window.TotalSeconds; seconds += 30)
        {
            double y = plot.Y + seconds / ConsoleModel.Window.TotalSeconds * plot.Height;
            context.DrawLine(
                new Pen(new SolidColorBrush(Color.FromArgb(0x14, 0x3E, 0x7A, 0x6E)), 1),
                new Point(plot.X, y), new Point(plot.Right, y));

            var label = PpiScope.Text($"-{seconds}s", 8.5, faint);
            context.DrawText(label, new Point(plot.Right - label.Width - 3, y - label.Height - 1));
        }
    }

    /// <summary>
    /// One contact's history as a trace. The line is split wherever the contact crosses north,
    /// because a bearing that wraps 359 to 001 has not travelled across the whole plot.
    /// </summary>
    private static void DrawTrace(
        DrawingContext context, Rect plot, TrackPoint[] points, Classification kind, DateTime now, bool isSelected)
    {
        Color colour = Colour(kind);
        byte alpha = isSelected ? (byte)0xFF : (byte)0x9C;
        double thickness = isSelected ? 2.0 : 1.2;

        var pen = new Pen(new SolidColorBrush(Color.FromArgb(alpha, colour.R, colour.G, colour.B)), thickness);

        for (int i = 1; i < points.Length; i++)
        {
            double previous = points[i - 1].Bearing;
            double current = points[i].Bearing;
            if (Math.Abs(current - previous) > 180) continue;   // wrapped through north

            context.DrawLine(pen,
                new Point(BearingToX(plot, previous), TimeToY(plot, points[i - 1].At, now)),
                new Point(BearingToX(plot, current), TimeToY(plot, points[i].At, now)));
        }

        // The head of the trace - where the contact is now - gets a mark, since that is the row the
        // eye goes to first.
        TrackPoint head = points[^1];
        var at = new Point(BearingToX(plot, head.Bearing), TimeToY(plot, head.At, now));
        context.DrawEllipse(new SolidColorBrush(colour), null, at, isSelected ? 3.0 : 2.0, isSelected ? 3.0 : 2.0);
    }

    /// <summary>A hairline showing where the beam is, so the two instruments read as one picture.</summary>
    private static void DrawBeamMarker(DrawingContext context, Rect plot, double beam)
    {
        double x = BearingToX(plot, beam);
        context.DrawLine(
            new Pen(new SolidColorBrush(Color.FromArgb(0x55, 0xC8, 0xF2, 0xE8)), 1),
            new Point(x, plot.Y), new Point(x, plot.Bottom));
    }

    private static Color Colour(Classification kind) => kind switch
    {
        Classification.Surface => Color.FromRgb(0xF2, 0xA6, 0x5A),
        Classification.Submerged => Color.FromRgb(0x8F, 0xB8, 0xFF),
        Classification.Biologic => Color.FromRgb(0x7F, 0xD4, 0xC1),
        _ => Color.FromRgb(0xD9, 0x6F, 0xA8),
    };
}
