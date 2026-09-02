// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using System.Globalization;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Media;
using Avalonia.Threading;
using Nerve.AgentSim.ViewModels;

namespace Nerve.AgentSim.Controls;

/// <summary>
/// The arbor: the orchestrator's soma on the left, one axon fanning out to each specialist, and the
/// impulses travelling them.
/// </summary>
/// <remarks>
/// <para>
/// Every spark on this control is a message that was really published on the hub. An outbound
/// impulse is a <c>SubTask</c> leaving <c>agents/task/{specialty}</c>; an inbound one is a
/// <c>SubResult</c> coming back on <c>agents/result/{specialty}</c>. Nothing is emitted on a timer
/// to make the picture livelier.
/// </para>
/// <para>
/// The control owns its own frame clock and advances the field itself. It never reads anything the
/// agents write directly - the panel drains the hub into the field, and the field is only ever
/// touched on the UI thread.
/// </para>
/// </remarks>
public sealed class Arbor : Control
{
    /// <summary>The impulses and terminals to draw.</summary>
    public static readonly StyledProperty<ArborField?> FieldProperty =
        AvaloniaProperty.Register<Arbor, ArborField?>(nameof(Field));

    private static readonly Typeface PlateItalic =
        new(new FontFamily("Constantia, Cambria, Georgia, serif"), FontStyle.Italic);

    private static readonly Typeface Plate =
        new(new FontFamily("Constantia, Cambria, Georgia, serif"));

    private static readonly Typeface Data =
        new(new FontFamily("Consolas, Cascadia Mono, monospace"));

    private static readonly Color Ink = Color.FromRgb(0x17, 0x22, 0x2C);
    private static readonly Color Stroma = Color.FromRgb(0x5B, 0x6D, 0x79);
    private static readonly Color Faint = Color.FromRgb(0x8A, 0x9A, 0xA5);
    private static readonly Color Etch = Color.FromRgb(0xC4, 0xD0, 0xD7);
    private static readonly Color Cresyl = Color.FromRgb(0x6B, 0x2F, 0xA0);

    private readonly IBrush _inkBrush = new SolidColorBrush(Ink);
    private readonly IBrush _stromaBrush = new SolidColorBrush(Stroma);
    private readonly IBrush _faintBrush = new SolidColorBrush(Faint);
    private readonly IPen _axonPen = new Pen(new SolidColorBrush(Etch), 1.4);
    private readonly IPen _bandPen = new Pen(new SolidColorBrush(Etch, 0.85), 1.0);

    private DispatcherTimer? _clock;
    private long _lastTick;

    /// <summary>The impulses and terminals to draw.</summary>
    public ArborField? Field
    {
        get => GetValue(FieldProperty);
        set => SetValue(FieldProperty, value);
    }

    /// <inheritdoc />
    protected override void OnAttachedToVisualTree(VisualTreeAttachmentEventArgs e)
    {
        base.OnAttachedToVisualTree(e);

        _lastTick = Environment.TickCount64;
        _clock = new DispatcherTimer(TimeSpan.FromMilliseconds(16), DispatcherPriority.Render, OnFrame);
        _clock.Start();
    }

    /// <inheritdoc />
    protected override void OnDetachedFromVisualTree(VisualTreeAttachmentEventArgs e)
    {
        _clock?.Stop();
        _clock = null;
        base.OnDetachedFromVisualTree(e);
    }

    private void OnFrame(object? sender, EventArgs e)
    {
        long now = Environment.TickCount64;
        double seconds = Math.Min(0.1, (now - _lastTick) / 1000.0);
        _lastTick = now;

        Field?.Advance(seconds);
        InvalidateVisual();
    }

    // ================================= Drawing =================================

    /// <inheritdoc />
    public override void Render(DrawingContext context)
    {
        ArborField? field = Field;
        if (field is null || field.Agents.Count == 0) return;

        Rect bounds = new(Bounds.Size);
        if (bounds.Width < 260 || bounds.Height < 160) return;

        Layout layout = Measure(bounds, field.Agents.Count);

        for (int i = 0; i < field.Agents.Count; i++)
        {
            AgentViewModel agent = field.Agents[i];
            Geometry axon = AxonGeometry(layout, i);

            context.DrawGeometry(null, agent.Busy ? BusyPen(agent) : _axonPen, axon);
            DrawMyelin(context, layout, i);
            DrawTerminal(context, layout, i, agent);
        }

        DrawSoma(context, layout, field.SomaPulse);

        foreach (ArborField.Impulse impulse in field.Impulses)
            DrawImpulse(context, layout, field, impulse);
    }

    /// <summary>Where the soma and every terminal sit, for one size of control.</summary>
    private readonly record struct Layout(Point Soma, double SomaRadius, Point[] Terminals, double TerminalRadius);

    private static Layout Measure(Rect bounds, int agents)
    {
        const double LabelGutter = 208;   // room for the plate label to the right of each terminal
        const double SomaRadius = 30;
        const double TerminalRadius = 13;

        var soma = new Point(70, bounds.Height / 2);
        double terminalX = Math.Max(soma.X + 180, bounds.Width - LabelGutter);

        double top = 34;
        double usable = bounds.Height - (top * 2);
        var terminals = new Point[agents];

        for (int i = 0; i < agents; i++)
        {
            double fraction = agents == 1 ? 0.5 : i / (double)(agents - 1);
            terminals[i] = new Point(terminalX, top + (usable * fraction));
        }

        return new Layout(soma, SomaRadius, terminals, TerminalRadius);
    }

    /// <summary>
    /// The four control points of one axon. Kept in one place so the curve the impulse travels and
    /// the curve that is drawn can never drift apart.
    /// </summary>
    private static (Point P0, Point C1, Point C2, Point P3) Axon(Layout layout, int index)
    {
        Point terminal = layout.Terminals[index];
        Point soma = layout.Soma;

        double dx = terminal.X - soma.X;
        double dy = terminal.Y - soma.Y;
        double length = Math.Sqrt((dx * dx) + (dy * dy));
        if (length < 1) length = 1;

        var start = new Point(soma.X + (dx / length * layout.SomaRadius), soma.Y + (dy / length * layout.SomaRadius));
        var end = new Point(terminal.X - (dx / length * layout.TerminalRadius), terminal.Y - (dy / length * layout.TerminalRadius));

        // Leaving the soma horizontally and arriving horizontally is what gives the fan its shape:
        // the axons separate immediately instead of running as a bundle of straight lines.
        var c1 = new Point(start.X + (dx * 0.45), start.Y);
        var c2 = new Point(end.X - (dx * 0.45), end.Y);

        return (start, c1, c2, end);
    }

    private static Point PointOn(Layout layout, int index, double t)
    {
        (Point p0, Point c1, Point c2, Point p3) = Axon(layout, index);

        double u = 1 - t;
        double a = u * u * u;
        double b = 3 * u * u * t;
        double c = 3 * u * t * t;
        double d = t * t * t;

        return new Point(
            (a * p0.X) + (b * c1.X) + (c * c2.X) + (d * p3.X),
            (a * p0.Y) + (b * c1.Y) + (c * c2.Y) + (d * p3.Y));
    }

    private static Geometry AxonGeometry(Layout layout, int index)
    {
        (Point p0, Point c1, Point c2, Point p3) = Axon(layout, index);

        var geometry = new StreamGeometry();
        using (StreamGeometryContext ctx = geometry.Open())
        {
            ctx.BeginFigure(p0, isFilled: false);
            ctx.CubicBezierTo(c1, c2, p3);
            ctx.EndFigure(false);
        }

        return geometry;
    }

    private static IPen BusyPen(AgentViewModel agent) =>
        new Pen(new SolidColorBrush(agent.Stain, 0.42), 2.2);

    /// <summary>
    /// The banding on a myelinated axon. It reads as texture, but it is also a distance scale: the
    /// bands are evenly spaced in curve parameter, so they crowd where the curve is tightest.
    /// </summary>
    private void DrawMyelin(DrawingContext context, Layout layout, int index)
    {
        const int Bands = 13;

        for (int b = 1; b < Bands; b++)
        {
            double t = b / (double)Bands;
            Point at = PointOn(layout, index, t);
            Point ahead = PointOn(layout, index, Math.Min(1, t + 0.01));

            double dx = ahead.X - at.X;
            double dy = ahead.Y - at.Y;
            double length = Math.Sqrt((dx * dx) + (dy * dy));
            if (length < 0.001) continue;

            double nx = -dy / length * 2.6;
            double ny = dx / length * 2.6;

            context.DrawLine(_bandPen, new Point(at.X - nx, at.Y - ny), new Point(at.X + nx, at.Y + ny));
        }
    }

    /// <summary>
    /// The orchestrator. Drawn with dendrites on the left because that is where instructions arrive
    /// from - the mission queue sits on that side of the panel.
    /// </summary>
    private void DrawSoma(DrawingContext context, Layout layout, double pulse)
    {
        Point soma = layout.Soma;
        double radius = layout.SomaRadius;

        for (int i = -2; i <= 2; i++)
        {
            double angle = Math.PI + (i * 0.42);
            var from = new Point(soma.X + (Math.Cos(angle) * radius), soma.Y + (Math.Sin(angle) * radius));
            var to = new Point(soma.X + (Math.Cos(angle) * (radius + 26)), soma.Y + (Math.Sin(angle) * (radius + 26)));
            context.DrawLine(_axonPen, from, to);
        }

        context.DrawEllipse(new SolidColorBrush(Cresyl, 0.09 + (pulse * 0.22)), null, soma, radius, radius);
        context.DrawEllipse(null, new Pen(new SolidColorBrush(Cresyl, 0.85), 1.8), soma, radius, radius);
        context.DrawEllipse(new SolidColorBrush(Cresyl, 0.55 + (pulse * 0.45)), null, soma, 7, 7);

        Label(context, "Orchestrator", PlateItalic, 13.5, _inkBrush,
            new Point(soma.X - 34, soma.Y + radius + 12), centreOn: soma.X);
        Label(context, "agents/mission/inbox", Data, 10, _faintBrush,
            new Point(soma.X - 34, soma.Y + radius + 30), centreOn: soma.X);
    }

    private void DrawTerminal(DrawingContext context, Layout layout, int index, AgentViewModel agent)
    {
        Point terminal = layout.Terminals[index];
        double radius = layout.TerminalRadius;
        double pulse = agent.Pulse;

        context.DrawEllipse(new SolidColorBrush(agent.Stain, 0.10 + (pulse * 0.30)), null, terminal, radius + (pulse * 5), radius + (pulse * 5));
        context.DrawEllipse(new SolidColorBrush(agent.Stain, agent.Busy ? 0.30 : 0.12), null, terminal, radius, radius);
        context.DrawEllipse(null, new Pen(new SolidColorBrush(agent.Stain, 0.9), 1.6), terminal, radius, radius);

        if (agent.Busy)
            context.DrawEllipse(new SolidColorBrush(agent.Stain), null, terminal, 4.5, 4.5);

        // Queue depth, as ticks stacked under the terminal. Counting three ticks is quicker than
        // reading the number, and the number is there anyway for when it matters.
        int ticks = Math.Min(agent.Queued, 6);
        for (int i = 0; i < ticks; i++)
        {
            double x = terminal.X - 12 + (i * 4.6);
            context.DrawLine(
                new Pen(new SolidColorBrush(agent.Stain, 0.75), 2.4),
                new Point(x, terminal.Y + radius + 6),
                new Point(x, terminal.Y + radius + 12));
        }

        double labelX = terminal.X + radius + 14;
        Label(context, agent.Display, PlateItalic, 14, _inkBrush, new Point(labelX, terminal.Y - 17));
        Label(context, agent.Caption, Data, 10.5, _stromaBrush, new Point(labelX, terminal.Y - 1));
        Label(context, Truncate(agent.LastFinding, 34), Plate, 10.5, _faintBrush, new Point(labelX, terminal.Y + 13));
    }

    /// <summary>
    /// One message in flight: a bright head with a fading tail behind it, so direction is readable
    /// without an arrowhead. Outbound impulses carry the orchestrator's violet, returning ones carry
    /// the specialist's own stain - the colour says who is speaking.
    /// </summary>
    private static void DrawImpulse(DrawingContext context, Layout layout, ArborField field, ArborField.Impulse impulse)
    {
        AgentViewModel agent = field.Agents[impulse.Agent];
        Color colour = impulse.Outbound ? Cresyl : agent.Stain;

        double t = Math.Clamp(impulse.Position, 0, 1);
        Point head = PointOn(layout, impulse.Agent, t);

        for (int i = 4; i >= 1; i--)
        {
            double back = impulse.Outbound ? t - (i * 0.028) : t + (i * 0.028);
            if (back is < 0 or > 1) continue;

            Point at = PointOn(layout, impulse.Agent, back);
            double fade = 0.26 - (i * 0.05);
            context.DrawEllipse(new SolidColorBrush(colour, fade), null, at, 3.4 - (i * 0.45), 3.4 - (i * 0.45));
        }

        context.DrawEllipse(new SolidColorBrush(colour, 0.18), null, head, 8.5, 8.5);
        context.DrawEllipse(new SolidColorBrush(colour), null, head, 3.6, 3.6);
    }

    // ================================== Text ===================================

    private static void Label(
        DrawingContext context, string text, Typeface typeface, double size, IBrush brush, Point at, double? centreOn = null)
    {
        var formatted = new FormattedText(
            text, CultureInfo.InvariantCulture, FlowDirection.LeftToRight, typeface, size, brush);

        Point origin = centreOn is null ? at : new Point(centreOn.Value - (formatted.Width / 2), at.Y);
        context.DrawText(formatted, origin);
    }

    private static string Truncate(string text, int limit) =>
        text.Length <= limit ? text : string.Concat(text.AsSpan(0, limit - 1), "…");
}
