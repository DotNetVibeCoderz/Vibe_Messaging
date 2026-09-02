// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;
using Avalonia.Controls;
using Avalonia.Media;

namespace BlackHole.IoTGateway.Controls;

/// <summary>
/// The same pen trace as <see cref="StripChart"/>, shrunk to fit one device row.
/// </summary>
/// <remarks>
/// Repeating the chart motif at row scale is the structural idea of the panel: one device is one
/// pen, and you read the same shape in the row and in the ribbon above. No axes here - at this size
/// only the shape is legible, and the number beside it carries the value.
/// </remarks>
public sealed class Sparkline : Control
{
    public static readonly StyledProperty<TraceBuffer?> SamplesProperty =
        AvaloniaProperty.Register<Sparkline, TraceBuffer?>(nameof(Samples));

    public static readonly StyledProperty<Color> PenColorProperty =
        AvaloniaProperty.Register<Sparkline, Color>(nameof(PenColor), Colors.SteelBlue);

    public static readonly StyledProperty<double> MinimumProperty =
        AvaloniaProperty.Register<Sparkline, double>(nameof(Minimum));

    public static readonly StyledProperty<double> MaximumProperty =
        AvaloniaProperty.Register<Sparkline, double>(nameof(Maximum), 100d);

    static Sparkline()
    {
        AffectsRender<Sparkline>(SamplesProperty, PenColorProperty, MinimumProperty, MaximumProperty);
    }

    public TraceBuffer? Samples
    {
        get => GetValue(SamplesProperty);
        set => SetValue(SamplesProperty, value);
    }

    public Color PenColor
    {
        get => GetValue(PenColorProperty);
        set => SetValue(PenColorProperty, value);
    }

    public double Minimum
    {
        get => GetValue(MinimumProperty);
        set => SetValue(MinimumProperty, value);
    }

    public double Maximum
    {
        get => GetValue(MaximumProperty);
        set => SetValue(MaximumProperty, value);
    }

    /// <summary>Repaints on the animation timer.</summary>
    public void Tick() => InvalidateVisual();

    public override void Render(DrawingContext context)
    {
        TraceBuffer? buffer = Samples;
        double range = Maximum - Minimum;
        if (buffer is null || range <= 0 || Bounds.Width < 8 || Bounds.Height < 4)
            return;

        int columns = Math.Max(8, Math.Min((int)Bounds.Width, 240));
        Span<double> samples = columns <= 256 ? stackalloc double[columns] : new double[columns];
        if (buffer.CopyLatest(samples) < 2)
            return;

        double height = Bounds.Height - 3;
        var geometry = new StreamGeometry();
        using (StreamGeometryContext pen = geometry.Open())
        {
            bool open = false;
            for (int i = 0; i < columns; i++)
            {
                double value = samples[i];
                if (double.IsNaN(value))
                {
                    open = false;
                    continue;
                }

                double x = Bounds.Width * i / (columns - 1.0);
                double y = 1.5 + height - height * Math.Clamp((value - Minimum) / range, 0, 1);

                if (!open)
                {
                    pen.BeginFigure(new Point(x, y), isFilled: false);
                    open = true;
                }
                else
                {
                    pen.LineTo(new Point(x, y));
                }
            }
            if (open)
                pen.EndFigure(false);
        }

        context.DrawGeometry(null, new Pen(new SolidColorBrush(PenColor), 1.4)
        {
            LineJoin = PenLineJoin.Round,
            LineCap = PenLineCap.Round,
        }, geometry);
    }
}
