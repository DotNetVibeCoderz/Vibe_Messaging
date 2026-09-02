// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;
using Avalonia.Controls;
using Avalonia.Media;

namespace BlackHole.IoTGateway.Controls;

/// <summary>One pen on the chart: a device's trace, its colour, and whether it is currently drawn bold.</summary>
public sealed class ChartChannel
{
    public required string Label { get; init; }
    public required Color Pen { get; init; }
    public required TraceBuffer Samples { get; init; }

    /// <summary>Value range this channel's samples are scaled against.</summary>
    public required double Minimum { get; init; }
    public required double Maximum { get; init; }

    /// <summary>Drawn at full weight; the rest recede so the selected device stands out.</summary>
    public bool IsEmphasised { get; set; }

    /// <summary>Skipped entirely while false.</summary>
    public bool IsVisible { get; set; } = true;
}

/// <summary>
/// The panel's signature: a multi-channel strip chart drawn the way a paper recorder draws one.
/// </summary>
/// <remarks>
/// <para>
/// Time runs right to left with the newest sample at the right edge, against a graticule with real
/// major and minor divisions. Each device is one pen, and the pen colour is the device's identity
/// everywhere else in the window, so the chart is a legend for the whole panel rather than a
/// decoration on top of it.
/// </para>
/// <para>
/// Rendering is a single <see cref="StreamGeometry"/> per channel built from a stack-allocated
/// sample buffer, so a repaint at 30 Hz with two dozen channels stays off the allocation path.
/// </para>
/// </remarks>
public sealed class StripChart : Control
{
    private const int GutterRight = 56;   // room for the live value readout
    private const int GutterLeft = 40;    // room for the scale labels

    /// <summary>Pens to draw, newest sample at the right edge.</summary>
    public static readonly StyledProperty<IReadOnlyList<ChartChannel>?> ChannelsProperty =
        AvaloniaProperty.Register<StripChart, IReadOnlyList<ChartChannel>?>(nameof(Channels));

    /// <summary>Seconds of history across the full width, used only to label the time axis.</summary>
    public static readonly StyledProperty<double> WindowSecondsProperty =
        AvaloniaProperty.Register<StripChart, double>(nameof(WindowSeconds), 60d);

    /// <summary>Graticule colour.</summary>
    public static readonly StyledProperty<IBrush?> GraticuleBrushProperty =
        AvaloniaProperty.Register<StripChart, IBrush?>(nameof(GraticuleBrush));

    /// <summary>Colour for axis labels.</summary>
    public static readonly StyledProperty<IBrush?> LabelBrushProperty =
        AvaloniaProperty.Register<StripChart, IBrush?>(nameof(LabelBrush));

    /// <summary>Face used for the axis labels.</summary>
    public static readonly StyledProperty<FontFamily> LabelFontFamilyProperty =
        AvaloniaProperty.Register<StripChart, FontFamily>(nameof(LabelFontFamily), FontFamily.Default);

    static StripChart()
    {
        AffectsRender<StripChart>(ChannelsProperty, WindowSecondsProperty, GraticuleBrushProperty, LabelBrushProperty);
    }

    public IReadOnlyList<ChartChannel>? Channels
    {
        get => GetValue(ChannelsProperty);
        set => SetValue(ChannelsProperty, value);
    }

    public double WindowSeconds
    {
        get => GetValue(WindowSecondsProperty);
        set => SetValue(WindowSecondsProperty, value);
    }

    public IBrush? GraticuleBrush
    {
        get => GetValue(GraticuleBrushProperty);
        set => SetValue(GraticuleBrushProperty, value);
    }

    public IBrush? LabelBrush
    {
        get => GetValue(LabelBrushProperty);
        set => SetValue(LabelBrushProperty, value);
    }

    public FontFamily LabelFontFamily
    {
        get => GetValue(LabelFontFamilyProperty);
        set => SetValue(LabelFontFamilyProperty, value);
    }

    /// <summary>Repaints without a property change, for the animation timer.</summary>
    public void Tick() => InvalidateVisual();

    public override void Render(DrawingContext context)
    {
        Rect bounds = new(Bounds.Size);
        if (bounds.Width <= GutterLeft + GutterRight || bounds.Height <= 20)
            return;

        var plot = new Rect(
            GutterLeft, 6,
            bounds.Width - GutterLeft - GutterRight,
            bounds.Height - 22);

        DrawGraticule(context, plot);
        DrawChannels(context, plot);
        DrawAxisLabels(context, bounds, plot);
    }

    /// <summary>
    /// The graticule: eight minor columns per major division and four horizontal bands, which is
    /// what a chart recorder's printed paper looks like. Major lines are brighter so the eye can
    /// count divisions without a ruler.
    /// </summary>
    private void DrawGraticule(DrawingContext context, Rect plot)
    {
        IBrush graticule = GraticuleBrush ?? Brushes.DimGray;
        var minor = new Pen(graticule, 0.5) { LineCap = PenLineCap.Flat };
        var major = new Pen(graticule, 1);

        // Slightly darker bed so the traces sit on paper rather than on the cabinet.
        context.FillRectangle(new SolidColorBrush(Color.FromArgb(70, 0, 0, 0)), plot);

        const int majorColumns = 6;
        const int minorPerMajor = 4;
        for (int i = 0; i <= majorColumns * minorPerMajor; i++)
        {
            double x = plot.X + plot.Width * i / (double)(majorColumns * minorPerMajor);
            bool isMajor = i % minorPerMajor == 0;
            context.DrawLine(
                isMajor ? major : minor,
                new Point(x, plot.Y),
                new Point(x, plot.Bottom));
            if (!isMajor)
            {
                // Minor lines are drawn once more at low alpha rather than with a second pen, so
                // there is only one pen allocation per repaint.
                context.FillRectangle(
                    new SolidColorBrush(Color.FromArgb(28, 255, 255, 255)),
                    new Rect(x, plot.Y, 0.5, plot.Height));
            }
        }

        for (int i = 0; i <= 4; i++)
        {
            double y = plot.Y + plot.Height * i / 4.0;
            context.DrawLine(major, new Point(plot.X, y), new Point(plot.Right, y));
        }
    }

    private void DrawChannels(DrawingContext context, Rect plot)
    {
        IReadOnlyList<ChartChannel>? channels = Channels;
        if (channels is null || channels.Count == 0)
            return;

        int columns = Math.Max(8, Math.Min((int)plot.Width, 720));
        double[] samples = new double[columns];

        // Emphasised pens draw last so the selected device is never hidden under the others.
        for (int pass = 0; pass < 2; pass++)
        {
            foreach (ChartChannel channel in channels)
            {
                if (!channel.IsVisible || channel.IsEmphasised != (pass == 1))
                    continue;

                int real = channel.Samples.CopyLatest(samples);
                if (real < 2)
                    continue;

                double range = channel.Maximum - channel.Minimum;
                if (range <= 0)
                    continue;

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

                        double x = plot.X + plot.Width * i / (columns - 1.0);
                        double normalised = Math.Clamp((value - channel.Minimum) / range, 0, 1);
                        double y = plot.Bottom - plot.Height * normalised;

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

                byte alpha = channel.IsEmphasised ? (byte)255 : (byte)150;
                var color = Color.FromArgb(alpha, channel.Pen.R, channel.Pen.G, channel.Pen.B);
                context.DrawGeometry(null, new Pen(new SolidColorBrush(color), channel.IsEmphasised ? 2.0 : 1.2)
                {
                    LineJoin = PenLineJoin.Round,
                    LineCap = PenLineCap.Round,
                }, geometry);

                // The pen head: a dot at the right edge, where a real recorder's nib would sit.
                double latest = channel.Samples.Latest;
                if (!double.IsNaN(latest))
                {
                    double y = plot.Bottom - plot.Height * Math.Clamp((latest - channel.Minimum) / range, 0, 1);
                    context.DrawEllipse(new SolidColorBrush(channel.Pen), null, new Point(plot.Right, y),
                        channel.IsEmphasised ? 3.5 : 2.0, channel.IsEmphasised ? 3.5 : 2.0);
                }
            }
        }
    }

    private void DrawAxisLabels(DrawingContext context, Rect bounds, Rect plot)
    {
        IBrush labelBrush = LabelBrush ?? Brushes.Gray;
        var typeface = new Typeface(LabelFontFamily);

        // Vertical scale is a percentage of each channel's own range: mixing °C and kPa on one
        // scale would be a lie, so the axis says what it really is.
        string[] scale = ["100", "75", "50", "25", "0"];
        for (int i = 0; i < scale.Length; i++)
        {
            var text = new FormattedText(scale[i], System.Globalization.CultureInfo.InvariantCulture,
                FlowDirection.LeftToRight, typeface, 9, labelBrush);
            double y = plot.Y + plot.Height * i / 4.0;
            context.DrawText(text, new Point(GutterLeft - text.Width - 6, y - text.Height / 2));
        }

        var percent = new FormattedText("% of range", System.Globalization.CultureInfo.InvariantCulture,
            FlowDirection.LeftToRight, typeface, 9, labelBrush);
        context.DrawText(percent, new Point(plot.X + 4, bounds.Height - percent.Height - 1));

        for (int i = 0; i <= 6; i++)
        {
            double seconds = WindowSeconds * (6 - i) / 6.0;
            string caption = i == 6 ? "now" : $"-{seconds:0}s";
            var text = new FormattedText(caption, System.Globalization.CultureInfo.InvariantCulture,
                FlowDirection.LeftToRight, typeface, 9, labelBrush);
            double x = plot.X + plot.Width * i / 6.0;
            context.DrawText(text, new Point(x - (i == 6 ? 2 : text.Width / 2), bounds.Height - text.Height - 1));
        }
    }
}
