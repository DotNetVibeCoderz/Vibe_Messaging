// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;
using Avalonia.Controls;
using Avalonia.Media;

namespace SocketSignal.SonarDemo.Controls;

/// <summary>
/// A bare trace of one number over time, for the telemetry strip. No axes and no labels - the
/// number itself is printed beside it, and this only has to answer "is it steady?".
/// </summary>
public sealed class Sparkline : Control
{
    public IReadOnlyList<double> Samples { get; set; } = [];

    /// <summary>Fixed ceiling, so the trace does not rescale itself into looking constant.</summary>
    public double Maximum { get; set; } = 25;

    public IBrush Stroke { get; set; } = new SolidColorBrush(Color.FromRgb(0x7F, 0xD4, 0xC1));

    public Sparkline() => ClipToBounds = true;

    public override void Render(DrawingContext context)
    {
        Size size = Bounds.Size;
        if (Samples.Count < 2 || size.Width < 8 || size.Height < 4) return;

        var pen = new Pen(Stroke, 1.2);
        double step = size.Width / (Samples.Count - 1);

        for (int i = 1; i < Samples.Count; i++)
        {
            double y0 = size.Height * (1 - Math.Clamp(Samples[i - 1] / Maximum, 0, 1));
            double y1 = size.Height * (1 - Math.Clamp(Samples[i] / Maximum, 0, 1));
            context.DrawLine(pen, new Point((i - 1) * step, y0), new Point(i * step, y1));
        }
    }
}
