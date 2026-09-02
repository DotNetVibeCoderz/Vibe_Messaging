// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;
using Avalonia.Controls;
using Avalonia.Media.Imaging;
using Avalonia.Threading;

namespace Nerve.AgentSim;

/// <summary>
/// Renders the live window to a PNG, so the pictures in the documentation are of the panel actually
/// working rather than a mock-up.
/// </summary>
/// <remarks>
/// The capture waits for the simulation to have something to show, then renders the same visual
/// tree that is on screen. Nothing is staged for it: whatever the agents happen to be doing at that
/// moment is what lands in the file.
/// </remarks>
internal static class Screenshot
{
    /// <summary>Captures the window and exits.</summary>
    /// <param name="window">The window to render.</param>
    /// <param name="path">Where to write the PNG.</param>
    /// <param name="delayMs">How long to let the simulation run first.</param>
    public static async Task CaptureAsync(Window window, string path, int delayMs)
    {
        await Task.Delay(delayMs).ConfigureAwait(true);

        await Dispatcher.UIThread.InvokeAsync(() =>
        {
            var size = new PixelSize((int)window.Bounds.Width, (int)window.Bounds.Height);
            if (size.Width <= 0 || size.Height <= 0) return;

            string? directory = Path.GetDirectoryName(Path.GetFullPath(path));
            if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);

            // 192 DPI: the panel is dense, and a 1x capture makes the 10.5pt monospace unreadable
            // in a README.
            using var bitmap = new RenderTargetBitmap(
                new PixelSize(size.Width * 2, size.Height * 2), new Vector(192, 192));

            bitmap.Render(window);
            bitmap.Save(path);

            Console.WriteLine($"wrote {path} ({size.Width * 2}x{size.Height * 2})");
        });

        await Task.Delay(120).ConfigureAwait(true);

        Dispatcher.UIThread.Post(() =>
        {
            if (Application.Current?.ApplicationLifetime is
                Avalonia.Controls.ApplicationLifetimes.IClassicDesktopStyleApplicationLifetime desktop)
            {
                desktop.Shutdown();
            }
        });
    }
}
