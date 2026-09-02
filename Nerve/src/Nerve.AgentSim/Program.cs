// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;

namespace Nerve.AgentSim;

internal static class Program
{
    [STAThread]
    public static void Main(string[] args)
    {
        int demo = Array.IndexOf(args, "--demo");
        if (demo >= 0)
        {
            App.DemoMissions = demo + 1 < args.Length && int.TryParse(args[demo + 1], out int count) ? count : 6;
        }

        int shot = Array.IndexOf(args, "--screenshot");
        if (shot >= 0 && shot + 1 < args.Length)
        {
            App.ScreenshotPath = args[shot + 1];
            if (App.DemoMissions == 0) App.DemoMissions = 6;

            int wait = Array.IndexOf(args, "--wait");
            if (wait >= 0 && wait + 1 < args.Length && int.TryParse(args[wait + 1], out int ms))
                App.ScreenshotDelayMs = ms;
        }

        BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
    }

    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .WithInterFont()
            .LogToTrace();
}
