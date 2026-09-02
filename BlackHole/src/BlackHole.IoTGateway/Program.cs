// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;

namespace BlackHole.IoTGateway;

internal static class Program
{
    [STAThread]
    public static void Main(string[] args)
    {
        int demo = Array.IndexOf(args, "--demo");
        if (demo >= 0)
        {
            App.DemoDeviceCount = demo + 1 < args.Length && int.TryParse(args[demo + 1], out int count)
                ? count
                : 12;
        }

        BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
    }

    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<App>()
            .UsePlatformDetect()
            .WithInterFont()
            .LogToTrace();
}
