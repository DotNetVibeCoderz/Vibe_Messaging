// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using Nerve.AgentSim.Agents;
using Nerve.AgentSim.ViewModels;
using Nerve.AgentSim.Views;

namespace Nerve.AgentSim;

/// <summary>The application. Owns the simulation for as long as the window is open.</summary>
public class App : Application
{
    private SimulationHost? _host;
    private MainViewModel? _view;

    /// <summary>Missions to feed in automatically at start-up, set by <c>--demo</c>.</summary>
    public static int DemoMissions { get; set; }

    /// <summary>Where to write a PNG of the window, set by <c>--screenshot</c>.</summary>
    public static string? ScreenshotPath { get; set; }

    /// <summary>How long to let the simulation run before the screenshot is taken.</summary>
    public static int ScreenshotDelayMs { get; set; } = 4_200;

    /// <inheritdoc />
    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    /// <inheritdoc />
    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            _host = new SimulationHost();
            _view = new MainViewModel(_host);

            var window = new MainWindow { DataContext = _view };
            desktop.MainWindow = window;

            desktop.ShutdownRequested += (_, _) =>
            {
                _view?.Dispose();
                _ = _host?.DisposeAsync();
            };

            if (DemoMissions > 0) _ = _view.RunDemoAsync(DemoMissions);
            if (ScreenshotPath is not null) _ = Screenshot.CaptureAsync(window, ScreenshotPath, ScreenshotDelayMs);
        }

        base.OnFrameworkInitializationCompleted();
    }
}
