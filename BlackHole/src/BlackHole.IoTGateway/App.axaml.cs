// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using BlackHole.IoTGateway.Views;

namespace BlackHole.IoTGateway;

public partial class App : Application
{
    /// <summary>
    /// Devices to bring up automatically once the window opens; 0 leaves the panel idle.
    /// Set by <c>--demo [count]</c>, which drives the same commands an operator would.
    /// </summary>
    public static int DemoDeviceCount { get; set; }

    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
            desktop.MainWindow = new MainWindow();

        base.OnFrameworkInitializationCompleted();
    }
}
