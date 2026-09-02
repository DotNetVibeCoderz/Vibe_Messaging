// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Interactivity;
using Avalonia.VisualTree;
using BlackHole.IoTGateway.Controls;
using BlackHole.IoTGateway.ViewModels;

namespace BlackHole.IoTGateway.Views;

public partial class MainWindow : Window
{
    private readonly MainViewModel _model = new();

    public MainWindow()
    {
        InitializeComponent();
        DataContext = _model;

        // The charts draw themselves rather than binding per sample, so the view model tells them
        // when a frame's worth of data has landed. One invalidate per frame, not one per reading.
        _model.FrameRendered += OnFrameRendered;

        if (App.DemoDeviceCount > 0)
            Opened += async (_, _) => await _model.RunDemoAsync(App.DemoDeviceCount);
    }

    private void OnFrameRendered()
    {
        // InitializeComponent assigns the named controls, so Ribbon is set by the time the render
        // timer starts. The null check covers the window being torn down mid-frame.
        Ribbon?.Tick();
        foreach (Sparkline sparkline in this.GetVisualDescendants().OfType<Sparkline>())
            sparkline.Tick();
    }

    /// <summary>Selecting a row emphasises that device's pen in the ribbon above.</summary>
    private void OnDeviceTapped(object? sender, TappedEventArgs e)
    {
        if (sender is Control { DataContext: DeviceViewModel device })
            _model.Selected = device;
    }

    protected override async void OnClosed(EventArgs e)
    {
        _model.FrameRendered -= OnFrameRendered;
        base.OnClosed(e);
        await _model.DisposeAsync();
    }

}
