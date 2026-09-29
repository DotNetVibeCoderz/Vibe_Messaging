using System.Collections.Specialized;
using System.Globalization;
using Avalonia.Controls;
using Avalonia.Data.Converters;
using Avalonia.Threading;
using BigPipe.Gallery.ViewModels;

namespace BigPipe.Gallery.Views;

public partial class MainWindow : Window
{
    private CaseViewModel? _watched;

    public MainWindow()
    {
        InitializeComponent();
        DataContextChanged += (_, _) => Watch();
    }

    private void Watch()
    {
        if (DataContext is not MainViewModel vm) return;
        vm.PropertyChanged += (_, e) =>
        {
            if (e.PropertyName == nameof(MainViewModel.Selected)) Attach(vm.Selected);
        };
        Attach(vm.Selected);
    }

    /// <summary>Keeps the output console scrolled to the newest line.</summary>
    private void Attach(CaseViewModel c)
    {
        if (_watched is not null) _watched.Output.CollectionChanged -= OnOutput;
        _watched = c;
        c.Output.CollectionChanged += OnOutput;
    }

    private void OnOutput(object? sender, NotifyCollectionChangedEventArgs e) =>
        Dispatcher.UIThread.Post(() => this.FindControl<ScrollViewer>("OutputScroll")?.ScrollToEnd(), DispatcherPriority.Background);
}

public static class Converters
{
    public static readonly IValueConverter Upper = new FuncValueConverter<string?, string?>(s => s?.ToUpper(CultureInfo.InvariantCulture));
}
