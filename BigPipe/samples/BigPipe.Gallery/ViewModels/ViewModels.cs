using System.Collections.ObjectModel;
using Avalonia.Threading;
using BigPipe.Client.Admin;
using BigPipe.Gallery.Cases;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace BigPipe.Gallery.ViewModels;

public sealed class MetricBar(string label, double value, string unit)
{
    public string Label { get; } = label;
    public double Value { get; } = value;
    public string Unit { get; } = unit;
    public string Display => Value >= 1000 ? $"{Value:N0} {Unit}".Trim() : $"{Value:0.##} {Unit}".Trim();

    /// <summary>Bar length relative to the largest bar (0..1), set by the owner.</summary>
    public double Ratio { get; set; }

    public double BarWidth => Math.Max(4, Ratio * 260);
}

public sealed class SnippetViewModel(string language, string code)
{
    public string Language { get; } = language;
    public string Code { get; } = code;
}

public sealed partial class CaseViewModel : ObservableObject
{
    private readonly GalleryEndpoints _endpoints;
    private CancellationTokenSource? _cts;

    public CaseViewModel(GalleryCase model, GalleryEndpoints endpoints, int number)
    {
        Model = model;
        _endpoints = endpoints;
        Number = number;
        Snippets = [new SnippetViewModel("C#", model.Code), .. model.OtherLanguages.Select(s => new SnippetViewModel(s.Language, s.Code))];
        _selectedSnippet = Snippets[0];
    }

    public GalleryCase Model { get; }
    public int Number { get; }
    public string Title => Model.Title;
    public string Summary => Model.Summary;
    public string Category => Model.Category;
    public IReadOnlyList<string> Highlights => Model.Highlights;
    public IReadOnlyList<SnippetViewModel> Snippets { get; }
    public ObservableCollection<string> Output { get; } = [];
    public ObservableCollection<MetricBar> Metrics { get; } = [];
    public string RequirementNote => Model.NeedsRegistry ? "Needs BigPipe + Schema Registry (port 8081)" : "Needs a BigPipe node (bigpiped --mode dev)";

    [ObservableProperty] private SnippetViewModel _selectedSnippet;
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(CanRun))] private bool _isRunning;
    [ObservableProperty] private string _status = "Ready";
    [ObservableProperty] [NotifyPropertyChangedFor(nameof(IsOk), nameof(IsError), nameof(IsBusy))] private string _statusKind = "idle";
    [ObservableProperty] private bool _isSelected;

    public bool IsOk => StatusKind == "ok";
    public bool IsError => StatusKind == "error";
    public bool IsBusy => StatusKind == "running";
    public string NumberLabel => Number.ToString("00");

    public bool CanRun => !IsRunning;

    [RelayCommand]
    public async Task RunAsync()
    {
        if (IsRunning) return;
        IsRunning = true;
        Output.Clear();
        Metrics.Clear();
        Status = "Running…";
        StatusKind = "running";
        _cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var started = DateTime.Now;
        var ctx = new GalleryContext(_endpoints,
            line => Dispatcher.UIThread.Post(() => Output.Add($"{DateTime.Now:HH:mm:ss.fff}  {line}")),
            (label, value, unit) => Dispatcher.UIThread.Post(() => AddMetric(label, value, unit)));
        try
        {
            await Task.Run(() => Model.RunAsync(ctx, _cts.Token));
            Status = $"Finished in {(DateTime.Now - started).TotalSeconds:0.0} s";
            StatusKind = "ok";
        }
        catch (OperationCanceledException)
        {
            Status = "Stopped";
            StatusKind = "idle";
        }
        catch (Exception e)
        {
            Output.Add($"{DateTime.Now:HH:mm:ss.fff}  ERROR {e.GetType().Name}: {e.Message}");
            Status = "Failed — is bigpiped running? See the output.";
            StatusKind = "error";
        }
        finally
        {
            IsRunning = false;
            _cts.Dispose();
            _cts = null;
        }
    }

    [RelayCommand]
    public void Stop() => _cts?.Cancel();

    private void AddMetric(string label, double value, string unit)
    {
        Metrics.Add(new MetricBar(label, value, unit));
        var max = Metrics.Max(m => Math.Abs(m.Value));
        var copy = Metrics.ToList();
        Metrics.Clear();
        foreach (var m in copy)
        {
            m.Ratio = max <= 0 ? 0 : Math.Abs(m.Value) / max;
            Metrics.Add(m);
        }
    }
}

public sealed class CaseGroup(string name, IReadOnlyList<CaseViewModel> cases)
{
    public string Name { get; } = name;
    public IReadOnlyList<CaseViewModel> Cases { get; } = cases;
}

public sealed partial class MainViewModel : ObservableObject
{
    public MainViewModel()
    {
        var number = 0;
        AllCases = CaseCatalog.All.Select(c => new CaseViewModel(c, Endpoints, ++number)).ToList();
        Groups = AllCases.GroupBy(c => c.Category).Select(g => new CaseGroup(g.Key, g.ToList())).ToList();
        _selected = AllCases[0];
        _selected.IsSelected = true;
        _ = CheckConnectionAsync();
    }

    public GalleryEndpoints Endpoints { get; } = new();
    public IReadOnlyList<CaseViewModel> AllCases { get; }
    public IReadOnlyList<CaseGroup> Groups { get; }
    public string Credit => "Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil";

    [ObservableProperty] private CaseViewModel _selected;
    [ObservableProperty] private string _connection = "Checking…";
    [ObservableProperty] private bool _connected;

    [RelayCommand]
    public void Select(CaseViewModel c) => Selected = c;

    partial void OnSelectedChanged(CaseViewModel? oldValue, CaseViewModel newValue)
    {
        if (oldValue is not null) oldValue.IsSelected = false;
        newValue.IsSelected = true;
    }

    [RelayCommand]
    public async Task CheckConnectionAsync()
    {
        try
        {
            using var admin = new BigPipeAdminClient(Endpoints.AdminUrl);
            var info = await admin.GetClusterAsync(new CancellationTokenSource(TimeSpan.FromSeconds(3)).Token);
            Connection = $"{info.ClusterId} · v{info.Version} · {info.Topics} topics · {Endpoints.Bootstrap}";
            Connected = true;
        }
        catch (Exception)
        {
            Connection = $"No BigPipe at {Endpoints.AdminUrl} — start it with: bigpiped --mode dev";
            Connected = false;
        }
    }
}
