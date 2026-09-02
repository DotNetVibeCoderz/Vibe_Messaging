// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using Avalonia.Controls;
using Avalonia.Controls.Shapes;
using Avalonia.Interactivity;
using Avalonia.Layout;
using Avalonia.Media;
using Avalonia.Threading;
using SocketSignal.SonarDemo.Controls;
using SocketSignal.SonarDemo.Simulation;

namespace SocketSignal.SonarDemo;

/// <summary>
/// The operator console. It owns no sea state of its own: everything it draws arrived from the
/// array over a WebSocket, and every button on it is a SocketSignal call.
/// </summary>
public sealed partial class MainWindow : Window
{
    private const string HttpPrefix = "http://localhost:8123/sonar/";
    private const string WsUri = "ws://localhost:8123/sonar/";

    private readonly ConsoleModel _model = new();
    private readonly Dictionary<string, ContactRow> _rows = new(StringComparer.Ordinal);
    private readonly DispatcherTimer _render = new() { Interval = TimeSpan.FromMilliseconds(16) };

    private SonarStation? _station;
    private SocketSignalClient? _console;
    private bool _held;

    private int _framesThisSecond;
    private DateTime _rateWindowStart = DateTime.UtcNow;
    private double _frameRate;

    public MainWindow()
    {
        InitializeComponent();

        Scope.Model = _model;
        Waterfall.Model = _model;
        Scope.ContactPicked += Select;

        _render.Tick += (_, _) => Draw();
        Opened += async (_, _) => await StartAsync();
        Closing += (_, _) => _ = ShutdownAsync();
    }

    // =========================================================================================
    // Wiring
    // =========================================================================================

    private async Task StartAsync()
    {
        _station = new SonarStation(HttpPrefix);
        await _station.StartAsync();

        _console = new SocketSignalClient(new SocketSignalOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(10),
            CallTimeout = TimeSpan.FromSeconds(10),
        })
        {
            AutoReconnect = true,
        };

        // The array pushes one of these twenty times a second to everyone in the operators group.
        // A single typed argument, so the hot path deserialises straight into the record.
        _console.On<SweepFrame, bool>("sonar.sweep", frame =>
        {
            if (frame is not null && !_held)
            {
                _model.Apply(frame);
                Interlocked.Increment(ref _framesThisSecond);
            }
            return ValueTask.FromResult(true);
        });

        _console.Connected += id => Dispatcher.UIThread.Post(() => SetLink(true, $"linked to array-01 as {id[..8]}"));
        _console.Disconnected += why => Dispatcher.UIThread.Post(() => SetLink(false, $"link lost: {why}"));
        _console.Reconnecting += attempt => Dispatcher.UIThread.Post(() => SetLink(false, $"reconnecting, attempt {attempt}"));

        await _console.ConnectAsync(new Uri(WsUri));

        // Ask to be put in the operators group. Until this returns, no sweep frames arrive.
        await _console.CallAsync<bool>("sonar.attach");

        _render.Start();
    }

    private async Task ShutdownAsync()
    {
        _render.Stop();
        if (_console is not null) await _console.DisposeAsync();
        if (_station is not null) await _station.DisposeAsync();
    }

    // =========================================================================================
    // Operator actions - each one is a call over the library
    // =========================================================================================

    private async void OnClassify(object? sender, RoutedEventArgs e)
    {
        if (_console is null || _model.SelectedId is not { } id) return;

        ClassifyButton.IsEnabled = false;
        ClassifyButton.Content = "STUDYING RETURN";
        try
        {
            // Client to server, with a return value. The array takes about half a second, which is
            // the whole reason this is a request-response call and not a broadcast.
            ClassificationResult? result = await _console.CallAsync<string, ClassificationResult>("sonar.classify", id);
            if (result is not null) ShowReport(result);
        }
        catch (SocketSignalException ex)
        {
            ShowReport(new ClassificationResult(id, Classification.Unidentified, 0, ex.Message));
        }
        finally
        {
            ClassifyButton.Content = "CLASSIFY CONTACT";
            ClassifyButton.IsEnabled = _model.SelectedId is not null;
        }
    }

    private async void OnActivePing(object? sender, RoutedEventArgs e)
    {
        if (_console is null) return;

        PingButton.IsEnabled = false;
        try
        {
            int illuminated = await _console.CallAsync<int>("sonar.ping");
            TransportText.Text = $"active ping: {illuminated} contacts illuminated";
        }
        catch (SocketSignalException ex)
        {
            TransportText.Text = ex.Message;
        }
        finally
        {
            PingButton.IsEnabled = true;
        }
    }

    private void OnHold(object? sender, RoutedEventArgs e)
    {
        _held = !_held;
        HoldButton.Content = _held ? "RESUME" : "HOLD";
        HoldButton.Foreground = _held
            ? (IBrush)Resources["Surface"]!
            : (IBrush)Resources["Chalk"]!;
    }

    private void Select(string? id)
    {
        _model.SelectedId = _model.SelectedId == id ? null : id;
        ClassifyButton.IsEnabled = _model.SelectedId is not null;
    }

    private void ShowReport(ClassificationResult result)
    {
        ReportPanel.IsVisible = true;
        ReportHeadline.Text = $"{result.Id}  {result.Class.ToString().ToUpperInvariant()}  {result.Confidence:P0}";
        ReportNote.Text = result.Note;
    }

    // =========================================================================================
    // Drawing
    // =========================================================================================

    private void Draw()
    {
        DateTime now = DateTime.UtcNow;

        // Frames per second, measured over a rolling second - the honest measure of the link.
        if (now - _rateWindowStart >= TimeSpan.FromSeconds(1))
        {
            _frameRate = Interlocked.Exchange(ref _framesThisSecond, 0) / (now - _rateWindowStart).TotalSeconds;
            _rateWindowStart = now;
            _model.RecordRate(_frameRate);
            RatePlot.Samples = _model.RateHistory();
            RateText.Text = $"{_frameRate:0.0}";
        }

        ContactEcho[] echoes = _model.Snapshot();
        SyncContactList(echoes);

        BeamText.Text = $"{_model.InterpolatedBeam(now):000.0}";
        TickText.Text = $"{_model.Tick:N0}";
        HeldCount.Text = echoes.Length.ToString();

        if (_station is not null)
        {
            ConsoleCount.Text = _station.ConsoleCount.ToString();
            (long frames, long bytes) = _station.Traffic;
            if (!TransportText.Text!.StartsWith("active ping", StringComparison.Ordinal))
            {
                TransportText.Text = $"{frames:N0} frames pushed, {bytes / 1024.0:N0} KB, " +
                                     $"{(frames == 0 ? 0 : bytes / (double)frames):0} B/frame";
            }
        }

        Scope.InvalidateVisual();
        Waterfall.InvalidateVisual();
        RatePlot.InvalidateVisual();
    }

    /// <summary>
    /// Keeps the contact list in step with what the array holds. Rows are created once and then
    /// only their text changes - rebuilding the list twenty times a second would flicker and would
    /// lose the operator's selection.
    /// </summary>
    private void SyncContactList(ContactEcho[] echoes)
    {
        foreach (ContactEcho echo in echoes)
        {
            if (!_rows.TryGetValue(echo.Id, out ContactRow? row))
            {
                row = new ContactRow(echo.Id, Select);
                _rows[echo.Id] = row;
                ContactList.Children.Add(row.Root);
            }
            row.Update(echo, echo.Id == _model.SelectedId);
        }

        foreach (string id in _rows.Keys.Where(id => !echoes.Any(e => e.Id == id)).ToList())
        {
            ContactList.Children.Remove(_rows[id].Root);
            _rows.Remove(id);
        }

        // Nearest first: on a console, range is the thing that decides what you look at.
        var ordered = echoes.Select(e => _rows[e.Id].Root).ToList();
        for (int i = 0; i < ordered.Count; i++)
        {
            if (ContactList.Children.IndexOf(ordered[i]) == i) continue;
            ContactList.Children.Remove(ordered[i]);
            ContactList.Children.Insert(i, ordered[i]);
        }
    }

    private void SetLink(bool up, string message)
    {
        LinkText.Text = message;
        LinkLamp.Fill = up ? (IBrush)Resources["Return"]! : (IBrush)Resources["Alarm"]!;
    }

    // =========================================================================================
    // One row of the contact list
    // =========================================================================================

    private sealed class ContactRow
    {
        private readonly Border _root;
        private readonly Rectangle _flag;
        private readonly TextBlock _id;
        private readonly TextBlock _bearing;
        private readonly TextBlock _detail;

        public Control Root => _root;

        public ContactRow(string id, Action<string> onPicked)
        {
            _flag = new Rectangle { Width = 3, Height = 30, VerticalAlignment = VerticalAlignment.Center };

            _id = new TextBlock { Classes = { "data" }, FontWeight = FontWeight.SemiBold };
            _bearing = new TextBlock { Classes = { "data" }, HorizontalAlignment = HorizontalAlignment.Right };
            _detail = new TextBlock { Classes = { "dataDim" }, FontSize = 10.5 };

            var grid = new Grid
            {
                ColumnDefinitions = new ColumnDefinitions("Auto,10,*,Auto"),
                RowDefinitions = new RowDefinitions("Auto,Auto"),
            };
            Grid.SetRowSpan(_flag, 2);
            grid.Children.Add(_flag);

            Grid.SetColumn(_id, 2);
            Grid.SetColumn(_bearing, 3);
            Grid.SetColumn(_detail, 2);
            Grid.SetColumnSpan(_detail, 2);
            Grid.SetRow(_detail, 1);
            grid.Children.Add(_id);
            grid.Children.Add(_bearing);
            grid.Children.Add(_detail);

            _root = new Border
            {
                Padding = new Avalonia.Thickness(11, 8),
                Margin = new Avalonia.Thickness(3, 1),
                CornerRadius = new Avalonia.CornerRadius(2),
                Cursor = new Avalonia.Input.Cursor(Avalonia.Input.StandardCursorType.Hand),
                Child = grid,
            };
            _root.PointerPressed += (_, _) => onPicked(id);
        }

        public void Update(ContactEcho echo, bool selected)
        {
            Color colour = PpiScope.ColourOf(echo);
            _flag.Fill = new SolidColorBrush(colour);

            _id.Text = echo.Id;
            _bearing.Text = $"{echo.Bearing:000.0}";
            _detail.Text = $"{echo.RangeKm,5:0.0} km   {echo.SpeedKnots,4:0} kn   " +
                           $"{echo.Class.ToString().ToLowerInvariant()}";

            _root.Background = selected
                ? new SolidColorBrush(Color.FromRgb(0x1B, 0x35, 0x43))
                : Brushes.Transparent;
            _root.BorderThickness = new Avalonia.Thickness(selected ? 1 : 0);
            _root.BorderBrush = new SolidColorBrush(Color.FromArgb(0x66, colour.R, colour.G, colour.B));
        }
    }
}
