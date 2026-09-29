// BigPipe Gallery — runnable use cases with sample code.
// Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
//
//   BigPipe.Gallery                         desktop app
//   BigPipe.Gallery --run-all               run every case in the console (smoke test)
//   BigPipe.Gallery --screenshot <dir>      render screenshots headlessly (docs)
using Avalonia;
using Avalonia.Headless;
using Avalonia.Threading;
using BigPipe.Gallery;
using BigPipe.Gallery.Cases;
using BigPipe.Gallery.ViewModels;
using BigPipe.Gallery.Views;

internal static class Program
{
    [STAThread]
    public static int Main(string[] args)
    {
        if (args.Contains("--run-all")) return RunAllAsync(args).GetAwaiter().GetResult();
        var shot = Array.IndexOf(args, "--screenshot");
        if (shot >= 0) return Screenshots(args.Length > shot + 1 ? args[shot + 1] : "docs/images", args);
        BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
        return 0;
    }

    public static AppBuilder BuildAvaloniaApp() =>
        AppBuilder.Configure<App>().UsePlatformDetect().LogToTrace();

    private static async Task<int> RunAllAsync(string[] args)
    {
        var only = args.SkipWhile(a => a != "--only").Skip(1).FirstOrDefault();
        var endpoints = new GalleryEndpoints();
        var failed = 0;
        foreach (var c in CaseCatalog.All.Where(c => only is null || c.Title.Contains(only, StringComparison.OrdinalIgnoreCase)))
        {
            Console.WriteLine($"\n=== {c.Category} · {c.Title}");
            var ctx = new GalleryContext(endpoints, l => Console.WriteLine("  " + l), (l, v, u) => Console.WriteLine($"  [metric] {l} = {v:0.##} {u}"));
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
                await c.RunAsync(ctx, cts.Token);
                Console.WriteLine($"  OK ({sw.Elapsed.TotalSeconds:0.0}s)");
            }
            catch (Exception e)
            {
                failed++;
                Console.WriteLine($"  FAILED: {e}");
            }
        }
        Console.WriteLine(failed == 0 ? "\nall cases passed" : $"\n{failed} case(s) failed");
        return failed == 0 ? 0 : 1;
    }

    /// <summary>Renders the main window headlessly with Skia and saves PNGs, optionally after running cases.</summary>
    private static int Screenshots(string dir, string[] args)
    {
        Directory.CreateDirectory(dir);
        AppBuilder.Configure<App>().UseSkia().UseHeadless(new AvaloniaHeadlessPlatformOptions { UseHeadlessDrawing = false }).SetupWithoutStarting();
        var theme = args.Contains("--dark") ? Avalonia.Styling.ThemeVariant.Dark : Avalonia.Styling.ThemeVariant.Light;
        Application.Current!.RequestedThemeVariant = theme;
        var vm = new MainViewModel();
        var window = new MainWindow { DataContext = vm, Width = 1440, Height = 920 };
        window.Show();
        void Pump(int ms)
        {
            var until = DateTime.UtcNow.AddMilliseconds(ms);
            while (DateTime.UtcNow < until)
            {
                Dispatcher.UIThread.RunJobs();
                AvaloniaHeadlessPlatform.ForceRenderTimerTick();
                Thread.Sleep(15);
            }
        }
        void Save(string name)
        {
            Pump(400);
            window.CaptureRenderedFrame()?.Save(Path.Combine(dir, name));
            Console.WriteLine($"saved {Path.Combine(dir, name)}");
        }
        Pump(1500); // connection check
        var picks = new (string File, string Title)[]
        {
            ("gallery-hello.png", "Produce and consume"),
            ("gallery-share-groups.png", "Share groups: work queues with a DLQ"),
            ("gallery-anomaly.png", "Anomaly detection with ML.NET"),
            ("gallery-scripting.png", "Dynamic scripting: C#, Python, JavaScript"),
            ("gallery-sentiment.png", "Review sentiment (MediaPipe.NET · GraviText)"),
        };
        foreach (var (file, title) in picks)
        {
            var c = vm.AllCases.First(x => x.Title == title);
            vm.Selected = c;
            var run = c.RunAsync();
            var deadline = DateTime.UtcNow.AddSeconds(90);
            while (!run.IsCompleted && DateTime.UtcNow < deadline) Pump(100);
            Save(theme == Avalonia.Styling.ThemeVariant.Dark ? file.Replace(".png", "-dark.png") : file);
        }
        return 0;
    }
}
