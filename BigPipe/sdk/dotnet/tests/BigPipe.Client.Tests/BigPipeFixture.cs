using System.Diagnostics;
using BigPipe.Client.Admin;

namespace BigPipe.Client.Tests;

/// <summary>
/// Starts a real <c>bigpiped</c> (from the Cargo target directory) on test ports with a
/// throwaway data directory. Set BIGPIPE_TEST_BOOTSTRAP to reuse a running node instead.
/// </summary>
public sealed class BigPipeFixture : IAsyncLifetime
{
    private Process? _process;
    private string? _dataDir;

    public string Bootstrap { get; private set; } = "localhost:19092";
    public string AdminUrl { get; private set; } = "http://localhost:19644";
    public string HttpUrl { get; private set; } = "http://localhost:18082";

    public BigPipeAdminClient Admin { get; private set; } = null!;

    private static string? FindBinary()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        var exe = OperatingSystem.IsWindows() ? "bigpiped.exe" : "bigpiped";
        while (dir is not null)
        {
            foreach (var profile in new[] { "release", "debug" })
            {
                var candidate = Path.Combine(dir.FullName, "target", profile, exe);
                if (File.Exists(candidate)) return candidate;
            }
            dir = dir.Parent;
        }
        return null;
    }

    public async Task InitializeAsync()
    {
        if (Environment.GetEnvironmentVariable("BIGPIPE_TEST_BOOTSTRAP") is { } boot)
        {
            Bootstrap = boot;
            AdminUrl = Environment.GetEnvironmentVariable("BIGPIPE_TEST_ADMIN") ?? "http://localhost:9644";
            HttpUrl = Environment.GetEnvironmentVariable("BIGPIPE_TEST_HTTP") ?? "http://localhost:8082";
        }
        else
        {
            var bin = FindBinary() ?? throw new InvalidOperationException("bigpiped not found; run `cargo build --release -p bigpiped` first");
            _dataDir = Path.Combine(Path.GetTempPath(), "bigpipe-test-" + Guid.NewGuid().ToString("N")[..8]);
            var psi = new ProcessStartInfo(bin,
                $"--data-dir \"{_dataDir}\" --kafka-addr 0.0.0.0:19092 --advertised-port 19092 --http-addr 0.0.0.0:18082 --admin-addr 0.0.0.0:19644 --metrics-addr 0.0.0.0:19645 --shards 4")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            _process = Process.Start(psi)!;
            _process.OutputDataReceived += (_, _) => { };
            _process.ErrorDataReceived += (_, _) => { };
            _process.BeginOutputReadLine();
            _process.BeginErrorReadLine();
        }
        Admin = new BigPipeAdminClient(AdminUrl);
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (true)
        {
            try
            {
                await Admin.GetClusterAsync();
                return;
            }
            catch when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(200);
            }
        }
    }

    public Task DisposeAsync()
    {
        Admin.Dispose();
        if (_process is { HasExited: false })
        {
            _process.Kill(entireProcessTree: true);
            _process.WaitForExit(5000);
        }
        if (_dataDir is not null)
        {
            try { Directory.Delete(_dataDir, true); } catch { /* files may still be locked on Windows */ }
        }
        return Task.CompletedTask;
    }

    public static string Unique(string prefix) => $"{prefix}-{Guid.NewGuid().ToString("N")[..8]}";
}

[CollectionDefinition("bigpipe")]
public sealed class BigPipeCollection : ICollectionFixture<BigPipeFixture>;
