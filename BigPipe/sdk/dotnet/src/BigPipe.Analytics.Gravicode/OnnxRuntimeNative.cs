using System.Reflection;
using System.Runtime.InteropServices;

namespace BigPipe.Analytics.Gravicode;

/// <summary>
/// Makes sure ONNX Runtime binds to the native library shipped in the Microsoft.ML.OnnxRuntime
/// NuGet package. Windows 11 has an older onnxruntime.dll in System32 that would otherwise be
/// picked up in hosts that do not add package native folders to the search path (notebooks,
/// plugins), crashing on first use.
/// </summary>
public static class OnnxRuntimeNative
{
    private static int _done;

    public static void EnsureLoaded()
    {
        if (Interlocked.Exchange(ref _done, 1) == 1) return;
        var ort = typeof(Microsoft.ML.OnnxRuntime.OrtEnv).Assembly;
        try
        {
            NativeLibrary.SetDllImportResolver(ort, Resolve);
        }
        catch (InvalidOperationException)
        {
            // A resolver is already registered (the host or ONNX Runtime handles it).
        }
    }

    private static IntPtr Resolve(string name, Assembly assembly, DllImportSearchPath? path)
    {
        if (!name.StartsWith("onnxruntime", StringComparison.OrdinalIgnoreCase)) return IntPtr.Zero;
        foreach (var candidate in Candidates(name, assembly))
            if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out var handle))
                return handle;
        return IntPtr.Zero;
    }

    private static IEnumerable<string> Candidates(string name, Assembly assembly)
    {
        var file = OperatingSystem.IsWindows() ? name.EndsWith(".dll", StringComparison.OrdinalIgnoreCase) ? name : name + ".dll"
            : OperatingSystem.IsMacOS() ? $"lib{name}.dylib" : $"lib{name}.so";
        var rid = RuntimeInformation.RuntimeIdentifier;
        var arch = RuntimeInformation.ProcessArchitecture.ToString().ToLowerInvariant();
        var os = OperatingSystem.IsWindows() ? "win" : OperatingSystem.IsMacOS() ? "osx" : "linux";
        var rids = new[] { rid, $"{os}-{arch}" }.Distinct().ToList();

        var dirs = new List<string> { AppContext.BaseDirectory };
        if (Path.GetDirectoryName(assembly.Location) is { Length: > 0 } asmDir) dirs.Add(asmDir);
        foreach (var d in dirs)
        {
            yield return Path.Combine(d, file);
            foreach (var r in rids) yield return Path.Combine(d, "runtimes", r, "native", file);
        }

        // NuGet global packages folder (notebook hosts load assemblies straight from it).
        var packages = Environment.GetEnvironmentVariable("NUGET_PACKAGES")
                       ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".nuget", "packages");
        var pkg = Path.Combine(packages, "microsoft.ml.onnxruntime");
        if (!Directory.Exists(pkg)) yield break;
        var version = assembly.GetName().Version;
        var versions = Directory.GetDirectories(pkg)
            .OrderByDescending(v => version is not null && Path.GetFileName(v).StartsWith($"{version.Major}.{version.Minor}.", StringComparison.Ordinal))
            .ThenByDescending(v => Path.GetFileName(v), StringComparer.Ordinal);
        foreach (var v in versions)
            foreach (var r in rids)
                yield return Path.Combine(v, "runtimes", r, "native", file);
    }
}
