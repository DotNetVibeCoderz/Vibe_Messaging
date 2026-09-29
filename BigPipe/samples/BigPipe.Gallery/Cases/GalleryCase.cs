using BigPipe.Client.Admin;

namespace BigPipe.Gallery.Cases;

/// <summary>Where the Gallery connects.</summary>
public sealed class GalleryEndpoints
{
    public string Bootstrap { get; set; } = Environment.GetEnvironmentVariable("BIGPIPE_BOOTSTRAP") ?? "localhost:9092";
    public string AdminUrl { get; set; } = Environment.GetEnvironmentVariable("BIGPIPE_ADMIN") ?? "http://localhost:9644";
    public string HttpUrl { get; set; } = Environment.GetEnvironmentVariable("BIGPIPE_HTTP") ?? "http://localhost:8082";
    public string RegistryUrl { get; set; } = Environment.GetEnvironmentVariable("BIGPIPE_REGISTRY") ?? "http://localhost:8081";
}

/// <summary>What a running case can report to the UI.</summary>
public sealed class GalleryContext(GalleryEndpoints endpoints, Action<string> log, Action<string, double, string> metric)
{
    public GalleryEndpoints Endpoints { get; } = endpoints;
    public BigPipeAdminClient Admin { get; } = new(endpoints.AdminUrl);

    /// <summary>Writes a line to the output console.</summary>
    public void Log(string line) => log(line);

    /// <summary>Adds a bar to the result chart (label, value, unit).</summary>
    public void Metric(string label, double value, string unit = "") => metric(label, value, unit);

    /// <summary>Topic/group names unique per run, so cases can be re-run safely.</summary>
    public string Unique(string prefix) => $"gallery-{prefix}-{Guid.NewGuid().ToString("N")[..6]}";
}

/// <summary>A snippet in another language showing the same idea.</summary>
public sealed record Snippet(string Language, string Code);

/// <summary>One runnable use case: description, sample code and a live run against BigPipe.</summary>
public abstract class GalleryCase
{
    public abstract string Category { get; }
    public abstract string Title { get; }
    public abstract string Summary { get; }

    /// <summary>What the run demonstrates, shown as bullet points.</summary>
    public virtual IReadOnlyList<string> Highlights => [];

    /// <summary>The C# code the case runs (kept in sync with <see cref="RunAsync"/>).</summary>
    public abstract string Code { get; }

    /// <summary>The same idea in other SDKs.</summary>
    public virtual IReadOnlyList<Snippet> OtherLanguages => [];

    /// <summary>True when the case needs the Schema Registry service as well.</summary>
    public virtual bool NeedsRegistry => false;

    public abstract Task RunAsync(GalleryContext ctx, CancellationToken ct);
}
