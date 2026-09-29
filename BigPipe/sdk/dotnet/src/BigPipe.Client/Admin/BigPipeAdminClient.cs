using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;

namespace BigPipe.Client.Admin;

/// <summary>
/// Client for the BigPipe Admin REST API (port 9644): topics, storage-mode migration, groups
/// and lag, flows, the message browser and cluster metrics.
/// </summary>
public sealed class BigPipeAdminClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly bool _owns;

    public BigPipeAdminClient(string baseUrl = "http://localhost:9644", string? apiKey = null)
        : this(new HttpClient { BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/") }, apiKey, owns: true)
    {
    }

    public BigPipeAdminClient(HttpClient http, string? apiKey = null, bool owns = false)
    {
        _http = http;
        _owns = owns;
        if (apiKey is not null) _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
    }

    public Uri? BaseAddress => _http.BaseAddress;

    internal static async Task<T> ReadAsync<T>(HttpResponseMessage resp, CancellationToken ct)
    {
        if (!resp.IsSuccessStatusCode)
        {
            string message;
            try
            {
                using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false));
                message = doc.RootElement.GetProperty("error").GetProperty("message").GetString() ?? resp.ReasonPhrase ?? "error";
            }
            catch
            {
                message = resp.ReasonPhrase ?? "request failed";
            }
            var code = resp.StatusCode switch
            {
                System.Net.HttpStatusCode.NotFound => ErrorCode.UnknownTopicOrPartition,
                System.Net.HttpStatusCode.Conflict => ErrorCode.TopicAlreadyExists,
                System.Net.HttpStatusCode.RequestedRangeNotSatisfiable => ErrorCode.OffsetOutOfRange,
                _ => ErrorCode.HttpError,
            };
            throw new BigPipeException(code, $"{(int)resp.StatusCode}: {message}");
        }
        return (await resp.Content.ReadFromJsonAsync<T>(AdminJson.Options, ct).ConfigureAwait(false))!;
    }

    private async Task<T> GetAsync<T>(string path, CancellationToken ct) =>
        await ReadAsync<T>(await _http.GetAsync(path, ct).ConfigureAwait(false), ct).ConfigureAwait(false);

    private async Task<T> SendAsync<T>(HttpMethod method, string path, object? body, CancellationToken ct)
    {
        using var req = new HttpRequestMessage(method, path);
        if (body is not null) req.Content = JsonContent.Create(body, body.GetType(), options: AdminJson.Options);
        return await ReadAsync<T>(await _http.SendAsync(req, ct).ConfigureAwait(false), ct).ConfigureAwait(false);
    }

    public Task<ClusterInfo> GetClusterAsync(CancellationToken ct = default) => GetAsync<ClusterInfo>("v1/cluster", ct);

    public Task<MetricsSnapshot> GetMetricsAsync(CancellationToken ct = default) => GetAsync<MetricsSnapshot>("v1/metrics", ct);

    public Task<List<TopicInfo>> ListTopicsAsync(CancellationToken ct = default) => GetAsync<List<TopicInfo>>("v1/topics", ct);

    public Task<TopicInfo> GetTopicAsync(string name, CancellationToken ct = default) => GetAsync<TopicInfo>($"v1/topics/{Uri.EscapeDataString(name)}", ct);

    public Task<TopicInfo> CreateTopicAsync(string name, int partitions = 3, StorageMode mode = StorageMode.Local,
        IDictionary<string, string>? config = null, CancellationToken ct = default) =>
        SendAsync<TopicInfo>(HttpMethod.Post, "v1/topics", new
        {
            name,
            partitions,
            mode = mode.ToString().ToLowerInvariant(),
            config = config ?? new Dictionary<string, string>(),
        }, ct);

    /// <summary>Creates the topic unless it already exists.</summary>
    public async Task<TopicInfo> EnsureTopicAsync(string name, int partitions = 3, StorageMode mode = StorageMode.Local,
        IDictionary<string, string>? config = null, CancellationToken ct = default)
    {
        try
        {
            return await CreateTopicAsync(name, partitions, mode, config, ct).ConfigureAwait(false);
        }
        catch (BigPipeException e) when (e.Code == ErrorCode.TopicAlreadyExists)
        {
            return await GetTopicAsync(name, ct).ConfigureAwait(false);
        }
    }

    public Task DeleteTopicAsync(string name, CancellationToken ct = default) =>
        SendAsync<JsonElement>(HttpMethod.Delete, $"v1/topics/{Uri.EscapeDataString(name)}", null, ct);

    /// <summary>Sets config keys; a null value removes the key.</summary>
    public Task<TopicInfo> UpdateConfigAsync(string name, IDictionary<string, string?> changes, CancellationToken ct = default) =>
        SendAsync<TopicInfo>(HttpMethod.Patch, $"v1/topics/{Uri.EscapeDataString(name)}/config", new { config = changes }, ct);

    /// <summary>Changes the storage mode online. Existing offsets stay where they are.</summary>
    public Task<MigrationResult> MigrateAsync(string name, StorageMode to, CancellationToken ct = default) =>
        SendAsync<MigrationResult>(HttpMethod.Post, $"v1/topics/{Uri.EscapeDataString(name)}/migrate", new { to = to.ToString().ToLowerInvariant() }, ct);

    public Task<TopicInfo> AddPartitionsAsync(string name, int count, CancellationToken ct = default) =>
        SendAsync<TopicInfo>(HttpMethod.Post, $"v1/topics/{Uri.EscapeDataString(name)}/partitions", new { count }, ct);

    /// <summary>Newest records of a topic (or from an offset), optionally filtered with bpql.</summary>
    public async Task<List<HttpRecord>> BrowseAsync(string topic, int limit = 50, int? partition = null, string? offset = null,
        string? filter = null, CancellationToken ct = default)
    {
        var q = new StringBuilder($"v1/topics/{Uri.EscapeDataString(topic)}/messages?limit={limit}");
        if (partition is not null) q.Append("&partition=").Append(partition);
        if (offset is not null) q.Append("&offset=").Append(Uri.EscapeDataString(offset));
        if (!string.IsNullOrWhiteSpace(filter)) q.Append("&filter=").Append(Uri.EscapeDataString(filter));
        var doc = await GetAsync<JsonElement>(q.ToString(), ct).ConfigureAwait(false);
        return doc.GetProperty("records").Deserialize<List<HttpRecord>>(AdminJson.Options)!;
    }

    public Task<List<GroupInfo>> ListGroupsAsync(CancellationToken ct = default) => GetAsync<List<GroupInfo>>("v1/groups", ct);

    public Task<GroupInfo> GetGroupAsync(string id, CancellationToken ct = default) => GetAsync<GroupInfo>($"v1/groups/{Uri.EscapeDataString(id)}", ct);

    public Task DeleteGroupAsync(string id, CancellationToken ct = default) =>
        SendAsync<JsonElement>(HttpMethod.Delete, $"v1/groups/{Uri.EscapeDataString(id)}", null, ct);

    /// <summary>Resets committed offsets: <paramref name="to"/> is earliest, latest or offset.</summary>
    public Task ResetGroupAsync(string id, string topic, string to, long? offset = null, CancellationToken ct = default) =>
        SendAsync<JsonElement>(HttpMethod.Post, $"v1/groups/{Uri.EscapeDataString(id)}/reset", new { topic, to, offset }, ct);

    public Task<List<FlowStatus>> ListFlowsAsync(CancellationToken ct = default) => GetAsync<List<FlowStatus>>("v1/flows", ct);

    public Task<FlowSpec> DeployFlowAsync(FlowSpec spec, CancellationToken ct = default) =>
        SendAsync<FlowSpec>(HttpMethod.Post, "v1/flows", spec, ct);

    /// <summary>Deploys a Kubernetes-style YAML Flow document.</summary>
    public async Task<FlowSpec> DeployFlowYamlAsync(string yaml, CancellationToken ct = default)
    {
        using var content = new StringContent(yaml, Encoding.UTF8, "application/yaml");
        return await ReadAsync<FlowSpec>(await _http.PostAsync("v1/flows", content, ct).ConfigureAwait(false), ct).ConfigureAwait(false);
    }

    public Task DeleteFlowAsync(string name, CancellationToken ct = default) =>
        SendAsync<JsonElement>(HttpMethod.Delete, $"v1/flows/{Uri.EscapeDataString(name)}", null, ct);

    public Task PauseFlowAsync(string name, CancellationToken ct = default) =>
        SendAsync<JsonElement>(HttpMethod.Post, $"v1/flows/{Uri.EscapeDataString(name)}/pause", new { }, ct);

    public Task ResumeFlowAsync(string name, CancellationToken ct = default) =>
        SendAsync<JsonElement>(HttpMethod.Post, $"v1/flows/{Uri.EscapeDataString(name)}/resume", new { }, ct);

    /// <summary>Validates a bpql filter (or mapping) and evaluates it against a sample value.</summary>
    public async Task<JsonElement?> ValidateExpressionAsync(string expr, object? sample = null, bool mapping = false, CancellationToken ct = default)
    {
        var r = await SendAsync<JsonElement>(HttpMethod.Post, "v1/expr/validate", new { expr, kind = mapping ? "mapping" : "filter", sample }, ct).ConfigureAwait(false);
        return r.TryGetProperty("result", out var v) ? v : null;
    }

    public void Dispose()
    {
        if (_owns) _http.Dispose();
    }
}
