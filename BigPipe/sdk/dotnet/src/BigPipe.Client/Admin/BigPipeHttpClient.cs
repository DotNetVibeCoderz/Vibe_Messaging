using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace BigPipe.Client.Admin;

public enum ShareAckAction
{
    Accept,
    Release,
    Reject,
}

public sealed record ShareAck(string Topic, int Partition, long Offset, ShareAckAction Action);

/// <summary>
/// Client for the BigPipe HTTP gateway (port 8082): REST produce/consume, Server-Sent Events
/// streaming with server-side filters, server-assigned HTTP groups and share groups.
/// </summary>
public sealed class BigPipeHttpClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly bool _owns;

    public BigPipeHttpClient(string baseUrl = "http://localhost:8082")
        : this(new HttpClient { BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/"), Timeout = Timeout.InfiniteTimeSpan }, owns: true)
    {
    }

    public BigPipeHttpClient(HttpClient http, bool owns = false)
    {
        _http = http;
        _owns = owns;
    }

    private async Task<T> PostAsync<T>(string path, object body, CancellationToken ct) =>
        await BigPipeAdminClient.ReadAsync<T>(await _http.PostAsJsonAsync(path, body, AdminJson.Options, ct).ConfigureAwait(false), ct).ConfigureAwait(false);

    private async Task<T> GetAsync<T>(string path, CancellationToken ct) =>
        await BigPipeAdminClient.ReadAsync<T>(await _http.GetAsync(path, ct).ConfigureAwait(false), ct).ConfigureAwait(false);

    /// <summary>Produces records (JSON values are stored as JSON text; strings as-is).</summary>
    public async Task<List<HttpOffset>> ProduceAsync(string topic, IEnumerable<HttpProduceRecord> records, string? compression = null, CancellationToken ct = default)
    {
        var doc = await PostAsync<JsonElement>($"v1/topics/{Uri.EscapeDataString(topic)}/records", new { records, compression }, ct).ConfigureAwait(false);
        return doc.GetProperty("offsets").Deserialize<List<HttpOffset>>(AdminJson.Options)!;
    }

    public async Task<HttpOffset> ProduceAsync(string topic, object? value, string? key = null, IDictionary<string, string>? headers = null, CancellationToken ct = default) =>
        (await ProduceAsync(topic, [new HttpProduceRecord { Key = key, Value = value, Headers = headers }], null, ct).ConfigureAwait(false))[0];

    /// <summary>Reads one partition from an offset (number, "earliest" or "latest").</summary>
    public async Task<(List<HttpRecord> Records, long NextOffset, long HighWatermark)> ReadAsync(string topic, int partition, string offset = "earliest",
        int limit = 500, string? filter = null, int timeoutMs = 0, CancellationToken ct = default)
    {
        var q = $"v1/topics/{Uri.EscapeDataString(topic)}/partitions/{partition}/records?offset={Uri.EscapeDataString(offset)}&limit={limit}&timeout_ms={timeoutMs}";
        if (!string.IsNullOrWhiteSpace(filter)) q += "&filter=" + Uri.EscapeDataString(filter);
        var doc = await GetAsync<JsonElement>(q, ct).ConfigureAwait(false);
        return (doc.GetProperty("records").Deserialize<List<HttpRecord>>(AdminJson.Options)!,
            doc.GetProperty("next_offset").GetInt64(), doc.GetProperty("high_watermark").GetInt64());
    }

    /// <summary>
    /// Streams records over Server-Sent Events. With <paramref name="group"/> the stream joins a
    /// server-assigned group and commits as it goes; <paramref name="filter"/> runs on the broker.
    /// </summary>
    public async IAsyncEnumerable<HttpRecord> StreamAsync(string topic, string from = "latest", string? filter = null, string? group = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var q = new StringBuilder($"v1/topics/{Uri.EscapeDataString(topic)}/stream?from={Uri.EscapeDataString(from)}");
        if (!string.IsNullOrWhiteSpace(filter)) q.Append("&filter=").Append(Uri.EscapeDataString(filter));
        if (group is not null) q.Append("&group=").Append(Uri.EscapeDataString(group));
        using var req = new HttpRequestMessage(HttpMethod.Get, q.ToString());
        req.Headers.Accept.ParseAdd("text/event-stream");
        using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode) await BigPipeAdminClient.ReadAsync<JsonElement>(resp, ct).ConfigureAwait(false);
        await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        string? evt = null;
        while (!ct.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync(ct).ConfigureAwait(false);
            if (line is null) yield break;
            if (line.StartsWith("event:", StringComparison.Ordinal)) evt = line[6..].Trim();
            else if (line.StartsWith("data:", StringComparison.Ordinal) && evt == "record")
                yield return JsonSerializer.Deserialize<HttpRecord>(line.AsSpan(5).Trim(), AdminJson.Options)!;
            else if (line.Length == 0) evt = null;
        }
    }

    // --------------------------------------------------------------------- HTTP groups --------

    public async Task<string> JoinGroupAsync(string group, IEnumerable<string> topics, bool autoCommit = true, string offsetReset = "earliest",
        string? filter = null, CancellationToken ct = default)
    {
        var doc = await PostAsync<JsonElement>($"v1/groups/{Uri.EscapeDataString(group)}/members",
            new { topics = topics.ToList(), auto_commit = autoCommit, offset_reset = offsetReset, filter }, ct).ConfigureAwait(false);
        return doc.GetProperty("member_id").GetString()!;
    }

    public async Task<List<HttpRecord>> PollAsync(string group, string memberId, int timeoutMs = 1000, int maxRecords = 500, CancellationToken ct = default)
    {
        var doc = await GetAsync<JsonElement>($"v1/groups/{Uri.EscapeDataString(group)}/members/{Uri.EscapeDataString(memberId)}/records?timeout_ms={timeoutMs}&max_records={maxRecords}", ct).ConfigureAwait(false);
        return doc.GetProperty("records").Deserialize<List<HttpRecord>>(AdminJson.Options)!;
    }

    public Task CommitGroupAsync(string group, string memberId, CancellationToken ct = default) =>
        PostAsync<JsonElement>($"v1/groups/{Uri.EscapeDataString(group)}/members/{Uri.EscapeDataString(memberId)}/commit", new { }, ct);

    public async Task LeaveGroupAsync(string group, string memberId, CancellationToken ct = default)
    {
        using var resp = await _http.DeleteAsync($"v1/groups/{Uri.EscapeDataString(group)}/members/{Uri.EscapeDataString(memberId)}", ct).ConfigureAwait(false);
    }

    // --------------------------------------------------------------------- share groups -------

    public async Task<List<HttpRecord>> SharePollAsync(string group, string member, IEnumerable<string> topics, int maxRecords = 100,
        TimeSpan? lockDuration = null, int maxAttempts = 5, string? deadLetterTopic = null, string? filter = null, int timeoutMs = 1000,
        CancellationToken ct = default)
    {
        var doc = await PostAsync<JsonElement>($"v1/share/{Uri.EscapeDataString(group)}/poll", new
        {
            member,
            topics = topics.ToList(),
            max_records = maxRecords,
            lock_ms = (long)(lockDuration ?? TimeSpan.FromSeconds(30)).TotalMilliseconds,
            max_attempts = maxAttempts,
            dlq_topic = deadLetterTopic,
            filter,
            timeout_ms = timeoutMs,
        }, ct).ConfigureAwait(false);
        return doc.GetProperty("records").Deserialize<List<HttpRecord>>(AdminJson.Options)!;
    }

    public async Task<List<bool>> ShareAckAsync(string group, string member, IEnumerable<ShareAck> acks, CancellationToken ct = default)
    {
        var doc = await PostAsync<JsonElement>($"v1/share/{Uri.EscapeDataString(group)}/ack", new
        {
            member,
            acks = acks.Select(a => new { topic = a.Topic, partition = a.Partition, offset = a.Offset, action = a.Action.ToString().ToLowerInvariant() }).ToList(),
        }, ct).ConfigureAwait(false);
        return doc.GetProperty("results").Deserialize<List<bool>>()!;
    }

    public void Dispose()
    {
        if (_owns) _http.Dispose();
    }
}
