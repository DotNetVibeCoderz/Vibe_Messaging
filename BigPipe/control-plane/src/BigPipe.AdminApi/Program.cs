// BigPipe Admin API gateway: RBAC + audit + OpenAPI in front of the bigpiped admin API.
// Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
using System.Diagnostics;
using System.Text.Json;
using BigPipe.AdminApi;
using BigPipe.Client;
using BigPipe.Client.Admin;

var builder = WebApplication.CreateBuilder(args);
var opt = builder.Configuration.GetSection("BigPipe").Get<GatewayOptions>() ?? new GatewayOptions();
builder.Services.AddSingleton(opt);
builder.Services.AddSingleton<AuditLog>();
builder.Services.AddHttpClient("bigpiped", c => c.BaseAddress = new Uri(opt.AdminUrl.TrimEnd('/') + "/"));
builder.Services.AddOpenApi();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));
builder.WebHost.UseUrls(builder.Configuration["Urls"] ?? "http://0.0.0.0:9650");

var app = builder.Build();
app.UseCors();
app.MapOpenApi();

// Authentication: "Authorization: Bearer <key>" or "X-Api-Key". No keys configured = open dev mode (admin).
app.Use(async (ctx, next) =>
{
    if (ctx.Request.Path.StartsWithSegments("/openapi") || ctx.Request.Path == "/healthz" || HttpMethods.IsOptions(ctx.Request.Method))
    {
        await next();
        return;
    }
    var identity = opt.Authenticate(ctx.Request);
    if (identity is null)
    {
        ctx.Response.StatusCode = 401;
        await ctx.Response.WriteAsJsonAsync(new { error = new { code = "unauthorized", message = "provide a valid API key (Authorization: Bearer <key>)" } });
        return;
    }
    ctx.Items["identity"] = identity;
    var needed = Rbac.Required(ctx.Request.Method, ctx.Request.Path);
    if (identity.Role < needed)
    {
        ctx.Response.StatusCode = 403;
        await ctx.Response.WriteAsJsonAsync(new { error = new { code = "forbidden", message = $"role '{identity.Role}' cannot {ctx.Request.Method} {ctx.Request.Path}; requires '{needed}'" } });
        return;
    }
    await next();
});

app.MapGet("/healthz", () => "ok");

app.MapGet("/v1/me", (HttpContext ctx) => ctx.Items["identity"])
    .WithSummary("Who am I and what may I do");

app.MapGet("/v1/overview", async (IHttpClientFactory f, CancellationToken ct) =>
{
    using var admin = new BigPipeAdminClient(f.CreateClient("bigpiped"));
    var cluster = await admin.GetClusterAsync(ct);
    var metrics = await admin.GetMetricsAsync(ct);
    var topics = await admin.ListTopicsAsync(ct);
    var groups = await admin.ListGroupsAsync(ct);
    return new
    {
        cluster,
        metrics,
        topics = new
        {
            count = topics.Count,
            records = topics.Sum(t => t.Records),
            bytes = new { local = topics.Sum(t => t.Bytes.Local), remote = topics.Sum(t => t.Bytes.Remote), diskless = topics.Sum(t => t.Bytes.Diskless) },
            by_mode = topics.GroupBy(t => t.Mode).ToDictionary(g => g.Key.ToString().ToLowerInvariant(), g => g.Count()),
        },
        groups = new { count = groups.Count, total_lag = groups.Sum(g => g.Lag), top_lag = groups.OrderByDescending(g => g.Lag).Take(5).Select(g => new { g.GroupId, g.Lag }) },
    };
}).WithSummary("Cluster, throughput and lag summary in one call");

app.MapGet("/v1/audit", async (AuditLog audit, int? limit, CancellationToken ct) => await audit.RecentAsync(limit ?? 100, ct))
    .WithSummary("Recent administrative actions (from the __bp_audit topic)");

// Everything else under /v1 is forwarded to bigpiped, with auditing of changes.
app.Map("/v1/{**rest}", async (HttpContext ctx, IHttpClientFactory f, AuditLog audit, string rest) =>
{
    var client = f.CreateClient("bigpiped");
    var target = $"v1/{rest}{ctx.Request.QueryString}";
    using var req = new HttpRequestMessage(new HttpMethod(ctx.Request.Method), target);
    string? body = null;
    if (ctx.Request.ContentLength > 0 || ctx.Request.Headers.TransferEncoding.Count > 0)
    {
        using var reader = new StreamReader(ctx.Request.Body);
        body = await reader.ReadToEndAsync();
        req.Content = new StringContent(body, System.Text.Encoding.UTF8, ctx.Request.ContentType?.Split(';')[0] ?? "application/json");
    }
    if (opt.UpstreamApiKey is { } key) req.Headers.Authorization = new("Bearer", key);
    var sw = Stopwatch.StartNew();
    using var resp = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ctx.RequestAborted);
    ctx.Response.StatusCode = (int)resp.StatusCode;
    ctx.Response.ContentType = resp.Content.Headers.ContentType?.ToString() ?? "application/json";
    await resp.Content.CopyToAsync(ctx.Response.Body, ctx.RequestAborted);
    if (!HttpMethods.IsGet(ctx.Request.Method))
        await audit.RecordAsync((Identity)ctx.Items["identity"]!, ctx.Request.Method, "/" + target, (int)resp.StatusCode, body, sw.Elapsed,
            ctx.Connection.RemoteIpAddress?.ToString());
}).WithSummary("Proxy to the bigpiped admin API (topics, groups, flows, migration, messages)");

app.Run();

namespace BigPipe.AdminApi
{
    public enum Role
    {
        Viewer = 1,
        Operator = 2,
        Admin = 3,
    }

    public sealed record Identity(string Name, Role Role);

    public sealed class ApiKeyEntry
    {
        public string Key { get; set; } = "";
        public string Name { get; set; } = "";
        public Role Role { get; set; } = Role.Viewer;
    }

    public sealed class GatewayOptions
    {
        public string AdminUrl { get; set; } = "http://localhost:9644";
        public string Bootstrap { get; set; } = "localhost:9092";
        public string AuditTopic { get; set; } = "__bp_audit";
        /// <summary>Key for bigpiped when its admin API is protected.</summary>
        public string? UpstreamApiKey { get; set; }
        public List<ApiKeyEntry> ApiKeys { get; set; } = [];

        public Identity? Authenticate(HttpRequest req)
        {
            if (ApiKeys.Count == 0) return new Identity("anonymous (dev mode)", Role.Admin);
            var auth = req.Headers.Authorization.ToString();
            var key = auth.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ? auth[7..] : req.Headers["X-Api-Key"].ToString();
            var entry = ApiKeys.FirstOrDefault(k => k.Key.Length > 0 && CryptographicEquals(k.Key, key));
            return entry is null ? null : new Identity(entry.Name, entry.Role);
        }

        private static bool CryptographicEquals(string a, string b) =>
            System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(
                System.Text.Encoding.UTF8.GetBytes(a), System.Text.Encoding.UTF8.GetBytes(b));
    }

    /// <summary>Which role an operation needs.</summary>
    public static class Rbac
    {
        public static Role Required(string method, PathString path)
        {
            if (HttpMethods.IsGet(method) || HttpMethods.IsHead(method)) return Role.Viewer;
            var p = path.Value ?? "";
            // Destructive or cluster-shaping operations are admin-only.
            if (HttpMethods.IsDelete(method) || p.Contains("/migrate") || p.StartsWith("/v1/flows") || p.EndsWith("/reset"))
                return Role.Admin;
            return Role.Operator;
        }
    }

    /// <summary>Writes one JSON record per administrative change to the audit topic.</summary>
    public sealed class AuditLog(GatewayOptions opt, IHttpClientFactory http, ILogger<AuditLog> log) : IAsyncDisposable
    {
        private readonly Lazy<Producer<string, string>> _producer = new(() =>
            new ProducerBuilder<string, string>().WithBootstrap(opt.Bootstrap).WithClientId("bigpipe-admin-api").Build());

        public async Task RecordAsync(Identity who, string method, string path, int status, string? body, TimeSpan took, string? ip)
        {
            var entry = new
            {
                ts = DateTimeOffset.UtcNow,
                user = who.Name,
                role = who.Role.ToString().ToLowerInvariant(),
                method,
                path,
                status,
                ms = (int)took.TotalMilliseconds,
                ip,
                body = body is { Length: > 2048 } ? body[..2048] + "…" : body,
            };
            try
            {
                await _producer.Value.SendAsync(opt.AuditTopic, who.Name, JsonSerializer.Serialize(entry));
            }
            catch (Exception e)
            {
                log.LogError(e, "audit write failed for {Method} {Path}", method, path);
            }
        }

        public async Task<IReadOnlyList<JsonElement>> RecentAsync(int limit, CancellationToken ct)
        {
            using var admin = new BigPipeAdminClient(http.CreateClient("bigpiped"));
            try
            {
                var records = await admin.BrowseAsync(opt.AuditTopic, limit, ct: ct);
                return records.Select(r => JsonDocument.Parse(r.Value ?? "{}").RootElement.Clone()).ToList();
            }
            catch (BigPipeException e) when (e.Code == ErrorCode.UnknownTopicOrPartition)
            {
                return [];
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_producer.IsValueCreated) await _producer.Value.DisposeAsync();
        }
    }
}
