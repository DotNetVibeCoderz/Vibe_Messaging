// BigPipe Schema Registry — Confluent-compatible REST API backed by the __bp_schemas topic.
// Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
using System.Text.Json.Nodes;
using BigPipe.SchemaRegistry;

var builder = WebApplication.CreateBuilder(args);
var options = builder.Configuration.GetSection("BigPipe").Get<RegistryOptions>() ?? new RegistryOptions();
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<RegistryStore>();
builder.Services.AddHostedService<RegistryStartup>();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));
builder.WebHost.UseUrls(builder.Configuration["Urls"] ?? "http://0.0.0.0:8081");

var app = builder.Build();
app.UseCors();

// Confluent clients send application/vnd.schemaregistry.v1+json; accept any JSON body.
static async Task<JsonObject> Body(HttpRequest req)
{
    var node = await JsonNode.ParseAsync(req.Body);
    return node as JsonObject ?? throw new RegistryException(42201, "request body must be a JSON object");
}

static IResult Error(RegistryException e) =>
    Results.Json(new { error_code = e.ErrorCode, message = e.Message }, statusCode: e.Status, contentType: "application/vnd.schemaregistry.v1+json");

app.Use(async (ctx, next) =>
{
    var store = ctx.RequestServices.GetRequiredService<RegistryStore>();
    if (!store.Ready && ctx.Request.Path != "/")
    {
        ctx.Response.StatusCode = 503;
        await ctx.Response.WriteAsJsonAsync(new { error_code = 50301, message = "registry is loading from BigPipe; retry shortly" });
        return;
    }
    try
    {
        await next();
    }
    catch (RegistryException e)
    {
        await Error(e).ExecuteAsync(ctx);
    }
});

static object VersionJson(SchemaVersion v)
{
    var o = new Dictionary<string, object> { ["subject"] = v.Subject, ["version"] = v.Version, ["id"] = v.Id, ["schema"] = v.Schema };
    if (v.Format != SchemaFormat.Avro) o["schemaType"] = v.Format.ToString().ToUpperInvariant();
    return o;
}

app.MapGet("/", () => Results.Json(new { service = "bigpipe-schema-registry", credit = "Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil" }));

app.MapGet("/schemas/types", () => new[] { "AVRO", "JSON", "PROTOBUF" });

app.MapGet("/schemas/ids/{id:int}", (int id, RegistryStore s) =>
{
    var (schema, fmt) = s.ById(id);
    return fmt == SchemaFormat.Avro ? Results.Json(new { schema }) : Results.Json(new { schema, schemaType = fmt.ToString().ToUpperInvariant() });
});

app.MapGet("/schemas/ids/{id:int}/versions", (int id, RegistryStore s) =>
    s.SubjectsForId(id).Select(x => new { subject = x.Subject, version = x.Version }));

app.MapGet("/subjects", (RegistryStore s) => s.Subjects());

app.MapGet("/subjects/{subject}/versions", (string subject, RegistryStore s) => s.Versions(subject).Select(v => v.Version));

app.MapGet("/subjects/{subject}/versions/{version}", (string subject, string version, RegistryStore s) => VersionJson(s.Version(subject, version)));

app.MapGet("/subjects/{subject}/versions/{version}/schema", (string subject, string version, RegistryStore s) =>
    Results.Text(s.Version(subject, version).Schema, "application/json"));

app.MapPost("/subjects/{subject}/versions", async (string subject, HttpRequest req, RegistryStore s, CancellationToken ct) =>
{
    var body = await Body(req);
    var schema = body["schema"]?.ToString() ?? throw new RegistryException(42201, "missing \"schema\"");
    var format = Compatibility.ParseFormat(body["schemaType"]?.ToString());
    return Results.Json(new { id = await s.RegisterAsync(subject, schema, format, ct) });
});

app.MapPost("/subjects/{subject}", async (string subject, HttpRequest req, RegistryStore s) =>
{
    var body = await Body(req);
    var schema = body["schema"]?.ToString() ?? throw new RegistryException(42201, "missing \"schema\"");
    var v = s.Lookup(subject, schema, Compatibility.ParseFormat(body["schemaType"]?.ToString()))
        ?? throw new RegistryException(40403, "Schema not found");
    return Results.Json(VersionJson(v));
});

app.MapDelete("/subjects/{subject}", async (string subject, RegistryStore s, CancellationToken ct) => await s.DeleteSubjectAsync(subject, ct));

app.MapDelete("/subjects/{subject}/versions/{version}", async (string subject, string version, RegistryStore s, CancellationToken ct) =>
    await s.DeleteVersionAsync(subject, version, ct));

app.MapPost("/compatibility/subjects/{subject}/versions/{version}", async (string subject, string version, HttpRequest req, RegistryStore s) =>
{
    var body = await Body(req);
    var schema = body["schema"]?.ToString() ?? throw new RegistryException(42201, "missing \"schema\"");
    var problems = s.CheckCompatibility(subject, schema, Compatibility.ParseFormat(body["schemaType"]?.ToString()), version);
    return Results.Json(new { is_compatible = problems.Count == 0, messages = problems });
});

app.MapGet("/config", (RegistryStore s) => new { compatibilityLevel = s.CompatibilityFor(null) });
app.MapPut("/config", async (HttpRequest req, RegistryStore s, CancellationToken ct) =>
{
    var level = (await Body(req))["compatibility"]?.ToString() ?? throw new RegistryException(42203, "missing \"compatibility\"");
    await s.SetCompatibilityAsync(null, level, ct);
    return new { compatibility = level.ToUpperInvariant() };
});
app.MapGet("/config/{subject}", (string subject, RegistryStore s) => new { compatibilityLevel = s.CompatibilityFor(subject) });
app.MapPut("/config/{subject}", async (string subject, HttpRequest req, RegistryStore s, CancellationToken ct) =>
{
    var level = (await Body(req))["compatibility"]?.ToString() ?? throw new RegistryException(42203, "missing \"compatibility\"");
    await s.SetCompatibilityAsync(subject, level, ct);
    return new { compatibility = level.ToUpperInvariant() };
});

app.Run();

public partial class Program;
