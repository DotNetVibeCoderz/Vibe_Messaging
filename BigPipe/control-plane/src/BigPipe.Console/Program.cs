// BigPipe Console — web UI for the BigPipe streaming platform.
// Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
using BigPipe.Console.Components;
using BigPipe.Console.Services;

var builder = WebApplication.CreateBuilder(args);
var options = builder.Configuration.GetSection("BigPipe").Get<ConsoleOptions>() ?? new ConsoleOptions();
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<ClusterState>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<ClusterState>());
builder.Services.AddHttpClient();
builder.Services.AddRazorComponents().AddInteractiveServerComponents();
builder.WebHost.UseUrls(builder.Configuration["Urls"] ?? "http://0.0.0.0:8080");
// Serve framework/static web assets when running from the build output in any environment.
builder.WebHost.UseStaticWebAssets();

var app = builder.Build();
if (!app.Environment.IsDevelopment())
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
app.UseStatusCodePagesWithReExecute("/not-found", createScopeForStatusCodePages: true);
app.UseAntiforgery();
app.MapStaticAssets();

// Preference switches: persisted in cookies, applied on the next render.
static IResult SetCookie(HttpContext ctx, string name, string value, string? returnUrl)
{
    ctx.Response.Cookies.Append(name, value, new CookieOptions { MaxAge = TimeSpan.FromDays(365), SameSite = SameSiteMode.Lax });
    return Results.LocalRedirect(string.IsNullOrEmpty(returnUrl) || !returnUrl.StartsWith('/') ? "/" : returnUrl);
}
app.MapGet("/prefs/lang/{code}", (HttpContext ctx, string code, string? returnUrl) => SetCookie(ctx, "bp_lang", code == "id" ? "id" : "en", returnUrl));
app.MapGet("/prefs/theme/{mode}", (HttpContext ctx, string mode, string? returnUrl) =>
    SetCookie(ctx, "bp_theme", mode is "dark" or "light" ? mode : "auto", returnUrl));

app.MapRazorComponents<App>().AddInteractiveServerRenderMode();
app.Run();
