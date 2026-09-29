using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using Jint;
using Jint.Native;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Microsoft.Scripting.Hosting;

namespace BigPipe.Analytics.Scripting;

public enum ScriptLanguage
{
    CSharp,
    Python,
    JavaScript,
}

/// <summary>
/// Compiles user scripts against stream records.
/// <list type="bullet">
/// <item><b>Filter</b>: an expression returning a boolean, e.g. <c>r.num("amount") &gt; 1000</c>.</item>
/// <item><b>Projection</b>: an expression returning a value.</item>
/// <item><b>Query</b>: a script over a window (<c>records</c>). C#/JS return the last expression; Python assigns <c>result</c>.</item>
/// </list>
/// Scripts see <c>r</c> (a <see cref="ScriptRecord"/>) with <c>key</c>, <c>value</c>, <c>json</c>, <c>headers</c>,
/// <c>topic</c>, <c>partition</c>, <c>offset</c>, <c>timestamp</c>, <c>num(path)</c>, <c>str(path)</c>, <c>header(name)</c>.
/// </summary>
public interface IScriptEngine : IDisposable
{
    ScriptLanguage Language { get; }
    Func<AnalyticsRecord, bool> CompileFilter(string expression);
    Func<AnalyticsRecord, object?> CompileProjection(string expression);
    object? Query(string script, IReadOnlyList<AnalyticsRecord> records);
}

/// <summary>Script-facing record: lowercase members so scripts read naturally in every language.</summary>
public sealed class ScriptRecord(AnalyticsRecord record)
{
    private object? _json;
    private bool _converted;

    public AnalyticsRecord Record { get; } = record;
#pragma warning disable IDE1006 // script-facing names
    public string? key => Record.Key;
    public string? value => Record.Value;
    public string topic => Record.Topic;
    public int partition => Record.Partition;
    public long offset => Record.Offset;
    public long timestamp => Record.Timestamp.ToUnixTimeMilliseconds();
    public IReadOnlyDictionary<string, string?> headers => Record.Headers;

    /// <summary>The JSON value as plain dictionaries/lists/numbers/strings.</summary>
    public object? json
    {
        get
        {
            if (!_converted)
            {
                _json = AnalyticsRecord.ToPlain(Record.Json);
                _converted = true;
            }
            return _json;
        }
    }

    public double num(string path) => Record.Num(path);
    public string? str(string path) => Record.Str(path);
    public bool flag(string path) => Record.Bool(path);
    public string? header(string name) => Record.Header(name);
#pragma warning restore IDE1006
}

public static class ScriptEngines
{
    public static IScriptEngine Create(ScriptLanguage language) => language switch
    {
        ScriptLanguage.CSharp => new CSharpScriptEngine(),
        ScriptLanguage.Python => new PythonScriptEngine(),
        ScriptLanguage.JavaScript => new JavaScriptEngine(),
        _ => throw new ArgumentOutOfRangeException(nameof(language)),
    };

    public static ScriptLanguage Parse(string name) => name.Trim().ToLowerInvariant() switch
    {
        "c#" or "cs" or "csharp" => ScriptLanguage.CSharp,
        "py" or "python" => ScriptLanguage.Python,
        "js" or "javascript" => ScriptLanguage.JavaScript,
        _ => throw new ArgumentException($"unknown script language '{name}' (use csharp, python or javascript)"),
    };
}

// ------------------------------------------------------------------------------ C# ------------

/// <summary>Globals for C# record scripts.</summary>
public sealed class RecordGlobals
{
#pragma warning disable IDE1006
    public required ScriptRecord r { get; init; }
    public JsonNode? json => r.Record.Json;
#pragma warning restore IDE1006
}

/// <summary>Globals for C# window queries.</summary>
public sealed class QueryGlobals
{
#pragma warning disable IDE1006
    public required IReadOnlyList<ScriptRecord> records { get; init; }
#pragma warning restore IDE1006
}

internal sealed class CSharpScriptEngine : IScriptEngine
{
    private static readonly ScriptOptions Options = ScriptOptions.Default
        .WithReferences(typeof(AnalyticsRecord).Assembly, typeof(JsonNode).Assembly, typeof(Enumerable).Assembly, typeof(ScriptRecord).Assembly)
        .WithImports("System", "System.Linq", "System.Collections.Generic", "System.Text.Json.Nodes", "BigPipe.Analytics", "BigPipe.Analytics.Scripting");

    private readonly ConcurrentDictionary<string, ScriptRunner<object?>> _queries = new();

    public ScriptLanguage Language => ScriptLanguage.CSharp;

    private static ScriptRunner<T> Compile<T>(string code, Type globals)
    {
        var script = CSharpScript.Create<T>(code, Options, globals);
        var diagnostics = script.Compile().Where(d => d.Severity == Microsoft.CodeAnalysis.DiagnosticSeverity.Error).ToList();
        if (diagnostics.Count > 0)
            throw new ScriptException(ScriptLanguage.CSharp, code, string.Join("; ", diagnostics.Select(d => d.GetMessage())));
        return script.CreateDelegate();
    }

    public Func<AnalyticsRecord, bool> CompileFilter(string expression)
    {
        var run = Compile<bool>(expression, typeof(RecordGlobals));
        return rec => run(new RecordGlobals { r = new ScriptRecord(rec) }).GetAwaiter().GetResult();
    }

    public Func<AnalyticsRecord, object?> CompileProjection(string expression)
    {
        var run = Compile<object?>(expression, typeof(RecordGlobals));
        return rec => run(new RecordGlobals { r = new ScriptRecord(rec) }).GetAwaiter().GetResult();
    }

    public object? Query(string script, IReadOnlyList<AnalyticsRecord> records)
    {
        var run = _queries.GetOrAdd(script, s => Compile<object?>(s, typeof(QueryGlobals)));
        return run(new QueryGlobals { records = records.Select(x => new ScriptRecord(x)).ToList() }).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
    }
}

// ------------------------------------------------------------------------------ Python --------

internal sealed class PythonScriptEngine : IScriptEngine
{
    private readonly ScriptEngine _engine = IronPython.Hosting.Python.CreateEngine();
    private readonly object _lock = new();

    public ScriptLanguage Language => ScriptLanguage.Python;

    private CompiledCode Compile(string code, Microsoft.Scripting.SourceCodeKind kind)
    {
        try
        {
            return _engine.CreateScriptSourceFromString(code, kind).Compile();
        }
        catch (Exception e)
        {
            throw new ScriptException(ScriptLanguage.Python, code, e.Message);
        }
    }

    private object? Eval(CompiledCode code, AnalyticsRecord rec)
    {
        lock (_lock)
        {
            var scope = _engine.CreateScope();
            scope.SetVariable("r", new ScriptRecord(rec));
            return code.Execute(scope);
        }
    }

    public Func<AnalyticsRecord, bool> CompileFilter(string expression)
    {
        var code = Compile(expression, Microsoft.Scripting.SourceCodeKind.Expression);
        return rec =>
        {
            var v = Eval(code, rec);
            return v is bool b ? b : Truthy(v);
        };
    }

    private static bool Truthy(object? o) => o switch
    {
        null => false,
        bool b => b,
        int i => i != 0,
        double d => d != 0,
        string s => s.Length > 0,
        _ => true,
    };

    public Func<AnalyticsRecord, object?> CompileProjection(string expression)
    {
        var code = Compile(expression, Microsoft.Scripting.SourceCodeKind.Expression);
        return rec => Eval(code, rec);
    }

    public object? Query(string script, IReadOnlyList<AnalyticsRecord> records)
    {
        var code = Compile(script, Microsoft.Scripting.SourceCodeKind.Statements);
        lock (_lock)
        {
            var scope = _engine.CreateScope();
            scope.SetVariable("records", records.Select(x => new ScriptRecord(x)).ToList());
            code.Execute(scope);
            return scope.TryGetVariable("result", out object? result) ? result : null;
        }
    }

    public void Dispose() => _engine.Runtime.Shutdown();
}

// ------------------------------------------------------------------------------ JavaScript ----

internal sealed class JavaScriptEngine : IScriptEngine
{
    private readonly Engine _engine = new(o => o.LimitRecursion(256).TimeoutInterval(TimeSpan.FromSeconds(5)));
    private readonly object _lock = new();
    private int _fn;

    public ScriptLanguage Language => ScriptLanguage.JavaScript;

    private string Define(string body, string param)
    {
        var name = $"__bp_{Interlocked.Increment(ref _fn)}";
        try
        {
            lock (_lock) _engine.Execute($"function {name}({param}) {{ {body} }}");
        }
        catch (Exception e)
        {
            throw new ScriptException(ScriptLanguage.JavaScript, body, e.Message);
        }
        return name;
    }

    private JsValue Call(string fn, object arg)
    {
        lock (_lock) return _engine.Invoke(fn, arg);
    }

    public Func<AnalyticsRecord, bool> CompileFilter(string expression)
    {
        var fn = Define($"return !!({expression});", "r");
        return rec => Call(fn, new ScriptRecord(rec)).AsBoolean();
    }

    public Func<AnalyticsRecord, object?> CompileProjection(string expression)
    {
        var fn = Define($"return ({expression});", "r");
        return rec => Call(fn, new ScriptRecord(rec)).ToObject();
    }

    public object? Query(string script, IReadOnlyList<AnalyticsRecord> records)
    {
        // Wrap so the value of the last expression statement is returned via eval semantics.
        var fn = Define($"return eval({System.Text.Json.JsonSerializer.Serialize(script)});", "records");
        return Call(fn, records.Select(x => new ScriptRecord(x)).ToArray()).ToObject();
    }

    public void Dispose() => _engine.Dispose();
}

public sealed class ScriptException(ScriptLanguage language, string code, string message)
    : Exception($"{language} script failed to compile: {message}\n---\n{code}")
{
    public ScriptLanguage Language { get; } = language;
    public string Code { get; } = code;
}

/// <summary>Stream operators that use scripts.</summary>
public static class ScriptingStreamExtensions
{
    public static IAsyncEnumerable<AnalyticsRecord> WhereScript(this IAsyncEnumerable<AnalyticsRecord> source, IScriptEngine engine, string filter,
        CancellationToken ct = default) => source.WhereAsync(engine.CompileFilter(filter), ct);

    public static IAsyncEnumerable<object?> SelectScript(this IAsyncEnumerable<AnalyticsRecord> source, IScriptEngine engine, string projection,
        CancellationToken ct = default) => source.SelectAsync(engine.CompileProjection(projection), ct);

    /// <summary>Runs a query script over every window.</summary>
    public static IAsyncEnumerable<(Window<AnalyticsRecord> Window, object? Result)> QueryWindows(this IAsyncEnumerable<Window<AnalyticsRecord>> windows,
        IScriptEngine engine, string query, CancellationToken ct = default) =>
        windows.SelectAsync(w => (w, engine.Query(query, w.Items)), ct);
}
