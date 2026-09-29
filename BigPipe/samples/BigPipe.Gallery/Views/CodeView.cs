using System.Text.RegularExpressions;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.Documents;
using Avalonia.Media;

namespace BigPipe.Gallery.Views;

/// <summary>Read-only code block with lightweight syntax colouring (keywords, strings, comments, numbers).</summary>
public sealed partial class CodeView : SelectableTextBlock
{
    public static readonly StyledProperty<string?> CodeProperty = AvaloniaProperty.Register<CodeView, string?>(nameof(Code));
    public static readonly StyledProperty<string?> LanguageProperty = AvaloniaProperty.Register<CodeView, string?>(nameof(Language));

    public string? Code
    {
        get => GetValue(CodeProperty);
        set => SetValue(CodeProperty, value);
    }

    public string? Language
    {
        get => GetValue(LanguageProperty);
        set => SetValue(LanguageProperty, value);
    }

    protected override Type StyleKeyOverride => typeof(SelectableTextBlock);

    private static readonly HashSet<string> Keywords =
    [
        "var", "new", "await", "async", "using", "foreach", "for", "in", "if", "else", "return", "public", "record", "class", "try", "catch",
        "true", "false", "null", "const", "let", "function", "import", "from", "def", "for", "with", "and", "or", "not", "func", "go",
        "defer", "range", "package", "type", "struct", "string", "int", "long", "decimal", "double", "bool", "float", "static", "void",
        "while", "break", "yield", "of", "export", "interface", "sealed", "this", "root", "deleted", "apiVersion", "kind", "metadata", "spec",
    ];

    [GeneratedRegex("""(?<comment>//[^\n]*|#[^\n]*)|(?<string>\$?@?"(?:[^"\\\n]|\\.)*"|'(?:[^'\\\n]|\\.)*'|`[^`]*`)|(?<number>\b\d[\d_.]*\b)|(?<word>\b[A-Za-z_][A-Za-z0-9_]*\b)""")]
    private static partial Regex Tokens();

    protected override void OnPropertyChanged(AvaloniaPropertyChangedEventArgs change)
    {
        base.OnPropertyChanged(change);
        if (change.Property == CodeProperty || change.Property == LanguageProperty) Render();
    }

    private IBrush Brush(string key, string fallback) =>
        this.TryFindResource(key, ActualThemeVariant, out var r) && r is IBrush b ? b : new SolidColorBrush(Color.Parse(fallback));

    private void Render()
    {
        var inlines = new InlineCollection();
        var code = (Code ?? "").Replace("\r\n", "\n");
        var comment = Brush("CodeComment", "#7F918B");
        var str = Brush("CodeString", "#2A8CA6");
        var kw = Brush("CodeKeyword", "#C0662F");
        var num = Brush("CodeNumber", "#8A5CB8");
        var type = Brush("CodeType", "#3F8A5A");
        var shellLike = Language is "bpctl" or "curl";
        var pos = 0;
        foreach (Match m in Tokens().Matches(code))
        {
            if (m.Index > pos) inlines.Add(new Run(code[pos..m.Index]));
            var text = m.Value;
            var run = new Run(text);
            if (m.Groups["comment"].Success && !(shellLike && text.StartsWith('#') == false)) run.Foreground = comment;
            else if (m.Groups["string"].Success) run.Foreground = str;
            else if (m.Groups["number"].Success) run.Foreground = num;
            else if (Keywords.Contains(text)) run.Foreground = kw;
            else if (char.IsUpper(text[0])) run.Foreground = type;
            inlines.Add(run);
            pos = m.Index + m.Length;
        }
        if (pos < code.Length) inlines.Add(new Run(code[pos..]));
        Inlines = inlines;
    }
}
