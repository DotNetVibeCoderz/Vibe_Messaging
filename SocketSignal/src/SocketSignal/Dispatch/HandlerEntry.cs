// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Text.Json;

namespace SocketSignal.Dispatch;

/// <summary>
/// One registered method. Arguments are decoded synchronously, straight off the frame buffer,
/// before the user delegate is started - which is why the buffer can be recycled the moment
/// <see cref="InvokeAsync"/> returns its task.
/// </summary>
internal abstract class HandlerEntry
{
    public abstract ValueTask<object?> InvokeAsync(object? sender, ReadOnlySpan<byte> argsJson, JsonSerializerOptions options);
}

/// <summary>Reads positional arguments out of the raw <c>args</c> array.</summary>
internal static class ArgReader
{
    /// <summary>Static empty list, so a frame with no <c>args</c> still yields a usable reader.</summary>
    private static ReadOnlySpan<byte> EmptyArgs => "[]"u8;

    /// <summary>Positions a reader just inside the <c>args</c> array. Handles a missing or null array.</summary>
    public static Utf8JsonReader Open(ReadOnlySpan<byte> argsJson)
    {
        var reader = new Utf8JsonReader(argsJson);
        if (reader.Read() && reader.TokenType == JsonTokenType.StartArray)
            return reader;

        var empty = new Utf8JsonReader(EmptyArgs);
        empty.Read();
        return empty;
    }

    /// <summary>The next argument, or <c>default</c> when the caller passed fewer than the handler wants.</summary>
    public static T? Next<T>(ref Utf8JsonReader reader, JsonSerializerOptions options)
    {
        if (!reader.Read() || reader.TokenType == JsonTokenType.EndArray)
            return default;
        return JsonSerializer.Deserialize<T>(ref reader, options);
    }

    /// <summary>The whole argument list as <see cref="JsonElement"/>s, for the untyped handler shape.</summary>
    public static JsonElement[] ToElements(ReadOnlySpan<byte> argsJson)
    {
        if (argsJson.IsEmpty) return [];

        var reader = new Utf8JsonReader(argsJson);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartArray)
            return [];

        var list = new List<JsonElement>(4);
        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            list.Add(JsonElement.ParseValue(ref reader));
        return [.. list];
    }
}

// ---------------------------------------------------------------------------------------------
// The untyped shape. Compatible with the v1 API: every argument arrives as a JsonElement.
// ---------------------------------------------------------------------------------------------

internal sealed class DynamicHandler(Func<object?, JsonElement[], Task<object?>> handler) : HandlerEntry
{
    public override ValueTask<object?> InvokeAsync(object? sender, ReadOnlySpan<byte> argsJson, JsonSerializerOptions options)
        => new(handler(sender, ArgReader.ToElements(argsJson)));
}

// ---------------------------------------------------------------------------------------------
// Typed shapes. Arguments deserialise straight into their target type, so an int argument costs
// nothing and a record argument costs exactly one object.
// ---------------------------------------------------------------------------------------------

internal sealed class TypedHandler<TResult>(Func<object?, ValueTask<TResult>> handler) : HandlerEntry
{
    public override ValueTask<object?> InvokeAsync(object? sender, ReadOnlySpan<byte> argsJson, JsonSerializerOptions options)
        => Await(handler(sender));

    private static async ValueTask<object?> Await(ValueTask<TResult> task) => await task.ConfigureAwait(false);
}

internal sealed class TypedHandler<T1, TResult>(Func<object?, T1?, ValueTask<TResult>> handler) : HandlerEntry
{
    public override ValueTask<object?> InvokeAsync(object? sender, ReadOnlySpan<byte> argsJson, JsonSerializerOptions options)
    {
        var reader = ArgReader.Open(argsJson);
        T1? a1 = ArgReader.Next<T1>(ref reader, options);
        return Await(handler(sender, a1));
    }

    private static async ValueTask<object?> Await(ValueTask<TResult> task) => await task.ConfigureAwait(false);
}

internal sealed class TypedHandler<T1, T2, TResult>(Func<object?, T1?, T2?, ValueTask<TResult>> handler) : HandlerEntry
{
    public override ValueTask<object?> InvokeAsync(object? sender, ReadOnlySpan<byte> argsJson, JsonSerializerOptions options)
    {
        var reader = ArgReader.Open(argsJson);
        T1? a1 = ArgReader.Next<T1>(ref reader, options);
        T2? a2 = ArgReader.Next<T2>(ref reader, options);
        return Await(handler(sender, a1, a2));
    }

    private static async ValueTask<object?> Await(ValueTask<TResult> task) => await task.ConfigureAwait(false);
}

internal sealed class TypedHandler<T1, T2, T3, TResult>(Func<object?, T1?, T2?, T3?, ValueTask<TResult>> handler) : HandlerEntry
{
    public override ValueTask<object?> InvokeAsync(object? sender, ReadOnlySpan<byte> argsJson, JsonSerializerOptions options)
    {
        var reader = ArgReader.Open(argsJson);
        T1? a1 = ArgReader.Next<T1>(ref reader, options);
        T2? a2 = ArgReader.Next<T2>(ref reader, options);
        T3? a3 = ArgReader.Next<T3>(ref reader, options);
        return Await(handler(sender, a1, a2, a3));
    }

    private static async ValueTask<object?> Await(ValueTask<TResult> task) => await task.ConfigureAwait(false);
}
