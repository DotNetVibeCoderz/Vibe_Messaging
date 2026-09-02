// SocketSignal - built by Gravicode Studios, led by Kang Fadhil.
using System.Text.Json;

namespace SocketSignal.Hosting;

/// <summary>
/// A call this peer issued and is still waiting on. The connection completes it straight from the
/// receive buffer, so a typed call never builds a <see cref="JsonDocument"/> it does not need.
/// </summary>
internal interface IPendingCall
{
    /// <summary>Completes the call by deserialising the raw <c>result</c> JSON.</summary>
    void Complete(ReadOnlySpan<byte> resultJson, JsonSerializerOptions options);

    /// <summary>Fails the call because the remote handler threw.</summary>
    void Fail(string remoteMessage);

    /// <summary>Fails the call because the connection went away underneath it.</summary>
    void Abort(string reason);
}

internal sealed class PendingCall<TResult>(string method) : IPendingCall
{
    private readonly TaskCompletionSource<TResult?> _completion =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task<TResult?> Task => _completion.Task;

    public void Complete(ReadOnlySpan<byte> resultJson, JsonSerializerOptions options)
    {
        try
        {
            if (resultJson.IsEmpty)
            {
                _completion.TrySetResult(default);
                return;
            }

            var reader = new Utf8JsonReader(resultJson);
            _completion.TrySetResult(JsonSerializer.Deserialize<TResult>(ref reader, options));
        }
        catch (Exception ex)
        {
            _completion.TrySetException(
                new SocketSignalException($"Could not read the result of '{method}' as {typeof(TResult).Name}.", ex));
        }
    }

    public void Fail(string remoteMessage)
    {
        Exception error = remoteMessage.EndsWith("not found", StringComparison.Ordinal)
            ? new MethodNotFoundException(method)
            : new SignalInvocationException(method, remoteMessage);
        _completion.TrySetException(error);
    }

    public void Abort(string reason) =>
        _completion.TrySetException(new SignalConnectionClosedException(reason));
}
