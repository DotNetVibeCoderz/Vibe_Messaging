// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve;

/// <summary>
/// The envelope a request travels in. It is an ordinary message on an ordinary topic - which is why
/// request/reply gets wildcards, statistics and multiple listeners for free.
/// </summary>
/// <typeparam name="TRequest">The request payload type.</typeparam>
/// <typeparam name="TResponse">The response type.</typeparam>
/// <remarks>
/// Exactly one reply wins. If two responders are listening on the same topic, the first to answer
/// completes the caller and the second is ignored rather than throwing - a race between responders
/// is a wiring mistake, not a runtime failure the caller can do anything about.
/// </remarks>
public sealed class NerveRequest<TRequest, TResponse>
{
    private readonly TaskCompletionSource<TResponse> _completion =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal NerveRequest(TRequest payload, CancellationToken cancellationToken)
    {
        Payload = payload;
        CancellationToken = cancellationToken;
    }

    /// <summary>What the caller sent.</summary>
    public TRequest Payload { get; }

    /// <summary>The caller's token. A responder doing real work should honour it.</summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>True once someone has replied or failed this request.</summary>
    public bool IsAnswered => _completion.Task.IsCompleted;

    internal Task<TResponse> Completion => _completion.Task;

    /// <summary>Answers the caller.</summary>
    /// <param name="response">The response.</param>
    /// <returns><see langword="true"/> if this reply was the one that reached the caller.</returns>
    public bool Reply(TResponse response) => _completion.TrySetResult(response);

    /// <summary>Fails the caller's request.</summary>
    /// <param name="exception">The failure to surface at the call site.</param>
    /// <returns><see langword="true"/> if this failure was the one that reached the caller.</returns>
    public bool Fail(Exception exception) => _completion.TrySetException(exception);
}
