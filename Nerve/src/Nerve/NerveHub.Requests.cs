// Nerve - built by Gravicode Studios, led by Kang Fadhil.
namespace Nerve;

/// <summary>
/// Request/reply, built on top of publish/subscribe rather than beside it.
/// </summary>
/// <remarks>
/// A request is a normal message whose payload happens to be a
/// <see cref="NerveRequest{TRequest, TResponse}"/> carrying somewhere to put the answer. Nothing
/// here is a special case in the dispatch path: responders can be registered on wildcards, requests
/// show up in the statistics, and a request topic can be observed by an ordinary subscriber.
/// </remarks>
public sealed partial class NerveHub
{
    /// <summary>Registers a responder.</summary>
    /// <typeparam name="TRequest">The request payload type.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="responder">Produces the answer. Exceptions surface at the call site.</param>
    /// <returns>Dispose it to stop responding.</returns>
    public IDisposable Respond<TRequest, TResponse>(
        string topicFilter, Func<TRequest, CancellationToken, ValueTask<TResponse>> responder)
    {
        ArgumentNullException.ThrowIfNull(responder);

        return Subscribe<NerveRequest<TRequest, TResponse>>(topicFilter, async request =>
        {
            try
            {
                TResponse response = await responder(request.Payload, request.CancellationToken)
                    .ConfigureAwait(false);
                request.Reply(response);
            }
            catch (Exception ex)
            {
                request.Fail(ex);
            }
        });
    }

    /// <summary>Registers a synchronous responder.</summary>
    /// <typeparam name="TRequest">The request payload type.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="topicFilter">A topic, or a filter using <c>+</c> and <c>#</c>.</param>
    /// <param name="responder">Produces the answer. Exceptions surface at the call site.</param>
    /// <returns>Dispose it to stop responding.</returns>
    public IDisposable Respond<TRequest, TResponse>(string topicFilter, Func<TRequest, TResponse> responder)
    {
        ArgumentNullException.ThrowIfNull(responder);

        return Subscribe<NerveRequest<TRequest, TResponse>>(topicFilter, request =>
        {
            try
            {
                request.Reply(responder(request.Payload));
            }
            catch (Exception ex)
            {
                request.Fail(ex);
            }
        });
    }

    /// <summary>Sends a request and waits for the first answer.</summary>
    /// <typeparam name="TRequest">The request payload type.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="topic">A concrete topic. Wildcards are not allowed here.</param>
    /// <param name="request">The payload.</param>
    /// <param name="timeout">How long to wait. Defaults to
    /// <see cref="NerveOptions.DefaultRequestTimeout"/>.</param>
    /// <param name="cancellationToken">Abandons the wait, and is handed to the responder.</param>
    /// <returns>The response.</returns>
    /// <exception cref="NerveNoResponderException">Nothing is registered to answer on this topic.</exception>
    /// <exception cref="TimeoutException">No answer arrived in time.</exception>
    public async Task<TResponse> RequestAsync<TRequest, TResponse>(
        string topic,
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Checked up front. Waiting out the full timeout only to discover the responder was never
        // registered is the most expensive way to find a wiring mistake.
        if (!HasSubscribers<NerveRequest<TRequest, TResponse>>(topic))
            throw new NerveNoResponderException(topic, typeof(TRequest), typeof(TResponse));

        var envelope = new NerveRequest<TRequest, TResponse>(request, cancellationToken);
        await PublishAsync(topic, envelope, cancellationToken).ConfigureAwait(false);

        TimeSpan wait = timeout ?? DefaultRequestTimeout;
        if (wait == Timeout.InfiniteTimeSpan)
            return await envelope.Completion.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            return await envelope.Completion.WaitAsync(wait, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new TimeoutException(
                $"No reply on '{topic}' for {typeof(TRequest).Name} -> {typeof(TResponse).Name} within {wait}.");
        }
    }
}
