using BigPipe.Client;
using BigPipe.Client.Admin;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Shared BigPipe settings for DI registrations.</summary>
public sealed class BigPipeOptions
{
    public string Bootstrap { get; set; } = "localhost:9092";
    public string AdminUrl { get; set; } = "http://localhost:9644";
    public string HttpUrl { get; set; } = "http://localhost:8082";
    public string? ApiKey { get; set; }
    public string ClientId { get; set; } = "bigpipe-dotnet";
}

/// <summary>Context passed to <see cref="IBigPipeHandler{TKey,TValue}"/>.</summary>
public sealed class ConsumeContext<TKey, TValue>
{
    public required ConsumeResult<TKey, TValue> Result { get; init; }
    public TKey Key => Result.Key;
    public TValue Value => Result.Value;
    public string Topic => Result.Topic;
    public int Partition => Result.Partition;
    public long Offset => Result.Offset;
}

/// <summary>Handles records for a hosted consumer. The offset is committed after the handler succeeds.</summary>
public interface IBigPipeHandler<TKey, TValue>
{
    Task HandleAsync(ConsumeContext<TKey, TValue> context, CancellationToken ct);
}

public sealed class BigPipeConsumerRegistration
{
    internal List<string> Topics { get; } = [];
    internal string? GroupId { get; private set; }
    internal OffsetReset Reset { get; private set; } = OffsetReset.Earliest;

    public BigPipeConsumerRegistration Topic(params string[] topics)
    {
        Topics.AddRange(topics);
        return this;
    }

    public BigPipeConsumerRegistration Group(string groupId)
    {
        GroupId = groupId;
        return this;
    }

    public BigPipeConsumerRegistration FromLatest()
    {
        Reset = OffsetReset.Latest;
        return this;
    }
}

/// <summary>Fluent registration returned by <c>AddBigPipe</c>.</summary>
public sealed class BigPipeBuilder(IServiceCollection services)
{
    public IServiceCollection Services { get; } = services;

    /// <summary>Registers a singleton <see cref="Producer{TKey,TValue}"/>.</summary>
    public BigPipeBuilder AddProducer<TKey, TValue>(Action<ProducerConfig>? configure = null)
    {
        Services.AddSingleton(sp =>
        {
            var o = sp.GetRequiredService<IOptions<BigPipeOptions>>().Value;
            var cfg = new ProducerConfig { Bootstrap = o.Bootstrap, ClientId = o.ClientId };
            configure?.Invoke(cfg);
            return new ProducerBuilder<TKey, TValue>(cfg).WithLogger(sp.GetRequiredService<ILoggerFactory>().CreateLogger("BigPipe.Producer")).Build();
        });
        return this;
    }

    /// <summary>Runs <typeparamref name="THandler"/> as a hosted consumer.</summary>
    public BigPipeBuilder AddConsumer<THandler, TKey, TValue>(Action<BigPipeConsumerRegistration> configure)
        where THandler : class, IBigPipeHandler<TKey, TValue>
    {
        var reg = new BigPipeConsumerRegistration();
        configure(reg);
        if (reg.Topics.Count == 0) throw new ArgumentException("the consumer needs at least one topic");
        Services.AddScoped<THandler>();
        Services.AddSingleton<IHostedService>(sp => new BigPipeHostedConsumer<THandler, TKey, TValue>(sp, reg));
        return this;
    }
}

internal sealed class BigPipeHostedConsumer<THandler, TKey, TValue>(IServiceProvider sp, BigPipeConsumerRegistration reg) : BackgroundService
    where THandler : class, IBigPipeHandler<TKey, TValue>
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var o = sp.GetRequiredService<IOptions<BigPipeOptions>>().Value;
        var log = sp.GetRequiredService<ILoggerFactory>().CreateLogger(typeof(THandler));
        await using var consumer = new ConsumerBuilder<TKey, TValue>(new ConsumerConfig
        {
            Bootstrap = o.Bootstrap,
            ClientId = o.ClientId,
            GroupId = reg.GroupId ?? typeof(THandler).Name,
            AutoOffsetReset = reg.Reset,
            EnableAutoCommit = false,
        }).WithLogger(log).Build();
        consumer.Subscribe(reg.Topics);
        await foreach (var msg in consumer.ConsumeAsync(stoppingToken))
        {
            using var scope = sp.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<THandler>();
            try
            {
                await handler.HandleAsync(new ConsumeContext<TKey, TValue> { Result = msg }, stoppingToken);
                await consumer.CommitAsync(msg, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                log.LogError(e, "handler failed for {Topic}-{Partition}@{Offset}", msg.Topic, msg.Partition, msg.Offset);
            }
        }
    }
}

public static class BigPipeServiceCollectionExtensions
{
    /// <summary>
    /// Adds BigPipe: options, <see cref="BigPipeAdminClient"/> and <see cref="BigPipeHttpClient"/>.
    /// Chain <c>AddProducer</c> / <c>AddConsumer</c> for data-plane clients.
    /// </summary>
    public static BigPipeBuilder AddBigPipe(this IServiceCollection services, Action<BigPipeOptions>? configure = null)
    {
        services.AddOptions<BigPipeOptions>().Configure(o => configure?.Invoke(o));
        services.AddSingleton(sp =>
        {
            var o = sp.GetRequiredService<IOptions<BigPipeOptions>>().Value;
            return new BigPipeAdminClient(o.AdminUrl, o.ApiKey);
        });
        services.AddSingleton(sp => new BigPipeHttpClient(sp.GetRequiredService<IOptions<BigPipeOptions>>().Value.HttpUrl));
        return new BigPipeBuilder(services);
    }
}
