using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using StackExchange.Redis;
using TransportR.Hub;

namespace TransportR.Storage.Redis;

public static class DataHubRedisExtensions
{
    /// <summary>
    /// Configures TransportR to use Redis for state storage, creating an <see cref="IConnectionMultiplexer"/>
    /// from <paramref name="connectionString"/>.
    /// </summary>
    public static DataHubBuilder UseRedisStore(
        this DataHubBuilder builder,
        string connectionString,
        Action<RedisStateStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Replace(ServiceDescriptor.Singleton<IConnectionMultiplexer>(_ =>
            ConnectionMultiplexer.Connect(connectionString)));

        return RegisterRedisStore(builder, configure);
    }

    /// <summary>
    /// Configures TransportR to use Redis for state storage, creating an <see cref="IConnectionMultiplexer"/>
    /// from <paramref name="configurationOptions"/>.
    /// </summary>
    public static DataHubBuilder UseRedisStore(
        this DataHubBuilder builder,
        ConfigurationOptions configurationOptions,
        Action<RedisStateStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configurationOptions);

        builder.Services.Replace(ServiceDescriptor.Singleton<IConnectionMultiplexer>(_ =>
            ConnectionMultiplexer.Connect(configurationOptions)));

        return RegisterRedisStore(builder, configure);
    }

    /// <summary>
    /// Configures TransportR to use Redis for state storage using the provided <paramref name="connectionMultiplexer"/>.
    /// </summary>
    public static DataHubBuilder UseRedisStore(
        this DataHubBuilder builder,
        IConnectionMultiplexer connectionMultiplexer,
        Action<RedisStateStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(connectionMultiplexer);

        builder.Services.Replace(ServiceDescriptor.Singleton(connectionMultiplexer));

        return RegisterRedisStore(builder, configure);
    }

    /// <summary>
    /// Configures TransportR to use Redis for state storage using the <see cref="IConnectionMultiplexer"/>
    /// already registered in the dependency injection container.
    /// </summary>
    public static DataHubBuilder UseRedisStore(
        this DataHubBuilder builder,
        Action<RedisStateStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return RegisterRedisStore(builder, configure);
    }

    private static DataHubBuilder RegisterRedisStore(
        DataHubBuilder builder,
        Action<RedisStateStoreOptions>? configure)
    {
        var optionsBuilder = builder.Services.AddOptions<RedisStateStoreOptions>()
            .Validate(o => o.KeyPrefix is not null, "RedisStateStoreOptions.KeyPrefix cannot be null.")
            .ValidateOnStart();

        if (configure != null)
        {
            optionsBuilder.Configure(configure);
        }

        // Replaces the in-memory store AddDataHub registered, so a hub never holds two.
        builder.Services.Replace(ServiceDescriptor.Singleton<IStateStore, RedisStateStore>());

        return builder;
    }
}
