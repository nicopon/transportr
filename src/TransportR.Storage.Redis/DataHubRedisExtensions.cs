using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
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

        return RegisterRedisStore(builder, configure, sp =>
            CreateOwnedStore(sp, () => ConnectionMultiplexer.Connect(connectionString)));
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

        return RegisterRedisStore(builder, configure, sp =>
            CreateOwnedStore(sp, () => ConnectionMultiplexer.Connect(configurationOptions)));
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

        return RegisterRedisStore(builder, configure, sp =>
            CreateStore(sp, connectionMultiplexer, ownsConnection: false));
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

        return RegisterRedisStore(builder, configure, sp =>
            CreateStore(sp, sp.GetRequiredService<IConnectionMultiplexer>(), ownsConnection: false));
    }

    private static DataHubBuilder RegisterRedisStore(
        DataHubBuilder builder,
        Action<RedisStateStoreOptions>? configure,
        Func<IServiceProvider, RedisStateStore> factory)
    {
        var optionsBuilder = builder.Services.AddOptions<RedisStateStoreOptions>()
            .Validate(o => o.KeyPrefix is not null, "RedisStateStoreOptions.KeyPrefix cannot be null.")
            .ValidateOnStart();

        if (configure != null)
        {
            optionsBuilder.Configure(configure);
        }

        // Replaces the in-memory store AddDataHub registered, so a hub never holds two.
        builder.Services.Replace(ServiceDescriptor.Singleton<IStateStore>(factory));

        return builder;
    }

    private static RedisStateStore CreateOwnedStore(
        IServiceProvider sp,
        Func<IConnectionMultiplexer> connect)
    {
        var registryConfig = sp.GetService<IOptions<ClientRegistryConfiguration>>();
        _ = registryConfig?.Value;
        var timeProvider = sp.GetService<TimeProvider>();
        var storeOptions = sp.GetService<IOptions<RedisStateStoreOptions>>();
        if (storeOptions?.Value is { KeyPrefix: null })
        {
            throw new ArgumentException("RedisStateStoreOptions.KeyPrefix cannot be null.", nameof(storeOptions));
        }

        var multiplexer = connect();
        try
        {
            return new RedisStateStore(multiplexer, ownsConnection: true, registryConfig, timeProvider, storeOptions);
        }
        catch
        {
            multiplexer.Dispose();
            throw;
        }
    }

    private static RedisStateStore CreateStore(
        IServiceProvider sp,
        IConnectionMultiplexer multiplexer,
        bool ownsConnection) =>
        new(
            multiplexer,
            ownsConnection,
            sp.GetService<IOptions<ClientRegistryConfiguration>>(),
            sp.GetService<TimeProvider>(),
            sp.GetService<IOptions<RedisStateStoreOptions>>());
}
