using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using StackExchange.Redis;
using TransportR.Hub;

namespace TransportR.Storage.Redis;

public static class DataHubRedisExtensions
{
    /// <summary>
    /// Configures TransportR to use Redis for state storage (Production).
    /// </summary>
    /// <param name="builder">The DataHub builder</param>
    /// <param name="connectionString">Redis connection string</param>
    public static DataHubBuilder UseRedisStore(this DataHubBuilder builder, string connectionString)
    {
        // Register StackExchange.Redis ConnectionMultiplexer
        builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
            ConnectionMultiplexer.Connect(connectionString));

        // Replaces the in-memory store AddDataHub registered, so a hub never holds two.
        builder.Services.Replace(ServiceDescriptor.Singleton<IStateStore, RedisStateStore>());

        return builder;
    }
}
