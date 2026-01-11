using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using TransportR.Hub.SignalR;
using TransportR.Interfaces;

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

        // Register RedisStateStore
        builder.Services.AddSingleton<IStateStore, RedisStateStore>();

        return builder;
    }
}
