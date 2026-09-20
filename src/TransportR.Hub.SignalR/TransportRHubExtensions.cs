using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.AspNetCore.Routing;
using TransportR.Configuration;
using TransportR.Hub.SignalR.Controllers;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Hubs;
using TransportR.Hub.SignalR.Services;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Services;

namespace TransportR.Hub.SignalR;

/// <summary>
/// Builder for configuring TransportR data hub services.
/// </summary>
public class DataHubBuilder
{
    public IServiceCollection Services { get; }
    internal bool IsDevMode { get; private set; }
    internal DevIdentityOptions? DevOptions { get; private set; }

    internal DataHubBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Enables development mode which trusts client-provided identity values.
    /// </summary>
    public DataHubBuilder UseDevMode(Action<DevIdentityOptions>? configure = null)
    {
        IsDevMode = true;
        DevOptions = new DevIdentityOptions();
        configure?.Invoke(DevOptions);
        return this;
    }
    
    /// <summary>
    /// Enables affinity handoff for HA deployment with multiple hub instances.
    /// Requires sticky sessions on the load balancer and a shared IStateStore.
    /// </summary>
    public DataHubBuilder UseAffinityHandoff(Action<AffinityOptions>? configure = null)
    {
        Services.Configure<AffinityOptions>(options =>
        {
            options.Enabled = true;
            configure?.Invoke(options);
        });
        return this;
    }
    
    /// <summary>
    /// Connects to TestProxy for multi-instance testing.
    /// Replaces default InMemoryStateStore with HTTP client to proxy's state API.
    /// </summary>
    /// <param name="proxyUrl">URL of TestProxy (e.g., http://localhost:5000)</param>
    public DataHubBuilder UseTestProxyStore(string proxyUrl)
    {
        // Remove default InMemoryStateStore and replace with TestProxy client
        Services.AddSingleton<IStateStore>(new TestProxyStateStore(proxyUrl));
        return this;
    }
}

public static class TransportRHubExtensions
{
    /// <summary>
    /// Adds DataHub services to the DI container.
    /// Uses InMemoryStateStore by default.
    /// </summary>
    public static DataHubBuilder AddDataHub(this IServiceCollection services)
    {
        // Default state store (in-memory)
        services.AddSingleton<IStateStore, InMemoryStateStore>();
        
        // TransferManager
        services.AddSingleton<ITransferManager, TransferManager>();
        
        // BridgeManager (manages HTTP stream bridges)
        services.AddSingleton<IBridgeManager, BridgeManager>();
        
        // Affinity options (disabled by default)
        services.Configure<AffinityOptions>(options => options.Enabled = false);
        
        // Data plane configuration (bridge timeout)
        services.Configure<DataPlaneConfiguration>(_ => { });
        services.Configure<ClientRegistryConfiguration>(_ => { });
        
        // HttpContextAccessor needed for affinity token extraction
        services.AddHttpContextAccessor();
        
        // Bridge cleanup service
        services.AddHostedService<BridgeCleanupService>();

        // Transfer cleanup service (Timeouts)
        services.AddHostedService<TransferCleanupService>();
        
        // Register Transfer Notifier (SignalR implementation)
        services.AddSingleton<ITransferNotifier, SignalRTransferNotifier>();

        // Register Broker Server
        services.AddSingleton<ITransportRServer, SignalRDataServer>();
        
        // Add SignalR
        services.AddSignalR();
        
        // Add Controllers (for StreamController)
        services.AddControllers()
                .AddApplicationPart(typeof(TransportRHubExtensions).Assembly);
        
        return new DataHubBuilder(services);
    }

    /// <summary>
    /// Finalizes DataHub configuration and registers the identity provider.
    /// Must be called after the builder is configured.
    /// </summary>
    internal static void FinalizeDataHub(this IServiceCollection services, DataHubBuilder builder)
    {
        if (builder.IsDevMode)
        {
            var options = builder.DevOptions ?? new DevIdentityOptions();
            services.AddSingleton(options);
            services.AddSingleton<IClientIdentityProvider, DevIdentityProvider>();
        }
        else
        {
            services.AddSingleton<IClientIdentityProvider, JwtIdentityProvider>();
        }
    }

    /// <summary>
    /// Maps the CommandHub and StreamController endpoints.
    /// </summary>
    /// <param name="requireAuth">
    /// Arms authorization on both planes. The host must have called <c>UseAuthentication()</c> and
    /// <c>UseAuthorization()</c>; leaving it false leaves the data plane open to anyone who knows a
    /// transfer id, which is the documented meaning of dev mode.
    /// </param>
    public static IEndpointRouteBuilder MapDataHub(this IEndpointRouteBuilder endpoints, bool requireAuth = false)
    {
        var hubEndpoint = endpoints.MapHub<CommandHub>(WireRoutes.CommandHub);

        if (requireAuth)
        {
            hubEndpoint.RequireAuthorization();
        }

        var controllers = endpoints.MapControllers();

        if (requireAuth)
        {
            // MapControllers hands back one convention builder for every controller the host has
            // registered — including the embedded IdP's token endpoint, which must stay anonymous
            // or no client could ever obtain the token this asks for. Hence the filter.
            controllers.Add(builder =>
            {
                var action = builder.Metadata.OfType<ControllerActionDescriptor>().FirstOrDefault();
                if (action?.ControllerTypeInfo.AsType() == typeof(StreamController))
                {
                    builder.Metadata.Add(new AuthorizeAttribute());
                }
            });
        }

        return endpoints;
    }
}

/// <summary>
/// Extension to complete DataHub registration after builder configuration.
/// </summary>
public static class DataHubBuilderExtensions
{
    /// <summary>
    /// Completes the DataHub configuration by registering the identity provider.
    /// </summary>
    public static IServiceCollection Build(this DataHubBuilder builder)
    {
        builder.Services.FinalizeDataHub(builder);
        return builder.Services;
    }
}
