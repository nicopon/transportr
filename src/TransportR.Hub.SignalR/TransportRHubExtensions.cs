using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.SignalR;
using TransportR.Configuration;
using TransportR.Hub.SignalR.Access;
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

    /// <summary>
    /// Registers <typeparamref name="THub"/> in place of <see cref="CommandHub"/> as the single hub
    /// this connection carries. <see cref="MapDataHub"/> maps whichever hub was registered here -
    /// last call wins, the same as any other singleton registration - so a host cannot register one
    /// hub and map another.
    /// </summary>
    /// <remarks>
    /// TransportR's own services resolve <see cref="IHubContext{CommandHub}"/>
    /// (<c>SignalRTransferInitiator</c>, <c>SignalRTransferNotifier</c>); redirecting that
    /// resolution to <typeparamref name="THub"/>'s own context - valid because
    /// <see cref="IHubContext{THub}"/> is covariant in <c>THub</c> - is what puts the host's own
    /// hub methods and TransportR's handshake on the one connection a client has, instead of a
    /// second hub the client would have to connect to as well.
    /// </remarks>
    public DataHubBuilder UseHub<THub>() where THub : CommandHub
    {
        Services.AddSingleton(new DataHubTypeMarker(typeof(THub), (endpoints, pattern) => endpoints.MapHub<THub>(pattern)));
        Services.AddSingleton<IHubContext<CommandHub>>(sp => sp.GetRequiredService<IHubContext<THub>>());
        return this;
    }
}

/// <summary>
/// Carries the hub type <see cref="DataHubBuilder.UseHub{THub}"/> registered from the builder to
/// <see cref="TransportRHubExtensions.MapDataHub"/>, which runs later against an
/// <see cref="IEndpointRouteBuilder"/> that has no reference to that builder instance. The mapping
/// call itself is captured as a delegate rather than reflected, since <c>THub</c> is only known as
/// a compile-time type parameter inside <see cref="DataHubBuilder.UseHub{THub}"/>.
/// </summary>
internal sealed class DataHubTypeMarker
{
    public Type HubType { get; }
    public Func<IEndpointRouteBuilder, string, HubEndpointConventionBuilder> MapHub { get; }

    public DataHubTypeMarker(Type hubType, Func<IEndpointRouteBuilder, string, HubEndpointConventionBuilder> mapHub)
    {
        HubType = hubType;
        MapHub = mapHub;
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
        // Every hub-side clock and timer reads this one. TryAdd, so that a host registering its own
        // before or after this call keeps it.
        services.TryAddSingleton(TimeProvider.System);

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

        // The one decision on tokens, groups and identities. A negative margin would make a token
        // outlive its expiry by a sign error, so it is refused when the host starts.
        services.AddOptions<AccessOptions>()
            .Validate(o => o.TokenExpiryMarginSeconds >= 0, "AccessOptions.TokenExpiryMarginSeconds cannot be negative.")
            .ValidateOnStart();
        services.AddSingleton<AccessPolicy>();
        
        // HttpContextAccessor needed for affinity token extraction
        services.AddHttpContextAccessor();
        
        // Bridge cleanup service
        services.AddHostedService<BridgeCleanupService>();

        // Terminates a transfer unilaterally (state, bridges, both participants notified) - the
        // one implementation, used by the cleanup service below and available to a host that wants
        // to terminate a run it owns.
        services.AddSingleton<ITransferTerminator, TransferTerminator>();

        // Transfer cleanup service (Timeouts)
        services.AddHostedService<TransferCleanupService>();
        
        // Register Transfer Notifier (SignalR implementation)
        services.AddSingleton<ITransferNotifier, SignalRTransferNotifier>();

        // Registered as itself too: the hub drives the same handshake through members the host
        // interface does not expose.
        services.AddSingleton<SignalRTransferInitiator>();
        services.AddSingleton<ITransferInitiator>(sp => sp.GetRequiredService<SignalRTransferInitiator>());

        // Register Broker Server
        services.AddSingleton<ITransportRServer, SignalRDataServer>();

        // SignalRDataServer requires an IHubProgressMonitor; TryAdd so a host that registers its
        // own before Build() keeps seeing it used, whichever order that registration happens in.
        services.TryAddSingleton<IHubProgressMonitor, NoopHubProgressMonitor>();
        
        // Add SignalR
        services.AddSignalR();

        // Add Controllers (for StreamController)
        services.AddControllers()
                .AddApplicationPart(typeof(TransportRHubExtensions).Assembly);

        // Default hub for MapDataHub to map; UseHub<THub>() overrides this registration.
        services.AddSingleton(new DataHubTypeMarker(typeof(CommandHub), (endpoints, pattern) => endpoints.MapHub<CommandHub>(pattern)));

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
    /// Maps the CommandHub, StreamController and ClientController endpoints.
    /// </summary>
    /// <param name="requireAuth">
    /// Arms authorization on both planes. The host must have called <c>UseAuthentication()</c> and
    /// <c>UseAuthorization()</c>; leaving it false leaves the data plane open to anyone who knows a
    /// transfer id, which is the documented meaning of dev mode.
    /// </param>
    public static IEndpointRouteBuilder MapDataHub(this IEndpointRouteBuilder endpoints, bool requireAuth = false)
    {
        // AddDataHub() registers everything except the identity provider, which only Build() knows
        // how to choose. Skipping Build() therefore yields a hub that starts, accepts connections
        // and fails on the first one — at the far end of a scenario, in a resolution error naming a
        // type the host never mentioned. Refusing here names the missing call instead.
        if (endpoints.ServiceProvider.GetService<IClientIdentityProvider>() is null)
        {
            throw new InvalidOperationException(
                "TransportR is not fully configured: AddDataHub() must be followed by Build().");
        }

        var marker = endpoints.ServiceProvider.GetRequiredService<DataHubTypeMarker>();
        var hubEndpoint = marker.MapHub(endpoints, WireRoutes.CommandHub);

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
                var controller = action?.ControllerTypeInfo.AsType();
                if (controller == typeof(StreamController) || controller == typeof(ClientController))
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
