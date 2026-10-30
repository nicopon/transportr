using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Configuration;
using TransportR.Hub.Access;
using TransportR.Hub.Controllers;
using TransportR.Hub.Configuration;
using TransportR.Hub.Hubs;
using TransportR.Hub.Services;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Protocol;
using TransportR.Services;

namespace TransportR.Hub;

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
    /// Registers <typeparamref name="THub"/> in place of <see cref="CommandHub"/> as the single hub
    /// this connection carries, for a test that probes the connection from the hub's side.
    /// <see cref="TransportRHubExtensions.MapDataHub"/> maps whichever hub was registered here - last
    /// call wins - so a test cannot register one hub and map another.
    /// </summary>
    /// <remarks>
    /// TransportR's own services keep resolving <see cref="IHubContext{CommandHub}"/>, which reaches
    /// none of <typeparamref name="THub"/>'s connections: no transfer opens on such a hub.
    /// </remarks>
    internal DataHubBuilder UseHub<THub>() where THub : CommandHub
    {
        Services.AddSingleton(new DataHubTypeMarker(typeof(THub), (endpoints, pattern) => endpoints.MapHub<THub>(pattern)));
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

        // Default state store (in-memory). TryAdd, so a store the host registered before this call is
        // kept; UseRedisStore and the other store extensions replace it.
        services.TryAddSingleton<IStateStore, InMemoryStateStore>();
        
        // TransferManager
        services.AddSingleton<ITransferManager, TransferManager>();
        
        // BridgeManager (manages HTTP stream bridges)
        services.AddSingleton<IBridgeManager, BridgeManager>();
        
        // Affinity options (disabled by default)
        services.Configure<AffinityOptions>(options => options.Enabled = false);
        
        // A timeout of zero or less would end every transfer on the first check, and a stalled-progress
        // bound at or below the bridge timeout reads a reconnection as a stall and kills transfers that
        // would have recovered. Both are refused when the host starts rather than found in production.
        services.AddOptions<DataPlaneConfiguration>()
            .Validate(o => o.BridgeTimeoutSeconds > 0, "DataPlaneConfiguration.BridgeTimeoutSeconds must be positive.")
            .Validate(o => o.TransferInactivityTimeoutSeconds > 0, "DataPlaneConfiguration.TransferInactivityTimeoutSeconds must be positive.")
            .Validate(
                o => o.StalledProgressTimeoutSeconds is not { } stalled || stalled > o.BridgeTimeoutSeconds,
                "DataPlaneConfiguration.StalledProgressTimeoutSeconds must be greater than BridgeTimeoutSeconds, or left unset.")
            .ValidateOnStart();
        services.AddOptions<ClientRegistryConfiguration>()
            .Validate(o => o.DisconnectedGraceSeconds > 0, "ClientRegistryConfiguration.DisconnectedGraceSeconds must be positive.")
            .Validate(o => o.ConnectedTtlSeconds > 0, "ClientRegistryConfiguration.ConnectedTtlSeconds must be positive.")
            .ValidateOnStart();

        // The one decision on tokens, groups and identities. A negative margin would make a token
        // outlive its expiry by a sign error, so it is refused when the host starts.
        services.AddOptions<AccessOptions>()
            .Validate(o => o.TokenExpiryMarginSeconds >= 0, "AccessOptions.TokenExpiryMarginSeconds cannot be negative.")
            .Validate(o => o.AccessSweepIntervalSeconds > 0, "AccessOptions.AccessSweepIntervalSeconds must be positive.")
            .ValidateOnStart();
        services.AddSingleton<AccessPolicy>();
        services.AddSingleton<ConnectionRegistry>();

        // Applies the policy to what is running: on the schedule below, after a renewal, and whenever
        // the host calls it.
        services.AddSingleton<IAccessReconciler, AccessReconciler>();
        services.AddHostedService<AccessSweepService>();
        
        // Bridge cleanup service
        services.AddHostedService<BridgeCleanupService>();

        // Terminates a transfer unilaterally (state, bridges, both participants notified) - the
        // one implementation, used by the cleanup service below and available to a host that wants
        // to terminate a run it owns. The same instance retires a transfer that ended well.
        services.AddSingleton<TransferTerminator>();
        services.AddSingleton<ITransferTerminator>(sp => sp.GetRequiredService<TransferTerminator>());
        services.AddSingleton<ITransferRetirer>(sp => sp.GetRequiredService<TransferTerminator>());

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

        // DI cannot call the internal constructor, so the hub's dependencies are assembled here. Transient
        // because a hub is created per invocation and holds nothing of its own.
        services.AddTransient(sp => new CommandHubDependencies(
            sp.GetRequiredService<IStateStore>(),
            sp.GetRequiredService<ITransferManager>(),
            sp.GetRequiredService<IClientIdentityProvider>(),
            sp.GetRequiredService<SignalRTransferInitiator>(),
            sp.GetRequiredService<AccessPolicy>(),
            sp.GetRequiredService<ConnectionRegistry>(),
            sp.GetRequiredService<IOptions<AffinityOptions>>(),
            sp.GetRequiredService<ILogger<CommandHub>>(),
            sp.GetRequiredService<TimeProvider>()));

        // SignalRDataServer requires an IHubProgressMonitor; TryAdd so a host that registers its
        // own before Build() keeps seeing it used, whichever order that registration happens in.
        services.TryAddSingleton<IHubProgressMonitor, NoopHubProgressMonitor>();
        
        // Add SignalR
        services.AddSignalR();

        // Add Controllers (for StreamController)
        services.AddControllers()
                .AddApplicationPart(typeof(TransportRHubExtensions).Assembly)
                .ConfigureApplicationPartManager(manager => manager.FeatureProviders.Add(new DataHubControllerFeatureProvider()));

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
