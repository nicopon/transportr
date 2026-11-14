using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using TransportR.Hub.Access;
using TransportR.Hub.Controllers;
using TransportR.Hub.Identity;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Services;
using TransportR.Hub.Streams;

namespace TransportR.Hub;

/// <summary>
/// Builder for configuring TransportR data hub services.
/// </summary>
public class DataHubBuilder
{
    public IServiceCollection Services { get; }

    internal DataHubBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Enables development mode which trusts client-provided identity values.
    /// </summary>
    /// <remarks>
    /// Replaces the identity provider registered so far: the token-validating one <c>AddDataHub()</c> adds
    /// by default, or one the host registered before it. A provider the host registers afterwards
    /// replaces this one in turn.
    /// </remarks>
    public DataHubBuilder UseDevMode(Action<DevIdentityOptions>? configure = null)
    {
        var options = new DevIdentityOptions();
        configure?.Invoke(options);
        Services.Replace(ServiceDescriptor.Singleton(options));
        Services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider, DevIdentityProvider>());
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
        services.AddSingleton<BridgeManager>();

        // Affinity options (disabled by default)
        services.Configure<AffinityOptions>(options => options.Enabled = false);

        // A timeout of zero or less would end every transfer on the first check, an inactivity timeout
        // too short to leave a heartbeat interval refuses every stream, and a stalled-progress bound at
        // or below the bridge timeout reads a reconnection as a stall and kills transfers that would
        // have recovered. All are refused when the host starts rather than found in production.
        services.AddOptions<DataPlaneConfiguration>()
            .Validate(o => o.BridgeTimeoutSeconds > 0, "DataPlaneConfiguration.BridgeTimeoutSeconds must be positive.")
            .Validate(o => o.TransferInactivityTimeoutSeconds > 0, "DataPlaneConfiguration.TransferInactivityTimeoutSeconds must be positive.")
            .Validate(
                o => AnnouncedHeartbeat.LeavesARange(TimeSpan.FromSeconds(o.TransferInactivityTimeoutSeconds)),
                $"DataPlaneConfiguration.TransferInactivityTimeoutSeconds must be more than twice {AnnouncedHeartbeat.Shortest.TotalSeconds} s, the shortest heartbeat interval a client may announce.")
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

        // The event streams of the clients this instance holds: how the hub reaches a client.
        services.AddSingleton<ClientStreams>();

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

        services.AddSingleton<ITransferNotifier, ClientStreamNotifier>();

        // Registered as itself too: the control route drives the same handshake through members the
        // host interface does not expose.
        services.AddSingleton<TransferInitiator>();
        services.AddSingleton<ITransferInitiator>(sp => sp.GetRequiredService<TransferInitiator>());

        // Register Broker Server
        services.AddSingleton<ITransportRServer, TransportRServer>();
        services.AddSingleton<UploadEpochs>();

        // TransportRServer requires an IHubProgressMonitor. TryAdd, so a monitor the host registered
        // before this call is kept; one it registers afterwards is the last, and wins.
        services.TryAddSingleton<IHubProgressMonitor, NoopHubProgressMonitor>();

        // Tokens are validated unless the host says otherwise: UseDevMode replaces this provider, and a
        // provider the host registered before this call is kept.
        services.TryAddSingleton<IClientIdentityProvider, JwtIdentityProvider>();

        // The data plane (StreamController) and the control plane (ClientController).
        services.AddControllers()
                .AddApplicationPart(typeof(TransportRHubExtensions).Assembly)
                .ConfigureApplicationPartManager(manager => manager.FeatureProviders.Add(new DataHubControllerFeatureProvider()));

        return new DataHubBuilder(services);
    }

    /// <summary>
    /// Maps the StreamController and ClientController endpoints: the data plane and the control plane.
    /// </summary>
    /// <param name="requireAuth">
    /// Whether authorization is armed on both planes. Left null, it follows the identity provider
    /// registered: armed for a hub that validates tokens or has a provider of the host's own, not for
    /// one in dev mode. Armed, the host must have called <c>UseAuthentication()</c> and
    /// <c>UseAuthorization()</c>. Not armed, the data plane is open to anyone who knows a transfer id,
    /// which is the documented meaning of dev mode; a hub that validates tokens and is mapped that way
    /// logs a warning when it starts, as a hub in dev mode does.
    /// </param>
    public static IEndpointRouteBuilder MapDataHub(this IEndpointRouteBuilder endpoints, bool? requireAuth = null)
    {
        // Read from the provider registered rather than from the requests: a request without a
        // token looks the same on a hub in dev mode and on one that validates tokens.
        var devMode = endpoints.ServiceProvider.GetRequiredService<IClientIdentityProvider>() is DevIdentityProvider;
        var armed = requireAuth ?? !devMode;

        var logger = endpoints.ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger(typeof(TransportRHubExtensions));
        if (devMode)
        {
            logger.LogWarning(
                "TransportR hub in dev mode: it trusts the identity and groups a client claims, and unless authorization is armed its data plane serves anyone who knows a transfer id. Not for production.");
        }
        else if (!armed)
        {
            logger.LogWarning(
                "TransportR hub validates tokens but is mapped with requireAuth: false: its data plane serves anyone who knows a transfer id, without a token.");
        }

        var controllers = endpoints.MapControllers();

        if (armed)
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
