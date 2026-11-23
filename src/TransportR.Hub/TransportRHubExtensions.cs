using System.Globalization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using TransportR.Hub.Access;
using TransportR.Hub.Diagnostics;
using TransportR.Hub.Endpoints;
using TransportR.Hub.Identity;
using TransportR.Hub.Interfaces;
using TransportR.Hub.Services;
using TransportR.Hub.Streams;
using TransportR.Protocol;

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
    /// Registers the group access rules: which group may send to which. Without them, any client
    /// holding a group may open a transfer to any other.
    /// </summary>
    /// <remarks>
    /// Replaces the flow control registered so far: one the host registered before this call, or the rules
    /// of an earlier call. A service the host registers afterwards replaces these in turn, which is how a
    /// host with rules of its own supplies its <see cref="IFlowControlService"/>.
    /// </remarks>
    /// <param name="configure">Declares the rules on a new <see cref="FlowControlOptions"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="configure"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// No group is declared (an empty rule set would refuse every transfer, so it is taken as a mistake), a
    /// group has a blank name, a group has no <see cref="GroupAccess"/> or no <c>CanSendTo</c> list, or a
    /// <c>CanSendTo</c> list holds a blank target.
    /// </exception>
    public DataHubBuilder UseFlowControl(Action<FlowControlOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new FlowControlOptions();
        configure(options);
        Validate(options, nameof(configure));

        Services.Replace(ServiceDescriptor.Singleton(options));
        Services.Replace(ServiceDescriptor.Singleton<IFlowControlService, DefaultFlowControlService>());
        return this;
    }

    private static void Validate(FlowControlOptions options, string paramName)
    {
        if (options.Groups is not { Count: > 0 })
        {
            throw new ArgumentException("No group is declared.", paramName);
        }

        foreach (var (group, access) in options.Groups)
        {
            if (string.IsNullOrWhiteSpace(group))
            {
                throw new ArgumentException("A group has a blank name.", paramName);
            }

            if (access is null)
            {
                throw new ArgumentException($"Group '{group}' has no access rule.", paramName);
            }

            if (access.CanSendTo is null)
            {
                throw new ArgumentException($"Group '{group}' has no CanSendTo list.", paramName);
            }

            if (access.CanSendTo.Any(string.IsNullOrWhiteSpace))
            {
                throw new ArgumentException($"Group '{group}' lists a blank target in CanSendTo.", paramName);
            }
        }
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
    /// Configures <see cref="DataPlaneConfiguration"/> timeouts for bridges, inactivity, and stalled progress.
    /// </summary>
    public DataHubBuilder ConfigureDataPlane(Action<DataPlaneConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        Services.Configure(configure);
        return this;
    }

    /// <summary>
    /// Configures <see cref="AccessOptions"/> for token expiry margin, access sweeps, and transfer limits.
    /// </summary>
    public DataHubBuilder ConfigureAccess(Action<AccessOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        Services.Configure(configure);
        return this;
    }

    /// <summary>
    /// Configures <see cref="ClientRegistryConfiguration"/> lifetimes for connected and disconnected clients.
    /// </summary>
    public DataHubBuilder ConfigureClientRegistry(Action<ClientRegistryConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        Services.Configure(configure);
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
            .Validate(o => o.DrainTimeoutSeconds >= 0, "DataPlaneConfiguration.DrainTimeoutSeconds cannot be negative.")
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
            .Validate(o => o.MaxHandshakesPerClient is not <= 0, "AccessOptions.MaxHandshakesPerClient must be positive, or null for no ceiling.")
            .Validate(o => o.MaxOpenTransfersPerClient is not <= 0, "AccessOptions.MaxOpenTransfersPerClient must be positive, or null for no ceiling.")
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

        // Built-in diagnostics, graceful shutdown drain, routing, and Minimal API endpoint handlers.
        services.AddRouting();
        services.AddSingleton<HubMetrics>();
        services.AddSingleton<HubDrainState>();
        services.AddHostedService<HubDrainService>();
        services.AddSingleton<ClientEndpoints>();
        services.AddSingleton<StreamEndpoints>();

        return new DataHubBuilder(services);
    }

    /// <summary>
    /// Maps the TransportR data plane and control plane endpoints at the root path.
    /// </summary>
    /// <param name="endpoints">The route builder to map onto.</param>
    /// <param name="requireAuth">
    /// Whether authorization is armed on both planes. Left null, it follows the identity provider
    /// registered: armed for a hub that validates tokens or has a provider of the host's own, not for
    /// one in dev mode. Armed, the host must have called <c>UseAuthentication()</c> and
    /// <c>UseAuthorization()</c>. Not armed, the data plane is open to anyone who knows a transfer id,
    /// which is the documented meaning of dev mode; a hub that validates tokens and is mapped that way
    /// logs a warning when it starts, as a hub in dev mode does.
    /// </param>
    public static IEndpointRouteBuilder MapDataHub(this IEndpointRouteBuilder endpoints, bool? requireAuth = null) =>
        endpoints.MapDataHub(string.Empty, requireAuth);

    /// <summary>
    /// Maps the TransportR data plane and control plane endpoints under <paramref name="routePrefix"/>.
    /// </summary>
    /// <param name="endpoints">The route builder to map onto.</param>
    /// <param name="routePrefix">
    /// Path prefix placed ahead of <see cref="WireRoutes.ClientBase"/> and <see cref="WireRoutes.StreamBase"/>
    /// (for example <c>"/transportr"</c>), or empty for root mapping. Must not contain whitespace or
    /// <c>?</c>, <c>#</c>, <c>{</c>, <c>}</c>.
    /// </param>
    /// <param name="requireAuth">See <see cref="MapDataHub(IEndpointRouteBuilder, bool?)"/>.</param>
    public static IEndpointRouteBuilder MapDataHub(this IEndpointRouteBuilder endpoints, string routePrefix, bool? requireAuth = null)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(routePrefix);
        if (routePrefix.Any(c => char.IsWhiteSpace(c) || c is '?' or '#' or '{' or '}'))
        {
            throw new ArgumentException("Route prefix cannot contain whitespace, query, fragment, or route parameter characters.", nameof(routePrefix));
        }

        var trimmed = routePrefix.Trim('/');
        var prefix = trimmed.Length == 0 ? string.Empty : "/" + trimmed;

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

        var clientEndpoints = endpoints.ServiceProvider.GetRequiredService<ClientEndpoints>();
        var streamEndpoints = endpoints.ServiceProvider.GetRequiredService<StreamEndpoints>();

        var clientGroup = endpoints.MapGroup(prefix + WireRoutes.ClientBase);
        clientGroup.MapGet("{clientId:guid}/" + WireRoutes.EventsSegment,
            (HttpContext ctx, Guid clientId) =>
            {
                if (!TryParseOptionalInt(ctx.Request.Query[ControlPlane.HeartbeatParameter], out var heartbeatMs))
                {
                    ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                    return Task.CompletedTask;
                }

                return clientEndpoints.EventsAsync(ctx, clientId, heartbeatMs);
            });
        clientGroup.MapPost("{clientId:guid}/" + WireRoutes.PresenceSegment + "/{connectionId}",
            (HttpContext ctx, Guid clientId, string connectionId) =>
                clientEndpoints.PresenceAsync(ctx, clientId, connectionId));
        clientGroup.MapGet("{clientId:guid}/" + WireRoutes.ReceiversSegment,
            (HttpContext ctx, Guid clientId) =>
                clientEndpoints.ReceiversAsync(ctx, clientId));
        clientGroup.MapPost("{clientId:guid}/" + WireRoutes.TransfersSegment,
            (HttpContext ctx, Guid clientId) =>
                clientEndpoints.InitTransferAsync(ctx, clientId));
        clientGroup.MapPost("{clientId:guid}/" + WireRoutes.RepliesSegment + "/{requestId:guid}",
            (HttpContext ctx, Guid clientId, Guid requestId) =>
                clientEndpoints.ReplyAsync(ctx, clientId, requestId));
        clientGroup.MapPost("{clientId:guid}/" + WireRoutes.TransfersSegment + "/{transferId}/" + WireRoutes.HeartbeatSegment,
            (HttpContext ctx, Guid clientId, string transferId) =>
                clientEndpoints.TransferHeartbeatAsync(ctx, clientId, transferId));
        clientGroup.MapPost("{clientId:guid}/" + WireRoutes.TransfersSegment + "/{transferId}/" + WireRoutes.PingSegment,
            (HttpContext ctx, Guid clientId, string transferId) =>
                clientEndpoints.PingAsync(ctx, clientId, transferId));
        clientGroup.MapPut("{clientId:guid}/" + WireRoutes.TokenSegment,
            (HttpContext ctx, Guid clientId) =>
                clientEndpoints.RenewAsync(ctx, clientId));

        var streamGroup = endpoints.MapGroup(prefix + WireRoutes.StreamBase);
        streamGroup.MapPost("{transferId}/" + WireRoutes.SendSegment,
            (HttpContext ctx, string transferId) =>
            {
                if (!TryParseOptionalGuid(ctx.Request.Query[DataPlane.EpochParameter], out var epoch)
                    || !TryParseOptionalLong(ctx.Request.Query[DataPlane.OffsetParameter], out var offset)
                    || !TryParseOptionalBool(ctx.Request.Query[DataPlane.FinalParameter], out var final)
                    || !TryParseOptionalInt(ctx.Request.Query[DataPlane.HeartbeatParameter], out var heartbeatMs))
                {
                    ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                    return Task.CompletedTask;
                }

                return streamEndpoints.SendAsync(ctx, transferId, epoch, offset, final ?? false, heartbeatMs);
            });

        // UseWebSockets wraps only the two upgrade routes so the host adds nothing to its pipeline
        // and group-level authorization runs before any WebSocket upgrade.
        streamGroup.MapGet("{transferId}/" + WireRoutes.SendSegment, BuildWebSocketDelegate(endpoints, ctx =>
        {
            if (!TryParseOptionalInt(ctx.Request.Query[DataPlane.HeartbeatParameter], out var heartbeatMs))
            {
                ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                return Task.CompletedTask;
            }

            var transferId = ctx.GetRouteValue("transferId")?.ToString() ?? string.Empty;
            return streamEndpoints.SendOverWebSocketAsync(ctx, transferId, heartbeatMs);
        }));

        streamGroup.MapGet(WireRoutes.ProbeSegment + "/" + WireRoutes.SendSegment, BuildWebSocketDelegate(endpoints, ctx =>
        {
            var connection = ctx.Request.Query[DataPlane.ConnectionParameter].FirstOrDefault();
            return streamEndpoints.ProbeOverWebSocketAsync(ctx, connection);
        }));

        Delegate probeOverSegments = (HttpContext ctx) =>
            streamEndpoints.ProbeOverSegmentsAsync(ctx, ctx.Request.Query[DataPlane.ConnectionParameter].FirstOrDefault());
        streamGroup.MapPost(WireRoutes.ProbeSegment + "/" + WireRoutes.SendSegment, probeOverSegments);
        streamGroup.MapPost("{transferId}/" + WireRoutes.AbortSegment,
            (HttpContext ctx, string transferId) =>
                streamEndpoints.AbortAsync(ctx, transferId));
        streamGroup.MapGet("{transferId}/" + WireRoutes.ReceiveSegment,
            (HttpContext ctx, string transferId) =>
                streamEndpoints.ReceiveAsync(ctx, transferId));

        if (armed)
        {
            clientGroup.RequireAuthorization();
            streamGroup.RequireAuthorization();
        }

        return endpoints;
    }

    private static RequestDelegate BuildWebSocketDelegate(IEndpointRouteBuilder endpoints, RequestDelegate handler)
    {
        var wsApp = endpoints.CreateApplicationBuilder();
        wsApp.UseWebSockets();
        wsApp.Run(handler);
        return wsApp.Build();
    }

    private delegate bool TryParseDelegate<T>(string raw, out T value);

    private static bool TryParseOptional<T>(StringValues values, TryParseDelegate<T> parse, out T? value) where T : struct
    {
        value = null;
        if (values.Count == 0) return true;

        var raw = values[0];
        if (string.IsNullOrEmpty(raw)) return true;
        if (!parse(raw, out var parsed)) return false;

        value = parsed;
        return true;
    }

    private static bool TryParseOptionalInt(StringValues values, out int? value) =>
        TryParseOptional(values, static (string s, out int v) => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out v), out value);

    private static bool TryParseOptionalLong(StringValues values, out long? value) =>
        TryParseOptional(values, static (string s, out long v) => long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out v), out value);

    private static bool TryParseOptionalGuid(StringValues values, out Guid? value) =>
        TryParseOptional(values, static (string s, out Guid v) => Guid.TryParse(s, out v), out value);

    private static bool TryParseOptionalBool(StringValues values, out bool? value) =>
        TryParseOptional(values, bool.TryParse, out value);
}
