using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.SignalR.Access;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Services;
using TransportR.Interfaces;

namespace TransportR.Hub.SignalR.Hubs;

/// <summary>
/// What <see cref="CommandHub"/> takes from the host's container. A hub derived from it passes the
/// instance to <c>base</c> without opening it; what it carries is not an API, and can grow without
/// breaking such a hub.
/// </summary>
public sealed class CommandHubDependencies
{
    internal IStateStore StateStore { get; }
    internal ITransferManager TransferManager { get; }
    internal IClientIdentityProvider IdentityProvider { get; }
    internal SignalRTransferInitiator Initiator { get; }
    internal AccessPolicy Policy { get; }
    internal ConnectionRegistry Connections { get; }
    internal IOptions<AffinityOptions> AffinityOptions { get; }
    internal ILogger<CommandHub> Logger { get; }
    internal TimeProvider TimeProvider { get; }

    internal CommandHubDependencies(
        IStateStore stateStore,
        ITransferManager transferManager,
        IClientIdentityProvider identityProvider,
        SignalRTransferInitiator initiator,
        AccessPolicy policy,
        ConnectionRegistry connections,
        IOptions<AffinityOptions> affinityOptions,
        ILogger<CommandHub> logger,
        TimeProvider timeProvider)
    {
        StateStore = stateStore;
        TransferManager = transferManager;
        IdentityProvider = identityProvider;
        Initiator = initiator;
        Policy = policy;
        Connections = connections;
        AffinityOptions = affinityOptions;
        Logger = logger;
        TimeProvider = timeProvider;
    }
}
