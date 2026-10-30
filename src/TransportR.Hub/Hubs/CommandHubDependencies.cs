using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Hub.Access;
using TransportR.Hub.Configuration;
using TransportR.Hub.Services;
using TransportR.Interfaces;

namespace TransportR.Hub.Hubs;

/// <summary>
/// What <see cref="CommandHub"/> takes from the host's container. A hub derived from it passes the
/// instance to <c>base</c> without opening it, so it can grow without breaking such a hub.
/// </summary>
internal sealed class CommandHubDependencies
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
