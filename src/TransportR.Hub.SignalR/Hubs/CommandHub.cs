using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Configuration;
using TransportR.Hub.SignalR.Configuration;
using TransportR.Hub.SignalR.Exceptions;
using TransportR.Hub.SignalR.Services;
using TransportR.Services;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace TransportR.Hub.SignalR.Hubs;

/// <summary>
/// SignalR hub for orchestrating data transfers between clients.
/// </summary>
public class CommandHub : Microsoft.AspNetCore.SignalR.Hub, ICommandHub
{
    private readonly IStateStore _stateStore;
    private readonly ITransferManager _transferManager;
    private readonly IFlowControlService? _flowControlService;
    private readonly IClientIdentityProvider _identityProvider;
    private readonly IBridgeManager _bridgeManager;
    private readonly AffinityOptions _affinityOptions;
    private readonly ILogger<CommandHub> _logger;

    public CommandHub(
        IStateStore stateStore,
        ITransferManager transferManager, 
        IClientIdentityProvider identityProvider,
        IBridgeManager bridgeManager,
        IOptions<AffinityOptions> affinityOptions,
        ILogger<CommandHub> logger,
        IFlowControlService? flowControlService = null)
    {
        _stateStore = stateStore;
        _transferManager = transferManager;
        _identityProvider = identityProvider;
        _bridgeManager = bridgeManager;
        _affinityOptions = affinityOptions.Value;
        _flowControlService = flowControlService;
        _logger = logger;
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        // Do NOT delete the client on disconnect. In chaos/unstable networks, the client
        // usually reconnects within seconds under the same ClientId, and deleting the
        // registration would break a transfer that was about to resume.
        //
        // But do not throw the signal away either: this is the only moment the hub knows for
        // certain that a connection is gone, and discovery has nothing else to go on. Record
        // it so GetReceivers can expose it; a fresh Connect writes a new registration with no
        // marker, which clears it.
        //
        // The store decides whether the mark still applies: a reconnect can overtake this
        // callback, and writing back a registration read before it would resurrect the dead
        // ConnectionId over the live one.
        await _stateStore.MarkDisconnectedAsync(Context.ConnectionId, DateTime.UtcNow);

        _logger.LogInformation("Client disconnected: {ConnectionId}", Context.ConnectionId);
        await base.OnDisconnectedAsync(exception);
    }

    public async Task Connect(Guid? clientId = null, IEnumerable<string>? groups = null)
    {
        var identity = _identityProvider.ResolveIdentity(Context.User, clientId, groups);
        
        if (identity == null)
        {
            _logger.LogWarning("Connect failed: Unable to resolve client identity");
            throw new HubException("Unable to resolve client identity. Ensure authentication is configured or use dev mode.");
        }

        // The store overwrites on save, so without this a caller who learned another client's id
        // from GetReceivers takes over its registration: transfers aimed at it land here, and the
        // data plane checks streams against the subject just written. A registration still within
        // its disconnection grace is protected too, since its owner is expected to reclaim it.
        // Dev mode attests no subject, so both sides are null and nothing is refused there.
        var existing = await _stateStore.GetClientAsync(identity.ClientId);
        if (existing != null && !string.Equals(existing.Subject, identity.Subject, StringComparison.Ordinal))
        {
            _logger.LogWarning("Connect refused: ClientId {ClientId} is registered under subject {ExistingSubject}, caller attests {Subject}",
                identity.ClientId, existing.Subject, identity.Subject);
            throw new HubException("Client identifier is already in use.");
        }

        // Extract affinity token from request cookies
        string? affinityToken = null;
        if (_affinityOptions.Enabled)
        {
            affinityToken = ExtractAffinityToken();
        }
        
        var registration = new ClientRegistration(
            identity.ClientId,
            Context.ConnectionId,
            identity.Groups.ToHashSet(),
            affinityToken,
            identity.TokenExpiry,
            DateTime.UtcNow,
            Subject: identity.Subject);
        
        await _stateStore.SaveClientAsync(registration);
        
        _logger.LogInformation("Client {ClientId} connected with groups [{Groups}] (expires: {Expiry})",
            identity.ClientId, string.Join(", ", identity.Groups), identity.TokenExpiry?.ToString("u") ?? "N/A");
    }

    public async Task<List<ClientInfo>> GetReceivers()
    {
        var sourceClient = await GetClientByConnectionAsync(Context.ConnectionId);
        
        if (sourceClient == null || sourceClient.Groups.Count == 0)
        {
            _logger.LogWarning("GetReceivers called by unregistered client {ConnectionId}", Context.ConnectionId);
            return [];
        }
        
        var allClients = await _stateStore.GetAllClientsAsync();
        
        // Exclude self
        var otherClients = allClients.Where(c => c.ClientId != sourceClient.ClientId).ToList();
        
        // If no flow control service, return all other clients
        if (_flowControlService == null)
        {
            return otherClients.Select(ToClientInfo).ToList();
        }
        
        // Filter to only clients where ANY source group can send to ANY target group
        var authorizedReceivers = otherClients
            .Where(r => _flowControlService.CanSendTo(sourceClient.Groups, r.Groups))
            .Select(ToClientInfo)
            .ToList();
        
        _logger.LogDebug("GetReceivers: [{SourceGroups}] -> {Count} authorized receivers", 
            string.Join(", ", sourceClient.Groups), authorizedReceivers.Count);
        
        return authorizedReceivers;
    }

    public async Task<string> InitTransfer(Guid targetClientId, int batchSize, int timeoutMs)
    {
        var targetClient = await _stateStore.GetClientAsync(targetClientId);
        if (targetClient == null)
        {
            _logger.LogWarning("InitTransfer failed: Target {TargetClientId} not found.", targetClientId);
            throw new HubException($"Target client {targetClientId} not found.");
        }

        var sourceClient = await GetClientByConnectionAsync(Context.ConnectionId);
        if (sourceClient == null) throw new HubException("Source client not found.");

        // Step 1: Validate tokens and flow control
        ValidateTransferRequest(sourceClient, targetClient);
        
        // Step 2: Check affinity for HA deployment
        CheckAffinityRedirect(targetClient);

        // Step 3: Execute handshake
        var transferId = Guid.NewGuid().ToString();

        _logger.LogInformation("Orchestrating Transfer: {TransferId}. Source: {SourceId} -> Target: {TargetId} (BatchSize={Size})", 
            transferId, sourceClient.ClientId, targetClientId, batchSize);

        await _transferManager.RegisterParticipantsAsync(transferId, sourceClient.ClientId, targetClientId);

        await ExecuteHandshakeAsync(transferId, batchSize, sourceClient, targetClient, timeoutMs);
        
        return transferId;
    }

    private void ValidateTransferRequest(ClientRegistration sourceClient, ClientRegistration targetClient)
    {
        if (IsTokenExpired(sourceClient))
        {
            _logger.LogWarning("InitTransfer denied: Source client {SourceClientId} token has expired", sourceClient.ClientId);
            throw new HubException("Source client token has expired. Please reconnect with a fresh token.");
        }
        
        if (IsTokenExpired(targetClient))
        {
            _logger.LogWarning("InitTransfer denied: Target client {TargetClientId} token has expired", targetClient.ClientId);
            throw new HubException("Target client token has expired.");
        }

        if (_flowControlService == null) return;
        
        if (sourceClient.Groups.Count == 0 || targetClient.Groups.Count == 0)
        {
            _logger.LogWarning("InitTransfer failed: Client groups not found. Source={SourceGroups}, Target={TargetGroups}", 
                string.Join(", ", sourceClient.Groups), string.Join(", ", targetClient.Groups));
            throw new HubException("Client groups not found.");
        }
        
        if (!_flowControlService.CanSendTo(sourceClient.Groups, targetClient.Groups))
        {
            _logger.LogWarning("InitTransfer denied: Groups [{SourceGroups}] are not authorized to send to groups [{TargetGroups}]", 
                string.Join(", ", sourceClient.Groups), string.Join(", ", targetClient.Groups));
            throw new HubException("Transfer not authorized.");
        }
        
        _logger.LogInformation("Flow control authorized: [{SourceGroups}] -> [{TargetGroups}]", 
            string.Join(", ", sourceClient.Groups), string.Join(", ", targetClient.Groups));
    }

    private void CheckAffinityRedirect(ClientRegistration targetClient)
    {
        if (!_affinityOptions.Enabled || string.IsNullOrEmpty(targetClient.AffinityToken)) 
            return;
        
        var senderAffinity = ExtractAffinityToken();
        if (senderAffinity == targetClient.AffinityToken) 
            return;
        
        var cookieName = _affinityOptions.CustomCookieName 
            ?? _affinityOptions.KnownCookieNames.FirstOrDefault() 
            ?? "affinity";
            
        _logger.LogInformation("Affinity redirect: Sender on {SenderAffinity}, Receiver on {ReceiverAffinity}",
            senderAffinity, targetClient.AffinityToken);
            
        throw new AffinityRedirectException(targetClient.AffinityToken, cookieName);
    }

    private async Task ExecuteHandshakeAsync(string transferId, int batchSize, ClientRegistration sourceClient, ClientRegistration targetClient, int timeoutMs)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        try
        {
            try
            {
                await Clients.Client(targetClient.ConnectionId)
                    .InvokeAsync<bool>(nameof(ICommandClient.GetReadyToReceive), transferId, sourceClient.ClientId, cts.Token);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The usual cause is a registration that outlived its process: the client is still
                // in the state store, but SignalR has no such connection any more. Left untranslated
                // this escapes as a non-HubException and reaches the caller as "An unexpected error
                // occurred invoking 'InitTransfer'", which says nothing and costs an hour to chase.
                // Detail stays in the hub log; the client gets something it can act on.
                _logger.LogWarning(ex, "Receiver {TargetClientId} unreachable on connection {ConnectionId} for {TransferId}. Registration is likely stale.",
                    targetClient.ClientId, targetClient.ConnectionId, transferId);
                await _transferManager.UnregisterParticipantsAsync(transferId);
                throw new HubException($"Target client {targetClient.ClientId} is not reachable.");
            }

            try
            {
                await Clients.Client(Context.ConnectionId)
                    .InvokeAsync<bool>(nameof(ICommandClient.GetReadyToSend), transferId, batchSize, targetClient.ClientId, cts.Token);
            }
            catch(Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Sender failed to ready for {TransferId}. Cancelling Receiver.", transferId);
                await _transferManager.UnregisterParticipantsAsync(transferId);
                await Clients.Client(targetClient.ConnectionId).SendAsync(nameof(ICommandClient.CancelTransfer), transferId);
                throw new HubException("Source client failed to prepare for the transfer.");
            }
        }
        catch(OperationCanceledException)
        {
            _logger.LogWarning("Handshake timed out for Transfer: {TransferId}", transferId);
            await _transferManager.UnregisterParticipantsAsync(transferId);
            await Clients.Client(targetClient.ConnectionId).SendAsync(nameof(ICommandClient.CancelTransfer), transferId);
            throw new HubException($"Handshake timed out after {timeoutMs}ms");
        }
    }
    
    public async Task<bool> Heartbeat()
    {
        // Resolving the client by its live connection is itself the keep-alive: a store with
        // server-side expiry pushes the registration's TTL back on this lookup.
        var client = await GetClientByConnectionAsync(Context.ConnectionId);

        if (client == null)
        {
            _logger.LogInformation(
                "Heartbeat from unregistered connection {ConnectionId}. Asking the client to register again.",
                Context.ConnectionId);
            return false;
        }

        return true;
    }

    public async Task SenderHeartbeat(string transferId, long itemsSent, long acksReceived)
    {
        await _transferManager.UpdateSenderStatsAsync(transferId, itemsSent, acksReceived);
    }

    public async Task ReceiverHeartbeat(string transferId, long itemsReceived, long acksSent)
    {
        await _transferManager.UpdateReceiverStatsAsync(transferId, itemsReceived, acksSent);
    }

    public async Task Ping(string transferId)
    {
        // In JWT mode, check if token was refreshed and update expiry
        // In dev mode, Context.User has no JWT claims so we skip this (identity will be null)
        if (Context.User?.Identity?.IsAuthenticated == true)
        {
            var identity = _identityProvider.ResolveIdentity(Context.User, null, null);
            if (identity?.TokenExpiry != null)
            {
                var client = await GetClientByConnectionAsync(Context.ConnectionId);
                if (client != null && client.TokenExpiry != identity.TokenExpiry)
                {
                    await _stateStore.UpdateClientTokenExpiryAsync(client.ClientId, identity.TokenExpiry.Value);
                    _logger.LogInformation("Token refreshed for client {ClientId}, new expiry: {Expiry}", 
                        client.ClientId, identity.TokenExpiry.Value.ToString("u"));
                }
            }
        }
        
        // Keep the transfer alive during reconnection attempts
        await _transferManager.KeepAliveAsync(transferId);
        
        // Respond with Pong to confirm connectivity
        await Clients.Caller.SendAsync(nameof(ICommandClient.Pong), transferId);
    }




    public async Task TransferComplete(string transferId)
    {
        _logger.LogInformation("TransferComplete received for Transfer={TransferId}", transferId);
        
        // Clear checkpoint and remove bridges
        // Note: Clients detect completion via dataplane EOF - no SignalR notification needed
        await _transferManager.RemoveTransferAsync(transferId);
        _bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Data));
        _bridgeManager.TryRemove(IBridgeManager.KeyFor(transferId, ConnectionType.Control));
    }

    // ========== Helpers ==========

    /// <summary>
    /// Projects a registration onto what discovery is allowed to reveal: identity, groups, and
    /// whether the control connection is currently up. Nothing about the hub itself.
    /// </summary>
    private static ClientInfo ToClientInfo(ClientRegistration client) => new()
    {
        ClientId = client.ClientId,
        Groups = client.Groups,
        DisconnectedAtUtc = client.DisconnectedAtUtc
    };

    private async Task<ClientRegistration?> GetClientByConnectionAsync(string connectionId)
    {
        return await _stateStore.GetClientByConnectionIdAsync(connectionId);
    }

    private static bool IsTokenExpired(ClientRegistration client)
    {
        return client.TokenExpiry.HasValue && DateTime.UtcNow > client.TokenExpiry.Value;
    }

    private string? ExtractAffinityToken()
    {
        var cookies = Context.GetHttpContext()?.Request.Cookies;
        if (cookies == null) return null;
        
        // Custom cookie takes priority
        if (!string.IsNullOrEmpty(_affinityOptions.CustomCookieName) &&
            cookies.TryGetValue(_affinityOptions.CustomCookieName, out var customValue))
        {
            return customValue;
        }
        
        // Auto-detect from known cookies
        foreach (var cookieName in _affinityOptions.KnownCookieNames)
        {
            if (cookies.TryGetValue(cookieName, out var value))
            {
                return value;
            }
        }
        
        return null;
    }
}
