using TransportR.Abstractions;
using TransportR.Interfaces;
using TransportR.Models;
using TransportR.Hub.Access;
using TransportR.Hub.Configuration;
using TransportR.Hub.Exceptions;
using TransportR.Hub.Services;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace TransportR.Hub.Hubs;

/// <summary>
/// SignalR hub for orchestrating data transfers between clients.
/// </summary>
/// <remarks>
/// Not sealed so that the tests can derive a hub that probes the connection; nothing outside this
/// assembly and its tests can.
/// </remarks>
internal class CommandHub : Microsoft.AspNetCore.SignalR.Hub, ICommandHub
{
    private readonly IStateStore _stateStore;
    private readonly ITransferManager _transferManager;
    private readonly AccessPolicy _policy;
    private readonly ConnectionRegistry _connections;
    private readonly IClientIdentityProvider _identityProvider;
    private readonly SignalRTransferInitiator _initiator;
    private readonly AffinityOptions _affinityOptions;
    private readonly ILogger<CommandHub> _logger;
    private readonly TimeProvider _time;

    public CommandHub(CommandHubDependencies dependencies)
    {
        _time = dependencies.TimeProvider;
        _stateStore = dependencies.StateStore;
        _transferManager = dependencies.TransferManager;
        _identityProvider = dependencies.IdentityProvider;
        _initiator = dependencies.Initiator;
        _policy = dependencies.Policy;
        _connections = dependencies.Connections;
        _affinityOptions = dependencies.AffinityOptions.Value;
        _logger = dependencies.Logger;
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
        _connections.Remove(Context.ConnectionId);
        await _stateStore.MarkDisconnectedAsync(Context.ConnectionId, _time.GetUtcNow().UtcDateTime);

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

        var existing = await _stateStore.GetClientAsync(identity.ClientId);

        // The principal is that of the token this connection opened with, however long ago, and the
        // registration it holds may have been renewed since: the renewal spoke after the principal did,
        // about the groups as much as about the expiry. Registering again over the same connection
        // keeps what the renewal recorded, and judges only the refusal below on what the identity
        // provider says now. Taking the expiry back would have the verdict read an identity with no
        // group as expired and let it register, and the next sweep end the client's transfers; taking
        // the groups back would bring back a scope the identity provider has withdrawn. A new
        // connection is registered from its own principal, as it always was.
        var groupsToRecord = identity.Groups;
        var expiryToRecord = identity.TokenExpiry;
        if (existing is { } held
            && held.ConnectionId == Context.ConnectionId
            && AccessPolicy.SameIdentity(held.Subject, identity.Subject) == AccessVerdict.Allowed
            && held.TokenExpiry > expiryToRecord)
        {
            groupsToRecord = held.Groups;
            expiryToRecord = held.TokenExpiry;
        }

        // A token that grants nothing is a valid token and no registration. Renewal is the one place
        // it means something else: there it withdraws what the client had.
        if (_policy.Evaluate(identity with { TokenExpiry = expiryToRecord }) == AccessVerdict.NoGroups)
        {
            _logger.LogWarning("Connect failed: the identity of {ClientId} holds no group", identity.ClientId);
            throw new HubException("The identity holds no group.");
        }

        // The store overwrites on save, so without this a caller who learned another client's id
        // from GetReceivers takes over its registration: transfers aimed at it land here, and the
        // data plane checks streams against the subject just written. A registration still within
        // its disconnection grace is protected too, since its owner is expected to reclaim it.
        // Dev mode attests no subject, so both sides are null and nothing is refused there.
        if (existing != null && AccessPolicy.SameIdentity(existing.Subject, identity.Subject) != AccessVerdict.Allowed)
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
        
        // Registered before the registration, and held for as long as the connection is: this is what
        // lets a token that is not renewed close it. An expired principal is no reason to refuse it
        // here; the next sweep closes it like any other.
        _connections.Add(Context);

        var registration = new ClientRegistration(
            identity.ClientId,
            Context.ConnectionId,
            groupsToRecord.ToHashSet(),
            affinityToken,
            expiryToRecord,
            _time.GetUtcNow().UtcDateTime,
            Subject: identity.Subject);
        
        await _stateStore.SaveClientAsync(registration);
        
        _logger.LogInformation("Client {ClientId} connected with groups [{Groups}] (expires: {Expiry})",
            identity.ClientId, string.Join(", ", groupsToRecord), expiryToRecord?.ToString("u") ?? "N/A");
    }

    public async Task<List<ClientInfo>> GetReceivers()
    {
        RefuseIfOnlyTheHostOpensTransfers(nameof(GetReceivers));

        var sourceClient = await GetClientByConnectionAsync(Context.ConnectionId);
        
        if (sourceClient == null)
        {
            _logger.LogWarning("GetReceivers called by unregistered client {ConnectionId}", Context.ConnectionId);
            return [];
        }

        var verdict = _policy.Evaluate(sourceClient);
        if (verdict != AccessVerdict.Allowed)
        {
            _logger.LogWarning("GetReceivers refused for client {ClientId}: {Verdict}", sourceClient.ClientId, verdict);
            return [];
        }
        
        var allClients = await _stateStore.GetAllClientsAsync();
        
        // Self excluded, then only the targets the same decision as InitTransfer lets this caller reach.
        var authorizedReceivers = allClients
            .Where(c => c.ClientId != sourceClient.ClientId && _policy.MayReach(sourceClient, c) == AccessVerdict.Allowed)
            .Select(ToClientInfo)
            .ToList();
        
        _logger.LogDebug("GetReceivers: [{SourceGroups}] -> {Count} authorized receivers", 
            string.Join(", ", sourceClient.Groups), authorizedReceivers.Count);
        
        return authorizedReceivers;
    }

    public async Task<string> InitTransfer(Guid targetClientId, int batchSize, int timeoutMs)
    {
        RefuseIfOnlyTheHostOpensTransfers(nameof(InitTransfer));

        var targetClient = await _stateStore.GetClientAsync(targetClientId);
        if (targetClient == null)
        {
            _logger.LogWarning("InitTransfer failed: Target {TargetClientId} not found.", targetClientId);
            throw new HubException($"Target client {targetClientId} not found.");
        }

        var sourceClient = await GetClientByConnectionAsync(Context.ConnectionId);
        if (sourceClient == null) throw new HubException("Source client not found.");

        try
        {
            // Step 1: Validate tokens and flow control
            _initiator.ValidateTransferRequest(sourceClient, targetClient);

            // Step 2: Check affinity for HA deployment
            CheckAffinityRedirect(targetClient);

            // Step 3: Execute handshake
            return await _initiator.HandshakeAsync(sourceClient, targetClient, batchSize, timeoutMs, requestSender: false);
        }
        catch (HubRequestFailedException ex)
        {
            // SignalR hands a calling client the message of a HubException and of nothing else.
            throw new HubException(ex.Message, ex);
        }
    }

    private void RefuseIfOnlyTheHostOpensTransfers(string method)
    {
        // An error rather than an empty list: a caller that finds nobody fails, but for another reason.
        if (_policy.MayClientOpenTransfers() == AccessVerdict.Allowed) return;

        _logger.LogWarning("{Method} refused for connection {ConnectionId}: only the host opens transfers",
            method, Context.ConnectionId);
        throw new HubException("Clients may not open transfers on this hub.");
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
        // Keep the transfer alive during reconnection attempts
        await _transferManager.KeepAliveAsync(transferId);
        
        // Respond with Pong to confirm connectivity
        await Clients.Caller.SendAsync(nameof(ICommandClient.Pong), transferId);
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
