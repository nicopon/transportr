using Microsoft.Extensions.Logging;
using TransportR.Interfaces;
using TransportR.Models;

namespace TransportR.Hub.Access;

/// <inheritdoc />
internal sealed class AccessReconciler : IAccessReconciler
{
    private readonly IStateStore _stateStore;
    private readonly ITransferManager _transferManager;
    private readonly ITransferTerminator _terminator;
    private readonly AccessPolicy _policy;
    private readonly ConnectionRegistry _connections;
    private readonly ILogger<AccessReconciler> _logger;

    public AccessReconciler(
        IStateStore stateStore,
        ITransferManager transferManager,
        ITransferTerminator terminator,
        AccessPolicy policy,
        ConnectionRegistry connections,
        ILogger<AccessReconciler> logger)
    {
        _stateStore = stateStore;
        _transferManager = transferManager;
        _terminator = terminator;
        _policy = policy;
        _connections = connections;
        _logger = logger;
    }

    public async Task ReconcileAllAsync(CancellationToken cancellationToken = default)
    {
        // One read of the store for the whole pass.
        var clients = (await _stateStore.GetAllClientsAsync()).ToDictionary(c => c.ClientId);

        await ReconcileAsync(clients.Values, id => Task.FromResult(clients.GetValueOrDefault(id)), only: null, cancellationToken);
    }

    public async Task ReconcileAsync(Guid clientId, CancellationToken cancellationToken = default)
    {
        var client = await _stateStore.GetClientAsync(clientId);
        if (client == null) return;

        // A renewal comes one client at a time, and reading the whole store for each would make a
        // fleet's renewals cost its size.
        var known = new Dictionary<Guid, ClientRegistration?> { [clientId] = client };
        async Task<ClientRegistration?> Lookup(Guid id)
        {
            if (!known.TryGetValue(id, out var found))
            {
                known[id] = found = await _stateStore.GetClientAsync(id);
            }

            return found;
        }

        await ReconcileAsync([client], Lookup, only: clientId, cancellationToken);
    }

    private async Task ReconcileAsync(
        IEnumerable<ClientRegistration> clients,
        Func<Guid, Task<ClientRegistration?>> lookup,
        Guid? only,
        CancellationToken cancellationToken)
    {
        var transfers = _transferManager.GetAllParticipants()
            .Where(t => only == null || t.Value.SenderId == only || t.Value.ReceiverId == only)
            .ToList();
        var ended = new HashSet<string>();

        // Clients first: what a client that lost its right to act has running ends before its
        // connection does, so that the client is told rather than cut.
        foreach (var client in clients)
        {
            var verdict = _policy.Evaluate(client);
            if (verdict == AccessVerdict.Allowed) continue;

            foreach (var (transferId, participants) in transfers)
            {
                if (participants.SenderId != client.ClientId && participants.ReceiverId != client.ClientId) continue;
                if (ended.Add(transferId))
                {
                    _logger.LogWarning("Ending transfer {TransferId}: client {ClientId} is {Verdict}", transferId, client.ClientId, verdict);
                    await _terminator.TerminateAsync(transferId, cancellationToken);
                }
            }

            if (_connections.TryClose(client.ConnectionId))
            {
                _logger.LogWarning(
                    "Closing the connection of client {ClientId}: {Verdict} (token expiry {Expiry:u}), and no renewal was received",
                    client.ClientId, verdict, client.TokenExpiry);
            }
        }

        foreach (var (transferId, participants) in transfers)
        {
            if (ended.Contains(transferId)) continue;

            var sender = await lookup(participants.SenderId);
            var receiver = await lookup(participants.ReceiverId);

            // Evicted since, or on another instance's side of the store: judging nothing here is the
            // inactivity check's to cover.
            if (sender == null || receiver == null) continue;

            AccessVerdict verdict;
            try
            {
                verdict = _policy.Evaluate(sender, receiver);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "The flow control service failed on transfer {TransferId}; it is left running", transferId);
                continue;
            }

            if (verdict != AccessVerdict.Allowed)
            {
                _logger.LogWarning("Ending transfer {TransferId}: {Verdict} between its participants", transferId, verdict);
                await _terminator.TerminateAsync(transferId, cancellationToken);
            }
        }
    }
}
