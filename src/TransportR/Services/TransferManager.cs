using System.Collections.Concurrent;
using TransportR.Interfaces;

using TransportR.Models;

namespace TransportR.Services;

/// <summary>
/// Manages active transfers and their statistics.
/// Delegates checkpoint persistence to injected IStateStore.
/// </summary>
public class TransferManager : ITransferManager
{
    private readonly ConcurrentDictionary<string, TransferState> _transfers = new();
    private readonly ConcurrentDictionary<string, TransferParticipants> _participants = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastHeartbeat = new();
    private readonly IStateStore _stateStore;
    private readonly TimeSpan _sessionTtl = TimeSpan.FromMinutes(5);
    private readonly TimeSpan _heartbeatInterval = TimeSpan.FromSeconds(10);

    public TransferManager(IStateStore stateStore)
    {
        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
    }

    public async Task RegisterParticipantsAsync(string transferId, Guid senderId, Guid receiverId)
    {
        _participants[transferId] = new TransferParticipants(senderId, receiverId);
        
        var session = new TransferSession(transferId, senderId, receiverId, DateTime.UtcNow, DateTime.UtcNow.Add(_sessionTtl));
        await _stateStore.SaveSessionAsync(session).ConfigureAwait(false);
    }

    public async Task UnregisterParticipantsAsync(string transferId)
    {
        _participants.TryRemove(transferId, out _);
        await _stateStore.DeleteSessionAsync(transferId).ConfigureAwait(false);
    }

    public async Task<TransferParticipants?> GetParticipantsAsync(string transferId)
    {
        // 1. Check local cache (Fast path)
        if (_participants.TryGetValue(transferId, out var p))
        {
            return p;
        }

        // 2. Check distributed store (HA / Failover path)
        var session = await _stateStore.GetSessionAsync(transferId).ConfigureAwait(false);
        if (session != null)
        {
            // Hydrate local cache
            var participants = new TransferParticipants(session.SenderId, session.ReceiverId);
            _participants.TryAdd(transferId, participants);
            
            // Also ensure we have a TransferState object locally
            _transfers.TryAdd(transferId, new TransferState(transferId));
            
            return participants;
        }

        return null;
    }

    public async Task UpdateSenderStatsAsync(string transferId, long itemsSent, long acksReceived)
    {
        var state = _transfers.GetOrAdd(transferId, _ => new TransferState(transferId));
        state.UpdateSender(itemsSent, acksReceived);
        await CheckHeartbeatAsync(transferId).ConfigureAwait(false);
    }

    public async Task UpdateReceiverStatsAsync(string transferId, long itemsReceived, long acksSent)
    {
        var state = _transfers.GetOrAdd(transferId, _ => new TransferState(transferId));
        state.UpdateReceiver(itemsReceived, acksSent);
        await CheckHeartbeatAsync(transferId).ConfigureAwait(false);
    }

    private async Task CheckHeartbeatAsync(string transferId)
    {
        var now = DateTime.UtcNow;
        if (!_lastHeartbeat.TryGetValue(transferId, out var last) || (now - last) > _heartbeatInterval)
        {
            _lastHeartbeat[transferId] = now;
            await _stateStore.ExtendSessionExpiryAsync(transferId, now.Add(_sessionTtl)).ConfigureAwait(false);
        }
    }
    
    public TransferState? GetTransfer(string transferId)
    {
        return _transfers.TryGetValue(transferId, out var state) ? state : null;
    }

    public async Task RemoveTransferAsync(string transferId)
    {
        _transfers.TryRemove(transferId, out _);
        _participants.TryRemove(transferId, out _);
        _lastHeartbeat.TryRemove(transferId, out _);
        await _stateStore.DeleteSessionAsync(transferId).ConfigureAwait(false);
    }

    public async Task CompleteTransferAsync(string transferId)
    {
        // Completion is cleanup, and that is the design rather than a shortcut: the hub keeps
        // no transfer history to move a finished transfer into.
        await RemoveTransferAsync(transferId).ConfigureAwait(false);
    }
    
    public IEnumerable<TransferState> GetAllTransfers()
    {
        return _transfers.Values;
    }

    public async Task TerminateTransferAsync(string transferId, string reason)
    {
        await RemoveTransferAsync(transferId).ConfigureAwait(false);
    }

    public async Task KeepAliveAsync(string transferId)
    {
        if (_transfers.TryGetValue(transferId, out var state))
        {
            state.Touch();
            await CheckHeartbeatAsync(transferId).ConfigureAwait(false);
        }
    }
}
