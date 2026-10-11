using System.Collections.Concurrent;
using TransportR.Interfaces;

using TransportR.Models;

namespace TransportR.Services;

/// <summary>
/// Manages active transfers and their statistics.
/// Keeps participants in the injected IStateStore and transfer statistics in memory.
/// </summary>
public class TransferManager : ITransferManager
{
    private readonly ConcurrentDictionary<string, TransferState> _transfers = new();
    private readonly ConcurrentDictionary<string, TransferParticipants> _participants = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastHeartbeat = new();
    private readonly IStateStore _stateStore;
    private readonly TimeProvider _time;
    private readonly TimeSpan _sessionTtl = TimeSpan.FromMinutes(5);
    private readonly TimeSpan _heartbeatInterval = TimeSpan.FromSeconds(10);

    public TransferManager(IStateStore stateStore, TimeProvider? timeProvider = null)
    {
        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
        _time = timeProvider ?? TimeProvider.System;
    }

    private DateTime Now => _time.GetUtcNow().UtcDateTime;

    public async Task RegisterParticipantsAsync(string transferId, Guid senderId, Guid receiverId)
    {
        _participants[transferId] = new TransferParticipants(senderId, receiverId);

        // Known to the inactivity sweep from the start. The first statistics report comes a heartbeat
        // after the transfer starts, and one that ends or dies sooner would otherwise have no state
        // for the sweep to find.
        _transfers[transferId] = new TransferState(transferId, _time);
        
        var now = Now;
        var session = new TransferSession(transferId, senderId, receiverId, now, now.Add(_sessionTtl));
        await _stateStore.SaveSessionAsync(session).ConfigureAwait(false);
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
            _transfers.TryAdd(transferId, new TransferState(transferId, _time));
            
            return participants;
        }

        return null;
    }

    public async Task UpdateSenderStatsAsync(string transferId, long itemsSent, long acksReceived)
    {
        var state = _transfers.GetOrAdd(transferId, _ => new TransferState(transferId, _time));
        state.UpdateSender(itemsSent, acksReceived);
        await CheckHeartbeatAsync(transferId).ConfigureAwait(false);
    }

    public async Task UpdateReceiverStatsAsync(string transferId, long itemsReceived, long acksSent)
    {
        var state = _transfers.GetOrAdd(transferId, _ => new TransferState(transferId, _time));
        state.UpdateReceiver(itemsReceived, acksSent);
        await CheckHeartbeatAsync(transferId).ConfigureAwait(false);
    }

    private async Task CheckHeartbeatAsync(string transferId)
    {
        var now = Now;
        if (!_lastHeartbeat.TryGetValue(transferId, out var last) || (now - last) > _heartbeatInterval)
        {
            _lastHeartbeat[transferId] = now;
            await _stateStore.ExtendSessionExpiryAsync(transferId, now.Add(_sessionTtl)).ConfigureAwait(false);
        }
    }
    
    public IReadOnlyDictionary<string, TransferParticipants> GetAllParticipants()
    {
        return new Dictionary<string, TransferParticipants>(_participants);
    }

    public TransferState? GetTransfer(string transferId)
    {
        return _transfers.TryGetValue(transferId, out var state) ? state : null;
    }

    // Completion is cleanup, and that is the design rather than a shortcut: the hub keeps no
    // transfer history to move a finished transfer into, so a success, a timeout and a handshake
    // that failed leave by the same door.
    public async Task RemoveTransferAsync(string transferId)
    {
        _transfers.TryRemove(transferId, out _);
        _participants.TryRemove(transferId, out _);
        _lastHeartbeat.TryRemove(transferId, out _);
        await _stateStore.DeleteSessionAsync(transferId).ConfigureAwait(false);
    }

    public IEnumerable<TransferState> GetAllTransfers()
    {
        return _transfers.Values;
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
