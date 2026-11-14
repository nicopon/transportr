using System.Collections.Concurrent;
using TransportR.Hub.Interfaces;


namespace TransportR.Hub.Services;

/// <summary>
/// Manages active transfers and their statistics.
/// Keeps participants in the injected IStateStore and transfer statistics in memory.
/// </summary>
internal class TransferManager : ITransferManager
{
    /// <summary>
    /// How long a transfer's session lives in the shared store without a keep-alive, and so the
    /// longest a handshake may be given to complete.
    /// </summary>
    public static readonly TimeSpan SessionTtl = TimeSpan.FromMinutes(5);

    private readonly ConcurrentDictionary<string, TransferState> _transfers = new();
    private readonly ConcurrentDictionary<string, TransferParticipants> _participants = new();
    private readonly ConcurrentDictionary<string, DateTime> _lastHeartbeat = new();
    private readonly ConcurrentDictionary<string, byte> _removing = new();
    private readonly IStateStore _stateStore;
    private readonly TimeProvider _time;
    private readonly TimeSpan _sessionTtl = SessionTtl;

    // A transfer's session expiry is pushed back at most this often: its heartbeats and the bytes of
    // its streams arrive far oftener than the store needs telling.
    private static readonly TimeSpan SessionExtensionInterval = TimeSpan.FromSeconds(10);

    public TransferManager(IStateStore stateStore, TimeProvider? timeProvider = null)
    {
        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
        _time = timeProvider ?? TimeProvider.System;
    }

    private DateTime Now => _time.GetUtcNow().UtcDateTime;

    public async Task RegisterParticipantsAsync(string transferId, Guid senderId, Guid receiverId, bool openedBySender = false)
    {
        // Saved first: a session the store refuses fails the handshake before anyone has heard of the
        // transfer, and must leave no state behind for the inactivity sweep to cancel with both clients.
        var now = Now;
        var session = new TransferSession(transferId, senderId, receiverId, now, now.Add(_sessionTtl)) { OpenedBySender = openedBySender };
        await _stateStore.SaveSessionAsync(session).ConfigureAwait(false);

        _participants[transferId] = new TransferParticipants(senderId, receiverId);

        // Known to the inactivity sweep from the start. The first statistics report comes a heartbeat
        // after the transfer starts, and one that ends or dies sooner would otherwise have no state
        // for the sweep to find.
        _transfers[transferId] = new TransferState(transferId, _time);
    }

    public async Task<TransferParticipants?> GetParticipantsAsync(string transferId)
    {
        if (_participants.TryGetValue(transferId, out var participants))
        {
            return participants;
        }

        var session = await _stateStore.GetSessionAsync(transferId).ConfigureAwait(false);
        return session == null ? null : new TransferParticipants(session.SenderId, session.ReceiverId);
    }

    public async Task<TransferState?> TrackAsync(string transferId)
    {
        if (_transfers.TryGetValue(transferId, out var state))
        {
            return state;
        }

        var session = await _stateStore.GetSessionAsync(transferId).ConfigureAwait(false);
        if (session == null || _removing.ContainsKey(transferId))
        {
            return null;
        }

        _participants.TryAdd(transferId, new TransferParticipants(session.SenderId, session.ReceiverId));
        var tracked = _transfers.GetOrAdd(transferId, id => new TransferState(id, _time));

        // A removal that started after the check above has already cleared the entries just added.
        if (_removing.ContainsKey(transferId))
        {
            _transfers.TryRemove(new KeyValuePair<string, TransferState>(transferId, tracked));
            _participants.TryRemove(transferId, out _);
            return null;
        }

        return tracked;
    }

    public async Task UpdateSenderStatsAsync(string transferId, long itemsSent, long acksReceived)
    {
        if (!_transfers.TryGetValue(transferId, out var state)) return;

        state.UpdateSender(itemsSent, acksReceived);
        await CheckHeartbeatAsync(transferId).ConfigureAwait(false);
    }

    public async Task UpdateReceiverStatsAsync(string transferId, long itemsReceived, long acksSent)
    {
        if (!_transfers.TryGetValue(transferId, out var state)) return;

        state.UpdateReceiver(itemsReceived, acksSent);
        await CheckHeartbeatAsync(transferId).ConfigureAwait(false);
    }

    private async Task CheckHeartbeatAsync(string transferId)
    {
        var now = Now;
        if (!_lastHeartbeat.TryGetValue(transferId, out var last) || (now - last) > SessionExtensionInterval)
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
        // Until the store has deleted the shared session, a stream reopening here would find it there
        // and bring the transfer back on this instance.
        _removing[transferId] = 0;
        try
        {
            _transfers.TryRemove(transferId, out _);
            _participants.TryRemove(transferId, out _);
            _lastHeartbeat.TryRemove(transferId, out _);
            await _stateStore.DeleteSessionAsync(transferId).ConfigureAwait(false);
        }
        finally
        {
            _removing.TryRemove(transferId, out _);
        }
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
