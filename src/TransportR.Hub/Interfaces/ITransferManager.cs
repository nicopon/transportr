namespace TransportR.Interfaces;

/// <summary>
/// Interface for managing active transfers and their statistics.
/// </summary>
internal interface ITransferManager
{
    /// <summary>
    /// Registers sender and receiver for a transfer, and starts its state: from here on the
    /// inactivity sweep knows the transfer, whether or not a heartbeat ever reports on it.
    /// </summary>
    Task RegisterParticipantsAsync(string transferId, Guid senderId, Guid receiverId);

    /// <summary>
    /// Gets the participants of a transfer this instance holds, or of one the shared store knows.
    /// A read and nothing more: see <see cref="TrackAsync"/>.
    /// </summary>
    Task<TransferParticipants?> GetParticipantsAsync(string transferId);

    /// <summary>
    /// Holds a transfer this instance accepts a stream of, and returns its state: the one started at
    /// registration, or one started now for a transfer another instance registered. Null when the hub
    /// does not know the transfer.
    /// </summary>
    /// <remarks>
    /// Holding a transfer arms the inactivity sweep on it, so only an instance that carries the
    /// transfer's streams may: one that merely looked the transfer up hears none of its heartbeats,
    /// and would end it, shared session included, for a silence that says nothing.
    /// </remarks>
    Task<TransferState?> TrackAsync(string transferId);

    /// <summary>
    /// Updates sender-side statistics for a transfer this instance holds. A report for any other
    /// transfer is ignored: one that outlives its transfer must not start a state the sweep would end.
    /// </summary>
    Task UpdateSenderStatsAsync(string transferId, long itemsSent, long acksReceived);

    /// <summary>
    /// Updates receiver-side statistics for a transfer this instance holds. See
    /// <see cref="UpdateSenderStatsAsync"/>.
    /// </summary>
    Task UpdateReceiverStatsAsync(string transferId, long itemsReceived, long acksSent);

    /// <summary>
    /// A snapshot of every transfer this instance knows the participants of, by transfer id.
    /// </summary>
    /// <remarks>
    /// Lists the same transfers as <see cref="GetAllTransfers"/>, but by transfer id with their
    /// participants: whoever needs to find every transfer a client takes part in has to start here.
    /// </remarks>
    IReadOnlyDictionary<string, TransferParticipants> GetAllParticipants();

    /// <summary>
    /// Gets the state of a transfer.
    /// </summary>
    TransferState? GetTransfer(string transferId);

    /// <summary>
    /// Drops the transfer state and its participants. This is the only way a transfer leaves the
    /// hub, whether it succeeded, timed out or never got past its handshake: the hub keeps no
    /// transfer history to move a finished one into, so there is no outcome for the caller to name here.
    /// </summary>
    /// <param name="transferId">The transfer identifier.</param>
    Task RemoveTransferAsync(string transferId);

    /// <summary>
    /// Gets all active transfers.
    /// </summary>
    IEnumerable<TransferState> GetAllTransfers();

    /// <summary>
    /// Updates the last activity time for a transfer to prevent cleanup.
    /// </summary>
    Task KeepAliveAsync(string transferId);
}

/// <summary>
/// Represents the sender and receiver of a transfer.
/// </summary>
internal record TransferParticipants(Guid SenderId, Guid ReceiverId);

/// <summary>
/// Represents the current state and statistics of a transfer.
/// </summary>
internal class TransferState
{
    private readonly TimeProvider _time;

    public string TransferId { get; }
    public DateTime LastActivityUtc { get; private set; }
    public DateTime LastProgressUtc { get; private set; }

    // Sender View
    public long ItemsSent { get; private set; }
    public long AcksReceived { get; private set; }

    // Receiver View
    public long ItemsReceived { get; private set; }
    public long AcksSent { get; private set; }

    public TransferState(string transferId, TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        TransferId = transferId;
        LastActivityUtc = Now;
        LastProgressUtc = LastActivityUtc;
    }

    private DateTime Now => _time.GetUtcNow().UtcDateTime;

    public void Touch()
    {
        LastActivityUtc = Now;
    }

    public void UpdateSender(long items, long acks)
    {
        var now = Now;
        if (items > ItemsSent || acks > AcksReceived)
        {
            LastProgressUtc = now;
        }
        ItemsSent = items;
        AcksReceived = acks;
        LastActivityUtc = now;
    }

    public void UpdateReceiver(long items, long acks)
    {
        var now = Now;
        if (items > ItemsReceived || acks > AcksSent)
        {
            LastProgressUtc = now;
        }
        ItemsReceived = items;
        AcksSent = acks;
        LastActivityUtc = now;
    }
}

