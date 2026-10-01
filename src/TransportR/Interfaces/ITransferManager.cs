namespace TransportR.Interfaces;

/// <summary>
/// Interface for managing active transfers and their statistics.
/// </summary>
public interface ITransferManager
{
    /// <summary>
    /// Registers sender and receiver for a transfer.
    /// </summary>
    Task RegisterParticipantsAsync(string transferId, Guid senderId, Guid receiverId);

    /// <summary>
    /// Unregisters participants for a transfer (used on handshake failure).
    /// </summary>
    Task UnregisterParticipantsAsync(string transferId);

    /// <summary>
    /// Gets the participants for a transfer.
    /// </summary>
    Task<TransferParticipants?> GetParticipantsAsync(string transferId);

    /// <summary>
    /// Updates sender-side statistics for a transfer.
    /// </summary>
    Task UpdateSenderStatsAsync(string transferId, long itemsSent, long acksReceived);

    /// <summary>
    /// Updates receiver-side statistics for a transfer.
    /// </summary>
    Task UpdateReceiverStatsAsync(string transferId, long itemsReceived, long acksSent);
    
    /// <summary>
    /// Gets the state of a transfer.
    /// </summary>
    TransferState? GetTransfer(string transferId);

    /// <summary>
    /// Drops the transfer state and its participants. This is the only way a transfer leaves the
    /// hub, whether it succeeded or timed out: the hub keeps no transfer history to move a
    /// finished one into, so there is no outcome for the caller to name here.
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
public record TransferParticipants(Guid SenderId, Guid ReceiverId);

/// <summary>
/// Represents the current state and statistics of a transfer.
/// </summary>
public class TransferState
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

