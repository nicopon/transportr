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
    /// Removes the transfer state and participants (cleanup).
    /// </summary>
    /// <param name="transferId">The transfer identifier.</param>
    Task RemoveTransferAsync(string transferId);

    /// <summary>
    /// Marks the transfer as successfully completed.
    /// </summary>
    /// <param name="transferId">The transfer identifier.</param>
    Task CompleteTransferAsync(string transferId);
    /// <summary>
    /// Gets all active transfers.
    /// </summary>
    IEnumerable<TransferState> GetAllTransfers();

    /// <summary>
    /// Terminates a transfer forcefully (e.g. timeout).
    /// </summary>
    Task TerminateTransferAsync(string transferId, string reason);
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
    public string TransferId { get; }
    public DateTime LastActivityUtc { get; private set; }
    public DateTime LastProgressUtc { get; private set; }
    
    // Sender View
    public long ItemsSent { get; private set; }
    public long AcksReceived { get; private set; }

    // Receiver View
    public long ItemsReceived { get; private set; }
    public long AcksSent { get; private set; }

    public TransferState(string transferId)
    {
        TransferId = transferId;
        LastActivityUtc = DateTime.UtcNow;
        LastProgressUtc = DateTime.UtcNow;
    }

    public void Touch()
    {
        LastActivityUtc = DateTime.UtcNow;
    }

    public void UpdateSender(long items, long acks)
    {
        if (items > ItemsSent || acks > AcksReceived)
        {
            LastProgressUtc = DateTime.UtcNow;
        }
        ItemsSent = items;
        AcksReceived = acks;
        LastActivityUtc = DateTime.UtcNow;
    }

    public void UpdateReceiver(long items, long acks)
    {
        if (items > ItemsReceived || acks > AcksSent)
        {
             LastProgressUtc = DateTime.UtcNow;
        }
        ItemsReceived = items;
        AcksSent = acks;
        LastActivityUtc = DateTime.UtcNow;
    }
}

